#!/usr/bin/env python3
"""
Monthly Coupon Segmentation Analysis
Analyzes all "Monthly Coupon" campaigns to determine whether segmenting
(excluding certain audiences) reduces unsubscribes vs. sending to everyone.
"""

import requests
import json
import csv
import time
import os
import glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from datetime import datetime
from typing import List, Dict

# Configuration
try:
    from config import API_KEY, BASE_URL, REVISION, RATE_LIMIT_DELAY
except ImportError:
    API_KEY = os.getenv("KLAVIYO_API_KEY", "YOUR_API_KEY_HERE")
    BASE_URL = "https://a.klaviyo.com/api"
    REVISION = "2024-10-15"
    RATE_LIMIT_DELAY = 30.0

HEADERS = {
    "Authorization": f"Klaviyo-API-Key {API_KEY}",
    "revision": REVISION,
    "Accept": "application/json",
    "Content-Type": "application/json"
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")

# Caches
_METRIC_ID_CACHE = None
_AUDIENCE_NAME_CACHE = {}


def get_metric_id() -> str:
    global _METRIC_ID_CACHE
    if _METRIC_ID_CACHE:
        return _METRIC_ID_CACHE

    print("Fetching metric ID from Klaviyo...")
    url = f"{BASE_URL}/metrics/"
    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        print(f"Warning: Could not fetch metrics (status {response.status_code})")
        return "PLACEHOLDER"

    data = response.json()
    metrics = data.get("data", [])
    if metrics:
        _METRIC_ID_CACHE = metrics[0]["id"]
        print(f"Using metric: {metrics[0].get('attributes', {}).get('name', 'Unknown')} (ID: {_METRIC_ID_CACHE})")
        return _METRIC_ID_CACHE

    return "PLACEHOLDER"


def resolve_audience_name(audience_id: str) -> str:
    """Resolve a list or segment ID to its human-readable name."""
    if audience_id in _AUDIENCE_NAME_CACHE:
        return _AUDIENCE_NAME_CACHE[audience_id]

    # Try as a list first
    url = f"{BASE_URL}/lists/{audience_id}/"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        name = response.json().get("data", {}).get("attributes", {}).get("name", audience_id)
        _AUDIENCE_NAME_CACHE[audience_id] = name
        return name

    # Try as a segment
    url = f"{BASE_URL}/segments/{audience_id}/"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        name = response.json().get("data", {}).get("attributes", {}).get("name", audience_id)
        _AUDIENCE_NAME_CACHE[audience_id] = name
        return name

    # Fallback to raw ID
    _AUDIENCE_NAME_CACHE[audience_id] = audience_id
    return audience_id


def get_monthly_coupon_campaigns() -> List[Dict]:
    """Fetch all sent campaigns with 'Monthly Coupon' in the name."""
    print("Fetching all email campaigns to find Monthly Coupon sends...")

    campaigns_data = []
    url = f"{BASE_URL}/campaigns/"
    params = {
        "filter": "equals(messages.channel,'email')",
        "include": "campaign-messages"
    }

    page = 0
    while url:
        page += 1
        print(f"  Fetching page {page}...")
        response = requests.get(url, headers=HEADERS, params=params if page == 1 else None)

        if response.status_code == 429:
            print("  Rate limited, waiting 30s...")
            time.sleep(30)
            continue

        if response.status_code != 200:
            print(f"Error fetching campaigns: {response.status_code}")
            print(response.text[:500])
            break

        data = response.json()

        # Build message lookup from included
        message_details = {}
        for included in data.get("included", []):
            if included["type"] == "campaign-message":
                content = included["attributes"].get("content", {})
                message_details[included["id"]] = {
                    "subject": content.get("subject", ""),
                    "preview_text": content.get("preview_text", ""),
                    "from_email": content.get("from_email", ""),
                    "from_label": content.get("from_label", ""),
                }

        for campaign in data.get("data", []):
            attrs = campaign.get("attributes", {})
            name = attrs.get("name", "")

            # Filter: only "Monthly Coupon" campaigns that were sent
            if "monthly coupon" not in name.lower():
                continue
            if attrs.get("status") != "Sent":
                continue

            # Get message ID and details
            message_id = None
            rels = campaign.get("relationships", {})
            if "campaign-messages" in rels:
                msgs = rels["campaign-messages"].get("data", [])
                if msgs:
                    message_id = msgs[0]["id"]

            msg = message_details.get(message_id, {})

            # Extract audiences - items can be dicts {"id": "..."} or plain strings
            audiences = attrs.get("audiences", {})
            raw_included = audiences.get("included", [])
            raw_excluded = audiences.get("excluded", [])

            def extract_ids(items):
                ids = []
                for item in items:
                    if isinstance(item, dict):
                        aid = item.get("id", "")
                        if aid:
                            ids.append(aid)
                    elif isinstance(item, str) and item:
                        ids.append(item)
                return ids

            included_ids = extract_ids(raw_included)
            excluded_ids = extract_ids(raw_excluded)

            campaigns_data.append({
                "campaign_id": campaign["id"],
                "campaign_name": name,
                "subject": msg.get("subject", ""),
                "status": attrs.get("status", ""),
                "send_time": attrs.get("send_time", ""),
                "from_label": msg.get("from_label", ""),
                "from_email": msg.get("from_email", ""),
                "included_ids": included_ids,
                "excluded_ids": excluded_ids,
            })

        url = data.get("links", {}).get("next")
        if url:
            time.sleep(0.5)

    print(f"Found {len(campaigns_data)} Monthly Coupon campaigns")
    return campaigns_data


def get_campaign_stats(campaign_id: str, campaign_name: str, metric_id: str, retry_count: int = 0) -> Dict:
    """Fetch performance statistics including unsubscribes for a campaign."""
    print(f"  Fetching stats for: {campaign_name}")

    url = f"{BASE_URL}/campaign-values-reports/"
    payload = {
        "data": {
            "type": "campaign-values-report",
            "attributes": {
                "timeframe": {"key": "last_365_days"},
                "filter": f'equals(campaign_id,"{campaign_id}")',
                "statistics": [
                    "recipients",
                    "delivered",
                    "bounced",
                    "opens_unique",
                    "open_rate",
                    "clicks_unique",
                    "click_rate",
                    "unsubscribes",
                    "unsubscribe_rate"
                ],
                "conversion_metric_id": metric_id
            }
        }
    }

    response = requests.post(url, headers=HEADERS, json=payload)

    if response.status_code == 429:
        if retry_count < 5:
            wait_time = (retry_count + 1) * 5
            print(f"    Rate limited. Waiting {wait_time}s...")
            time.sleep(wait_time)
            return get_campaign_stats(campaign_id, campaign_name, metric_id, retry_count + 1)
        else:
            print(f"    Rate limit exceeded after 5 retries")

    empty = {
        "recipients": 0, "delivered": 0, "bounced": 0,
        "opens_unique": 0, "open_rate": 0,
        "clicks_unique": 0, "click_rate": 0,
        "unsubscribes": 0, "unsubscribe_rate": 0
    }

    if response.status_code != 200:
        print(f"    Warning: Could not fetch stats (status {response.status_code})")
        return empty

    data = response.json()
    results = data.get("data", {}).get("attributes", {}).get("results", [])

    if results:
        stats = results[0].get("statistics", {})
        return {
            "recipients": stats.get("recipients", 0),
            "delivered": stats.get("delivered", 0),
            "bounced": stats.get("bounced", 0),
            "opens_unique": stats.get("opens_unique", 0),
            "open_rate": round(stats.get("open_rate", 0) * 100, 2),
            "clicks_unique": stats.get("clicks_unique", 0),
            "click_rate": round(stats.get("click_rate", 0) * 100, 2),
            "unsubscribes": stats.get("unsubscribes", 0),
            "unsubscribe_rate": round(stats.get("unsubscribe_rate", 0) * 100, 4),
        }

    return empty


def export_to_csv(campaigns: List[Dict], filename: str):
    """Export campaign data to CSV."""
    if not campaigns:
        print("No campaigns to export")
        return

    fieldnames = [
        "campaign_name", "subject", "send_time",
        "recipients", "delivered", "opens_unique", "open_rate",
        "clicks_unique", "click_rate", "unsubscribes", "unsubscribe_rate",
        "included_audiences", "excluded_audiences", "num_excluded"
    ]

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for c in campaigns:
            writer.writerow({
                "campaign_name": c.get("campaign_name", ""),
                "subject": c.get("subject", ""),
                "send_time": c.get("send_time", ""),
                "recipients": c.get("recipients", 0),
                "delivered": c.get("delivered", 0),
                "opens_unique": c.get("opens_unique", 0),
                "open_rate": c.get("open_rate", 0),
                "clicks_unique": c.get("clicks_unique", 0),
                "click_rate": c.get("click_rate", 0),
                "unsubscribes": c.get("unsubscribes", 0),
                "unsubscribe_rate": c.get("unsubscribe_rate", 0),
                "included_audiences": c.get("included_audiences", ""),
                "excluded_audiences": c.get("excluded_audiences", ""),
                "num_excluded": c.get("num_excluded", 0),
            })

    print(f"Exported to {filename}")


def get_output_filepath(extension: str = "csv") -> str:
    os.makedirs(RESULTS_DIR, exist_ok=True)
    filename = f"monthly_coupon_analysis_{datetime.now().strftime('%Y%m%d')}.{extension}"
    return os.path.join(RESULTS_DIR, filename)


def generate_report(df: pd.DataFrame, output_file: str):
    """Generate a 5-panel PNG report analyzing segmentation vs unsubscribes."""

    colors = {
        'primary': '#2E86AB',
        'secondary': '#A23B72',
        'accent': '#F18F01',
        'danger': '#C73E1D',
        'success': '#4CAF50',
        'light': '#E8E8E8',
    }

    fig = plt.figure(figsize=(24, 16))
    fig.suptitle('Monthly Coupon Campaign Analysis: Segmentation vs. Unsubscribes',
                 fontsize=20, fontweight='bold', y=0.99)
    gs = GridSpec(2, 3, figure=fig, hspace=0.35, wspace=0.3)

    # Parse dates
    df['send_datetime'] = pd.to_datetime(df['send_time'], format='ISO8601', errors='coerce')
    df = df.sort_values('send_datetime').reset_index(drop=True)

    # --- Panel 1: Unsubscribe Rate Over Time ---
    ax1 = fig.add_subplot(gs[0, 0])
    valid = df.dropna(subset=['send_datetime'])
    if len(valid) > 0:
        ax1.plot(valid['send_datetime'], valid['unsubscribe_rate'],
                 marker='o', color=colors['primary'], linewidth=2, markersize=8, zorder=3)
        ax1.fill_between(valid['send_datetime'], valid['unsubscribe_rate'],
                         alpha=0.15, color=colors['primary'])
        mean_unsub = valid['unsubscribe_rate'].mean()
        ax1.axhline(mean_unsub, color=colors['accent'], linestyle='--', linewidth=1.5,
                     label=f'Mean: {mean_unsub:.3f}%')

        # Trend line
        if len(valid) >= 3:
            x_num = np.arange(len(valid))
            z = np.polyfit(x_num, valid['unsubscribe_rate'].values, 1)
            p = np.poly1d(z)
            ax1.plot(valid['send_datetime'], p(x_num), '--', color=colors['danger'],
                     linewidth=1.5, alpha=0.7, label=f'Trend: {"+" if z[0] > 0 else ""}{z[0]:.4f}/send')

        ax1.legend(fontsize=9)

    ax1.set_xlabel('Send Date')
    ax1.set_ylabel('Unsubscribe Rate (%)')
    ax1.set_title('Unsubscribe Rate Over Time', fontweight='bold')
    ax1.tick_params(axis='x', rotation=45)

    # --- Panel 2: Unsub Rate by # of Excluded Segments ---
    ax2 = fig.add_subplot(gs[0, 1])
    if 'num_excluded' in df.columns:
        group = df.groupby('num_excluded').agg(
            avg_unsub=('unsubscribe_rate', 'mean'),
            count=('unsubscribe_rate', 'count'),
            avg_recipients=('recipients', 'mean')
        ).reset_index()

        bar_colors = [colors['danger'] if n == 0 else colors['primary'] for n in group['num_excluded']]
        bars = ax2.bar(group['num_excluded'].astype(str), group['avg_unsub'],
                       color=bar_colors, edgecolor='white', linewidth=1.5)

        for i, row in group.iterrows():
            ax2.text(i, row['avg_unsub'] + (group['avg_unsub'].max() * 0.03),
                     f'{row["avg_unsub"]:.3f}%\n(n={int(row["count"])})',
                     ha='center', fontsize=10, fontweight='bold')

    ax2.set_xlabel('Number of Excluded Segments')
    ax2.set_ylabel('Avg Unsubscribe Rate (%)')
    ax2.set_title('Unsub Rate by Exclusion Count', fontweight='bold', fontsize=13)

    # --- Panel 3: Recipients vs Unsub Rate Scatter ---
    ax3 = fig.add_subplot(gs[0, 2])
    if len(df) > 0:
        scatter_colors = df['num_excluded'].apply(
            lambda x: colors['danger'] if x == 0 else (colors['primary'] if x == 1 else colors['success']))
        ax3.scatter(df['recipients'], df['unsubscribe_rate'],
                    c=scatter_colors, s=100, edgecolors='white', linewidth=1, zorder=3)

        # Correlation
        if len(df) >= 3:
            corr = df['recipients'].corr(df['unsubscribe_rate'])
            ax3.annotate(f'r = {corr:.3f}', xy=(0.05, 0.95), xycoords='axes fraction',
                         fontsize=11, fontweight='bold',
                         bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='gray'))

        # Legend
        from matplotlib.lines import Line2D
        legend_elements = [
            Line2D([0], [0], marker='o', color='w', markerfacecolor=colors['danger'], markersize=10, label='0 exclusions'),
            Line2D([0], [0], marker='o', color='w', markerfacecolor=colors['primary'], markersize=10, label='1 exclusion'),
            Line2D([0], [0], marker='o', color='w', markerfacecolor=colors['success'], markersize=10, label='2+ exclusions'),
        ]
        ax3.legend(handles=legend_elements, fontsize=9)

    ax3.set_xlabel('Recipients')
    ax3.set_ylabel('Unsubscribe Rate (%)')
    ax3.set_title('Recipients vs. Unsubscribe Rate', fontweight='bold')

    # --- Panel 4: Summary Table ---
    ax4 = fig.add_subplot(gs[1, 0])
    ax4.axis('off')

    table_df = df[['campaign_name', 'send_time', 'recipients', 'unsubscribe_rate', 'num_excluded', 'excluded_audiences']].copy()
    table_df['send_time'] = pd.to_datetime(table_df['send_time'], errors='coerce').dt.strftime('%Y-%m-%d')
    table_df['campaign_name'] = table_df['campaign_name'].str[:30]
    table_df['excluded_audiences'] = table_df['excluded_audiences'].str[:30]
    table_df['recipients'] = table_df['recipients'].apply(lambda x: f'{int(x):,}' if pd.notna(x) else '0')
    table_df['unsubscribe_rate'] = table_df['unsubscribe_rate'].apply(lambda x: f'{x:.3f}%')
    table_df['num_excluded'] = table_df['num_excluded'].astype(int)

    header = [['Campaign', 'Date', 'Recip.', 'Unsub %', '# Excl.', 'Excluded']]
    table_data = header + table_df.values.tolist()

    table = ax4.table(cellText=table_data, loc='center', cellLoc='left',
                      colWidths=[0.22, 0.12, 0.1, 0.1, 0.08, 0.22])
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.4)
    for i in range(len(table_data[0])):
        table[(0, i)].set_facecolor(colors['primary'])
        table[(0, i)].set_text_props(color='white', fontweight='bold')

    ax4.set_title('All Monthly Coupon Campaigns', fontweight='bold', pad=20)

    # --- Panel 5: Key Insights & Recommendation ---
    ax5 = fig.add_subplot(gs[1, 1:])
    ax5.axis('off')

    # Calculate insights
    insights = []
    insights.append(f"Total Monthly Coupon Campaigns Analyzed: {len(df)}")
    insights.append("")

    if len(df) > 0:
        avg_unsub = df['unsubscribe_rate'].mean()
        insights.append(f"Average Unsubscribe Rate: {avg_unsub:.3f}%")
        insights.append(f"Average Recipients per Send: {df['recipients'].mean():,.0f}")
        insights.append("")

        # Compare segmented vs unsegmented
        no_exclusions = df[df['num_excluded'] == 0]
        has_exclusions = df[df['num_excluded'] > 0]

        if len(no_exclusions) > 0 and len(has_exclusions) > 0:
            unsub_no_excl = no_exclusions['unsubscribe_rate'].mean()
            unsub_with_excl = has_exclusions['unsubscribe_rate'].mean()
            recip_no_excl = no_exclusions['recipients'].mean()
            recip_with_excl = has_exclusions['recipients'].mean()

            insights.append("SEGMENTED vs. UNSEGMENTED:")
            insights.append(f"  No exclusions ({len(no_exclusions)} campaigns):")
            insights.append(f"    Avg unsub rate: {unsub_no_excl:.3f}%  |  Avg recipients: {recip_no_excl:,.0f}")
            insights.append(f"  With exclusions ({len(has_exclusions)} campaigns):")
            insights.append(f"    Avg unsub rate: {unsub_with_excl:.3f}%  |  Avg recipients: {recip_with_excl:,.0f}")
            insights.append("")

            diff = unsub_no_excl - unsub_with_excl
            if diff > 0:
                insights.append(f"  Segmenting REDUCES unsubs by {diff:.3f} percentage points")
                insights.append("")
                insights.append("RECOMMENDATION: Keep segmenting. Excluding audiences is")
                insights.append("associated with a lower unsubscribe rate.")
            elif diff < 0:
                insights.append(f"  Segmenting INCREASES unsubs by {abs(diff):.3f} percentage points")
                insights.append("")
                insights.append("RECOMMENDATION: Consider sending to everyone. Exclusions")
                insights.append("are not reducing unsubscribes in these campaigns.")
            else:
                insights.append("  No measurable difference in unsub rate.")
                insights.append("")
                insights.append("RECOMMENDATION: Segmentation has no unsub impact here.")
                insights.append("Consider sending to everyone to maximize reach.")
        elif len(no_exclusions) == 0:
            insights.append("All campaigns used exclusions - no unsegmented baseline to compare.")
            insights.append("Consider running a test send without exclusions.")
        elif len(has_exclusions) == 0:
            insights.append("No campaigns used exclusions - all were sent to everyone.")
            insights.append("Consider testing with segment exclusions.")

        # Trend
        if len(df) >= 3:
            x_num = np.arange(len(df))
            z = np.polyfit(x_num, df['unsubscribe_rate'].values, 1)
            trend_dir = "increasing" if z[0] > 0 else "decreasing"
            insights.append("")
            insights.append(f"Unsubscribe trend: {trend_dir} ({z[0]:+.4f} per send)")

        # Best / worst
        best = df.loc[df['unsubscribe_rate'].idxmin()]
        worst = df.loc[df['unsubscribe_rate'].idxmax()]
        insights.append("")
        insights.append(f"Best unsub rate: {best['unsubscribe_rate']:.3f}% - {best['campaign_name'][:40]}")
        insights.append(f"Worst unsub rate: {worst['unsubscribe_rate']:.3f}% - {worst['campaign_name'][:40]}")

    ax5.text(0.05, 0.95, 'KEY INSIGHTS & RECOMMENDATION',
             fontsize=16, fontweight='bold', transform=ax5.transAxes, va='top')

    for i, line in enumerate(insights):
        fontweight = 'bold' if line.startswith('RECOMMENDATION') else 'normal'
        color = colors['danger'] if line.startswith('RECOMMENDATION') else 'black'
        ax5.text(0.05, 0.87 - i * 0.045, line,
                 fontsize=11, transform=ax5.transAxes, va='top',
                 fontweight=fontweight, color=color,
                 fontfamily='monospace')

    plt.savefig(output_file, dpi=150, bbox_inches='tight', facecolor='white')
    print(f"Report saved to: {output_file}")


def main():
    print("=" * 60)
    print("Monthly Coupon Segmentation Analysis")
    print("=" * 60)
    print()

    if API_KEY == "YOUR_API_KEY_HERE" or not API_KEY:
        print("ERROR: Please set your API key in config.py or KLAVIYO_API_KEY env var")
        return

    # Step 1: Fetch campaigns
    campaigns = get_monthly_coupon_campaigns()
    if not campaigns:
        print("No Monthly Coupon campaigns found.")
        return

    # Step 2: Resolve audience names
    print("\nResolving audience names...")
    all_ids = set()
    for c in campaigns:
        all_ids.update(c.get("included_ids", []))
        all_ids.update(c.get("excluded_ids", []))

    for i, aid in enumerate(all_ids, 1):
        print(f"  [{i}/{len(all_ids)}] Resolving {aid[:20]}...")
        resolve_audience_name(aid)
        time.sleep(0.3)

    # Map names onto campaigns
    for c in campaigns:
        c["included_audiences"] = "; ".join(
            resolve_audience_name(aid) for aid in c.get("included_ids", []))
        c["excluded_audiences"] = "; ".join(
            resolve_audience_name(aid) for aid in c.get("excluded_ids", []))
        c["num_excluded"] = len(c.get("excluded_ids", []))

    # Step 3: Fetch stats
    metric_id = get_metric_id()
    print(f"\nFetching stats for {len(campaigns)} campaigns...")
    print("(This may take several minutes due to rate limiting...)\n")

    for i, c in enumerate(campaigns, 1):
        print(f"[{i}/{len(campaigns)}]", end="")
        stats = get_campaign_stats(c["campaign_id"], c["campaign_name"], metric_id)
        c.update(stats)
        if i < len(campaigns):
            time.sleep(RATE_LIMIT_DELAY)

    # Step 4: Export CSV
    csv_file = get_output_filepath("csv")
    export_to_csv(campaigns, csv_file)

    # Step 5: Generate report
    print("\nGenerating report...")
    df = pd.read_csv(csv_file)
    png_file = get_output_filepath("png")
    generate_report(df, png_file)

    # Summary
    print("\n" + "=" * 60)
    print("Analysis Complete!")
    print("=" * 60)
    print(f"Campaigns analyzed: {len(campaigns)}")
    print(f"CSV: {csv_file}")
    print(f"PNG: {png_file}")
    print()


if __name__ == "__main__":
    main()
