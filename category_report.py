#!/usr/bin/env python3
"""
Category Performance Report
Generates a landscape PNG showing open rate, click rate, and revenue by product category.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import glob

# Import config settings
try:
    from config import MONTHS_BACK
except ImportError:
    MONTHS_BACK = 6
try:
    from config import START_DATE, END_DATE
except ImportError:
    START_DATE = None
    END_DATE = None

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(SCRIPT_DIR, 'results')


def get_latest_csv(prefix='klaviyo_campaigns_export_'):
    """Find the most recent CSV file matching prefix in results folder."""
    pattern = os.path.join(RESULTS_DIR, f'{prefix}*.csv')
    files = glob.glob(pattern)
    if files:
        return max(files, key=os.path.getmtime)
    return None


def get_output_filepath():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    if START_DATE and END_DATE:
        return os.path.join(RESULTS_DIR, f"category_report_{START_DATE}_to_{END_DATE}.png")
    else:
        from datetime import datetime
        date_str = datetime.now().strftime("%Y%m%d")
        return os.path.join(RESULTS_DIR, f"category_report_{date_str}_{MONTHS_BACK}_months.png")


# Load campaign data
campaign_csv = get_latest_csv('klaviyo_campaigns_export_')
if not campaign_csv:
    raise FileNotFoundError("No campaign CSV found. Run export_campaigns.py first.")
print(f"Reading campaigns from: {campaign_csv}")
df = pd.read_csv(campaign_csv)

# Load revenue data and merge
revenue_csv = get_latest_csv('klaviyo_campaign_revenue_')
if revenue_csv:
    print(f"Reading revenue from: {revenue_csv}")
    rev_df = pd.read_csv(revenue_csv)
    # Keep only campaign_id, revenue, orders from revenue file
    rev_df = rev_df[['campaign_id', 'revenue', 'orders']].copy()
    df = df.merge(rev_df, on='campaign_id', how='left')
    df['revenue'] = df['revenue'].fillna(0)
    df['orders'] = df['orders'].fillna(0)
else:
    print("WARNING: No revenue CSV found. Run export_campaign_revenue.py first.")
    print("Revenue will show as $0 for all categories.")
    df['revenue'] = 0
    df['orders'] = 0

# Define categories from tags
categories = {
    'THCA': df['tags'].str.contains('THCA', na=False, case=False),
    'CBD': df['tags'].str.contains('CBD', na=False, case=False),
    'Delta 8': df['tags'].str.contains('Delta 8', na=False, case=False),
    'Wholesale': df['tags'].str.contains('Wholesale', na=False, case=False),
    'Promotion': df['tags'].str.contains('Promotion', na=False, case=False),
}

# Build category stats
cat_stats = []
for label, mask in categories.items():
    subset = df[mask]
    if len(subset) == 0:
        continue
    cat_stats.append({
        'category': label,
        'campaigns': len(subset),
        'avg_open_rate': subset['open_rate'].mean(),
        'avg_click_rate': subset['click_rate'].mean(),
        'total_revenue': subset['revenue'].sum(),
        'total_orders': subset['orders'].sum(),
        'avg_recipients': subset['recipients'].mean(),
    })

stats_df = pd.DataFrame(cat_stats)

# Console output
print("\n" + "=" * 70)
print("CATEGORY PERFORMANCE BREAKDOWN")
print("=" * 70)
for _, row in stats_df.iterrows():
    print(f"\n  {row['category']} ({int(row['campaigns'])} campaigns)")
    print(f"    Avg Open Rate:  {row['avg_open_rate']:.1f}%")
    print(f"    Avg Click Rate: {row['avg_click_rate']:.2f}%")
    print(f"    Total Revenue:  ${row['total_revenue']:,.2f}")
    print(f"    Total Orders:   {int(row['total_orders'])}")

# =============================================================================
# CREATE PNG REPORT (landscape)
# =============================================================================

fig, axes = plt.subplots(1, 3, figsize=(24, 8))

date_label = f"{START_DATE} to {END_DATE}" if START_DATE and END_DATE else f"Last {MONTHS_BACK} months"
fig.suptitle(f'Category Performance Breakdown  |  {date_label}', fontsize=18, fontweight='bold', y=0.97)

colors = {
    'THCA': '#2E86AB',
    'CBD': '#A23B72',
    'Delta 8': '#F18F01',
    'Wholesale': '#C73E1D',
    'Promotion': '#4CAF50',
}
bar_colors = [colors.get(c, '#888888') for c in stats_df['category']]

x = np.arange(len(stats_df))
bar_width = 0.5

# --- Panel 1: Open Rate ---
ax1 = axes[0]
bars1 = ax1.bar(x, stats_df['avg_open_rate'], width=bar_width, color=bar_colors, edgecolor='white', linewidth=1.5)
ax1.set_xticks(x)
ax1.set_xticklabels(stats_df['category'], fontsize=12)
ax1.set_ylabel('Avg Open Rate (%)', fontsize=12)
ax1.set_title('Open Rate by Category', fontsize=14, fontweight='bold')
for i, (rate, count) in enumerate(zip(stats_df['avg_open_rate'], stats_df['campaigns'])):
    ax1.text(i, rate + 0.5, f'{rate:.1f}%', ha='center', fontsize=11, fontweight='bold')
    ax1.text(i, -2.5, f'n={count}', ha='center', fontsize=9, color='#666666')
ax1.axhline(df['open_rate'].mean(), color='#999999', linestyle='--', alpha=0.7, label=f'Overall avg: {df["open_rate"].mean():.1f}%')
ax1.legend(fontsize=10)
ax1.set_ylim(bottom=-4)

# --- Panel 2: Click Rate ---
ax2 = axes[1]
bars2 = ax2.bar(x, stats_df['avg_click_rate'], width=bar_width, color=bar_colors, edgecolor='white', linewidth=1.5)
ax2.set_xticks(x)
ax2.set_xticklabels(stats_df['category'], fontsize=12)
ax2.set_ylabel('Avg Click Rate (%)', fontsize=12)
ax2.set_title('Click Rate by Category', fontsize=14, fontweight='bold')
for i, rate in enumerate(stats_df['avg_click_rate']):
    ax2.text(i, rate + 0.05, f'{rate:.2f}%', ha='center', fontsize=11, fontweight='bold')
ax2.axhline(df['click_rate'].mean(), color='#999999', linestyle='--', alpha=0.7, label=f'Overall avg: {df["click_rate"].mean():.2f}%')
ax2.legend(fontsize=10)

# --- Panel 3: Revenue ---
ax3 = axes[2]
bars3 = ax3.bar(x, stats_df['total_revenue'], width=bar_width, color=bar_colors, edgecolor='white', linewidth=1.5)
ax3.set_xticks(x)
ax3.set_xticklabels(stats_df['category'], fontsize=12)
ax3.set_ylabel('Total Revenue ($)', fontsize=12)
ax3.set_title('Revenue by Category', fontsize=14, fontweight='bold')
for i, (rev, orders) in enumerate(zip(stats_df['total_revenue'], stats_df['total_orders'])):
    ax3.text(i, rev + (stats_df['total_revenue'].max() * 0.02), f'${rev:,.0f}', ha='center', fontsize=11, fontweight='bold')
    ax3.text(i, rev * -0.06 if rev > 0 else 0, f'{int(orders)} orders', ha='center', fontsize=9, color='#666666')

plt.tight_layout(rect=[0, 0.02, 1, 0.93])

OUTPUT_FILE = get_output_filepath()
plt.savefig(OUTPUT_FILE, dpi=150, bbox_inches='tight', facecolor='white')
print(f"\n{'=' * 70}")
print(f"Category report saved to: {OUTPUT_FILE}")
print(f"{'=' * 70}")
