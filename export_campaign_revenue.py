#!/usr/bin/env python3
"""
Klaviyo Campaign Revenue Export
Fetches revenue attribution for campaigns using the Metrics API.
"""

import requests
import csv
import time
import os
from datetime import datetime, timedelta

# Import configuration
try:
    from config import API_KEY, BASE_URL, REVISION, MONTHS_BACK
except ImportError:
    API_KEY = os.getenv("KLAVIYO_API_KEY", "YOUR_API_KEY_HERE")
    BASE_URL = "https://a.klaviyo.com/api"
    REVISION = "2024-10-15"
    MONTHS_BACK = 3

HEADERS = {
    "Authorization": f"Klaviyo-API-Key {API_KEY}",
    "revision": REVISION,
    "Accept": "application/json",
    "Content-Type": "application/json"
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")


def get_placed_order_metric_id():
    """Find the Placed Order metric ID."""
    print("Finding Placed Order metric...")
    url = f"{BASE_URL}/metrics/"

    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Error fetching metrics: {response.status_code}")
            return None

        data = response.json()
        for metric in data.get("data", []):
            name = metric.get("attributes", {}).get("name", "")
            if "placed order" in name.lower():
                print(f"Found: {name} (ID: {metric['id']})")
                return metric["id"]

        url = data.get("links", {}).get("next")
        time.sleep(0.5)

    print("Placed Order metric not found")
    return None


def get_campaign_message_id(campaign_id):
    """Fetch the message ID for a campaign from Klaviyo API."""
    url = f"{BASE_URL}/campaign-messages/?filter=equals(campaign.id,\"{campaign_id}\")"

    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        data = response.json()
        messages = data.get("data", [])
        if messages:
            return messages[0].get("id")
    return None


def get_campaigns():
    """Get list of campaigns from the existing CSV and fetch their message IDs."""
    import glob

    # Find the most recent CSV
    pattern = os.path.join(RESULTS_DIR, 'klaviyo_campaigns_export_*.csv')
    files = glob.glob(pattern)

    if not files:
        # Fallback to old location
        old_file = os.path.join(SCRIPT_DIR, 'klaviyo_campaigns_export.csv')
        if os.path.exists(old_file):
            csv_file = old_file
        else:
            print("No campaign CSV found. Run export_campaigns.py first.")
            return []
    else:
        csv_file = max(files, key=os.path.getmtime)

    print(f"Reading campaigns from: {csv_file}")

    campaigns = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            campaigns.append({
                "campaign_id": row["campaign_id"],
                "campaign_name": row["campaign_name"],
                "subject": row["subject"],
                "send_time": row["send_time"]
            })

    # Fetch message IDs for each campaign
    print(f"\nFetching message IDs for {len(campaigns)} campaigns...")
    for i, campaign in enumerate(campaigns):
        message_id = get_campaign_message_id(campaign["campaign_id"])
        campaign["message_id"] = message_id
        if (i + 1) % 10 == 0:
            print(f"  {i + 1}/{len(campaigns)} done...")
        time.sleep(0.5)  # Rate limiting

    # Show how many have message IDs
    with_msg = sum(1 for c in campaigns if c.get("message_id"))
    print(f"Found message IDs for {with_msg}/{len(campaigns)} campaigns")

    return campaigns


def get_all_campaign_revenue(metric_id, start_date, end_date):
    """
    Fetch all campaign-attributed revenue grouped by campaign.
    Returns a dict mapping campaign_id -> {revenue, orders}
    """
    print("Fetching all campaign-attributed revenue...")

    url = f"{BASE_URL}/metric-aggregates/"

    # Get all campaign-attributed revenue, grouped by the attributed message
    payload = {
        "data": {
            "type": "metric-aggregate",
            "attributes": {
                "metric_id": metric_id,
                "measurements": ["sum_value", "count"],
                "filter": [
                    f"greater-or-equal(datetime,{start_date})",
                    f"less-than(datetime,{end_date})"
                ],
                "by": ["$attributed_message"],
                "timezone": "America/Los_Angeles"
            }
        }
    }

    response = requests.post(url, headers=HEADERS, json=payload)

    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        try:
            print(response.json())
        except:
            print(response.text[:500])
        return {}

    data = response.json()
    results = data.get("data", {}).get("attributes", {}).get("data", [])

    # Debug: print raw response structure
    print(f"Raw results count: {len(results)}")
    if results:
        print(f"First result structure: {results[0]}")

    revenue_by_campaign = {}
    for result in results:
        dimensions = result.get("dimensions", [])

        # Dimensions can be a list or dict
        if isinstance(dimensions, list):
            message_id = dimensions[0] if dimensions else ""
        elif isinstance(dimensions, dict):
            message_id = dimensions.get("$attributed_message", "")
        else:
            message_id = str(dimensions) if dimensions else ""

        if not message_id:
            continue

        measurements = result.get("measurements", {})
        sum_val = measurements.get("sum_value", [0])
        count_val = measurements.get("count", [0])

        revenue = sum_val[0] if isinstance(sum_val, list) else sum_val
        orders = count_val[0] if isinstance(count_val, list) else count_val

        revenue_by_campaign[message_id] = {
            "revenue": round(float(revenue or 0), 2),
            "orders": int(orders or 0)
        }

    print(f"Found revenue data for {len(revenue_by_campaign)} attributed messages")
    return revenue_by_campaign


def get_campaign_revenue(campaign_id, campaign_name, metric_id, start_date, end_date, debug=False):
    """
    Fetch revenue attributed to a specific campaign using Query Metric Aggregates.
    """
    print(f"Fetching revenue for: {campaign_name[:40]}...", end=" ")

    url = f"{BASE_URL}/metric-aggregates/"

    payload = {
        "data": {
            "type": "metric-aggregate",
            "attributes": {
                "metric_id": metric_id,
                "measurements": ["sum_value", "count"],
                "filter": [
                    f"greater-or-equal(datetime,{start_date})",
                    f"less-than(datetime,{end_date})",
                    f"equals($attributed_message,\"{campaign_id}\")"
                ],
                "timezone": "America/Los_Angeles"
            }
        }
    }

    if debug:
        print(f"\nPayload: {payload}")

    response = requests.post(url, headers=HEADERS, json=payload)

    if response.status_code == 429:
        print("Rate limited, waiting 30s...")
        time.sleep(30)
        return get_campaign_revenue(campaign_id, campaign_name, metric_id, start_date, end_date)

    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        try:
            print(f"  {response.json()}")
        except:
            pass
        return {"revenue": 0, "orders": 0}

    data = response.json()

    # Extract results
    results = data.get("data", {}).get("attributes", {}).get("data", [])

    revenue = 0
    orders = 0

    if results:
        measurements = results[0].get("measurements", {})
        # Values come as lists, extract first element
        sum_val = measurements.get("sum_value", [0])
        count_val = measurements.get("count", [0])

        # Handle list or direct value
        if isinstance(sum_val, list):
            revenue = sum_val[0] if sum_val else 0
        else:
            revenue = sum_val or 0

        if isinstance(count_val, list):
            orders = count_val[0] if count_val else 0
        else:
            orders = count_val or 0

    print(f"${revenue:,.2f} ({orders} orders)")
    return {"revenue": round(float(revenue), 2), "orders": int(orders)}


def main():
    print("=" * 60)
    print("Klaviyo Campaign Revenue Export")
    print("=" * 60)
    print()

    if API_KEY == "YOUR_API_KEY_HERE" or not API_KEY:
        print("ERROR: Set your API key in config.py")
        return

    # Get Placed Order metric ID
    metric_id = get_placed_order_metric_id()
    if not metric_id:
        print("Cannot continue without Placed Order metric")
        return

    # Get campaigns from existing CSV
    campaigns = get_campaigns()
    if not campaigns:
        return

    print(f"\nFound {len(campaigns)} campaigns")

    # Date range for attribution
    end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    start_date = (datetime.now() - timedelta(days=30 * MONTHS_BACK)).strftime("%Y-%m-%dT%H:%M:%S")

    print(f"Attribution window: {start_date[:10]} to {end_date[:10]}")
    print()

    # Fetch ALL campaign revenue in one query (grouped by attributed message)
    print("-" * 60)
    revenue_data = get_all_campaign_revenue(metric_id, start_date, end_date)

    # Debug: show what message IDs we found
    if revenue_data:
        print(f"\nSample attributed message IDs found:")
        for msg_id in list(revenue_data.keys())[:5]:
            print(f"  {msg_id}: ${revenue_data[msg_id]['revenue']:,.2f}")
        print()

    # Match revenue to campaigns using message IDs
    print("Matching revenue to campaigns...")
    print("-" * 60)

    total_revenue = 0
    total_orders = 0
    matched_count = 0

    for campaign in campaigns:
        message_id = campaign.get("message_id")
        campaign["revenue"] = 0
        campaign["orders"] = 0

        if message_id and message_id in revenue_data:
            campaign["revenue"] = revenue_data[message_id]["revenue"]
            campaign["orders"] = revenue_data[message_id]["orders"]
            matched_count += 1

        total_revenue += campaign["revenue"]
        total_orders += campaign["orders"]

        if campaign["revenue"] > 0:
            print(f"  {campaign['campaign_name'][:40]}: ${campaign['revenue']:,.2f}")

    print(f"\nMatched {matched_count} campaigns with revenue data")

    # Export to CSV
    os.makedirs(RESULTS_DIR, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    output_file = os.path.join(RESULTS_DIR, f"klaviyo_campaign_revenue_{date_str}_{MONTHS_BACK}_months.csv")

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["campaign_id", "campaign_name", "subject", "send_time", "revenue", "orders"])
        writer.writeheader()
        for campaign in campaigns:
            writer.writerow(campaign)

    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total Revenue: ${total_revenue:,.2f}")
    print(f"Total Orders: {total_orders:,}")
    print(f"Campaigns: {len(campaigns)}")
    print(f"\nExported to: {output_file}")


if __name__ == "__main__":
    main()
