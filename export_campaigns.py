#!/usr/bin/env python3
"""
Klaviyo Campaign Data Export Script
Exports campaign data from the last 6 months including:
- Subject lines
- Total messages sent
- Total opens
- Total clicks
"""

import requests
import json
import csv
from datetime import datetime, timedelta
from typing import List, Dict
import time
import os

# Try to import from config file, fall back to defaults
try:
    from config import API_KEY, BASE_URL, REVISION, MONTHS_BACK, OUTPUT_FILENAME, RATE_LIMIT_DELAY
    try:
        from config import START_DATE, END_DATE
    except ImportError:
        START_DATE = None
        END_DATE = None
except ImportError:
    # Configuration (edit these or create a config.py file)
    API_KEY = os.getenv("KLAVIYO_API_KEY", "YOUR_API_KEY_HERE")
    BASE_URL = "https://a.klaviyo.com/api"
    REVISION = "2024-10-15"
    MONTHS_BACK = 6
    OUTPUT_FILENAME = "klaviyo_campaigns_export.csv"
    RATE_LIMIT_DELAY = 30.0  # 30 second between requests
    START_DATE = None
    END_DATE = None

# Headers for API requests
HEADERS = {
    "Authorization": f"Klaviyo-API-Key {API_KEY}",
    "revision": REVISION,
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# Cache for metric ID (to avoid repeated API calls)
_METRIC_ID_CACHE = None


def get_metric_id() -> str:
    """
    Fetch a valid metric ID from Klaviyo.
    Uses the first available metric (typically "Placed Order" or similar).
    Results are cached to avoid repeated API calls.
    """
    global _METRIC_ID_CACHE

    if _METRIC_ID_CACHE:
        return _METRIC_ID_CACHE

    print("Fetching metric ID from Klaviyo...")
    url = f"{BASE_URL}/metrics/"

    # Don't use page[size] - Klaviyo doesn't support it for metrics endpoint
    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        print(f"Warning: Could not fetch metrics (status {response.status_code})")
        try:
            error_detail = response.json()
            print(f"Error details: {error_detail}")
        except:
            print(f"Response text: {response.text[:200]}")
        print("Using PLACEHOLDER - some statistics may not be available")
        return "PLACEHOLDER"

    data = response.json()
    metrics = data.get("data", [])

    if metrics:
        _METRIC_ID_CACHE = metrics[0]["id"]
        print(f"Using metric: {metrics[0].get('attributes', {}).get('name', 'Unknown')} (ID: {_METRIC_ID_CACHE})")
        return _METRIC_ID_CACHE

    print("Warning: No metrics found. Using PLACEHOLDER")
    return "PLACEHOLDER"


def get_campaigns_with_messages() -> List[Dict]:
    """
    Fetch all email campaigns from the last X months with their message details.
    """
    from datetime import timezone

    # Determine date range
    if START_DATE and END_DATE:
        print(f"Fetching campaigns from {START_DATE} to {END_DATE}...")
        range_start = datetime.strptime(START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        range_end = datetime.strptime(END_DATE, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    else:
        print(f"Fetching campaigns from the last {MONTHS_BACK} months...")
        range_start = datetime.now(timezone.utc) - timedelta(days=30 * MONTHS_BACK)
        range_end = datetime.now(timezone.utc)
    
    campaigns_data = []
    tag_names = {}  # Map tag IDs to tag names (collected across all pages)
    url = f"{BASE_URL}/campaigns/"

    # Filter for email campaigns and include campaign messages and tags
    # Add date filter to API request so Klaviyo returns campaigns from the right period
    date_filter = f",greater-or-equal(created_at,{range_start.strftime('%Y-%m-%dT%H:%M:%S')}Z),less-or-equal(created_at,{range_end.strftime('%Y-%m-%dT%H:%M:%S')}Z)"
    params = {
        "filter": f"and(equals(messages.channel,'email'){date_filter})",
        "include": "campaign-messages,tags"
    }

    while url:
        response = requests.get(url, headers=HEADERS, params=params if url == f"{BASE_URL}/campaigns/" else None)
        
        if response.status_code != 200:
            print(f"Error fetching campaigns: {response.status_code}")
            print(response.text)
            return campaigns_data
        
        data = response.json()
        
        # Process campaigns
        for campaign in data.get("data", []):
            campaign_info = {
                "campaign_id": campaign["id"],
                "campaign_name": campaign["attributes"].get("name", ""),
                "status": campaign["attributes"].get("status", ""),
                "created_at": campaign["attributes"].get("created_at", ""),
                "send_time": campaign["attributes"].get("send_time", ""),
                "message_id": None,
                "subject": None,
                "preview_text": None,
                "from_email": None,
                "from_label": None,
                "tag_ids": []
            }

            # Get message ID from relationships
            if "relationships" in campaign and "campaign-messages" in campaign["relationships"]:
                messages = campaign["relationships"]["campaign-messages"].get("data", [])
                if messages:
                    campaign_info["message_id"] = messages[0]["id"]

            # Get tag IDs from relationships
            if "relationships" in campaign and "tags" in campaign["relationships"]:
                tags = campaign["relationships"]["tags"].get("data", [])
                campaign_info["tag_ids"] = [tag["id"] for tag in tags]

            campaigns_data.append(campaign_info)
        
        # Extract message details and tag names from included section
        for included_item in data.get("included", []):
            if included_item["type"] == "campaign-message":
                message_id = included_item["id"]

                # Find the campaign this message belongs to
                for campaign_info in campaigns_data:
                    if campaign_info["message_id"] == message_id:
                        content = included_item["attributes"].get("content", {})
                        campaign_info["subject"] = content.get("subject", "")
                        campaign_info["preview_text"] = content.get("preview_text", "")
                        campaign_info["from_email"] = content.get("from_email", "")
                        campaign_info["from_label"] = content.get("from_label", "")
                        break

            elif included_item["type"] == "tag":
                tag_id = included_item["id"]
                tag_name = included_item["attributes"].get("name", "")
                tag_names[tag_id] = tag_name

        # Check for next page
        url = data.get("links", {}).get("next")
        if url:
            params = None  # Don't send params for next page URL
            time.sleep(0.1)  # Rate limiting courtesy

    # Map tag IDs to tag names for all campaigns (after all pages are fetched)
    for campaign_info in campaigns_data:
        campaign_info["tags"] = ", ".join([tag_names.get(tag_id, tag_id) for tag_id in campaign_info.get("tag_ids", [])])
        if "tag_ids" in campaign_info:
            del campaign_info["tag_ids"]  # Remove tag IDs, keep only names

    # Debug: show the date range of fetched campaigns
    all_dates = []
    for c in campaigns_data:
        dt = c.get("send_time") or c.get("created_at")
        if dt:
            all_dates.append(dt[:10])
    if all_dates:
        print(f"DEBUG: Fetched campaigns date range: {min(all_dates)} to {max(all_dates)}")
        print(f"DEBUG: Statuses found: {set(c.get('status') for c in campaigns_data)}")

    # Filter to only sent campaigns within the date range
    print(f"Filtering for SENT campaigns between: {range_start.strftime('%Y-%m-%d')} and {range_end.strftime('%Y-%m-%d')}")
    filtered_campaigns = []
    no_date_count = 0
    parse_error_count = 0
    non_sent_count = 0

    for campaign in campaigns_data:
        # Skip non-sent campaigns (drafts, scheduled, etc.)
        if campaign.get("status") != "Sent":
            non_sent_count += 1
            continue
        if campaign.get("send_time"):
            try:
                send_date = datetime.fromisoformat(campaign["send_time"].replace("Z", "+00:00"))
                if range_start <= send_date <= range_end:
                    filtered_campaigns.append(campaign)
            except Exception as e:
                # If we can't parse the date, skip it (don't include)
                parse_error_count += 1
                print(f"  Warning: Could not parse send_time for campaign {campaign.get('campaign_name')}: {e}")
        elif campaign.get("created_at"):
            # Fall back to created_at if send_time not available
            try:
                created_date = datetime.fromisoformat(campaign["created_at"].replace("Z", "+00:00"))
                if range_start <= created_date <= range_end:
                    filtered_campaigns.append(campaign)
            except Exception as e:
                parse_error_count += 1
                print(f"  Warning: Could not parse created_at for campaign {campaign.get('campaign_name')}: {e}")
        else:
            no_date_count += 1

    if non_sent_count > 0:
        print(f"Skipped {non_sent_count} campaigns with non-Sent status (drafts, scheduled, etc.)")
    if parse_error_count > 0:
        print(f"Skipped {parse_error_count} campaigns due to date parsing errors")
    if no_date_count > 0:
        print(f"Skipped {no_date_count} campaigns with no send_time or created_at")

    date_range_label = f"{START_DATE} to {END_DATE}" if START_DATE and END_DATE else f"last {MONTHS_BACK} months"
    print(f"Total fetched: {len(campaigns_data)} | After filtering: {len(filtered_campaigns)} campaigns ({date_range_label})")
    if len(campaigns_data) > len(filtered_campaigns):
        print(f"Filtered out {len(campaigns_data) - len(filtered_campaigns)} campaigns outside the date range")
    return filtered_campaigns


def get_campaign_stats(campaign_id: str, campaign_name: str, metric_id: str, retry_count: int = 0) -> Dict:
    """
    Fetch performance statistics for a specific campaign.
    Requires a valid metric_id from Klaviyo (fetched via get_metric_id()).
    """
    print(f"Fetching stats for: {campaign_name}")

    url = f"{BASE_URL}/campaign-values-reports/"

    payload = {
        "data": {
            "type": "campaign-values-report",
            "attributes": {
                "timeframe": {
                    "key": "last_365_days"
                },
                "filter": f'equals(campaign_id,"{campaign_id}")',
                "statistics": [
                    "recipients",
                    "opens",
                    "opens_unique",
                    "clicks",
                    "clicks_unique",
                    "open_rate",
                    "click_rate",
                    "bounced",
                    "delivered"
                ],
                "conversion_metric_id": metric_id
            }
        }
    }

    response = requests.post(url, headers=HEADERS, json=payload)

    # Handle rate limiting with exponential backoff
    if response.status_code == 429:
        if retry_count < 5:
            wait_time = (retry_count + 1) * 5  # 5, 10, 15, 20, 25 seconds
            print(f"  Rate limited. Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
            return get_campaign_stats(campaign_id, campaign_name, metric_id, retry_count + 1)
        else:
            print(f"  Warning: Rate limit exceeded after 5 retries")
            return {
                "recipients": 0,
                "opens": 0,
                "opens_unique": 0,
                "clicks": 0,
                "clicks_unique": 0,
                "open_rate": 0,
                "click_rate": 0,
                "bounced": 0,
                "delivered": 0
            }

    if response.status_code != 200:
        print(f"  Warning: Could not fetch stats (status {response.status_code})")
        try:
            error_detail = response.json()
            print(f"  Error details: {error_detail}")
        except:
            print(f"  Response text: {response.text[:200]}")
        return {
            "recipients": 0,
            "opens": 0,
            "opens_unique": 0,
            "clicks": 0,
            "clicks_unique": 0,
            "open_rate": 0,
            "click_rate": 0,
            "bounced": 0,
            "delivered": 0
        }

    data = response.json()
    
    # Extract statistics
    results = data.get("data", {}).get("attributes", {}).get("results", [])
    
    if results:
        stats = results[0].get("statistics", {})
        return {
            "recipients": stats.get("recipients", 0),
            "opens": stats.get("opens", 0),
            "opens_unique": stats.get("opens_unique", 0),
            "clicks": stats.get("clicks", 0),
            "clicks_unique": stats.get("clicks_unique", 0),
            "open_rate": round(stats.get("open_rate", 0) * 100, 2),  # Convert to percentage
            "click_rate": round(stats.get("click_rate", 0) * 100, 2),  # Convert to percentage
            "bounced": stats.get("bounced", 0),
            "delivered": stats.get("delivered", 0)
        }

    return {
        "recipients": 0,
        "opens": 0,
        "opens_unique": 0,
        "clicks": 0,
        "clicks_unique": 0,
        "open_rate": 0,
        "click_rate": 0,
        "bounced": 0,
        "delivered": 0
    }


def export_to_csv(campaigns: List[Dict], filename: str = None):
    """
    Export campaign data to CSV file.
    """
    if filename is None:
        filename = OUTPUT_FILENAME
        
    if not campaigns:
        print("No campaigns to export")
        return
    
    # Define CSV columns
    fieldnames = [
        "campaign_id",
        "campaign_name",
        "subject",
        "status",
        "send_time",
        "from_label",
        "from_email",
        "preview_text",
        "tags",
        "recipients",
        "delivered",
        "bounced",
        "opens",
        "opens_unique",
        "open_rate",
        "clicks",
        "clicks_unique",
        "click_rate"
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for campaign in campaigns:
            # Flatten the data structure for CSV
            row = {
                "campaign_id": campaign.get("campaign_id", ""),
                "campaign_name": campaign.get("campaign_name", ""),
                "subject": campaign.get("subject", ""),
                "status": campaign.get("status", ""),
                "send_time": campaign.get("send_time", ""),
                "from_label": campaign.get("from_label", ""),
                "from_email": campaign.get("from_email", ""),
                "preview_text": campaign.get("preview_text", ""),
                "tags": campaign.get("tags", ""),
                "recipients": campaign.get("recipients", 0),
                "delivered": campaign.get("delivered", 0),
                "bounced": campaign.get("bounced", 0),
                "opens": campaign.get("opens", 0),
                "opens_unique": campaign.get("opens_unique", 0),
                "open_rate": campaign.get("open_rate", 0),
                "clicks": campaign.get("clicks", 0),
                "clicks_unique": campaign.get("clicks_unique", 0),
                "click_rate": campaign.get("click_rate", 0)
            }
            writer.writerow(row)
    
    print(f"\nExported to {filename}")


def get_output_filepath(extension: str = "csv") -> str:
    """
    Generate output filepath with date range or months.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    results_dir = os.path.join(script_dir, "results")
    os.makedirs(results_dir, exist_ok=True)

    if START_DATE and END_DATE:
        # Use date range in filename (e.g., 2024-12-01_to_2024-12-31)
        filename = f"klaviyo_campaigns_export_{START_DATE}_to_{END_DATE}.{extension}"
    else:
        date_str = datetime.now().strftime("%Y%m%d")
        filename = f"klaviyo_campaigns_export_{date_str}_{MONTHS_BACK}_months.{extension}"
    return os.path.join(results_dir, filename)


def main():
    """
    Main execution function.
    """
    print("=" * 60)
    print("Klaviyo Campaign Export Tool")
    print("=" * 60)
    print()

    # Check if API key is set
    if API_KEY == "YOUR_API_KEY_HERE" or not API_KEY:
        print("ERROR: Please set your API key")
        print("Options:")
        print("  1. Set KLAVIYO_API_KEY environment variable")
        print("  2. Copy config.example.py to config.py and add your API key")
        print("  3. Edit API_KEY variable at the top of this script")
        return

    # Step 1: Get all campaigns with their messages
    campaigns = get_campaigns_with_messages()

    if not campaigns:
        print("No campaigns found")
        return

    # Step 2: Fetch a valid metric ID (required for campaign stats API)
    metric_id = get_metric_id()
    print()

    # Step 3: Get performance stats for each campaign
    print(f"\nFetching performance stats for {len(campaigns)} campaigns...")
    print("(This may take several minutes due to rate limiting...)")
    print()

    for i, campaign in enumerate(campaigns, 1):
        print(f"[{i}/{len(campaigns)}] ", end="")
        stats = get_campaign_stats(campaign["campaign_id"], campaign["campaign_name"], metric_id)
        campaign.update(stats)
        time.sleep(RATE_LIMIT_DELAY)  # Rate limiting

    # Step 4: Export to CSV
    output_file = get_output_filepath("csv")
    print("\nExporting data...")
    export_to_csv(campaigns, output_file)
    
    # Print summary
    print("\n" + "=" * 60)
    print("Export Complete!")
    print("=" * 60)
    print(f"Total campaigns: {len(campaigns)}")
    print(f"Total recipients: {sum(c.get('recipients', 0) for c in campaigns):,}")
    print(f"Total opens: {sum(c.get('opens', 0) for c in campaigns):,}")
    print(f"Total clicks: {sum(c.get('clicks', 0) for c in campaigns):,}")
    print()


if __name__ == "__main__":
    main()
