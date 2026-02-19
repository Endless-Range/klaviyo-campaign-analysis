#!/usr/bin/env python3
"""
Klaviyo Segment Analysis Script - FAST VERSION

Uses segment count endpoint for instant counts.
Only fetches profiles when calculating overlaps (optional).
"""

import requests
import time
from typing import Dict, List, Set, Optional

# Import configuration
try:
    from config import API_KEY, BASE_URL, REVISION, RATE_LIMIT_DELAY
except ImportError:
    import os
    API_KEY = os.getenv("KLAVIYO_API_KEY", "YOUR_API_KEY_HERE")
    BASE_URL = "https://a.klaviyo.com/api"
    REVISION = "2024-10-15"
    RATE_LIMIT_DELAY = 0.5

# Headers for API requests
HEADERS = {
    "Authorization": f"Klaviyo-API-Key {API_KEY}",
    "revision": REVISION,
    "Accept": "application/json",
    "Content-Type": "application/json"
}


def get_all_segments() -> List[Dict]:
    """Fetch all segments from Klaviyo."""
    print("Fetching segments...")
    segments = []
    url = f"{BASE_URL}/segments/"

    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            return segments

        data = response.json()
        for segment in data.get("data", []):
            segments.append({
                "id": segment["id"],
                "name": segment["attributes"].get("name", "")
            })

        url = data.get("links", {}).get("next")
        if url:
            time.sleep(0.3)

    return segments


def get_segment_count(segment_id: str) -> int:
    """Get profile count for a segment using the profiles endpoint with count."""
    # Use the segment profiles endpoint with page size 1 to get count from meta
    url = f"{BASE_URL}/segments/{segment_id}/profiles/"
    params = {"page[size]": 1}

    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code != 200:
        return -1

    data = response.json()
    # The count might be in meta.page.total or we might need to estimate
    meta = data.get("meta", {})
    if "total" in meta:
        return meta["total"]

    # Alternative: check page cursor info
    page_info = meta.get("page", {})
    if "total" in page_info:
        return page_info["total"]

    # If no count available, return -1 (we'll need to paginate)
    return -1


def get_segment_profile_ids(segment_id: str, segment_name: str, max_profiles: int = 50000) -> Set[str]:
    """
    Fetch profile IDs from a segment.
    Limits to max_profiles to avoid long waits.
    """
    print(f"  Fetching profiles from '{segment_name}'...")
    profile_ids = set()
    url = f"{BASE_URL}/segments/{segment_id}/profiles/"
    params = {"page[size]": 100}  # Max page size

    while url and len(profile_ids) < max_profiles:
        response = requests.get(url, headers=HEADERS, params=params if "?" not in url else None)

        if response.status_code == 429:
            print("    Rate limited, waiting...")
            time.sleep(30)
            continue

        if response.status_code != 200:
            print(f"    Error: {response.status_code}")
            break

        data = response.json()
        for profile in data.get("data", []):
            profile_ids.add(profile["id"])

        if len(profile_ids) % 1000 == 0 and len(profile_ids) > 0:
            print(f"    {len(profile_ids):,} profiles...")

        url = data.get("links", {}).get("next")
        if url:
            time.sleep(RATE_LIMIT_DELAY)

    print(f"    Total: {len(profile_ids):,} profiles")
    return profile_ids


def find_segment(segments: List[Dict], search: str) -> Optional[Dict]:
    """Find segment by name (case-insensitive)."""
    search_lower = search.lower()

    # Exact match first
    for s in segments:
        if s["name"].lower() == search_lower:
            return s

    # Partial match
    matches = [s for s in segments if search_lower in s["name"].lower()]
    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
        print(f"\nMultiple matches for '{search}':")
        for m in matches:
            print(f"  - {m['name']}")
        return None

    return None


def main():
    print("=" * 50)
    print("Klaviyo Segment Analyzer")
    print("=" * 50)
    print()

    # Check API key
    if API_KEY == "YOUR_API_KEY_HERE" or not API_KEY:
        print("ERROR: Set API key in config.py")
        return

    # Get all segments
    segments = get_all_segments()
    print(f"Found {len(segments)} segments\n")

    # List all segments
    print("--- Your Segments ---")
    for s in sorted(segments, key=lambda x: x["name"]):
        print(f"  {s['name']}")
    print()

    # Target segments to find
    targets = ["No Delta 8", "No THCA", "Semi Active"]

    found = {}
    print("--- Looking for target segments ---")
    for target in targets:
        seg = find_segment(segments, target)
        if seg:
            found[target] = seg
            print(f"Found: {seg['name']} (ID: {seg['id']})")
        else:
            print(f"NOT FOUND: {target}")

    if len(found) < 2:
        print("\nCouldn't find enough segments. Check the names above.")
        return

    # Ask user what they want to do
    print("\n" + "=" * 50)
    print("OPTIONS")
    print("=" * 50)
    print("\n1. Quick count (fast) - just show segment sizes")
    print("2. Full overlap analysis (slower) - calculate actual overlaps")
    print()

    choice = input("Enter 1 or 2 (default: 1): ").strip() or "1"

    if choice == "1":
        # Quick mode - just show we found the segments
        print("\nSegment counts require fetching profiles (Klaviyo API limitation).")
        print("For quick counts, check Klaviyo dashboard directly.")
        print("\nSegments found:")
        for name, seg in found.items():
            print(f"  - {seg['name']}")

    else:
        # Full overlap analysis
        print("\n" + "=" * 50)
        print("FETCHING PROFILES FOR OVERLAP ANALYSIS")
        print("(This may take a few minutes for large segments)")
        print("=" * 50)

        segment_profiles = {}
        for name, seg in found.items():
            segment_profiles[name] = get_segment_profile_ids(seg["id"], seg["name"])
            time.sleep(1)

        # Calculate overlaps
        print("\n" + "=" * 50)
        print("RESULTS")
        print("=" * 50)

        for name, profiles in segment_profiles.items():
            print(f"\n{name}: {len(profiles):,} profiles")

        # No Delta 8 + No THCA
        if "No Delta 8" in segment_profiles and "No THCA" in segment_profiles:
            overlap = segment_profiles["No Delta 8"] & segment_profiles["No THCA"]
            print(f"\nNo Delta 8 AND No THCA: {len(overlap):,}")

            only_d8 = segment_profiles["No Delta 8"] - segment_profiles["No THCA"]
            print(f"No Delta 8 but HAVE tried THCA: {len(only_d8):,}")
            print("  ^ These are good candidates for THCA campaigns!")

        # No Delta 8 + Semi Active
        if "No Delta 8" in segment_profiles and "Semi Active" in segment_profiles:
            overlap = segment_profiles["No Delta 8"] & segment_profiles["Semi Active"]
            print(f"\nNo Delta 8 AND Semi Active: {len(overlap):,}")
            print("  ^ Engaged customers who haven't tried Delta 8")

        # Three-way if all found
        if all(name in segment_profiles for name in ["No Delta 8", "No THCA", "Semi Active"]):
            three_way = segment_profiles["No Delta 8"] & segment_profiles["No THCA"] & segment_profiles["Semi Active"]
            print(f"\nAll three (No D8 + No THCA + Semi Active): {len(three_way):,}")

    print("\nDone!")


if __name__ == "__main__":
    main()
