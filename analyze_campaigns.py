#!/usr/bin/env python3
"""
Example analysis script for Klaviyo campaign data
Shows how to analyze the exported CSV
"""

import csv
import os
import glob
from collections import defaultdict
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(SCRIPT_DIR, 'results')


def get_latest_csv():
    """Find the most recent CSV file in results folder."""
    pattern = os.path.join(RESULTS_DIR, 'klaviyo_campaigns_export_*.csv')
    files = glob.glob(pattern)
    if files:
        return max(files, key=os.path.getmtime)
    # Fallback to old location
    old_file = os.path.join(SCRIPT_DIR, 'klaviyo_campaigns_export.csv')
    if os.path.exists(old_file):
        return old_file
    raise FileNotFoundError("No campaign CSV found. Run export_campaigns.py first.")


def analyze_campaigns(filename=None):
    if filename is None:
        filename = get_latest_csv()
    print(f"Analyzing: {filename}\n")
    """
    Load and analyze campaign data from CSV.
    """
    campaigns = []
    
    # Read the CSV
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert numeric fields
            for field in ['recipients', 'delivered', 'bounced', 'opens', 'opens_unique', 
                         'clicks', 'clicks_unique', 'open_rate', 'click_rate']:
                try:
                    row[field] = float(row[field]) if row[field] else 0
                except:
                    row[field] = 0
            campaigns.append(row)
    
    if not campaigns:
        print("No campaigns found in CSV")
        return
    
    print("=" * 70)
    print("KLAVIYO CAMPAIGN ANALYSIS")
    print("=" * 70)
    print()
    
    # Overall Statistics
    print("📊 OVERALL STATISTICS")
    print("-" * 70)
    total_campaigns = len(campaigns)
    total_recipients = sum(c['recipients'] for c in campaigns)
    total_opens = sum(c['opens'] for c in campaigns)
    total_clicks = sum(c['clicks'] for c in campaigns)
    total_unique_opens = sum(c['opens_unique'] for c in campaigns)
    total_unique_clicks = sum(c['clicks_unique'] for c in campaigns)
    
    avg_open_rate = sum(c['open_rate'] for c in campaigns) / total_campaigns if total_campaigns > 0 else 0
    avg_click_rate = sum(c['click_rate'] for c in campaigns) / total_campaigns if total_campaigns > 0 else 0
    
    print(f"Total Campaigns: {total_campaigns}")
    print(f"Total Recipients: {total_recipients:,.0f}")
    print(f"Total Opens: {total_opens:,.0f} ({total_unique_opens:,.0f} unique)")
    print(f"Total Clicks: {total_clicks:,.0f} ({total_unique_clicks:,.0f} unique)")
    print(f"Average Open Rate: {avg_open_rate:.2f}%")
    print(f"Average Click Rate: {avg_click_rate:.2f}%")
    print()
    
    # Best Performing Campaigns
    print("🏆 TOP 5 CAMPAIGNS BY OPEN RATE")
    print("-" * 70)
    sorted_by_opens = sorted(campaigns, key=lambda x: x['open_rate'], reverse=True)[:5]
    for i, campaign in enumerate(sorted_by_opens, 1):
        print(f"{i}. {campaign['subject'][:50]}")
        print(f"   Open Rate: {campaign['open_rate']:.2f}% | Recipients: {campaign['recipients']:,.0f}")
        print()
    
    print("🏆 TOP 5 CAMPAIGNS BY CLICK RATE")
    print("-" * 70)
    sorted_by_clicks = sorted(campaigns, key=lambda x: x['click_rate'], reverse=True)[:5]
    for i, campaign in enumerate(sorted_by_clicks, 1):
        print(f"{i}. {campaign['subject'][:50]}")
        print(f"   Click Rate: {campaign['click_rate']:.2f}% | Recipients: {campaign['recipients']:,.0f}")
        print()
    
    # Worst Performing
    print("⚠️  BOTTOM 5 CAMPAIGNS BY OPEN RATE")
    print("-" * 70)
    sorted_by_opens_asc = sorted(campaigns, key=lambda x: x['open_rate'])[:5]
    for i, campaign in enumerate(sorted_by_opens_asc, 1):
        print(f"{i}. {campaign['subject'][:50]}")
        print(f"   Open Rate: {campaign['open_rate']:.2f}% | Recipients: {campaign['recipients']:,.0f}")
        print()
    
    # Subject Line Analysis
    print("📝 SUBJECT LINE INSIGHTS")
    print("-" * 70)
    
    # Average subject line length
    subject_lengths = [len(c['subject']) for c in campaigns if c['subject']]
    avg_length = sum(subject_lengths) / len(subject_lengths) if subject_lengths else 0
    
    print(f"Average Subject Line Length: {avg_length:.0f} characters")
    
    # Find common words in high-performing subjects
    high_performers = [c for c in campaigns if c['open_rate'] > avg_open_rate]
    word_freq = defaultdict(int)
    
    for campaign in high_performers:
        words = campaign['subject'].lower().split()
        for word in words:
            # Filter out very short words
            if len(word) > 3:
                word_freq[word] += 1
    
    top_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:10]
    
    if top_words:
        print("\nMost Common Words in High-Performing Subject Lines:")
        for word, count in top_words:
            print(f"  - {word}: {count} times")
    
    print()
    
    # Engagement Analysis
    print("💡 ENGAGEMENT INSIGHTS")
    print("-" * 70)
    
    # Click-to-Open Rate (people who clicked after opening)
    campaigns_with_opens = [c for c in campaigns if c['opens_unique'] > 0]
    if campaigns_with_opens:
        avg_cto = sum(c['clicks_unique'] / c['opens_unique'] * 100 
                     for c in campaigns_with_opens) / len(campaigns_with_opens)
        print(f"Average Click-to-Open Rate: {avg_cto:.2f}%")
    
    # Deliverability
    total_delivered = sum(c['delivered'] for c in campaigns)
    total_bounced = sum(c['bounced'] for c in campaigns)
    if total_recipients > 0:
        delivery_rate = (total_delivered / total_recipients) * 100
        bounce_rate = (total_bounced / total_recipients) * 100
        print(f"Overall Delivery Rate: {delivery_rate:.2f}%")
        print(f"Overall Bounce Rate: {bounce_rate:.2f}%")
    
    print()
    print("=" * 70)
    print("Analysis complete! Check klaviyo_campaigns_export.csv for full data.")
    print("=" * 70)


if __name__ == "__main__":
    analyze_campaigns()
