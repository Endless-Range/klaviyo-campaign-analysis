#!/usr/bin/env python3
"""
Campaign Analysis Report
Analyzes Klaviyo campaign data and generates a PNG report.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import re
import os
import glob
from datetime import datetime

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

# Get the directory where this script is located
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

def get_output_filepath():
    """Generate output PNG filepath with date range or months."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    if START_DATE and END_DATE:
        filename = f"klaviyo_campaigns_export_{START_DATE}_to_{END_DATE}.png"
    else:
        date_str = datetime.now().strftime("%Y%m%d")
        filename = f"klaviyo_campaigns_export_{date_str}_{MONTHS_BACK}_months.png"
    return os.path.join(RESULTS_DIR, filename)

CSV_FILE = get_latest_csv()
OUTPUT_FILE = get_output_filepath()

print(f"Reading from: {CSV_FILE}")

# Load data
df = pd.read_csv(CSV_FILE)

# Parse send_time and convert to PST
df['send_datetime'] = pd.to_datetime(df['send_time'], format='ISO8601')
df['send_datetime_pst'] = df['send_datetime'].dt.tz_convert('America/Los_Angeles')
df['day_of_week'] = df['send_datetime_pst'].dt.day_name()
df['hour'] = df['send_datetime_pst'].dt.hour

# Subject line features
df['subject_length'] = df['subject'].str.len()
df['has_emoji'] = df['subject'].apply(lambda x: bool(re.search(r'[^\x00-\x7F]', str(x))))
df['has_percent'] = df['subject'].str.contains('%', na=False)
df['has_numbers'] = df['subject'].str.contains(r'\d', na=False, regex=True)
df['is_question'] = df['subject'].str.contains(r'\?', na=False)
df['has_urgency'] = df['subject'].str.lower().str.contains(r'last|only|ends|left|today|now|flash|hurry', na=False, regex=True)
df['has_colon'] = df['subject'].str.contains(':', na=False)

# Product category from tags
df['is_thca'] = df['tags'].str.contains('THCA', na=False, case=False)
df['is_cbd'] = df['tags'].str.contains('CBD', na=False, case=False)
df['is_d8'] = df['tags'].str.contains('Delta 8', na=False, case=False)
df['is_wholesale'] = df['tags'].str.contains('Wholesale', na=False, case=False)
df['is_promotion'] = df['tags'].str.contains('Promotion', na=False, case=False)

# List size buckets
df['size_bucket'] = pd.cut(df['recipients'], bins=[0, 1000, 3000, 5000, 10000],
                           labels=['Small (<1k)', 'Medium (1-3k)', 'Large (3-5k)', 'XL (5k+)'])

# Print console report
print("="*60)
print("CAMPAIGN ANALYSIS: WHAT DRIVES OPEN RATES?")
print("="*60)

print(f"\nTotal campaigns: {len(df)}")
print(f"Open rate range: {df['open_rate'].min():.1f}% - {df['open_rate'].max():.1f}%")
print(f"Average open rate: {df['open_rate'].mean():.1f}%")
print(f"Median open rate: {df['open_rate'].median():.1f}%")

# Top and bottom performers
print("\n" + "-"*60)
print("TOP 5 OPEN RATES:")
print("-"*60)
for _, row in df.nlargest(5, 'open_rate').iterrows():
    print(f"  {row['open_rate']:.1f}% | {row['day_of_week'][:3]} {row['hour']}:00 | {row['subject'][:50]}")

print("\n" + "-"*60)
print("BOTTOM 5 OPEN RATES:")
print("-"*60)
for _, row in df.nsmallest(5, 'open_rate').iterrows():
    print(f"  {row['open_rate']:.1f}% | {row['day_of_week'][:3]} {row['hour']}:00 | {row['subject'][:50]}")

# Day of week analysis
print("\n" + "="*60)
print("DAY OF WEEK ANALYSIS")
print("="*60)
day_stats = df.groupby('day_of_week').agg({
    'open_rate': ['mean', 'count', 'std']
}).round(2)
day_stats.columns = ['avg_open_rate', 'count', 'std_dev']
day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
day_stats = day_stats.reindex([d for d in day_order if d in day_stats.index])
print(day_stats.to_string())

# Hour analysis
print("\n" + "="*60)
print("HOUR OF DAY ANALYSIS")
print("="*60)
hour_stats = df.groupby('hour').agg({
    'open_rate': ['mean', 'count']
}).round(2)
hour_stats.columns = ['avg_open_rate', 'count']
print(hour_stats.sort_values('avg_open_rate', ascending=False).to_string())

# Subject line characteristics
print("\n" + "="*60)
print("SUBJECT LINE ANALYSIS")
print("="*60)

features = [
    ('has_emoji', 'Has emoji'),
    ('has_percent', 'Has % sign'),
    ('has_numbers', 'Has numbers'),
    ('has_urgency', 'Urgency words (last/only/ends/left/flash)'),
    ('has_colon', 'Has colon (format: "Topic: detail")'),
]

for col, label in features:
    with_feature = df[df[col]]['open_rate'].mean()
    without_feature = df[~df[col]]['open_rate'].mean()
    count_with = df[col].sum()
    diff = with_feature - without_feature
    direction = "+" if diff > 0 else ""
    print(f"\n{label}:")
    print(f"  With: {with_feature:.1f}% ({count_with} campaigns)")
    print(f"  Without: {without_feature:.1f}% ({len(df)-count_with} campaigns)")
    print(f"  Difference: {direction}{diff:.1f} percentage points")

# Subject length correlation
print("\n" + "-"*60)
print("SUBJECT LENGTH:")
print("-"*60)
correlation = df['subject_length'].corr(df['open_rate'])
short = df[df['subject_length'] <= 35]['open_rate'].mean()
medium = df[(df['subject_length'] > 35) & (df['subject_length'] <= 45)]['open_rate'].mean()
long = df[df['subject_length'] > 45]['open_rate'].mean()
print(f"  Short (<=35 chars): {short:.1f}%")
print(f"  Medium (36-45 chars): {medium:.1f}%")
print(f"  Long (>45 chars): {long:.1f}%")
print(f"  Correlation with open rate: {correlation:.3f}")

# Product category
print("\n" + "="*60)
print("PRODUCT CATEGORY ANALYSIS")
print("="*60)
categories = [
    ('is_thca', 'THCA'),
    ('is_cbd', 'CBD'),
    ('is_d8', 'Delta 8'),
    ('is_wholesale', 'Wholesale'),
    ('is_promotion', 'Promotion'),
]
for col, label in categories:
    if df[col].sum() > 0:
        avg = df[df[col]]['open_rate'].mean()
        count = df[col].sum()
        print(f"  {label}: {avg:.1f}% avg ({count} campaigns)")

# Click rate analysis
print("\n" + "="*60)
print("CLICK RATE LEADERS (conversion potential)")
print("="*60)
for _, row in df.nlargest(5, 'click_rate').iterrows():
    print(f"  {row['click_rate']:.2f}% CTR | {row['open_rate']:.1f}% open | {row['subject'][:45]}")

# List size impact
print("\n" + "="*60)
print("LIST SIZE IMPACT")
print("="*60)
size_stats = df.groupby('size_bucket', observed=True)['open_rate'].agg(['mean', 'count']).round(2)
print(size_stats.to_string())


# =============================================================================
# CREATE PNG REPORT
# =============================================================================

fig = plt.figure(figsize=(24, 14))
fig.suptitle('Campaign Performance Analysis', fontsize=20, fontweight='bold', y=0.99)

# Color palette
colors = {
    'primary': '#2E86AB',
    'secondary': '#A23B72',
    'accent': '#F18F01',
    'success': '#C73E1D',
    'light': '#E8E8E8'
}

# 1. Open Rate Distribution (top left)
ax1 = fig.add_subplot(3, 3, 1)
ax1.hist(df['open_rate'], bins=10, color=colors['primary'], edgecolor='white', alpha=0.8)
ax1.axvline(df['open_rate'].mean(), color=colors['accent'], linestyle='--', linewidth=2, label=f'Mean: {df["open_rate"].mean():.1f}%')
ax1.set_xlabel('Open Rate (%)')
ax1.set_ylabel('Number of Campaigns')
ax1.set_title('Open Rate Distribution', fontweight='bold')
ax1.legend()

# 2. Day of Week Performance (top middle)
ax2 = fig.add_subplot(3, 3, 2)
day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
day_data = df.groupby('day_of_week')['open_rate'].mean().reindex([d for d in day_order if d in df['day_of_week'].values])
bars = ax2.bar(range(len(day_data)), day_data.values, color=colors['primary'], edgecolor='white')
ax2.set_xticks(range(len(day_data)))
ax2.set_xticklabels([d[:3] for d in day_data.index], rotation=0)
ax2.set_ylabel('Avg Open Rate (%)')
ax2.set_title('Open Rate by Day of Week', fontweight='bold')
ax2.axhline(df['open_rate'].mean(), color=colors['accent'], linestyle='--', alpha=0.7)
# Highlight best day
if len(day_data) > 0:
    best_idx = day_data.values.argmax()
    bars[best_idx].set_color(colors['success'])

# 3. Hour of Day Performance (top right)
ax3 = fig.add_subplot(3, 3, 3)
hour_data = df.groupby('hour')['open_rate'].mean().sort_index()
ax3.plot(hour_data.index, hour_data.values, marker='o', color=colors['primary'], linewidth=2, markersize=8)
ax3.fill_between(hour_data.index, hour_data.values, alpha=0.3, color=colors['primary'])
ax3.set_xlabel('Hour of Day')
ax3.set_ylabel('Avg Open Rate (%)')
ax3.set_title('Open Rate by Send Hour', fontweight='bold')
ax3.axhline(df['open_rate'].mean(), color=colors['accent'], linestyle='--', alpha=0.7)

# 4. Subject Line Features Impact (middle left)
ax4 = fig.add_subplot(3, 3, 4)
feature_labels = ['Emoji', '% Sign', 'Numbers', 'Urgency', 'Colon']
feature_cols = ['has_emoji', 'has_percent', 'has_numbers', 'has_urgency', 'has_colon']
with_rates = [df[df[col]]['open_rate'].mean() if df[col].sum() > 0 else 0 for col in feature_cols]
without_rates = [df[~df[col]]['open_rate'].mean() if (~df[col]).sum() > 0 else 0 for col in feature_cols]

x = range(len(feature_labels))
width = 0.35
bars1 = ax4.bar([i - width/2 for i in x], with_rates, width, label='With Feature', color=colors['primary'])
bars2 = ax4.bar([i + width/2 for i in x], without_rates, width, label='Without Feature', color=colors['light'])
ax4.set_xticks(x)
ax4.set_xticklabels(feature_labels, rotation=45, ha='right')
ax4.set_ylabel('Avg Open Rate (%)')
ax4.set_title('Subject Line Features Impact', fontweight='bold')
ax4.legend(loc='upper right')

# 5. Product Category Performance (middle center)
ax5 = fig.add_subplot(3, 3, 5)
cat_labels = []
cat_rates = []
cat_counts = []
for col, label in [('is_thca', 'THCA'), ('is_cbd', 'CBD'), ('is_d8', 'Delta 8'), ('is_promotion', 'Promo')]:
    if df[col].sum() > 0:
        cat_labels.append(label)
        cat_rates.append(df[df[col]]['open_rate'].mean())
        cat_counts.append(df[col].sum())

bars = ax5.barh(cat_labels, cat_rates, color=colors['secondary'], edgecolor='white')
ax5.set_xlabel('Avg Open Rate (%)')
ax5.set_title('Performance by Category', fontweight='bold')
for i, (rate, count) in enumerate(zip(cat_rates, cat_counts)):
    ax5.text(rate + 0.5, i, f'{rate:.1f}% (n={count})', va='center', fontsize=9)
ax5.axvline(df['open_rate'].mean(), color=colors['accent'], linestyle='--', alpha=0.7)

# 6. List Size Impact (middle right)
ax6 = fig.add_subplot(3, 3, 6)
size_data = df.groupby('size_bucket', observed=True)['open_rate'].agg(['mean', 'count'])
if len(size_data) > 0:
    bars = ax6.bar(range(len(size_data)), size_data['mean'].values, color=colors['accent'], edgecolor='white')
    ax6.set_xticks(range(len(size_data)))
    ax6.set_xticklabels(size_data.index, rotation=45, ha='right')
    ax6.set_ylabel('Avg Open Rate (%)')
    ax6.set_title('Open Rate by List Size', fontweight='bold')
    for i, (rate, count) in enumerate(zip(size_data['mean'], size_data['count'])):
        ax6.text(i, rate + 0.5, f'n={count}', ha='center', fontsize=9)

# 7. Top Performers Table (bottom left)
ax7 = fig.add_subplot(3, 3, 7)
ax7.axis('off')
top5 = df.nlargest(5, 'open_rate')[['subject', 'open_rate', 'click_rate']].copy()
top5['subject'] = top5['subject'].str[:35] + '...'
top5['open_rate'] = top5['open_rate'].apply(lambda x: f'{x:.1f}%')
top5['click_rate'] = top5['click_rate'].apply(lambda x: f'{x:.2f}%')
table_data = [['Subject', 'Open', 'Click']] + top5.values.tolist()
table = ax7.table(cellText=table_data, loc='center', cellLoc='left',
                  colWidths=[0.6, 0.2, 0.2])
table.auto_set_font_size(False)
table.set_fontsize(9)
table.scale(1, 1.5)
for i in range(len(table_data[0])):
    table[(0, i)].set_facecolor(colors['primary'])
    table[(0, i)].set_text_props(color='white', fontweight='bold')
ax7.set_title('Top 5 Campaigns by Open Rate', fontweight='bold', pad=20)

# 8. Subject Length Analysis (bottom middle)
ax8 = fig.add_subplot(3, 3, 8)
length_labels = ['Short\n(<=35)', 'Medium\n(36-45)', 'Long\n(>45)']
short_rate = df[df['subject_length'] <= 35]['open_rate'].mean() if len(df[df['subject_length'] <= 35]) > 0 else 0
medium_rate = df[(df['subject_length'] > 35) & (df['subject_length'] <= 45)]['open_rate'].mean() if len(df[(df['subject_length'] > 35) & (df['subject_length'] <= 45)]) > 0 else 0
long_rate = df[df['subject_length'] > 45]['open_rate'].mean() if len(df[df['subject_length'] > 45]) > 0 else 0
length_rates = [short_rate, medium_rate, long_rate]
bars = ax8.bar(length_labels, length_rates, color=[colors['success'], colors['primary'], colors['secondary']], edgecolor='white')
ax8.set_ylabel('Avg Open Rate (%)')
ax8.set_title('Open Rate by Subject Length', fontweight='bold')
for i, rate in enumerate(length_rates):
    if rate > 0:
        ax8.text(i, rate + 0.5, f'{rate:.1f}%', ha='center', fontsize=10, fontweight='bold')

# 9. Key Insights (bottom right)
ax9 = fig.add_subplot(3, 3, 9)
ax9.axis('off')

# Calculate insights
best_day = day_data.idxmax() if len(day_data) > 0 else 'N/A'
best_day_rate = day_data.max() if len(day_data) > 0 else 0
best_hour = hour_data.idxmax() if len(hour_data) > 0 else 0
best_hour_rate = hour_data.max() if len(hour_data) > 0 else 0

emoji_lift = df[df['has_emoji']]['open_rate'].mean() - df[~df['has_emoji']]['open_rate'].mean() if df['has_emoji'].sum() > 0 else 0
urgency_lift = df[df['has_urgency']]['open_rate'].mean() - df[~df['has_urgency']]['open_rate'].mean() if df['has_urgency'].sum() > 0 else 0

insights = [
    f"Total Campaigns Analyzed: {len(df)}",
    f"",
    f"Best Day: {best_day} ({best_day_rate:.1f}%)",
    f"Best Hour: {best_hour}:00 ({best_hour_rate:.1f}%)",
    f"",
    f"Emoji Impact: {'+' if emoji_lift > 0 else ''}{emoji_lift:.1f} pts",
    f"Urgency Impact: {'+' if urgency_lift > 0 else ''}{urgency_lift:.1f} pts",
    f"",
    f"Avg Open Rate: {df['open_rate'].mean():.1f}%",
    f"Avg Click Rate: {df['click_rate'].mean():.2f}%",
]

ax9.text(0.1, 0.95, 'KEY INSIGHTS', fontsize=14, fontweight='bold', transform=ax9.transAxes, va='top')
for i, insight in enumerate(insights):
    ax9.text(0.1, 0.85 - i*0.08, insight, fontsize=11, transform=ax9.transAxes, va='top')

plt.tight_layout(rect=[0, 0.02, 1, 0.96])
plt.savefig(OUTPUT_FILE, dpi=150, bbox_inches='tight', facecolor='white')
print(f"\n{'='*60}")
print(f"PNG report saved to: {OUTPUT_FILE}")
print(f"{'='*60}")
