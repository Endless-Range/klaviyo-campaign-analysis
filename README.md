# Klaviyo Campaign Export Tool

A Python toolkit to export Klaviyo email campaign performance data — subject
lines, sends, opens, clicks, and revenue — and turn it into ready-to-share PNG
charts. Run one command (`python3 run_all.py`) to go from API to dashboard.

## Features

✅ Export campaign data for a custom date range (or the last N months)  
✅ Get complete campaign details: subject lines, send times, sender info, tags  
✅ Pull performance metrics: opens, clicks, bounces, delivery rates, revenue  
✅ **Generate PNG charts**: a 9-panel performance dashboard + a category breakdown  
✅ One-command pipeline (`run_all.py`) that exports and charts in one go  
✅ Bonus console analysis scripts with insights and benchmarks  
✅ Export to CSV for easy analysis in Excel, Python, or BI tools

## Setup

1. **Get your Klaviyo API Key:**
   - Log into Klaviyo
   - Go to Settings → API Keys
   - Create a Private API Key with these scopes:
     - `campaigns:read`
     - (Optional: `flows:read` if you want to expand this later)
   - Copy the API key

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure your API key** (choose one method):

   **Option A: Config file (recommended)**
   ```bash
   cp config.example.py config.py
   # Edit config.py and add your API key
   ```

   **Option B: Environment variable**
   ```bash
   export KLAVIYO_API_KEY="your_api_key_here"
   ```

   **Option C: Edit the script directly**
   - Open `export_campaigns.py`
   - Replace `YOUR_API_KEY_HERE` with your actual API key

## Usage

### Quick start: run the full pipeline

The easiest way to export **and** chart your campaigns is the pipeline runner:

```bash
python3 run_all.py
```

This runs four steps in order and writes all output to the `results/` folder:

1. `export_campaigns.py` — fetch campaigns + performance stats → CSV
2. `export_campaign_revenue.py` — fetch revenue attribution → CSV
3. `campaign_report.py` — generate the campaign performance dashboard → PNG
4. `category_report.py` — generate the category breakdown chart → PNG

When it finishes, open the `results/` folder to see your CSVs and PNG charts.

### Running steps individually

You don't have to run the whole pipeline. Each script can be run on its own
(the report scripts automatically pick up the most recent CSV in `results/`):

```bash
python3 export_campaigns.py          # 1. Export campaign data (run this first)
python3 export_campaign_revenue.py   # 2. Export revenue (needed for revenue chart)
python3 campaign_report.py           # 3. Build the performance dashboard PNG
python3 category_report.py           # 4. Build the category breakdown PNG
```

> The report scripts (`campaign_report.py`, `category_report.py`) only read the
> CSVs in `results/` — they do **not** call the Klaviyo API. So once you've
> exported, you can re-run them as many times as you like without hitting rate
> limits.

## Configuration

Customize the export by editing `config.py`:

```python
API_KEY = "pk_..."           # Your Klaviyo Private API key

# Choose the time window (see below):
MONTHS_BACK = 6              # Export the last N months of campaigns
START_DATE  = "2026-04-01"   # ...OR an explicit date range (overrides MONTHS_BACK)
END_DATE    = "2026-04-30"

RATE_LIMIT_DELAY = 1.0       # Delay between API calls in seconds
```

**Picking a date range:**

- Set **both** `START_DATE` and `END_DATE` (format `YYYY-MM-DD`) to export a
  specific window. This takes priority and is what shows up in the chart titles
  and output filenames.
- Leave `START_DATE`/`END_DATE` as `None` to fall back to `MONTHS_BACK`.

All scripts in the pipeline read the same `config.py`, so changing the date
range in one place re-points the entire export + chart workflow.

## Output

Everything lands in the `results/` folder, named after your date range so
exports don't overwrite each other:

| File | Produced by | What it is |
| --- | --- | --- |
| `klaviyo_campaigns_export_<range>.csv` | `export_campaigns.py` | One row per campaign with all performance metrics (see below) |
| `klaviyo_campaign_revenue_<range>.csv` | `export_campaign_revenue.py` | Revenue + order count attributed to each campaign |
| `klaviyo_campaigns_export_<range>.png` | `campaign_report.py` | **Performance dashboard** (9 panels) |
| `category_report_<range>.png` | `category_report.py` | **Category breakdown** (open rate, click rate, revenue) |

### The charts

**Performance dashboard** (`campaign_report.py`) — a 9-panel image covering:
open-rate distribution, open rate by day of week, open rate by send hour,
subject-line feature impact (emoji / %, numbers / urgency words / colon),
performance by product category, open rate by list size, a top-5 campaigns
table, open rate by subject length, and a key-insights summary.

**Category breakdown** (`category_report.py`) — a landscape image with three
side-by-side panels comparing **open rate**, **click rate**, and **total
revenue** across product categories (THCA, CBD, Delta 8, Wholesale, Promotion).

> Categories are derived from each campaign's **tags** in Klaviyo (the `tags`
> column in the CSV). If your campaigns aren't tagged, the category panels will
> be empty — tag them in Klaviyo, then re-export.

### CSV columns

The main campaign CSV includes:
- Campaign ID and Name
- Subject Line
- Status (sent, draft, etc.)
- Send Time
- From Name/Email
- Preview Text
- Tags
- Recipients (total attempted sends)
- Delivered
- Bounced
- Opens (total)
- Opens (unique)
- Open Rate (%)
- Clicks (total)
- Clicks (unique)
- Click Rate (%)

## Notes

- The script respects Klaviyo's rate limits (5 requests/second)
- If you have many campaigns, it may take a few minutes
- The conversion_metric_id in the script is set to "PLACEHOLDER" - if you get errors about this, you may need to fetch a real metric ID first (like your "Placed Order" metric)

## Troubleshooting

**"Error fetching campaigns: 400"**
- Check your API key has the correct scopes
- Make sure you're using a Private API Key, not Public

**"Error fetching stats: 400"**  
- You may need to update the conversion_metric_id
- Run this command to get your metric IDs:
  ```bash
  curl --request GET \
    --url 'https://a.klaviyo.com/api/metrics/' \
    --header 'Authorization: Klaviyo-API-Key YOUR_API_KEY' \
    --header 'revision: 2024-10-15'
  ```
- Look for "Placed Order" or another metric and use its ID

**No campaigns found**
- Check that you have campaigns sent in the last 6 months
- Check they're email campaigns (not SMS)

## Analyzing the Data

After exporting (`export_campaigns.py` or `run_all.py`), you have several ways
to dig in:

### Generate the visual reports

```bash
python3 campaign_report.py    # 9-panel performance dashboard PNG
python3 category_report.py    # category open/click/revenue breakdown PNG
```

Both also print a detailed text summary to the console and save the chart to
`results/`. These are the scripts to use when someone asks for "charts."

### Quick text analysis (no charts)

```bash
python3 analyze_campaigns.py
```

Prints overall stats, top/bottom campaigns by open and click rate, subject-line
insights, and engagement/deliverability numbers for the latest export — handy
for a fast read without opening an image.

### Specialized reports

- **`monthly_coupon_report.py`** — analyzes "Monthly Coupon" campaigns to see
  whether audience segmentation reduces unsubscribes. Fetches its own data and
  saves a PNG to `results/`.
- **`analyze_segments.py`** — pulls Klaviyo segment sizes (and optional
  overlaps) to console.

### Roll your own

The CSVs are standard, so you can also:
- Open them in Excel / Google Sheets for ad-hoc analysis
- Load them into Python with pandas (point at the file in `results/`):
  ```python
  import pandas as pd, glob, os
  latest = max(glob.glob('results/klaviyo_campaigns_export_*.csv'), key=os.path.getmtime)
  df = pd.read_csv(latest)
  print(df.describe())
  ```
- Import them into a database or BI tool

## Contributing

Contributions are welcome! Here are some ways you can help:

- 🐛 Report bugs or issues
- 💡 Suggest new features
- 📝 Improve documentation
- 🔧 Submit pull requests


## License

MIT License - see [LICENSE](LICENSE) file for details

## Disclaimer

This is an unofficial tool and is not affiliated with or endorsed by Klaviyo. Use at your own risk.

## Support

If you find this tool helpful, please:
- ⭐ Star this repo
- 🐦 Share it with others
- 🙏 Consider contributing improvements

---

Made with ☕ for the Klaviyo community
