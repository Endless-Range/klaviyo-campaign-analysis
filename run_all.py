#!/usr/bin/env python3
"""
Run the full Klaviyo campaign analysis pipeline.
Edit config.py to set your date range, then run this script.

Steps:
  1. Export campaign data (open rate, click rate, etc.)
  2. Export revenue data
  3. Generate main campaign report PNG
  4. Generate category breakdown PNG
"""

import subprocess
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

steps = [
    ("export_campaigns.py", "Exporting campaign data from Klaviyo..."),
    ("export_campaign_revenue.py", "Exporting revenue data from Klaviyo..."),
    ("campaign_report.py", "Generating main campaign report..."),
    ("category_report.py", "Generating category breakdown report..."),
]

def main():
    # Show current config
    try:
        from config import START_DATE, END_DATE, MONTHS_BACK
        if START_DATE and END_DATE:
            print(f"Date range: {START_DATE} to {END_DATE}")
        else:
            print(f"Date range: last {MONTHS_BACK} months")
    except ImportError:
        pass

    print()

    for script, message in steps:
        print("=" * 60)
        print(message)
        print("=" * 60)
        result = subprocess.run(
            [sys.executable, os.path.join(SCRIPT_DIR, script)],
            cwd=SCRIPT_DIR,
        )
        if result.returncode != 0:
            print(f"\nERROR: {script} failed (exit code {result.returncode})")
            print("Fix the issue above and re-run.")
            sys.exit(1)
        print()

    print("=" * 60)
    print("All done! Check the results/ folder for your reports.")
    print("=" * 60)


if __name__ == "__main__":
    main()
