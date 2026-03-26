#!/usr/bin/env python3
"""
Meta Ads Daily Monitor — Run Script
Fetches data, generates dashboard, and starts the server.

Usage:
    python run.py           # Fetch + generate + start server + open browser
    python run.py --no-open # Fetch + generate + start server (don't open browser)
    python run.py --no-serve # Fetch + generate only (no server)
"""

import os
import sys
import webbrowser

from fetcher import run_daily_fetch
from dashboard import generate_dashboard


def main():
    print("=" * 60)
    print("  META ADS DAILY MONITOR")
    print("=" * 60)
    print()

    # Step 1: Fetch data
    run_daily_fetch()
    print()

    # Step 2: Generate dashboard
    generate_dashboard()
    print()

    # Step 3: Start server (unless --no-serve)
    if "--no-serve" in sys.argv:
        print("Done! Dashboard at output/dashboard.html")
        print("=" * 60)
        return

    port = int(os.getenv("PORT", "8765"))
    url = f"http://127.0.0.1:{port}"

    if "--no-open" not in sys.argv:
        webbrowser.open(url)

    print(f"  → Dashboard server running at {url}")
    print("  → Click 'Refresh Data' button in the dashboard to re-fetch")
    print("  → Press Ctrl+C to stop")
    print("=" * 60)
    print()

    from server import app
    app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    main()
