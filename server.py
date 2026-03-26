#!/usr/bin/env python3
"""Tiny Flask server for Meta Ads Dashboard with refresh support."""

import threading

from flask import Flask, jsonify, send_file
from pathlib import Path

from fetcher import run_daily_fetch
from dashboard import generate_dashboard

app = Flask(__name__)
OUTPUT_DIR = Path(__file__).parent / "output"
refresh_lock = threading.Lock()


@app.route("/")
def index():
    return send_file(OUTPUT_DIR / "dashboard.html")


@app.route("/refresh", methods=["POST"])
def refresh():
    if not refresh_lock.acquire(blocking=False):
        return jsonify(status="already_running"), 409
    try:
        run_daily_fetch()
        generate_dashboard()
        return jsonify(status="ok")
    except Exception as e:
        return jsonify(status="error", message=str(e)), 500
    finally:
        refresh_lock.release()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8765, debug=False)
