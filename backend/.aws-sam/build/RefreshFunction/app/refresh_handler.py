"""
refresh_handler.py
==================
Standalone AWS Lambda function invoked by EventBridge Scheduler.

Responsibilities:
  • Quotes-only refresh  → runs every day at configured IST time
  • Full refresh         → runs every Sunday (weekly deep pull)

Environment variables:
  REFRESH_MODE      quotes_only | full   (set by EventBridge rule)
  All DDB_TBL_* and TICKERS vars shared with main.py
"""

from __future__ import annotations

import logging
import os
from datetime import date

# Re-use all shared logic from the main app module
# The import triggers ticker loading (fast, cached in warm container)
from app.main import (
    refresh_quotes_only,
    refresh_all,
    _get_meta,
    _set_meta,
)

log = logging.getLogger("screener.refresh")
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")


def handler(event: dict, context) -> dict:
    """
    EventBridge Scheduler passes the event payload we configured.
    Expected payload shape:
        { "mode": "quotes_only" }   or   { "mode": "full" }
    Falls back to REFRESH_MODE env var, then defaults to quotes_only.
    """
    mode = (
        event.get("mode")
        or os.getenv("REFRESH_MODE", "quotes_only")
    ).lower().strip()

    log.info("Refresh handler invoked. mode=%s", mode)

    if mode == "full":
        refresh_all()
    else:
        refresh_quotes_only()

    return {
        "status":            "ok",
        "mode":              mode,
        "last_refresh_date": _get_meta("last_refresh_date"),
    }