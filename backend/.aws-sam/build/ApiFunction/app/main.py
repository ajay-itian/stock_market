"""
Indian Equity Screener – AWS Lambda + DynamoDB Edition
=======================================================
Architecture changes vs SQLite version:
  • Storage  : SQLite → DynamoDB (on-demand billing)
  • Server   : Uvicorn process → Lambda + Mangum ASGI adapter
  • Scheduler: APScheduler → EventBridge Scheduler (separate Lambda)
  • State    : In-process news cache → ElastiCache-free TTL via DynamoDB TTL attr

Tables (DynamoDB):
  equity-quotes        PK=symbol  SK=date
  equity-info          PK=symbol
  equity-financials    PK=symbol  SK=fiscal_year
  equity-balance-sheet PK=symbol  SK=fiscal_year
  equity-history       PK=symbol  SK=date
  equity-meta          PK=key  (last_refresh_date etc.)
  equity-news-cache    PK=symbol  TTL attr for auto-expiry

All table names are overridden by env vars (see Config section).
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import html
import io
import logging
import math
import os
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Any, Callable

import boto3
import yfinance as yf
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from gnews import GNews
from mangum import Mangum
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("screener")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

AWS_REGION: str = os.getenv("AWS_REGION", "ap-south-1")
ALLOWED_ORIGINS: list[str] = os.getenv(
    "SCREENER_UI_ORIGIN", "http://localhost:5173,http://localhost:4173"
).split(",")
ADMIN_API_KEY: str | None = os.getenv("SCREENER_ADMIN_KEY") or None

NIFTY500_URL: str = os.getenv(
    "NIFTY500_URL",
    "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
)

# DynamoDB table names (override in env for multi-env deployments)
TBL_QUOTES:        str = os.getenv("DDB_TBL_QUOTES",        "equity-quotes")
TBL_INFO:          str = os.getenv("DDB_TBL_INFO",          "equity-info")
TBL_FINANCIALS:    str = os.getenv("DDB_TBL_FINANCIALS",    "equity-financials")
TBL_BALANCE_SHEET: str = os.getenv("DDB_TBL_BALANCE_SHEET", "equity-balance-sheet")
TBL_HISTORY:       str = os.getenv("DDB_TBL_HISTORY",       "equity-history")
TBL_META:          str = os.getenv("DDB_TBL_META",          "equity-meta")
TBL_NEWS_CACHE:    str = os.getenv("DDB_TBL_NEWS_CACHE",    "equity-news-cache")

# News tuning
NEWS_CACHE_TTL:       int   = int(os.getenv("NEWS_CACHE_TTL",       "600"))
NEWS_CIRCUIT_OPEN_S:  int   = int(os.getenv("NEWS_CIRCUIT_OPEN_S",  "120"))
NEWS_MAX_FAILURES:    int   = int(os.getenv("NEWS_MAX_FAILURES",     "3"))
NEWS_FETCH_TIMEOUT:   float = float(os.getenv("NEWS_FETCH_TIMEOUT", "6.0"))
NEWS_MAX_WORKERS:     int   = int(os.getenv("NEWS_MAX_WORKERS",      "8"))
NEWS_MAX_AGE_HOURS:   int   = int(os.getenv("NEWS_MAX_AGE_HOURS",   "24"))

_FALLBACK_TICKERS = (
    "RELIANCE.NS,TCS.NS,HDFCBANK.NS,INFY.NS,ICICIBANK.NS,"
    "HINDUNILVR.NS,SBIN.NS,BHARTIARTL.NS,ITC.NS,KOTAKBANK.NS,"
    "LT.NS,AXISBANK.NS,ASIANPAINT.NS,MARUTI.NS,TITAN.NS,"
    "SUNPHARMA.NS,WIPRO.NS,ULTRACEMCO.NS,BAJFINANCE.NS,HCLTECH.NS,"
    "NESTLEIND.NS,POWERGRID.NS,TATAMOTORS.NS,ONGC.NS,BAJAJFINSV.NS,"
    "JSWSTEEL.NS,ADANIENT.NS,GRASIM.NS,TECHM.NS,NTPC.NS,"
    "M&M.NS,COALINDIA.NS,DIVISLAB.NS,APOLLOHOSP.NS,"
    "BPCL.NS,TATACONSUM.NS,HINDALCO.NS,BAJAJ-AUTO.NS,DRREDDY.NS,"
    "EICHERMOT.NS,CIPLA.NS,HEROMOTOCO.NS,SBILIFE.NS,"
    "ADANIPORTS.NS,UPL.NS,HDFCLIFE.NS,BRITANNIA.NS,INDUSINDBK.NS,"
    "TATASTEEL.NS,SHRIRAMFIN.NS"
)

_RSS_FEEDS: list[tuple[str, str]] = [
    ("Economic Times",    "https://economictimes.indiatimes.com/rssfeedstopstories.cms"),
    ("Moneycontrol",      "https://www.moneycontrol.com/rss/marketsindia.xml"),
    ("Business Standard", "https://www.business-standard.com/rss/markets-106.rss"),
    ("Mint",              "https://www.livemint.com/rss/markets"),
    ("Hindu BusinessLine","https://www.thehindubusinessline.com/markets/feeder/default.rss"),
]

# ---------------------------------------------------------------------------
# DynamoDB client (module-level – reused across warm Lambda invocations)
# ---------------------------------------------------------------------------

_ddb = boto3.resource("dynamodb", region_name=AWS_REGION)


def _tbl(name: str):
    return _ddb.Table(name)


# ---------------------------------------------------------------------------
# Decimal helpers (DynamoDB requires Decimal, not float)
# ---------------------------------------------------------------------------

def _to_decimal(v: Any) -> Decimal | None:
    """Convert a Python scalar to Decimal for DynamoDB storage."""
    if v is None:
        return None
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return Decimal(str(f))
    except (TypeError, ValueError, InvalidOperation):
        return None


def _from_decimal(v: Any) -> Any:
    """Convert Decimal back to int/float for API responses."""
    if isinstance(v, Decimal):
        if v == v.to_integral_value():
            return int(v)
        return float(v)
    if isinstance(v, dict):
        return {k: _from_decimal(vv) for k, vv in v.items()}
    if isinstance(v, list):
        return [_from_decimal(i) for i in v]
    return v


def _safe_ddb(v: Any) -> Any:
    """Prepare any value for DynamoDB (strip NaN/Inf, convert to Decimal)."""
    if v is None:
        return None
    try:
        import numpy as np
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return _to_decimal(v)
        if isinstance(v, np.bool_):
            return bool(v)
    except ImportError:
        pass
    try:
        import pandas as pd
        if isinstance(v, pd.Timestamp):
            return v.isoformat()
        if pd.isna(v):
            return None
    except (ImportError, TypeError, ValueError):
        pass
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, float):
        return _to_decimal(v)
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v
    return str(v) if v is not None else None


def _clean_item(d: dict) -> dict:
    """Remove None values (DynamoDB rejects them) and convert floats."""
    return {k: _safe_ddb(v) for k, v in d.items() if v is not None and _safe_ddb(v) is not None}


# ---------------------------------------------------------------------------
# Ticker loading
# ---------------------------------------------------------------------------

def _normalise_ticker(symbol: str) -> str:
    symbol = symbol.strip().upper()
    return symbol if symbol.endswith(".NS") else f"{symbol}.NS"


def _is_valid_nse_symbol(symbol: str) -> bool:
    symbol = symbol.strip().upper().replace(".NS", "")
    return bool(symbol) and "DUMMY" not in symbol


def load_nifty500_tickers() -> list[str]:
    env_tickers = os.getenv("TICKERS")
    if env_tickers:
        tickers = [
            _normalise_ticker(t)
            for t in env_tickers.split(",")
            if _is_valid_nse_symbol(t)
        ]
        log.info("Loaded %d tickers from TICKERS env var.", len(tickers))
        return tickers

    try:
        req = urllib.request.Request(
            NIFTY500_URL,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "text/csv,*/*"},
        )
        with urllib.request.urlopen(req, timeout=20) as res:
            text_data = res.read().decode("utf-8-sig")

        reader = csv.DictReader(io.StringIO(text_data))
        tickers = list(dict.fromkeys(
            _normalise_ticker(row["Symbol"])
            for row in reader
            if row.get("Symbol") and _is_valid_nse_symbol(row["Symbol"])
        ))

        if len(tickers) < 400:
            raise RuntimeError(f"NSE CSV returned only {len(tickers)} valid symbols")

        log.info("Loaded %d valid Nifty 500 tickers from NSE.", len(tickers))
        return tickers

    except Exception as exc:
        log.warning("Could not load Nifty 500 list: %s – using fallback.", exc)
        fallback = [
            _normalise_ticker(t)
            for t in _FALLBACK_TICKERS.split(",")
            if _is_valid_nse_symbol(t)
        ]
        log.warning("Fallback ticker list: %d symbols.", len(fallback))
        return fallback


TICKERS: list[str] = load_nifty500_tickers()

# ---------------------------------------------------------------------------
# Utility / math helpers
# ---------------------------------------------------------------------------

def _n(v) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None

# ---------------------------------------------------------------------------
# DynamoDB write helpers
# ---------------------------------------------------------------------------

def _batch_write(table_name: str, items: list[dict]) -> None:
    """Write items in batches of 25 (DynamoDB BatchWrite limit)."""
    if not items:
        return
    table = _tbl(table_name)
    BATCH = 25
    total = 0
    for i in range(0, len(items), BATCH):
        chunk = items[i:i + BATCH]
        with table.batch_writer() as bw:
            for item in chunk:
                cleaned = _clean_item(item)
                if cleaned:
                    bw.put_item(Item=cleaned)
        total += len(chunk)
    log.info("batch_write → %s: %d items", table_name, total)


def _put_item(table_name: str, item: dict) -> None:
    _tbl(table_name).put_item(Item=_clean_item(item))


def _get_item(table_name: str, key: dict) -> dict | None:
    resp = _tbl(table_name).get_item(Key=key)
    return _from_decimal(resp.get("Item"))


def _query_items(table_name: str, pk_name: str, pk_val: str) -> list[dict]:
    resp = _tbl(table_name).query(
        KeyConditionExpression=Key(pk_name).eq(pk_val)
    )
    return [_from_decimal(i) for i in resp.get("Items", [])]


def _scan_table(table_name: str) -> list[dict]:
    """Full table scan – acceptable for dashboard-scale data."""
    table  = _tbl(table_name)
    items: list[dict] = []
    kwargs: dict      = {}

    while True:
        resp = table.scan(**kwargs)
        items.extend(_from_decimal(i) for i in resp.get("Items", []))
        last = resp.get("LastEvaluatedKey")
        if not last:
            break
        kwargs["ExclusiveStartKey"] = last

    return items

# ---------------------------------------------------------------------------
# Meta helpers (last_refresh_date etc.)
# ---------------------------------------------------------------------------

def _get_meta(key: str) -> str | None:
    item = _get_item(TBL_META, {"key": key})
    return item.get("value") if item else None


def _set_meta(key: str, value: str) -> None:
    _put_item(TBL_META, {"key": key, "value": value})


def _already_refreshed_today() -> bool:
    today = date.today().isoformat()
    if _get_meta("last_refresh_date") == today:
        log.info("Already refreshed today (%s) – skipping.", today)
        return True
    return False

# ---------------------------------------------------------------------------
# Yahoo Finance fetchers (identical logic, DDB-friendly output)
# ---------------------------------------------------------------------------

def fetch_quotes(tickers: list[str]) -> list[dict]:
    log.info("Fetching quotes for %d tickers …", len(tickers))
    now  = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []

    # yf.download chunks itself; safe for up to ~500 tickers in one call
    try:
        data = yf.download(
            tickers,
            period="2d",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
    except Exception as exc:
        log.error("yf.download failed: %s", exc)
        return rows

    for ticker in tickers:
        try:
            df = data if len(tickers) == 1 else data[ticker]
            if df is None or df.empty:
                continue
            last = df.iloc[-1]
            prev = df.iloc[-2] if len(df) >= 2 else last
            close      = float(last["Close"])
            prev_close = float(prev["Close"])
            volume     = last["Volume"]
            if close <= 0 or volume is None:
                continue
            change = close - prev_close
            pct    = (change / prev_close * 100) if prev_close else 0.0
            rows.append({
                "symbol":     ticker,
                "date":       str(last.name.date()) if hasattr(last.name, "date") else str(last.name),
                "open":       round(float(last["Open"]),  2),
                "high":       round(float(last["High"]),  2),
                "low":        round(float(last["Low"]),   2),
                "close":      round(close, 2),
                "volume":     int(volume),
                "prev_close": round(prev_close, 2),
                "change":     round(change, 2),
                "change_pct": round(pct, 2),
                "fetched_at": now,
            })
        except Exception as exc:
            log.warning("quotes: skip %s – %s", ticker, exc)

    log.info("quotes: %d rows", len(rows))
    return rows


def fetch_info(tickers: list[str]) -> list[dict]:
    log.info("Fetching info for %d tickers …", len(tickers))
    fields = [
        "symbol","shortName","longName","sector","industry","exchange",
        "currency","country","website","marketCap","enterpriseValue",
        "trailingPE","forwardPE","priceToBook","priceToSalesTrailing12Months",
        "trailingEps","forwardEps","dividendYield","dividendRate",
        "payoutRatio","returnOnEquity","returnOnAssets","debtToEquity",
        "currentRatio","quickRatio","totalRevenue","revenuePerShare",
        "grossProfits","ebitda","netIncomeToCommon","operatingMargins",
        "profitMargins","52WeekChange","fiftyTwoWeekHigh","fiftyTwoWeekLow",
        "fiftyDayAverage","twoHundredDayAverage","beta","sharesOutstanding",
        "floatShares","heldPercentInsiders","heldPercentInstitutions",
        "recommendationKey","numberOfAnalystOpinions","targetMeanPrice",
        "targetHighPrice","targetLowPrice","totalDebt","totalCash",
        "totalCashPerShare","operatingCashflow","freeCashflow",
    ]
    now  = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []
    for ticker in tickers:
        try:
            info  = yf.Ticker(ticker).info
            row   = {"symbol": info.get("symbol") or ticker, "fetched_at": now}
            for k in fields:
                row[k] = info.get(k)
            rows.append(row)
        except Exception as exc:
            log.warning("info: skip %s – %s", ticker, exc)
    log.info("info: %d rows", len(rows))
    return rows


def _pivot_statement(ticker: str, df) -> list[dict]:
    rows: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()
    if df is None or df.empty:
        return rows
    for col in df.columns:
        row: dict[str, Any] = {
            "symbol":      ticker,
            "fiscal_year": str(col.date()) if hasattr(col, "date") else str(col),
            "fetched_at":  now,
        }
        for idx in df.index:
            key = re.sub(r"[^a-z0-9_]", "_", str(idx).lower().strip())
            key = re.sub(r"_+", "_", key).strip("_")
            row[key] = df.at[idx, col]
        rows.append(row)
    return rows


def fetch_financials(tickers: list[str]) -> list[dict]:
    log.info("Fetching income statements …")
    rows: list[dict] = []
    for ticker in tickers:
        try:
            rows.extend(_pivot_statement(ticker, yf.Ticker(ticker).financials))
        except Exception as exc:
            log.warning("financials: skip %s – %s", ticker, exc)
    log.info("financials: %d rows", len(rows))
    return rows


def fetch_balance_sheet(tickers: list[str]) -> list[dict]:
    log.info("Fetching balance sheets …")
    rows: list[dict] = []
    for ticker in tickers:
        try:
            rows.extend(_pivot_statement(ticker, yf.Ticker(ticker).balance_sheet))
        except Exception as exc:
            log.warning("balance_sheet: skip %s – %s", ticker, exc)
    log.info("balance_sheet: %d rows", len(rows))
    return rows


def fetch_history(tickers: list[str], period: str = "30d") -> list[dict]:
    log.info("Fetching %s history …", period)
    now  = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []
    for ticker in tickers:
        try:
            df = yf.Ticker(ticker).history(period=period, auto_adjust=True)
            if df is None or df.empty:
                continue
            for ts, r in df.iterrows():
                rows.append({
                    "symbol":       ticker,
                    "date":         str(ts.date()),
                    "open":         _n(r.get("Open")),
                    "high":         _n(r.get("High")),
                    "low":          _n(r.get("Low")),
                    "close":        _n(r.get("Close")),
                    "volume":       r.get("Volume"),
                    "dividends":    _n(r.get("Dividends")),
                    "stock_splits": _n(r.get("Stock Splits")),
                    "fetched_at":   now,
                })
        except Exception as exc:
            log.warning("history: skip %s – %s", ticker, exc)
    log.info("history: %d rows", len(rows))
    return rows

# ---------------------------------------------------------------------------
# Refresh orchestrators
# ---------------------------------------------------------------------------

def refresh_quotes_only() -> None:
    log.info("=== Fast quotes refresh started ===")
    _batch_write(TBL_QUOTES, fetch_quotes(TICKERS))
    _set_meta("last_refresh_date", date.today().isoformat())
    log.info("=== Fast quotes refresh complete ===")


def refresh_all() -> None:
    log.info("=== Full refresh started ===")
    for name, fetcher, tbl in [
        ("quotes",        lambda: fetch_quotes(TICKERS),       TBL_QUOTES),
        ("info",          lambda: fetch_info(TICKERS),         TBL_INFO),
        ("financials",    lambda: fetch_financials(TICKERS),   TBL_FINANCIALS),
        ("balance_sheet", lambda: fetch_balance_sheet(TICKERS),TBL_BALANCE_SHEET),
        ("history",       lambda: fetch_history(TICKERS),      TBL_HISTORY),
    ]:
        try:
            _batch_write(tbl, fetcher())
        except Exception as exc:
            log.error("%s refresh failed: %s", name, exc)
    _set_meta("last_refresh_date", date.today().isoformat())
    log.info("=== Full refresh complete ===")

# ---------------------------------------------------------------------------
# Browse API helpers (scan + filter in Python – viable for screener scale)
# ---------------------------------------------------------------------------

def _apply_filters(rows: list[dict], params: dict[str, str]) -> list[dict]:
    """
    Supports eq__, like__, gte__, lte__ filter prefixes.
    DynamoDB FilterExpression could replace this, but Python-side filtering
    keeps the code simple and avoids read-unit waste on small datasets.
    """
    for key, raw in params.items():
        val = raw.strip()
        if not val:
            continue
        if key.startswith("eq__"):
            col = key[4:]
            rows = [r for r in rows if str(r.get(col, "")).lower() == val.lower()]
        elif key.startswith("like__"):
            col = key[6:]
            rows = [r for r in rows if val.lower() in str(r.get(col, "")).lower()]
        elif key.startswith("gte__"):
            col = key[5:]
            rows = [r for r in rows if _n(r.get(col)) is not None and _n(r.get(col)) >= float(val)]
        elif key.startswith("lte__"):
            col = key[5:]
            rows = [r for r in rows if _n(r.get(col)) is not None and _n(r.get(col)) <= float(val)]
    return rows


def _table_meta(table_name: str) -> dict:
    """Return column metadata derived from a sample DynamoDB item."""
    table = _tbl(table_name)
    resp  = table.scan(Limit=1)
    items = resp.get("Items", [])
    if not items:
        return {"name": table_name, "columns": []}
    sample = _from_decimal(items[0])

    def _kind(v):
        if isinstance(v, bool):
            return "text"
        if isinstance(v, (int, float, Decimal)):
            return "numeric"
        return "text"

    cols = [
        {
            "name":       k,
            "data_type":  type(v).__name__,
            "udt_name":   None,
            "kind":       _kind(v),
            "filterable": k != "fetched_at",
        }
        for k, v in sample.items()
    ]
    return {"name": table_name, "columns": cols}


_BROWSE_TABLES = {
    "quotes":        TBL_QUOTES,
    "info":          TBL_INFO,
    "financials":    TBL_FINANCIALS,
    "balance_sheet": TBL_BALANCE_SHEET,
    "history":       TBL_HISTORY,
}

# ---------------------------------------------------------------------------
# Scoring / signal helpers (unchanged from original)
# ---------------------------------------------------------------------------

def _score_stock(q: dict, fi: dict) -> float:
    score = 0.0
    chg = _n(q.get("change_pct"))
    if chg is not None:
        score += max(-10.0, min(10.0, chg * 2))

    high52 = _n(fi.get("fiftyTwoWeekHigh"))
    low52  = _n(fi.get("fiftyTwoWeekLow"))
    close  = _n(q.get("close"))

    if high52 and low52 and close and high52 > low52:
        pct_range = (close - low52) / (high52 - low52)
        score += 10.0 if 0.50 <= pct_range <= 0.85 else 6.0 if pct_range >= 0.35 else 2.0

    pe = _n(fi.get("trailingPE"))
    if pe and pe > 0:
        score += 25.0 if pe < 15 else 20.0 if pe < 20 else 15.0 if pe < 25 else 10.0 if pe < 35 else 5.0 if pe < 50 else 0

    fwd_pe = _n(fi.get("forwardPE"))
    if pe and fwd_pe and 0 < fwd_pe < pe:
        score += 5.0

    ptb = _n(fi.get("priceToBook"))
    if ptb and ptb > 0:
        score += 5.0 if ptb < 1.5 else 3.0 if ptb < 3.0 else 1.0 if ptb < 5.0 else 0

    roe = _n(fi.get("returnOnEquity"))
    if roe is not None:
        score += 10.0 if roe > 0.25 else 7.0 if roe > 0.15 else 4.0 if roe > 0.08 else 1.0 if roe > 0 else 0

    pm = _n(fi.get("profitMargins"))
    if pm is not None:
        score += 8.0 if pm > 0.20 else 5.0 if pm > 0.10 else 2.0 if pm > 0.05 else 0

    cr = _n(fi.get("currentRatio"))
    if cr is not None:
        score += 5.0 if cr > 2.0 else 3.0 if cr > 1.5 else 1.0 if cr > 1.0 else 0

    de = _n(fi.get("debtToEquity"))
    if de is not None:
        score += 7.0 if de < 0.3 else 4.0 if de < 0.8 else 2.0 if de < 1.5 else 0

    rec = (fi.get("recommendationKey") or "").lower()
    score += {"strong_buy": 12.0, "strongbuy": 12.0, "buy": 9.0,
              "hold": 3.0, "underperform": 0.0, "sell": 0.0}.get(rec, 3.0)

    target = _n(fi.get("targetMeanPrice"))
    if target and close and close > 0:
        upside = (target - close) / close
        score += 8.0 if upside > 0.30 else 6.0 if upside > 0.15 else 3.0 if upside > 0.05 else 1.0 if upside > 0 else 0

    ma50  = _n(fi.get("fiftyDayAverage"))
    ma200 = _n(fi.get("twoHundredDayAverage"))
    if close and ma50  and close > ma50:
        score += 5.0
    if close and ma200 and close > ma200:
        score += 5.0

    return round(score, 2)


def _signal(score: float) -> str:
    if score >= 60: return "Strong Buy"
    if score >= 40: return "Buy"
    return "Hold"


def _catalysts(q: dict, fi: dict, score: float) -> list[str]:
    cats: list[str] = []
    chg    = _n(q.get("change_pct"))
    roe    = _n(fi.get("returnOnEquity"))
    pe     = _n(fi.get("trailingPE"))
    de     = _n(fi.get("debtToEquity"))
    cr     = _n(fi.get("currentRatio"))
    rec    = (fi.get("recommendationKey") or "").lower()
    target = _n(fi.get("targetMeanPrice"))
    close  = _n(q.get("close"))

    if chg and chg > 0:              cats.append(f"Positive momentum +{chg:.1f}%")
    if roe and roe > 0.15:           cats.append(f"Strong ROE {roe * 100:.1f}%")
    if pe  and pe < 20:              cats.append(f"Attractive P/E {pe:.1f}x")
    if de  and de < 0.5:             cats.append("Low leverage")
    if cr  and cr > 1.5:             cats.append("Healthy liquidity")
    if "buy" in rec:                 cats.append("Analyst buy consensus")
    if target and close and (target - close) / close > 0.10:
        cats.append(f"Analyst upside {((target - close) / close * 100):.0f}%")
    return cats[:4]


def _rationale(q: dict, fi: dict, score: float) -> str:
    close  = _n(q.get("close"))
    pe     = _n(fi.get("trailingPE"))
    roe    = _n(fi.get("returnOnEquity"))
    chg    = _n(q.get("change_pct"))
    target = _n(fi.get("targetMeanPrice"))
    rec    = (fi.get("recommendationKey") or "").replace("_", " ").title()
    sector = fi.get("sector") or "Equity"

    s1 = f"{sector} play"
    if pe:  s1 += f" trading at {pe:.1f}x P/E"
    if roe: s1 += f" with {roe * 100:.1f}% ROE"

    s2 = ""
    if chg is not None:    s2 += f"Recent 1-day move of {chg:+.2f}%"
    if target and close:   s2 += f"; analyst target implies {(target - close) / close * 100:.0f}% upside"
    if rec:                s2 += f" ({rec})"

    return f"{s1}. {s2}." if s2 else f"{s1}."


def _price_target(q: dict, fi: dict) -> float | None:
    close  = _n(q.get("close"))
    target = _n(fi.get("targetMeanPrice"))
    if not close:
        return None
    if target and target > close:
        chg = _n(q.get("change_pct")) or 0
        momentum_3m = close * (1 + (chg / 100) * 30)
        return round(target * 0.6 + momentum_3m * 0.4, 2)
    return round(close * 1.08, 2)

# ===========================================================================
# News Service (adapted for Lambda – in-memory cache per warm instance)
# ===========================================================================

@dataclass
class _NewsItem:
    title: str
    link: str
    publisher: str
    age: str
    published_at: str
    relevance_score: float = 0.0

    def to_dict(self) -> dict:
        return {
            "title":        self.title,
            "link":         self.link,
            "publisher":    self.publisher,
            "age":          self.age,
            "published_at": self.published_at,
        }


@dataclass
class _CircuitBreaker:
    name: str
    max_failures: int  = NEWS_MAX_FAILURES
    open_seconds: int  = NEWS_CIRCUIT_OPEN_S
    _failures:    int  = field(default=0, repr=False)
    _opened_at: float | None = field(default=None, repr=False)

    @property
    def is_open(self) -> bool:
        if self._opened_at is None:
            return False
        if time.monotonic() - self._opened_at > self.open_seconds:
            self._opened_at = None
            self._failures  = 0
            return False
        return True

    def record_success(self) -> None:
        self._failures  = 0
        self._opened_at = None

    def record_failure(self) -> None:
        self._failures += 1
        if self._failures >= self.max_failures and self._opened_at is None:
            self._opened_at = time.monotonic()


@dataclass
class _CacheEntry:
    items: list[_NewsItem]
    ts:    float = field(default_factory=time.monotonic)

    def is_fresh(self, ttl: int) -> bool:
        return (time.monotonic() - self.ts) < ttl


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(raw: Any) -> datetime | None:
    if raw is None:
        return None
    try:
        if isinstance(raw, datetime):
            return raw.astimezone(timezone.utc)
        if isinstance(raw, (int, float)):
            return datetime.fromtimestamp(raw, tz=timezone.utc)
        if isinstance(raw, str):
            raw = raw.strip()
            for fmt in (
                "%a, %d %b %Y %H:%M:%S %Z",
                "%a, %d %b %Y %H:%M:%S GMT",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d %H:%M:%S",
            ):
                try:
                    dt = datetime.strptime(raw, fmt)
                    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    pass
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    return None


def _age_label(published: datetime) -> str:
    delta_s = max(0, int((_now_utc() - published).total_seconds()))
    hours, rem = divmod(delta_s, 3600)
    if hours >= 1:
        return f"{hours}h ago"
    return f"{max(rem // 60, 1)}m ago"


def _normalise_url(url: str) -> str:
    try:
        p      = urllib.parse.urlparse(url)
        qs     = urllib.parse.parse_qs(p.query, keep_blank_values=False)
        cqs    = {k: v for k, v in qs.items() if not k.lower().startswith(("utm_","ref","source","campaign"))}
        clean  = p._replace(query=urllib.parse.urlencode(cqs, doseq=True), fragment="")
        return urllib.parse.urlunparse(clean).rstrip("/").lower()
    except Exception:
        return url.lower().strip()


def _title_fingerprint(title: str) -> str:
    words = sorted(re.sub(r"[^a-z0-9 ]", " ", title.lower()).split())
    return hashlib.md5(" ".join(words).encode()).hexdigest()[:12]


def _clean_company_name(name: str) -> str:
    name = re.sub(
        r"\b(Limited|Ltd\.?|Corporation|Corp\.?|Company|Co\.|Bank|Industries|Enterprises)\b",
        "", name, flags=re.I,
    )
    return re.sub(r"\s+", " ", name).strip()


def _stock_keywords(stock: dict) -> list[tuple[str, float]]:
    symbol     = str(stock.get("symbol") or "").replace(".NS", "").strip()
    short_name = _clean_company_name(str(stock.get("short_name") or symbol))
    sector     = str(stock.get("sector") or "").strip()
    industry   = str(stock.get("industry") or "").strip()

    pairs: list[tuple[str, float]] = []
    if symbol: pairs.append((symbol.lower(), 3.0))
    if short_name and short_name.lower() != symbol.lower():
        for part in short_name.split()[:3]:
            if len(part) > 3: pairs.append((part.lower(), 1.5))
    if industry: pairs.append((industry.lower(), 0.5))
    if sector:   pairs.append((sector.lower(),   0.3))
    return pairs


def _relevance(title: str, description: str, stock: dict) -> float:
    txt = f"{title} {description}".lower()
    return round(sum(w for kw, w in _stock_keywords(stock) if kw in txt), 3)


def _src_gnews(stock: dict, timeout: float) -> list[_NewsItem]:
    symbol     = str(stock.get("symbol") or "").replace(".NS","").strip()
    short_name = _clean_company_name(str(stock.get("short_name") or symbol))
    query      = f'"{short_name}" OR "{symbol}" NSE India stock'

    gn  = GNews(language="en", country="IN", period="1d", max_results=15)
    raw = gn.get_news(query) or []
    now = _now_utc()
    items: list[_NewsItem] = []

    for r in raw:
        pub = _parse_dt(r.get("published date") or r.get("published_date") or r.get("published") or r.get("pubDate"))
        if pub is None or (now - pub).total_seconds() > NEWS_MAX_AGE_HOURS * 3600:
            continue
        title = html.unescape(str(r.get("title") or "")).strip()
        link  = r.get("url") or r.get("link") or ""
        if not title or not link:
            continue
        publisher = r.get("publisher") or {}
        publisher = publisher.get("title") if isinstance(publisher, dict) else str(publisher)
        items.append(_NewsItem(
            title=title, link=link, publisher=publisher or "Google News",
            age=_age_label(pub), published_at=pub.isoformat(),
            relevance_score=_relevance(title, str(r.get("description") or ""), stock),
        ))
    return items


def _src_rss(publisher: str, url: str, stock: dict, timeout: float) -> list[_NewsItem]:
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; EquityScreenerBot/3.2)",
        "Accept":     "application/rss+xml,application/xml,*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw_xml = resp.read()

    root = ET.fromstring(raw_xml)
    ns   = {"atom": "http://www.w3.org/2005/Atom"}
    now  = _now_utc()
    items: list[_NewsItem] = []

    for item in root.iter("item"):
        title_el = item.find("title")
        link_el  = item.find("link")
        pub_el   = item.find("pubDate") or item.find("dc:date", {"dc": "http://purl.org/dc/elements/1.1/"})
        desc_el  = item.find("description")
        title = html.unescape((title_el.text or "").strip()) if title_el is not None else ""
        link  = (link_el.text or "").strip() if link_el is not None else ""
        desc  = html.unescape((desc_el.text or "").strip()) if desc_el is not None else ""
        pub   = _parse_dt((pub_el.text or "").strip() if pub_el is not None else "")
        if not title or not link or pub is None: continue
        if (now - pub).total_seconds() > NEWS_MAX_AGE_HOURS * 3600: continue
        rel = _relevance(title, desc, stock)
        if rel <= 0: continue
        items.append(_NewsItem(title=title, link=link, publisher=publisher,
                               age=_age_label(pub), published_at=pub.isoformat(),
                               relevance_score=rel))

    for entry in root.findall(".//atom:entry", ns):
        title_el   = entry.find("atom:title", ns)
        link_el    = entry.find("atom:link",  ns)
        pub_el     = entry.find("atom:published", ns) or entry.find("atom:updated", ns)
        summary_el = entry.find("atom:summary", ns)
        title = html.unescape((title_el.text or "").strip()) if title_el is not None else ""
        link  = link_el.get("href","").strip() if link_el is not None else ""
        desc  = html.unescape((summary_el.text or "").strip()) if summary_el is not None else ""
        pub   = _parse_dt((pub_el.text or "").strip() if pub_el is not None else "")
        if not title or not link or pub is None: continue
        if (now - pub).total_seconds() > NEWS_MAX_AGE_HOURS * 3600: continue
        rel = _relevance(title, desc, stock)
        if rel <= 0: continue
        items.append(_NewsItem(title=title, link=link, publisher=publisher,
                               age=_age_label(pub), published_at=pub.isoformat(),
                               relevance_score=rel))
    return items


def _src_yfinance(stock: dict, _timeout: float) -> list[_NewsItem]:
    ticker_sym = str(stock.get("symbol") or "")
    raw_news   = yf.Ticker(ticker_sym).news or []
    now        = _now_utc()
    items: list[_NewsItem] = []
    for r in raw_news:
        pub = _parse_dt(r.get("providerPublishTime"))
        if pub is None or (now - pub).total_seconds() > NEWS_MAX_AGE_HOURS * 3600: continue
        title = html.unescape(str(r.get("title") or "")).strip()
        link  = r.get("link") or ""
        if not title or not link: continue
        items.append(_NewsItem(title=title, link=link,
                               publisher=r.get("publisher") or "Yahoo Finance",
                               age=_age_label(pub), published_at=pub.isoformat(),
                               relevance_score=_relevance(title, "", stock)))
    return items


class _Deduplicator:
    def __init__(self) -> None:
        self._urls: set[str] = set()
        self._fps:  set[str] = set()

    def is_duplicate(self, item: _NewsItem) -> bool:
        norm = _normalise_url(item.link)
        fp   = _title_fingerprint(item.title)
        if norm in self._urls or fp in self._fps:
            return True
        self._urls.add(norm)
        self._fps.add(fp)
        return False


class NewsService:
    """Lambda-aware news service.

    Differences from the process-model version:
      • No background pre-fetch task (Lambda has no persistent background threads).
      • In-memory cache survives within the same warm Lambda container only.
      • DynamoDB news-cache table provides cross-invocation warm hits (optional).
    """

    def __init__(self) -> None:
        self._executor       = ThreadPoolExecutor(max_workers=NEWS_MAX_WORKERS, thread_name_prefix="news")
        self._cache:         dict[str, _CacheEntry]  = {}
        self._sym_locks:     dict[str, asyncio.Lock] = {}
        self._global_lock:   asyncio.Lock | None     = None

        source_names = ["gnews"] + [pub for pub, _ in _RSS_FEEDS] + ["yfinance"]
        self._breakers: dict[str, _CircuitBreaker] = {
            n: _CircuitBreaker(n) for n in source_names
        }
        self._sources: list[tuple[str, Callable]] = [
            ("gnews", _src_gnews),
            *[(pub, lambda s, t, _u=url, _p=pub: _src_rss(_p, _u, s, t)) for pub, url in _RSS_FEEDS],
            ("yfinance", _src_yfinance),
        ]

    async def start(self) -> None:
        self._global_lock = asyncio.Lock()
        log.info("[news] NewsService started (lambda mode, cache_ttl=%ds)", NEWS_CACHE_TTL)

    async def stop(self) -> None:
        self._executor.shutdown(wait=False)

    async def get_news(self, stock: dict, limit: int = 5) -> list[dict]:
        symbol = str(stock.get("symbol") or "unknown")
        try:
            items = await self._cached(symbol, stock)
            return [i.to_dict() for i in items[:limit]]
        except Exception:
            log.exception("[news] get_news failed for %s", symbol)
            return []

    async def _cached(self, symbol: str, stock: dict) -> list[_NewsItem]:
        entry = self._cache.get(symbol)
        if entry and entry.is_fresh(NEWS_CACHE_TTL):
            return entry.items

        if self._global_lock is None:
            self._global_lock = asyncio.Lock()
        async with self._global_lock:
            if symbol not in self._sym_locks:
                self._sym_locks[symbol] = asyncio.Lock()

        async with self._sym_locks[symbol]:
            entry = self._cache.get(symbol)
            if entry and entry.is_fresh(NEWS_CACHE_TTL):
                return entry.items
            items = await self._fan_out(stock)
            self._cache[symbol] = _CacheEntry(items=items)
            return items

    async def _fan_out(self, stock: dict) -> list[_NewsItem]:
        loop    = asyncio.get_event_loop()
        symbol  = str(stock.get("symbol") or "")
        tasks:  list[asyncio.Future] = []
        names:  list[str]            = []

        for name, fn in self._sources:
            if self._breakers[name].is_open:
                continue
            tasks.append(loop.run_in_executor(self._executor, fn, stock, NEWS_FETCH_TIMEOUT))
            names.append(name)

        if not tasks:
            return []

        results    = await asyncio.gather(*tasks, return_exceptions=True)
        collected: list[_NewsItem] = []
        dedup      = _Deduplicator()

        for name, result in zip(names, results):
            cb = self._breakers[name]
            if isinstance(result, Exception):
                log.warning("[news] %s failed for %s: %s", name, symbol, result)
                cb.record_failure()
                continue
            cb.record_success()
            fresh = [i for i in result if not dedup.is_duplicate(i)]
            collected.extend(fresh)

        collected.sort(key=lambda i: (
            -i.relevance_score,
            -(datetime.fromisoformat(i.published_at).timestamp() if i.published_at else 0),
        ))
        return collected

    def cache_stats(self) -> dict:
        total = len(self._cache)
        fresh = sum(1 for e in self._cache.values() if e.is_fresh(NEWS_CACHE_TTL))
        return {
            "total_entries":  total,
            "fresh_entries":  fresh,
            "stale_entries":  total - fresh,
            "circuit_breakers": {
                name: {"open": cb.is_open, "consecutive_failures": cb._failures}
                for name, cb in self._breakers.items()
            },
        }


_news_service: NewsService = NewsService()

# ===========================================================================
# Scoring engine
# ===========================================================================

async def compute_top5() -> dict:
    quotes_rows = _scan_table(TBL_QUOTES)
    info_rows   = _scan_table(TBL_INFO)

    if not quotes_rows:
        raise HTTPException(503, detail="No quotes data yet. POST /api/refresh first.")

    # Latest quote per symbol
    quotes_map: dict[str, dict] = {}
    for q in quotes_rows:
        sym = q.get("symbol")
        if not sym:
            continue
        prev = quotes_map.get(sym)
        if prev is None or (q.get("date","") or "") >= (prev.get("date","") or ""):
            quotes_map[sym] = q

    info_map: dict[str, dict] = {r["symbol"]: r for r in info_rows if r.get("symbol")}
    scored:   list[dict]      = []

    for sym, q in quotes_map.items():
        fi         = info_map.get(sym, {})
        raw_score  = _score_stock(q, fi)
        normalised = min(100, round(raw_score / 1.3, 1))

        scored.append({
            "symbol":        sym,
            "sector":        fi.get("sector")      or "Equity",
            "industry":      fi.get("industry")    or "",
            "short_name":    fi.get("shortName")   or sym,
            "close":         _n(q.get("close")),
            "change_pct":    _n(q.get("change_pct")),
            "volume":        q.get("volume"),
            "pe":            _n(fi.get("trailingPE")),
            "forward_pe":    _n(fi.get("forwardPE")),
            "roe":           _n(fi.get("returnOnEquity")),
            "roa":           _n(fi.get("returnOnAssets")),
            "market_cap":    _n(fi.get("marketCap")),
            "debt_equity":   _n(fi.get("debtToEquity")),
            "profit_margin": _n(fi.get("profitMargins")),
            "current_ratio": _n(fi.get("currentRatio")),
            "div_yield":     _n(fi.get("dividendYield")),
            "beta":          _n(fi.get("beta")),
            "wk52_high":     _n(fi.get("fiftyTwoWeekHigh")),
            "wk52_low":      _n(fi.get("fiftyTwoWeekLow")),
            "ma50":          _n(fi.get("fiftyDayAverage")),
            "ma200":         _n(fi.get("twoHundredDayAverage")),
            "analyst_target":_n(fi.get("targetMeanPrice")),
            "rec_key":       fi.get("recommendationKey") or "",
            "score":         normalised,
            "signal":        _signal(raw_score),
            "confidence":    int(min(96, max(55, normalised))),
            "target":        _price_target(q, fi),
            "rationale":     _rationale(q, fi, raw_score),
            "catalysts":     _catalysts(q, fi, raw_score),
        })

    top5 = sorted(scored, key=lambda x: x["score"], reverse=True)[:5]

    news_results = await asyncio.gather(
        *[_news_service.get_news(s, limit=3) for s in top5]
    )
    for stock, news in zip(top5, news_results):
        stock["news"] = news

    valid_changes = [s["change_pct"] for s in scored if s["change_pct"] is not None]
    avg_chg       = sum(valid_changes) / max(1, len(valid_changes))
    sentiment     = "Bullish" if avg_chg > 0.5 else "Cautious" if avg_chg < -0.5 else "Neutral"

    upsides = [(s["target"] - s["close"]) / s["close"] * 100
               for s in top5 if s["target"] and s["close"]]
    avg_upside = round(sum(upsides) / len(upsides), 1) if upsides else 0.0

    betas    = [s["beta"] for s in top5 if s["beta"] is not None]
    avg_beta = sum(betas) / max(1, len(betas))
    risk     = "High" if avg_beta > 1.3 else "Low" if avg_beta < 0.8 else "Medium"

    sector_count: dict[str, int] = {}
    for s in scored:
        sector_count[s["sector"]] = sector_count.get(s["sector"], 0) + 1
    top_sector = max(sector_count, key=sector_count.get) if sector_count else "Diversified"

    market_summary = {
        "sentiment":    sentiment,
        "avg_upside":   avg_upside,
        "risk_level":   risk,
        "top_sector":   top_sector,
        "nifty_outlook": {
            "Bullish":  "Positive bias; momentum favours longs.",
            "Neutral":  "Range-bound; select stock picking advised.",
            "Cautious": "Broader market weakness; manage risk carefully.",
        }[sentiment],
        "avg_score":      round(sum(s["score"] for s in top5) / max(1, len(top5)), 1),
        "last_refresh":   _get_meta("last_refresh_date") or "never",
        "stock_universe": len(scored),
    }

    return {"picks": top5, "market_summary": market_summary}

# ===========================================================================
# FastAPI app
# ===========================================================================

_api_key_header = APIKeyHeader(name="X-Admin-Api-Key", auto_error=False)


def require_api_key(key: str | None = Depends(_api_key_header)) -> None:
    if ADMIN_API_KEY and key != ADMIN_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")


class ColMeta(BaseModel):
    name:       str
    data_type:  str = ""
    udt_name:   str | None = None
    kind:       str
    filterable: bool


class TableInfo(BaseModel):
    name:    str
    columns: list[ColMeta]


class TablesResponse(BaseModel):
    tables: list[TableInfo]


class RowsResponse(BaseModel):
    table:  str
    total:  int
    limit:  int
    offset: int
    rows:   list[dict[str, Any]]


app = FastAPI(
    title="Indian Equity Screener",
    description="Yahoo Finance data + multi-factor Top-5 scoring engine (Lambda + DynamoDB).",
    version="5.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/api/v1/browse/tables", response_model=TablesResponse)
def list_tables(_: None = Depends(require_api_key)):
    tables = []
    for logical_name, ddb_table in _BROWSE_TABLES.items():
        meta = _table_meta(ddb_table)
        meta["name"] = logical_name          # expose logical names to clients
        tables.append(TableInfo(**meta))
    return TablesResponse(tables=tables)


@app.get("/api/v1/browse/tables/{table_name}/rows", response_model=RowsResponse)
def get_rows(
    table_name: str,
    request:    Request,
    limit:      int  = Query(default=50, ge=1, le=500),
    offset:     int  = Query(default=0,  ge=0),
    _:          None = Depends(require_api_key),
):
    if table_name not in _BROWSE_TABLES:
        raise HTTPException(404, f"Table '{table_name}' not found.")

    ddb_table = _BROWSE_TABLES[table_name]
    all_rows  = _scan_table(ddb_table)

    fp = {
        k: v for k, v in request.query_params.items()
        if k.startswith(("eq__", "like__", "gte__", "lte__"))
    }
    filtered = _apply_filters(all_rows, fp)
    total    = len(filtered)
    page     = filtered[offset: offset + limit]

    return RowsResponse(table=table_name, total=total, limit=limit, offset=offset, rows=page)


@app.get("/api/v1/insights/top5")
async def top5_insights(_: None = Depends(require_api_key)):
    return await compute_top5()


@app.post("/api/refresh")
async def manual_refresh(
    full: bool  = Query(default=False),
    _:    None  = Depends(require_api_key),
):
    loop = asyncio.get_event_loop()
    if full:
        await loop.run_in_executor(None, refresh_all)
        mode = "full"
    else:
        await loop.run_in_executor(None, refresh_quotes_only)
        mode = "quotes_only"

    return {
        "status":       "ok",
        "mode":         mode,
        "ticker_count": len(TICKERS),
        "tickers":      TICKERS,
    }


@app.get("/api/tickers")
def list_tickers():
    return {"ticker_count": len(TICKERS), "tickers": TICKERS}


@app.get("/api/health")
def health():
    return {
        "status":               "ok",
        "runtime":              "lambda",
        "dynamodb_tables": {
            "quotes":        TBL_QUOTES,
            "info":          TBL_INFO,
            "financials":    TBL_FINANCIALS,
            "balance_sheet": TBL_BALANCE_SHEET,
            "history":       TBL_HISTORY,
            "meta":          TBL_META,
            "news_cache":    TBL_NEWS_CACHE,
        },
        "ticker_count":         len(TICKERS),
        "sample_tickers":       TICKERS[:10],
        "last_refresh_date":    _get_meta("last_refresh_date"),
        "nifty500_url":         NIFTY500_URL,
    }


@app.get("/api/news/cache-stats")
def news_cache_stats(_: None = Depends(require_api_key)):
    return _news_service.cache_stats()


# ---------------------------------------------------------------------------
# Startup / shutdown lifecycle
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup():
    await _news_service.start()
    log.info("FastAPI startup complete (Lambda mode).")


@app.on_event("shutdown")
async def on_shutdown():
    await _news_service.stop()

# ---------------------------------------------------------------------------
# Lambda handler  (Mangum wraps the ASGI app)
# ---------------------------------------------------------------------------

handler = Mangum(app, lifespan="off")
# lifespan="off" because Lambda doesn't keep a persistent server process;
# startup/shutdown events still fire per-invocation via the lifespan protocol
# but we avoid issues with Mangum's lifespan management by disabling it here
# and relying on FastAPI's own startup handler fired on first request.