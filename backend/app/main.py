"""
Indian Equity Screener – AWS Lambda + DynamoDB Edition
=======================================================
Architecture:
  • Storage  : DynamoDB (on-demand billing)
  • Server   : Lambda + Mangum ASGI adapter
  • Scheduler: EventBridge Scheduler (separate Lambda)

Tables (DynamoDB):
  equity-quotes        PK=symbol  SK=date
  equity-info          PK=symbol
  equity-financials    PK=symbol  SK=fiscal_year
  equity-balance-sheet PK=symbol  SK=fiscal_year
  equity-history       PK=symbol  SK=date
  equity-meta          PK=key
  equity-news-cache    PK=symbol  TTL attr for auto-expiry
  equity-top5-recommendations  PK=symbol  SK=snapshot_ts
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# COMPATIBILITY SHIMS — applied FIRST, before ANY other import
# ---------------------------------------------------------------------------
import numpy as np
import pandas as pd

if not hasattr(np, "NaN"):
    np.NaN = float("nan")
if not hasattr(np, "bool"):
    np.bool = np.bool_
if not hasattr(np, "int"):
    np.int = int
if not hasattr(np, "float"):
    np.float = float
if not hasattr(np, "complex"):
    np.complex = complex
if not hasattr(np, "object"):
    np.object = object
if not hasattr(np, "str"):
    np.str = str

if not hasattr(pd.Series, "append"):
    def _series_append_compat(self: pd.Series, other, verify_integrity=False, **kwargs) -> pd.Series:
        if not isinstance(other, pd.Series):
            other = pd.Series(other)
        return pd.concat([self, other], ignore_index=False)
    pd.Series.append = _series_append_compat  # type: ignore[attr-defined]

if not hasattr(pd.DataFrame, "append"):
    def _df_append_compat(self: pd.DataFrame, other, ignore_index=False, verify_integrity=False, sort=False, **kwargs) -> pd.DataFrame:
        if isinstance(other, dict):
            other = pd.DataFrame([other])
        elif isinstance(other, pd.Series):
            other = other.to_frame().T
        return pd.concat([self, other], ignore_index=ignore_index, sort=sort)
    pd.DataFrame.append = _df_append_compat  # type: ignore[attr-defined]
# ---------------------------------------------------------------------------
# End compatibility shims
# ---------------------------------------------------------------------------

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
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

import pandas_ta as ta
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

from . import portfolio_analysis

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("screener")

# ===========================================================================
# 1. CONFIG  (must come before anything that references these names)
# ===========================================================================

AWS_REGION: str = os.getenv("AWS_REGION", "ap-south-1")
ALLOWED_ORIGINS: list[str] = os.getenv(
    "SCREENER_UI_ORIGIN", "http://localhost:5173,http://localhost:4173"
).split(",")
ADMIN_API_KEY: str | None = os.getenv("SCREENER_ADMIN_KEY") or None

NIFTY500_URL: str = os.getenv(
    "NIFTY500_URL",
    "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
)

# DynamoDB table names
TBL_QUOTES:        str = os.getenv("DDB_TBL_QUOTES",        "equity-quotes")
TBL_INFO:          str = os.getenv("DDB_TBL_INFO",          "equity-info")
TBL_FINANCIALS:    str = os.getenv("DDB_TBL_FINANCIALS",    "equity-financials")
TBL_BALANCE_SHEET: str = os.getenv("DDB_TBL_BALANCE_SHEET", "equity-balance-sheet")
TBL_HISTORY:       str = os.getenv("DDB_TBL_HISTORY",       "equity-history")
TBL_META:          str = os.getenv("DDB_TBL_META",          "equity-meta")
TBL_NEWS_CACHE:    str = os.getenv("DDB_TBL_NEWS_CACHE",    "equity-news-cache")
TBL_TOP5_RECS:     str = os.getenv("DDB_TBL_TOP5_RECS",     "equity-top5-recommendations")

DYNAMODB_ENDPOINT_URL: str | None = os.getenv("DYNAMODB_ENDPOINT_URL")
DYNAMODB_AUTO_CREATE_TABLES: bool = os.getenv(
    "DYNAMODB_AUTO_CREATE_TABLES", "false"
).lower() in ("1", "true", "yes")

NEWS_CACHE_TTL:      int   = int(os.getenv("NEWS_CACHE_TTL",       "600"))
NEWS_CIRCUIT_OPEN_S: int   = int(os.getenv("NEWS_CIRCUIT_OPEN_S",  "120"))
NEWS_MAX_FAILURES:   int   = int(os.getenv("NEWS_MAX_FAILURES",     "3"))
NEWS_FETCH_TIMEOUT:  float = float(os.getenv("NEWS_FETCH_TIMEOUT", "6.0"))
NEWS_MAX_WORKERS:    int   = int(os.getenv("NEWS_MAX_WORKERS",      "8"))
NEWS_MAX_AGE_HOURS:  int   = int(os.getenv("NEWS_MAX_AGE_HOURS",   "24"))

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

# ===========================================================================
# 2. FASTAPI APP  (must be created before route decorators are applied)
# ===========================================================================

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

# ===========================================================================
# 3. AUTH  (must be defined before routes that use Depends(require_api_key))
# ===========================================================================

_api_key_header = APIKeyHeader(name="X-Admin-Api-Key", auto_error=False)


def require_api_key(key: str | None = Depends(_api_key_header)) -> None:
    if ADMIN_API_KEY and key != ADMIN_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")


# ===========================================================================
# 4. PYDANTIC MODELS
# ===========================================================================

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


# ===========================================================================
# 5. DYNAMODB CLIENT
# ===========================================================================

_ddb_kwargs: dict[str, str] = {"region_name": AWS_REGION}
if DYNAMODB_ENDPOINT_URL:
    _ddb_kwargs["endpoint_url"] = DYNAMODB_ENDPOINT_URL

_ddb = boto3.resource("dynamodb", **_ddb_kwargs)


def _auto_create_tables_enabled() -> bool:
    return DYNAMODB_AUTO_CREATE_TABLES or bool(DYNAMODB_ENDPOINT_URL)


def _table_schema(table_name: str) -> dict[str, Any] | None:
    if table_name == TBL_QUOTES:
        return {
            "TableName": table_name,
            "AttributeDefinitions": [
                {"AttributeName": "symbol", "AttributeType": "S"},
                {"AttributeName": "date",   "AttributeType": "S"},
            ],
            "KeySchema": [
                {"AttributeName": "symbol", "KeyType": "HASH"},
                {"AttributeName": "date",   "KeyType": "RANGE"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
    if table_name == TBL_INFO:
        return {
            "TableName": table_name,
            "AttributeDefinitions": [{"AttributeName": "symbol", "AttributeType": "S"}],
            "KeySchema":            [{"AttributeName": "symbol", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
        }
    if table_name in (TBL_FINANCIALS, TBL_BALANCE_SHEET):
        return {
            "TableName": table_name,
            "AttributeDefinitions": [
                {"AttributeName": "symbol",      "AttributeType": "S"},
                {"AttributeName": "fiscal_year", "AttributeType": "S"},
            ],
            "KeySchema": [
                {"AttributeName": "symbol",      "KeyType": "HASH"},
                {"AttributeName": "fiscal_year", "KeyType": "RANGE"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
    if table_name == TBL_HISTORY:
        return {
            "TableName": table_name,
            "AttributeDefinitions": [
                {"AttributeName": "symbol", "AttributeType": "S"},
                {"AttributeName": "date",   "AttributeType": "S"},
            ],
            "KeySchema": [
                {"AttributeName": "symbol", "KeyType": "HASH"},
                {"AttributeName": "date",   "KeyType": "RANGE"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
    if table_name == TBL_META:
        return {
            "TableName": table_name,
            "AttributeDefinitions": [{"AttributeName": "key", "AttributeType": "S"}],
            "KeySchema":            [{"AttributeName": "key", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
        }
    if table_name == TBL_NEWS_CACHE:
        return {
            "TableName": table_name,
            "AttributeDefinitions": [{"AttributeName": "symbol", "AttributeType": "S"}],
            "KeySchema":            [{"AttributeName": "symbol", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
        }
    if table_name == TBL_TOP5_RECS:
        return {
            "TableName": table_name,
            "AttributeDefinitions": [
                {"AttributeName": "symbol",      "AttributeType": "S"},
                {"AttributeName": "snapshot_ts", "AttributeType": "S"},
            ],
            "KeySchema": [
                {"AttributeName": "symbol",      "KeyType": "HASH"},
                {"AttributeName": "snapshot_ts", "KeyType": "RANGE"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
    return None


def _ensure_table_exists(table_name: str) -> None:
    if not _auto_create_tables_enabled():
        return
    try:
        _ddb.meta.client.describe_table(TableName=table_name)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code != "ResourceNotFoundException":
            raise
        schema = _table_schema(table_name)
        if schema is None:
            log.warning("No schema defined for missing table %s; cannot create it.", table_name)
            return
        log.warning("DynamoDB table %s not found; creating it locally.", table_name)
        _ddb.create_table(**schema)
        waiter = _ddb.meta.client.get_waiter("table_exists")
        waiter.wait(TableName=table_name)
        log.info("Created DynamoDB table %s.", table_name)


def _tbl(name: str):
    _ensure_table_exists(name)
    return _ddb.Table(name)


# ===========================================================================
# 6. DECIMAL / TYPE HELPERS
# ===========================================================================

def _to_decimal(v: Any) -> Decimal | None:
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
    if v is None:
        return None
    try:
        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, np.floating):
            return _to_decimal(v)
        if isinstance(v, np.bool_):
            return bool(v)
    except ImportError:
        pass
    try:
        if isinstance(v, pd.Timestamp):
            return v.isoformat()
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
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
    return {k: _safe_ddb(v) for k, v in d.items() if v is not None and _safe_ddb(v) is not None}


def _n(v) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


# ===========================================================================
# 7. TICKER LOADING
# ===========================================================================

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

# ===========================================================================
# 8. DYNAMODB CRUD HELPERS
# ===========================================================================

def _batch_write(table_name: str, items: list[dict]) -> None:
    if not items:
        return
    table = _tbl(table_name)
    BATCH = 25
    total = 0
    for i in range(0, len(items), BATCH):
        chunk = items[i: i + BATCH]
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
    resp = _tbl(table_name).query(KeyConditionExpression=Key(pk_name).eq(pk_val))
    return [_from_decimal(i) for i in resp.get("Items", [])]


def _scan_table(table_name: str) -> list[dict]:
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


# ===========================================================================
# 9. META HELPERS
# ===========================================================================

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


# ===========================================================================
# 10. YAHOO FINANCE FETCHERS
# ===========================================================================

def fetch_quotes(tickers: list[str]) -> list[dict]:
    log.info("Fetching quotes for %d tickers …", len(tickers))
    now  = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []

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
            last       = df.iloc[-1]
            prev       = df.iloc[-2] if len(df) >= 2 else last
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
        "symbol", "shortName", "longName", "sector", "industry", "exchange",
        "currency", "country", "website", "marketCap", "enterpriseValue",
        "trailingPE", "forwardPE", "priceToBook", "priceToSalesTrailing12Months",
        "trailingEps", "forwardEps", "dividendYield", "dividendRate",
        "payoutRatio", "returnOnEquity", "returnOnAssets", "debtToEquity",
        "currentRatio", "quickRatio", "totalRevenue", "revenuePerShare",
        "grossProfits", "ebitda", "netIncomeToCommon", "operatingMargins",
        "profitMargins", "52WeekChange", "fiftyTwoWeekHigh", "fiftyTwoWeekLow",
        "fiftyDayAverage", "twoHundredDayAverage", "beta", "sharesOutstanding",
        "floatShares", "heldPercentInsiders", "heldPercentInstitutions",
        "recommendationKey", "numberOfAnalystOpinions", "targetMeanPrice",
        "targetHighPrice", "targetLowPrice", "totalDebt", "totalCash",
        "totalCashPerShare", "operatingCashflow", "freeCashflow",
    ]
    now  = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []
    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).info
            row  = {"symbol": info.get("symbol") or ticker, "fetched_at": now}
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


# ===========================================================================
# 11. REFRESH ORCHESTRATORS
# ===========================================================================

def refresh_quotes_only() -> None:
    log.info("=== Fast quotes refresh started ===")
    _batch_write(TBL_QUOTES, fetch_quotes(TICKERS))
    _set_meta("last_refresh_date", date.today().isoformat())
    log.info("=== Fast quotes refresh complete ===")


def refresh_all() -> None:
    log.info("=== Full refresh started ===")
    for name, fetcher, tbl in [
        ("quotes",        lambda: fetch_quotes(TICKERS),        TBL_QUOTES),
        ("info",          lambda: fetch_info(TICKERS),          TBL_INFO),
        ("financials",    lambda: fetch_financials(TICKERS),    TBL_FINANCIALS),
        ("balance_sheet", lambda: fetch_balance_sheet(TICKERS), TBL_BALANCE_SHEET),
        ("history",       lambda: fetch_history(TICKERS),       TBL_HISTORY),
    ]:
        try:
            _batch_write(tbl, fetcher())
        except Exception as exc:
            log.error("%s refresh failed: %s", name, exc)
    _set_meta("last_refresh_date", date.today().isoformat())
    log.info("=== Full refresh complete ===")


# ===========================================================================
# 12. TECHNICAL INDICATORS
# ===========================================================================

def _safe_ta_value(series_or_none: Any) -> float | None:
    if series_or_none is None:
        return None
    try:
        if not isinstance(series_or_none, pd.Series):
            return None
        if series_or_none.empty:
            return None
        val = series_or_none.iloc[-1]
        return _n(val)
    except (IndexError, TypeError, ValueError):
        return None


def _extract_ohlcv(data: pd.DataFrame, ticker: str, n_tickers: int) -> pd.DataFrame | None:
    try:
        if data is None or data.empty:
            return None
        df = data.copy()
        if isinstance(df.columns, pd.MultiIndex):
            try:
                if ticker in df.columns.get_level_values(1):
                    df = df.xs(ticker, axis=1, level=1)
                elif ticker in df.columns.get_level_values(0):
                    df = df[ticker]
                else:
                    log.warning("_extract_ohlcv: ticker %s not found in MultiIndex levels", ticker)
                    return None
            except Exception as e:
                log.warning("_extract_ohlcv: failed to extract from MultiIndex for %s: %s", ticker, e)
                return None
        if df.columns.nlevels > 1:
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
        required  = {"Open", "High", "Low", "Close", "Volume"}
        available = set(df.columns)
        if not required.issubset(available):
            missing = required - available
            log.warning("_extract_ohlcv: missing columns for %s: %s", ticker, missing)
            return None
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df = df.dropna(subset=["Close"])
        if df.empty:
            return None
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            except Exception:
                pass
        return df
    except Exception as exc:
        log.warning("_extract_ohlcv: failed for %s – %s", ticker, exc)
        return None


def _fetch_historical_indicators(ticker: str, retries: int = 2) -> dict:
    _EMPTY: dict = {}
    for attempt in range(1, retries + 1):
        try:
            raw = yf.download(
                tickers=ticker,
                period="3mo",
                interval="1d",
                auto_adjust=True,
                progress=False,
                threads=False,
            )
            df = _extract_ohlcv(raw, ticker, n_tickers=1)
            if df is None:
                log.warning("historical: no usable data for %s attempt=%d", ticker, attempt)
                continue

            close  = df["Close"].astype(float)
            high   = df["High"].astype(float)
            low    = df["Low"].astype(float)
            volume = df["Volume"].astype(float)
            n = len(close)
            if n < 2:
                log.warning("historical: insufficient rows (%d) for %s", n, ticker)
                continue

            sma5   = _safe_ta_value(ta.sma(close, length=5))   if n >= 5   else None
            sma20  = _safe_ta_value(ta.sma(close, length=20))  if n >= 20  else None
            sma50  = _safe_ta_value(ta.sma(close, length=50))  if n >= 50  else None
            sma200 = _safe_ta_value(ta.sma(close, length=200)) if n >= 200 else None
            ema9   = _safe_ta_value(ta.ema(close, length=9))   if n >= 9   else None
            ema20  = _safe_ta_value(ta.ema(close, length=20))  if n >= 20  else None
            rsi    = _safe_ta_value(ta.rsi(close, length=14))  if n >= 14  else None

            macd_line = macd_signal_line = None
            if n >= 35:
                macd_df = ta.macd(close, fast=12, slow=26, signal=9)
                if macd_df is not None and isinstance(macd_df, pd.DataFrame):
                    macd_line        = _safe_ta_value(macd_df.get("MACD_12_26_9"))
                    macd_signal_line = _safe_ta_value(macd_df.get("MACDs_12_26_9"))

            atr = _safe_ta_value(ta.atr(high, low, close, length=14)) if n >= 14 else None

            avg_vol_20 = relative_volume = None
            if n >= 20:
                rolling_vol = volume.rolling(20).mean()
                if not rolling_vol.empty:
                    avg_vol_20 = _n(rolling_vol.iloc[-1])
                    last_vol   = _n(volume.iloc[-1])
                    if avg_vol_20 and avg_vol_20 > 0 and last_vol is not None:
                        relative_volume = round(last_vol / avg_vol_20, 4)

            window         = close.iloc[-30:] if n >= 30 else close
            support_30d    = _n(window.min())
            resistance_30d = _n(window.max())
            last_close     = _n(close.iloc[-1])

            support_pct = resistance_pct = None
            if last_close and last_close > 0:
                if support_30d is not None:
                    support_pct    = round((last_close - support_30d)    / last_close * 100, 4)
                if resistance_30d is not None:
                    resistance_pct = round((resistance_30d - last_close) / last_close * 100, 4)

            volatility_20d = None
            if n >= 20:
                pct_changes = close.pct_change().dropna()
                if len(pct_changes) >= 20:
                    volatility_20d = _n(pct_changes.rolling(20).std().iloc[-1] * 100)

            gap_pct = None
            if n >= 2:
                prev_c = _n(close.iloc[-2])
                curr_c = _n(close.iloc[-1])
                if prev_c and prev_c > 0 and curr_c is not None:
                    gap_pct = round((curr_c - prev_c) / prev_c * 100, 4)

            return {
                "sma5": sma5, "sma20": sma20, "sma50": sma50, "sma200": sma200,
                "ema9": ema9, "ema20": ema20, "rsi": rsi,
                "macd": macd_line, "macd_signal": macd_signal_line,
                "atr": atr, "avg_volume_20d": avg_vol_20,
                "relative_volume": relative_volume,
                "support_30d": support_30d, "resistance_30d": resistance_30d,
                "distance_to_support_pct": support_pct,
                "distance_to_resistance_pct": resistance_pct,
                "volatility_20d": volatility_20d, "gap_pct": gap_pct,
            }

        except Exception as exc:
            log.warning("historical: fetch failed %s attempt=%d – %s", ticker, attempt, exc, exc_info=True)

    log.error("historical: all %d attempts exhausted for %s", retries, ticker)
    return _EMPTY


# ===========================================================================
# 13. SCORING HELPERS
# ===========================================================================

def _calculate_risk_metrics(q: dict, indicators: dict) -> tuple[str, float, float, float]:
    close      = _n(q.get("close")) or 0.0
    support    = _n(indicators.get("support_30d"))
    resistance = _n(indicators.get("resistance_30d"))
    atr        = _n(indicators.get("atr"))

    if close and support is not None:
        buy_zone  = round(support + (close - support) * 0.25, 2)
        stop_loss = round(max(support * 0.99, close - atr * 1.0) if atr else support * 0.99, 2)
    else:
        buy_zone  = close
        stop_loss = round(close * 0.97, 2)

    target   = round(close + ((resistance - close) if resistance is not None else close * 0.03), 2)
    rr_ratio = (
        round((target - close) / (close - stop_loss), 2)
        if close and stop_loss and close != stop_loss
        else 0.0
    )

    risk_warning = ""
    if rr_ratio < 1.3:
        risk_warning = "Risk/reward ratio is low; consider a smaller position."
    vol_20d = _n(indicators.get("volatility_20d"))
    if vol_20d and vol_20d > 4.5:
        risk_warning = "Volatility is elevated; use tighter risk controls."

    return risk_warning, buy_zone, stop_loss, rr_ratio


def _is_btst_candidate(q: dict, fi: dict, indicators: dict) -> bool:
    close       = _n(q.get("close"))
    rel_vol     = _n(indicators.get("relative_volume"))
    rsi         = _n(indicators.get("rsi"))
    macd        = _n(indicators.get("macd"))
    macd_signal = _n(indicators.get("macd_signal"))
    if close is None:
        return False
    if rel_vol and rel_vol > 1.1 and rsi is not None and 35 <= rsi <= 65:
        if macd is not None and macd_signal is not None and macd > macd_signal:
            return True
    return False


def _signal(score: float) -> str:
    if score >= 80:   return "Strong Buy"
    if score >= 65:   return "Buy"
    if score >= 50:   return "Watch"
    return "Hold"


def _technical_summary(indicators: dict, q: dict) -> dict:
    rsi         = _n(indicators.get("rsi"))
    macd        = _n(indicators.get("macd"))
    macd_signal = _n(indicators.get("macd_signal"))
    sma5        = _n(indicators.get("sma5"))
    sma20       = _n(indicators.get("sma20"))
    sma50       = _n(indicators.get("sma50"))
    sma200      = _n(indicators.get("sma200"))
    support     = _n(indicators.get("support_30d"))
    resistance  = _n(indicators.get("resistance_30d"))

    current_trend = (
        "Bullish"  if rsi and rsi > 55 else
        "Neutral"  if rsi and rsi >= 40 else
        "Cautious"
    )
    signals: list[str] = []
    if sma5 and sma20 and sma5 > sma20:
        signals.append("Short-term momentum above 20-day average")
    if sma20 and sma50 and sma20 > sma50:
        signals.append("Medium-term trend is upward")
    if macd is not None and macd_signal is not None and macd > macd_signal:
        signals.append("MACD crossover is bullish")
    if rsi is not None:
        if rsi < 40:  signals.append("RSI indicates potential oversold bounce")
        elif rsi > 70: signals.append("RSI is overbought")
    if indicators.get("relative_volume") and indicators["relative_volume"] > 1.2:
        signals.append("Volume is above 20-day average")
    if support is not None and resistance is not None:
        signals.append("Recent range is defined by support/resistance levels")

    return {
        "technical_score":              round((rsi or 50) / 10 if rsi is not None else 5, 1),
        "trend":                        current_trend,
        "rsi14":                        rsi,
        "rsi_signal": (
            "Buy"     if rsi and rsi < 45  else
            "Neutral" if rsi and rsi <= 65 else
            "Sell"    if rsi else None
        ),
        "sma5": sma5, "sma20": sma20, "sma50": sma50, "sma200": sma200,
        "support_30d": support, "resistance_30d": resistance,
        "distance_to_support_pct":    _n(indicators.get("distance_to_support_pct")),
        "distance_to_resistance_pct": _n(indicators.get("distance_to_resistance_pct")),
        "volatility_20d":  _n(indicators.get("volatility_20d")),
        "avg_volume_20d":  _n(indicators.get("avg_volume_20d")),
        "signals":     signals,
        "explanation": " | ".join(signals) if signals else "No clean technical edge found.",
    }


def _score_stock(q: dict, fi: dict, indicators: dict) -> tuple[float, dict]:
    close   = _n(q.get("close")) or 0.0
    score   = 0.0
    details: dict[str, Any] = {}

    chg = _n(q.get("change_pct"))
    if chg is not None:
        ms = max(-10.0, min(10.0, chg * 2))
        score += ms; details["momentum_score"] = ms

    rsi = _n(indicators.get("rsi"))
    if rsi is not None:
        rs = 10.0 if rsi < 40 else 7.0 if rsi < 50 else 5.0 if rsi < 60 else 2.0
        score += rs; details["rsi_score"] = rs

    macd = _n(indicators.get("macd")); macd_signal = _n(indicators.get("macd_signal"))
    if macd is not None and macd_signal is not None:
        ms2 = 8.0 if macd > macd_signal else 3.0
        score += ms2; details["macd_score"] = ms2

    sma20 = _n(indicators.get("sma20")); sma50 = _n(indicators.get("sma50"))
    if close and sma20 is not None:
        ts = 5.0 if close > sma20 else 1.0; score += ts; details["trend_score"] = ts
    if close and sma50 is not None:
        lts = 5.0 if close > sma50 else 1.0; score += lts; details["longer_term_score"] = lts

    rel_vol = _n(indicators.get("relative_volume"))
    if rel_vol is not None:
        vs = 6.0 if rel_vol > 1.2 else 3.0 if rel_vol > 0.8 else 0
        score += vs; details["volume_score"] = vs

    gap_pct = _n(indicators.get("gap_pct"))
    if gap_pct is not None and abs(gap_pct) <= 5.0:
        score += 3.0; details["gap_score"] = 3.0

    low52 = _n(fi.get("fiftyTwoWeekLow")); high52 = _n(fi.get("fiftyTwoWeekHigh"))
    if low52 and high52 and close and high52 > low52:
        pct_range = (close - low52) / (high52 - low52)
        rng = 10.0 if 0.40 <= pct_range <= 0.75 else 6.0 if pct_range >= 0.30 else 2.0
        score += rng; details["52w_range_score"] = rng

    pe = _n(fi.get("trailingPE"))
    if pe and pe > 0:
        pes = 25.0 if pe < 15 else 20.0 if pe < 20 else 15.0 if pe < 25 else 10.0 if pe < 35 else 5.0 if pe < 50 else 0
        score += pes; details["pe_score"] = pes

    fwd_pe = _n(fi.get("forwardPE"))
    if pe and fwd_pe and 0 < fwd_pe < pe:
        score += 5.0; details["forward_pe_score"] = 5.0

    roe = _n(fi.get("returnOnEquity"))
    if roe is not None:
        rs2 = 10.0 if roe > 0.25 else 7.0 if roe > 0.15 else 4.0 if roe > 0.08 else 1.0 if roe > 0 else 0
        score += rs2; details["roe_score"] = rs2

    pm = _n(fi.get("profitMargins"))
    if pm is not None:
        pms = 8.0 if pm > 0.20 else 5.0 if pm > 0.10 else 2.0 if pm > 0.05 else 0
        score += pms; details["profit_margin_score"] = pms

    cr = _n(fi.get("currentRatio"))
    if cr is not None:
        crs = 5.0 if cr > 2.0 else 3.0 if cr > 1.5 else 1.0 if cr > 1.0 else 0
        score += crs; details["liquidity_score"] = crs

    de = _n(fi.get("debtToEquity"))
    if de is not None:
        des = 7.0 if de < 0.3 else 4.0 if de < 0.8 else 2.0 if de < 1.5 else 0
        score += des; details["de_score"] = des

    rec = (fi.get("recommendationKey") or "").lower()
    rec_score = {"strong_buy": 12.0, "strongbuy": 12.0, "buy": 9.0, "hold": 3.0, "underperform": 0.0, "sell": 0.0}.get(rec, 3.0)
    score += rec_score; details["analyst_score"] = rec_score

    target = _n(fi.get("targetMeanPrice"))
    if target and close and close > 0:
        upside = (target - close) / close
        us = 8.0 if upside > 0.30 else 6.0 if upside > 0.15 else 3.0 if upside > 0.05 else 1.0 if upside > 0 else 0
        score += us; details["analyst_upside_score"] = us

    score = max(0.0, min(100.0, score))
    details["final_score"] = round(score, 1)
    return round(score, 1), details


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
    if chg and chg > 0:             cats.append(f"Positive momentum +{chg:.1f}%")
    if roe and roe > 0.15:          cats.append(f"Strong ROE {roe * 100:.1f}%")
    if pe and pe < 20:              cats.append(f"Attractive P/E {pe:.1f}x")
    if de and de < 0.5:             cats.append("Low leverage")
    if cr and cr > 1.5:             cats.append("Healthy liquidity")
    if "buy" in rec:                cats.append("Analyst buy consensus")
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
    if chg is not None:  s2 += f"Recent 1-day move of {chg:+.2f}%"
    if target and close: s2 += f"; analyst target implies {(target - close) / close * 100:.0f}% upside"
    if rec:              s2 += f" ({rec})"
    return f"{s1}. {s2}." if s2 else f"{s1}."


def _price_target(q: dict, fi: dict) -> float | None:
    close  = _n(q.get("close"))
    target = _n(fi.get("targetMeanPrice"))
    if not close:
        return None
    if target and target > close:
        chg         = _n(q.get("change_pct")) or 0
        momentum_3m = close * (1 + (chg / 100) * 30)
        return round(target * 0.6 + momentum_3m * 0.4, 2)
    return round(close * 1.08, 2)


# ===========================================================================
# 14. BROWSE API HELPERS
# ===========================================================================

def _apply_filters(rows: list[dict], params: dict[str, str]) -> list[dict]:
    for key, raw in params.items():
        val = raw.strip()
        if not val:
            continue
        if key.startswith("eq__"):
            col  = key[4:]
            rows = [r for r in rows if str(r.get(col, "")).lower() == val.lower()]
        elif key.startswith("like__"):
            col  = key[6:]
            rows = [r for r in rows if val.lower() in str(r.get(col, "")).lower()]
        elif key.startswith("gte__"):
            col  = key[5:]
            rows = [r for r in rows if _n(r.get(col)) is not None and _n(r.get(col)) >= float(val)]
        elif key.startswith("lte__"):
            col  = key[5:]
            rows = [r for r in rows if _n(r.get(col)) is not None and _n(r.get(col)) <= float(val)]
    return rows


def _table_meta(table_name: str) -> dict:
    try:
        table = _tbl(table_name)
        resp  = table.scan(Limit=1)
        items = resp.get("Items", [])
        if not items:
            return {"name": table_name, "columns": []}
        sample = _from_decimal(items[0])

        def _kind(v):
            if isinstance(v, bool):              return "text"
            if isinstance(v, (int, float, Decimal)): return "numeric"
            return "text"

        cols = [
            {"name": k, "data_type": type(v).__name__, "udt_name": None,
             "kind": _kind(v), "filterable": k != "fetched_at"}
            for k, v in sample.items()
        ]
        return {"name": table_name, "columns": cols}
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code == "ResourceNotFoundException":
            log.warning("Table %s not found during metadata scan.", table_name)
            return {"name": table_name, "columns": []}
        raise


_BROWSE_TABLES = {
    "quotes":               TBL_QUOTES,
    "info":                 TBL_INFO,
    "financials":           TBL_FINANCIALS,
    "balance_sheet":        TBL_BALANCE_SHEET,
    "history":              TBL_HISTORY,
    "top5_recommendations": TBL_TOP5_RECS,
}

# ===========================================================================
# 15. NEWS SERVICE
# ===========================================================================

@dataclass
class _NewsItem:
    title: str; link: str; publisher: str; age: str; published_at: str
    relevance_score: float = 0.0

    def to_dict(self) -> dict:
        return {"title": self.title, "link": self.link, "publisher": self.publisher,
                "age": self.age, "published_at": self.published_at}


@dataclass
class _CircuitBreaker:
    name: str
    max_failures: int = NEWS_MAX_FAILURES
    open_seconds: int = NEWS_CIRCUIT_OPEN_S
    _failures:    int = field(default=0, repr=False)
    _opened_at: float | None = field(default=None, repr=False)

    @property
    def is_open(self) -> bool:
        if self._opened_at is None:
            return False
        if time.monotonic() - self._opened_at > self.open_seconds:
            self._opened_at = None; self._failures = 0; return False
        return True

    def record_success(self) -> None:
        self._failures = 0; self._opened_at = None

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
                "%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S GMT",
                "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S",
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
    return f"{hours}h ago" if hours >= 1 else f"{max(rem // 60, 1)}m ago"


def _normalise_url(url: str) -> str:
    try:
        p   = urllib.parse.urlparse(url)
        qs  = urllib.parse.parse_qs(p.query, keep_blank_values=False)
        cqs = {k: v for k, v in qs.items() if not k.lower().startswith(("utm_", "ref", "source", "campaign"))}
        clean = p._replace(query=urllib.parse.urlencode(cqs, doseq=True), fragment="")
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
    symbol     = str(stock.get("symbol") or "").replace(".NS", "").strip()
    short_name = _clean_company_name(str(stock.get("short_name") or symbol))
    query      = f'"{short_name}" OR "{symbol}" NSE India stock'
    gn  = GNews(language="en", country="IN", period="1d", max_results=15)
    raw = gn.get_news(query) or []
    now = _now_utc()
    items: list[_NewsItem] = []
    for r in raw:
        pub = _parse_dt(r.get("published date") or r.get("published_date") or r.get("published") or r.get("pubDate"))
        if pub is None or (now - pub).total_seconds() > NEWS_MAX_AGE_HOURS * 3600: continue
        title = html.unescape(str(r.get("title") or "")).strip()
        link  = r.get("url") or r.get("link") or ""
        if not title or not link: continue
        publisher = r.get("publisher") or {}
        publisher = publisher.get("title") if isinstance(publisher, dict) else str(publisher)
        items.append(_NewsItem(title=title, link=link, publisher=publisher or "Google News",
                               age=_age_label(pub), published_at=pub.isoformat(),
                               relevance_score=_relevance(title, str(r.get("description") or ""), stock)))
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
        title_el = item.find("title"); link_el = item.find("link")
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
                               age=_age_label(pub), published_at=pub.isoformat(), relevance_score=rel))
    for entry in root.findall(".//atom:entry", ns):
        title_el   = entry.find("atom:title",     ns); link_el    = entry.find("atom:link",      ns)
        pub_el     = entry.find("atom:published",  ns) or entry.find("atom:updated", ns)
        summary_el = entry.find("atom:summary",    ns)
        title = html.unescape((title_el.text or "").strip()) if title_el is not None else ""
        link  = link_el.get("href", "").strip() if link_el is not None else ""
        desc  = html.unescape((summary_el.text or "").strip()) if summary_el is not None else ""
        pub   = _parse_dt((pub_el.text or "").strip() if pub_el is not None else "")
        if not title or not link or pub is None: continue
        if (now - pub).total_seconds() > NEWS_MAX_AGE_HOURS * 3600: continue
        rel = _relevance(title, desc, stock)
        if rel <= 0: continue
        items.append(_NewsItem(title=title, link=link, publisher=publisher,
                               age=_age_label(pub), published_at=pub.isoformat(), relevance_score=rel))
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
        items.append(_NewsItem(title=title, link=link, publisher=r.get("publisher") or "Yahoo Finance",
                               age=_age_label(pub), published_at=pub.isoformat(),
                               relevance_score=_relevance(title, "", stock)))
    return items


class _Deduplicator:
    def __init__(self) -> None:
        self._urls: set[str] = set(); self._fps: set[str] = set()

    def is_duplicate(self, item: _NewsItem) -> bool:
        norm = _normalise_url(item.link); fp = _title_fingerprint(item.title)
        if norm in self._urls or fp in self._fps: return True
        self._urls.add(norm); self._fps.add(fp); return False


class NewsService:
    """Lambda-aware news service with in-memory cache per warm container."""

    def __init__(self) -> None:
        self._executor     = ThreadPoolExecutor(max_workers=NEWS_MAX_WORKERS, thread_name_prefix="news")
        self._cache:       dict[str, _CacheEntry]  = {}
        self._sym_locks:   dict[str, asyncio.Lock] = {}
        self._global_lock: asyncio.Lock | None      = None
        source_names = ["gnews"] + [pub for pub, _ in _RSS_FEEDS] + ["yfinance"]
        self._breakers: dict[str, _CircuitBreaker] = {n: _CircuitBreaker(n) for n in source_names}
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
        loop   = asyncio.get_event_loop()
        symbol = str(stock.get("symbol") or "")
        tasks: list[asyncio.Future] = []; names: list[str] = []
        for name, fn in self._sources:
            if self._breakers[name].is_open: continue
            tasks.append(loop.run_in_executor(self._executor, fn, stock, NEWS_FETCH_TIMEOUT))
            names.append(name)
        if not tasks:
            return []
        results    = await asyncio.gather(*tasks, return_exceptions=True)
        collected: list[_NewsItem] = []; dedup = _Deduplicator()
        for name, result in zip(names, results):
            cb = self._breakers[name]
            if isinstance(result, Exception):
                log.warning("[news] %s failed for %s: %s", name, symbol, result)
                cb.record_failure(); continue
            cb.record_success()
            collected.extend(i for i in result if not dedup.is_duplicate(i))
        collected.sort(key=lambda i: (
            -i.relevance_score,
            -(datetime.fromisoformat(i.published_at).timestamp() if i.published_at else 0),
        ))
        return collected

    def cache_stats(self) -> dict:
        total = len(self._cache); fresh = sum(1 for e in self._cache.values() if e.is_fresh(NEWS_CACHE_TTL))
        return {
            "total_entries": total, "fresh_entries": fresh, "stale_entries": total - fresh,
            "circuit_breakers": {name: {"open": cb.is_open, "consecutive_failures": cb._failures}
                                  for name, cb in self._breakers.items()},
        }


_news_service: NewsService = NewsService()


# ===========================================================================
# 16. SCORING ENGINE
# ===========================================================================

async def compute_top5() -> dict:
    quotes_rows = _scan_table(TBL_QUOTES)
    info_rows   = _scan_table(TBL_INFO)

    if not quotes_rows:
        raise HTTPException(503, detail="No quotes data yet. POST /api/refresh first.")

    quotes_map: dict[str, dict] = {}
    for q in quotes_rows:
        sym = q.get("symbol")
        if not sym: continue
        prev = quotes_map.get(sym)
        if prev is None or (q.get("date", "") or "") >= (prev.get("date", "") or ""):
            quotes_map[sym] = q

    info_map: dict[str, dict] = {r["symbol"]: r for r in info_rows if r.get("symbol")}
    scored:   list[dict]      = []

    for sym, q in quotes_map.items():
        fi         = info_map.get(sym, {})
        indicators = _fetch_historical_indicators(sym)
        raw_score, scoring_details = _score_stock(q, fi, indicators)
        normalised = min(100, round(raw_score, 1))
        risk_warning, buy_zone, stop_loss, rr_ratio = _calculate_risk_metrics(q, indicators)
        technical_analysis = _technical_summary(indicators, q)

        scored.append({
            "symbol": sym, "sector": fi.get("sector") or "Equity",
            "industry": fi.get("industry") or "",
            "short_name": fi.get("shortName") or sym,
            "close": _n(q.get("close")), "change_pct": _n(q.get("change_pct")),
            "volume": q.get("volume"),
            "pe": _n(fi.get("trailingPE")), "forward_pe": _n(fi.get("forwardPE")),
            "roe": _n(fi.get("returnOnEquity")), "roa": _n(fi.get("returnOnAssets")),
            "market_cap": _n(fi.get("marketCap")), "debt_equity": _n(fi.get("debtToEquity")),
            "profit_margin": _n(fi.get("profitMargins")), "current_ratio": _n(fi.get("currentRatio")),
            "div_yield": _n(fi.get("dividendYield")), "beta": _n(fi.get("beta")),
            "wk52_high": _n(fi.get("fiftyTwoWeekHigh")), "wk52_low": _n(fi.get("fiftyTwoWeekLow")),
            "ma50": _n(fi.get("fiftyDayAverage")), "ma200": _n(fi.get("twoHundredDayAverage")),
            "analyst_target": _n(fi.get("targetMeanPrice")),
            "rec_key": fi.get("recommendationKey") or "",
            "score": normalised, "signal": _signal(raw_score),
            "confidence": int(min(96, max(55, normalised))),
            "target": _price_target(q, fi),
            "rationale": _rationale(q, fi, raw_score),
            "catalysts": _catalysts(q, fi, raw_score),
            "btst_candidate": _is_btst_candidate(q, fi, indicators),
            "risk_warning": risk_warning, "buy_zone": buy_zone,
            "stop_loss": stop_loss, "rr_ratio": rr_ratio,
            "price_target": _price_target(q, fi),
            "technical_analysis": technical_analysis,
            "technical_reasons": scoring_details,
            "news_sentiment_reason": "",
        })

    top5 = sorted(scored, key=lambda x: x["score"], reverse=True)[:5]

    news_results = await asyncio.gather(*[_news_service.get_news(s, limit=3) for s in top5])
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

    _store_top5_recommendations(top5)

    sector_count: dict[str, int] = {}
    for s in scored:
        sector_count[s["sector"]] = sector_count.get(s["sector"], 0) + 1
    top_sector = max(sector_count, key=sector_count.get) if sector_count else "Diversified"

    market_summary = {
        "sentiment": sentiment, "avg_upside": avg_upside, "risk_level": risk,
        "top_sector": top_sector,
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


def _store_top5_recommendations(top5: list[dict]) -> None:
    if not top5:
        return
    snapshot_ts = datetime.now(timezone.utc).isoformat()
    items: list[dict] = []
    for rank, pick in enumerate(top5, start=1):
        target = _n(pick.get("target")); close = _n(pick.get("close"))
        achieved = bool(target is not None and close is not None and target <= close)
        items.append({
            "symbol": pick["symbol"], "snapshot_ts": snapshot_ts,
            "snapshot_date": snapshot_ts[:10], "top5_rank": rank,
            "short_name": pick.get("short_name"), "sector": pick.get("sector"),
            "industry": pick.get("industry"), "close": close,
            "change_pct": _n(pick.get("change_pct")), "volume": pick.get("volume"),
            "score": _n(pick.get("score")), "signal": pick.get("signal"),
            "confidence": pick.get("confidence"), "buy_zone": pick.get("buy_zone"),
            "stop_loss": pick.get("stop_loss"), "rr_ratio": pick.get("rr_ratio"),
            "target": target, "price_target": _n(pick.get("price_target")),
            "analyst_target": _n(pick.get("analyst_target")), "rec_key": pick.get("rec_key"),
            "target_achieved": achieved,
            "target_achieved_at": snapshot_ts if achieved else None,
            "created_at": snapshot_ts, "updated_at": snapshot_ts, "payload": pick,
        })
    _batch_write(TBL_TOP5_RECS, items)


# ===========================================================================
# 17. ROUTES  (app, require_api_key, and all helpers are defined by now)
# ===========================================================================

@app.get("/api/v1/browse/tables", response_model=TablesResponse)
def list_tables(_: None = Depends(require_api_key)):
    tables = []
    for logical_name, ddb_table in _BROWSE_TABLES.items():
        meta = _table_meta(ddb_table)
        meta["name"] = logical_name
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
    fp = {k: v for k, v in request.query_params.items()
          if k.startswith(("eq__", "like__", "gte__", "lte__"))}
    filtered = _apply_filters(all_rows, fp)
    return RowsResponse(table=table_name, total=len(filtered), limit=limit, offset=offset,
                        rows=filtered[offset: offset + limit])


@app.get("/api/v1/insights/top5")
async def top5_insights(_: None = Depends(require_api_key)):
    return await compute_top5()


@app.get("/api/v1/insights/top5/history")
async def top5_history(
    symbol: str | None = None,
    _: None = Depends(require_api_key),
):
    if symbol:
        symbol = _normalise_ticker(symbol)
        return {"items": _query_items(TBL_TOP5_RECS, "symbol", symbol)}
    return {"items": _scan_table(TBL_TOP5_RECS)}


@app.post("/api/refresh")
async def manual_refresh(
    full: bool = Query(default=False),
    _:    None = Depends(require_api_key),
):
    loop = asyncio.get_event_loop()
    if full:
        await loop.run_in_executor(None, refresh_all)
        mode = "full"
    else:
        await loop.run_in_executor(None, refresh_quotes_only)
        mode = "quotes_only"
    return {"status": "ok", "mode": mode, "ticker_count": len(TICKERS), "tickers": TICKERS}


@app.get("/api/tickers")
def list_tickers():
    return {"ticker_count": len(TICKERS), "tickers": TICKERS}


@app.get("/api/health")
def health():
    return {
        "status": "ok", "runtime": "lambda",
        "dynamodb_tables": {
            "quotes": TBL_QUOTES, "info": TBL_INFO, "financials": TBL_FINANCIALS,
            "balance_sheet": TBL_BALANCE_SHEET, "history": TBL_HISTORY,
            "meta": TBL_META, "news_cache": TBL_NEWS_CACHE,
            "top5_recommendations": TBL_TOP5_RECS,
        },
        "ticker_count": len(TICKERS), "sample_tickers": TICKERS[:10],
        "last_refresh_date": _get_meta("last_refresh_date"),
        "nifty500_url": NIFTY500_URL,
    }


@app.get("/api/news/cache-stats")
def news_cache_stats(_: None = Depends(require_api_key)):
    return _news_service.cache_stats()


@app.get("/api/v1/portfolio/analysis")
async def analyze_portfolio_endpoint(_: None = Depends(require_api_key)):
    """Analyze portfolio sentiment and news using LangChain + Ollama."""
    try:
        analysis = portfolio_analysis.analyze_portfolio()
        return {"status": "ok", "data": analysis.model_dump()}
    except Exception as e:
        log.error("Portfolio analysis failed: %s", e)
        raise HTTPException(status_code=503,
                            detail=f"Portfolio analysis unavailable: {e}. Ensure Ollama is running locally.")


@app.get("/api/v1/portfolio/top5-analysis")
async def analyze_top5_endpoint(_: None = Depends(require_api_key)):
    """Analyze sentiment and news for the current Top 5 stocks from the screener."""
    try:
        top5_data = await compute_top5()
        picks     = top5_data.get("picks", [])
        if not picks:
            raise HTTPException(status_code=503,
                                detail="No top 5 stocks available. Run /api/refresh first.")
        portfolio_names = [pick["short_name"] for pick in picks[:5]]
        stock_details   = {
            pick["symbol"]: {
                "short_name": pick["short_name"], "score": pick.get("score"),
                "signal": pick.get("signal"), "sector": pick.get("sector"),
                "close": pick.get("close"), "change_pct": pick.get("change_pct"),
            }
            for pick in picks[:5]
        }
        log.info("Analyzing top 5 stocks: %s", portfolio_names)
        analysis = portfolio_analysis.analyze_portfolio(portfolio=portfolio_names, stock_details=stock_details)
        return {"status": "ok", "top5_stocks": [pick["symbol"] for pick in picks[:5]],
                "analysis": analysis.model_dump()}
    except HTTPException:
        raise
    except Exception as e:
        log.error("Top 5 portfolio analysis failed: %s", e)
        raise HTTPException(status_code=503,
                            detail=f"Top 5 analysis unavailable: {e}. Ensure Ollama is running.")


# ===========================================================================
# 18. LIFECYCLE + LAMBDA HANDLER
# ===========================================================================

@app.on_event("startup")
async def on_startup():
    await _news_service.start()
    log.info("FastAPI startup complete (Lambda mode).")


@app.on_event("shutdown")
async def on_shutdown():
    await _news_service.stop()


handler = Mangum(app, lifespan="off")