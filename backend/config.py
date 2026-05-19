# ---------------------------------------------------------------------------
# Config and Constants
# ---------------------------------------------------------------------------

import csv
import io
import logging
import os
import urllib.request

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("screener")

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
# Meta helpers
# ---------------------------------------------------------------------------

import boto3
from datetime import date

_ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
_tbl_meta = _ddb.Table(TBL_META)


def _get_meta(key: str) -> str | None:
    try:
        res = _tbl_meta.get_item(Key={"key": key})
        return res.get("Item", {}).get("value")
    except Exception as exc:
        log.warning("Failed to get meta %s: %s", key, exc)
        return None


def _set_meta(key: str, value: str) -> None:
    try:
        _tbl_meta.put_item(Item={"key": key, "value": value})
    except Exception as exc:
        log.warning("Failed to set meta %s: %s", key, exc)


def _already_refreshed_today() -> bool:
    last_refresh = _get_meta("last_refresh_date")
    return last_refresh == date.today().isoformat()