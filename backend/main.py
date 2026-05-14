"""
Indian Equity Screener - FastAPI Backend
========================================
Tables: quotes, info, financials, balance_sheet, history
Extra : GET  /api/v1/insights/top5
        POST /api/refresh
        GET  /api/health
"""

from __future__ import annotations

import csv
import io
import logging
import math
import os
import re
import urllib.request
from datetime import date, datetime, timezone
from typing import Any

import uvicorn
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("screener")

DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./screener.db")
NIFTY500_URL: str = os.getenv(
    "NIFTY500_URL",
    "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
)

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
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "text/csv,application/csv,*/*",
            },
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
        log.warning("Could not load Nifty 500 list from NSE: %s", exc)
        fallback = [
            _normalise_ticker(t)
            for t in _FALLBACK_TICKERS.split(",")
            if _is_valid_nse_symbol(t)
        ]
        log.warning("Using fallback ticker list with %d symbols.", len(fallback))
        return fallback


TICKERS: list[str] = load_nifty500_tickers()

ALLOWED_ORIGINS: list[str] = os.getenv(
    "SCREENER_UI_ORIGIN", "http://localhost:5173,http://localhost:4173"
).split(",")

ADMIN_API_KEY: str | None = os.getenv("SCREENER_ADMIN_KEY") or None
DAILY_REFRESH_TIME: str = os.getenv("DAILY_REFRESH_TIME", "07:00")
_refresh_hour, _refresh_minute = (int(x) for x in DAILY_REFRESH_TIME.split(":"))

engine: Engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _safe(v: Any) -> Any:
    if v is None:
        return None
    try:
        import numpy as np
        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, np.floating):
            return None if np.isnan(v) else float(v)
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
        return None if (math.isnan(v) or math.isinf(v)) else v
    return v


def fetch_quotes(tickers: list[str]) -> list[dict]:
    log.info("Fetching quotes for %d tickers ...", len(tickers))
    now = datetime.now(timezone.utc).isoformat()
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

            last = df.iloc[-1]
            prev = df.iloc[-2] if len(df) >= 2 else last
            close = float(last["Close"])
            prev_close = float(prev["Close"])
            volume = _safe(last["Volume"])

            if close <= 0 or volume is None:
                continue

            change = close - prev_close
            pct = (change / prev_close * 100) if prev_close else 0.0

            rows.append({
                "symbol": ticker,
                "date": str(last.name.date()) if hasattr(last.name, "date") else str(last.name),
                "open": round(float(last["Open"]), 2),
                "high": round(float(last["High"]), 2),
                "low": round(float(last["Low"]), 2),
                "close": round(close, 2),
                "volume": int(volume),
                "prev_close": round(prev_close, 2),
                "change": round(change, 2),
                "change_pct": round(pct, 2),
                "fetched_at": now,
            })
        except Exception as exc:
            log.warning("quotes: skip %s - %s", ticker, exc)

    log.info("quotes: %d rows", len(rows))
    return rows


def fetch_info(tickers: list[str]) -> list[dict]:
    log.info("Fetching info for %d tickers ...", len(tickers))
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

    now = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []

    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).info
            row: dict[str, Any] = {"fetched_at": now}
            for k in fields:
                row[k] = _safe(info.get(k))
            if not row.get("symbol"):
                row["symbol"] = ticker
            rows.append(row)
        except Exception as exc:
            log.warning("info: skip %s - %s", ticker, exc)

    log.info("info: %d rows", len(rows))
    return rows


def _pivot_statement(ticker: str, df, label: str) -> list[dict]:
    rows: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()
    if df is None or df.empty:
        return rows

    for col in df.columns:
        row: dict[str, Any] = {
            "symbol": ticker,
            "fiscal_year": str(col.date()) if hasattr(col, "date") else str(col),
            "fetched_at": now,
        }
        for idx in df.index:
            key = re.sub(r"[^a-z0-9_]", "_", str(idx).lower().strip())
            key = re.sub(r"_+", "_", key).strip("_")
            row[key] = _safe(df.at[idx, col])
        rows.append(row)

    return rows


def fetch_financials(tickers: list[str]) -> list[dict]:
    log.info("Fetching income statements ...")
    rows: list[dict] = []
    for ticker in tickers:
        try:
            rows.extend(_pivot_statement(ticker, yf.Ticker(ticker).financials, "financials"))
        except Exception as exc:
            log.warning("financials: skip %s - %s", ticker, exc)
    log.info("financials: %d rows", len(rows))
    return rows


def fetch_balance_sheet(tickers: list[str]) -> list[dict]:
    log.info("Fetching balance sheets ...")
    rows: list[dict] = []
    for ticker in tickers:
        try:
            rows.extend(_pivot_statement(ticker, yf.Ticker(ticker).balance_sheet, "balance_sheet"))
        except Exception as exc:
            log.warning("balance_sheet: skip %s - %s", ticker, exc)
    log.info("balance_sheet: %d rows", len(rows))
    return rows


def fetch_history(tickers: list[str], period: str = "30d") -> list[dict]:
    log.info("Fetching %s history ...", period)
    now = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []

    for ticker in tickers:
        try:
            df = yf.Ticker(ticker).history(period=period, auto_adjust=True)
            if df is None or df.empty:
                continue
            for ts, r in df.iterrows():
                rows.append({
                    "symbol": ticker,
                    "date": str(ts.date()),
                    "open": _safe(r.get("Open")),
                    "high": _safe(r.get("High")),
                    "low": _safe(r.get("Low")),
                    "close": _safe(r.get("Close")),
                    "volume": _safe(r.get("Volume")),
                    "dividends": _safe(r.get("Dividends")),
                    "stock_splits": _safe(r.get("Stock Splits")),
                    "fetched_at": now,
                })
        except Exception as exc:
            log.warning("history: skip %s - %s", ticker, exc)

    log.info("history: %d rows", len(rows))
    return rows


def _infer_col_ddl(v: Any) -> str:
    if isinstance(v, bool):
        return "TEXT"
    if isinstance(v, int):
        return "INTEGER"
    if isinstance(v, float):
        return "REAL"
    return "TEXT"


def _ensure_table(conn, table: str, sample: dict) -> None:
    cols = ", ".join(f'"{k}" {_infer_col_ddl(v)}' for k, v in sample.items())
    conn.execute(text(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols})'))

    existing = {
        row[1]
        for row in conn.execute(text(f'PRAGMA table_info("{table}")')).fetchall()
    }

    for k, v in sample.items():
        if k not in existing:
            conn.execute(text(f'ALTER TABLE "{table}" ADD COLUMN "{k}" {_infer_col_ddl(v)}'))


def _upsert(table: str, rows: list[dict]) -> None:
    if not rows:
        return

    all_keys: list[str] = list(dict.fromkeys(k for r in rows for k in r))
    sample = {k: rows[0].get(k) for k in all_keys}

    with engine.begin() as conn:
        _ensure_table(conn, table, sample)
        placeholders = ", ".join(f":{k}" for k in all_keys)
        cols_sql = ", ".join(f'"{k}"' for k in all_keys)
        stmt = text(f'INSERT INTO "{table}" ({cols_sql}) VALUES ({placeholders})')
        norm = [{k: r.get(k) for k in all_keys} for r in rows]
        conn.execute(stmt, norm)

    log.info("inserted %d rows -> %s", len(rows), table)


_META_TABLE = "_screener_meta"


def _get_last_refresh_date() -> str | None:
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT value FROM {_META_TABLE} WHERE key = 'last_refresh_date'")
            ).fetchone()
            return result[0] if result else None
    except Exception:
        return None


def _set_last_refresh_date(date_str: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(
            f"CREATE TABLE IF NOT EXISTS {_META_TABLE} (key TEXT PRIMARY KEY, value TEXT)"
        ))
        conn.execute(text(
            f"INSERT OR REPLACE INTO {_META_TABLE} (key, value) VALUES ('last_refresh_date', :d)"
        ), {"d": date_str})


def _already_refreshed_today() -> bool:
    today = date.today().isoformat()
    if _get_last_refresh_date() == today:
        log.info("Already refreshed today (%s) - skipping.", today)
        return True
    return False


def refresh_quotes_only() -> None:
    log.info("=== Fast quotes refresh started ===")
    _upsert("quotes", fetch_quotes(TICKERS))
    _set_last_refresh_date(date.today().isoformat())
    log.info("=== Fast quotes refresh complete ===")


def refresh_all() -> None:
    log.info("=== Full Yahoo Finance refresh started ===")

    for name, fetcher in [
        ("quotes", lambda: fetch_quotes(TICKERS)),
        ("info", lambda: fetch_info(TICKERS)),
        ("financials", lambda: fetch_financials(TICKERS)),
        ("balance_sheet", lambda: fetch_balance_sheet(TICKERS)),
        ("history", lambda: fetch_history(TICKERS, period="30d")),
    ]:
        try:
            _upsert(name, fetcher())
        except Exception as exc:
            log.error("%s refresh failed: %s", name, exc)

    _set_last_refresh_date(date.today().isoformat())
    log.info("=== Full Yahoo Finance refresh complete ===")


_NUMERIC_SQLITE = {"INTEGER", "INT", "BIGINT", "REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL"}
_TEXT_SQLITE = {"VARCHAR", "TEXT", "CHAR", "CLOB"}
_INTERNAL_TABLES = {"_screener_meta"}


def _col_kind(t: str) -> str:
    up = t.upper().split("(")[0].strip()
    if up in _NUMERIC_SQLITE:
        return "numeric"
    if up in _TEXT_SQLITE:
        return "text"
    return "other"


def _is_filterable(name: str, kind: str) -> bool:
    return name != "fetched_at" and kind in ("numeric", "text")


def get_table_infos() -> list[dict]:
    insp = inspect(engine)
    return [
        {
            "name": t,
            "columns": [
                {
                    "name": c["name"],
                    "data_type": str(c["type"]),
                    "udt_name": None,
                    "kind": _col_kind(str(c["type"])),
                    "filterable": _is_filterable(c["name"], _col_kind(str(c["type"]))),
                }
                for c in insp.get_columns(t)
            ],
        }
        for t in sorted(insp.get_table_names())
        if t not in _INTERNAL_TABLES
    ]


def _cast(v: str, kind: str) -> Any:
    if kind == "numeric":
        try:
            return float(v)
        except ValueError:
            pass
    return v


def _build_where(col_kinds: dict[str, str], params: dict[str, str]):
    clauses: list[str] = []
    binds: dict[str, Any] = {}

    for key, raw in params.items():
        val = raw.strip()
        if not val:
            continue

        if key.startswith("eq__"):
            col, op = key[4:], "eq"
        elif key.startswith("like__"):
            col, op = key[6:], "like"
        elif key.startswith("gte__"):
            col, op = key[5:], "gte"
        elif key.startswith("lte__"):
            col, op = key[5:], "lte"
        else:
            continue

        if col not in col_kinds or not re.fullmatch(r"\w+", col):
            continue

        kind = col_kinds[col]
        bk = f"{op}_{col}"

        if op == "eq":
            clauses.append(f'"{col}" = :{bk}')
            binds[bk] = _cast(val, kind)
        elif op == "like":
            clauses.append(f'"{col}" LIKE :{bk}')
            binds[bk] = f"%{val}%"
        elif op == "gte":
            clauses.append(f'"{col}" >= :{bk}')
            binds[bk] = _cast(val, kind)
        elif op == "lte":
            clauses.append(f'"{col}" <= :{bk}')
            binds[bk] = _cast(val, kind)

    return clauses, binds


def query_rows(
    db: Session,
    table: str,
    col_kinds: dict[str, str],
    limit: int,
    offset: int,
    filter_params: dict[str, str],
) -> tuple[list[dict], int]:
    clauses, binds = _build_where(col_kinds, filter_params)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    qt = f'"{table}"'

    total: int = db.execute(text(f"SELECT COUNT(*) FROM {qt} {where}"), binds).scalar_one()
    raw = db.execute(
        text(f"SELECT * FROM {qt} {where} LIMIT :limit OFFSET :offset"),
        {**binds, "limit": limit, "offset": offset},
    )

    cols = list(raw.keys())
    return [dict(zip(cols, row)) for row in raw.fetchall()], total


def _n(v) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def _score_stock(q: dict, fi: dict) -> float:
    score = 0.0

    chg = _n(q.get("change_pct"))
    if chg is not None:
        score += max(-10.0, min(10.0, chg * 2))

    high52 = _n(fi.get("fiftyTwoWeekHigh"))
    low52 = _n(fi.get("fiftyTwoWeekLow"))
    close = _n(q.get("close"))

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
    score += {
        "strong_buy": 12.0,
        "strongbuy": 12.0,
        "buy": 9.0,
        "hold": 3.0,
        "underperform": 0.0,
        "sell": 0.0,
    }.get(rec, 3.0)

    target = _n(fi.get("targetMeanPrice"))
    if target and close and close > 0:
        upside = (target - close) / close
        score += 8.0 if upside > 0.30 else 6.0 if upside > 0.15 else 3.0 if upside > 0.05 else 1.0 if upside > 0 else 0

    ma50 = _n(fi.get("fiftyDayAverage"))
    ma200 = _n(fi.get("twoHundredDayAverage"))
    if close and ma50 and close > ma50:
        score += 5.0
    if close and ma200 and close > ma200:
        score += 5.0

    return round(score, 2)


def _signal(score: float) -> str:
    if score >= 60:
        return "Strong Buy"
    if score >= 40:
        return "Buy"
    return "Hold"


def _catalysts(q: dict, fi: dict, score: float) -> list[str]:
    cats: list[str] = []
    chg = _n(q.get("change_pct"))
    roe = _n(fi.get("returnOnEquity"))
    pe = _n(fi.get("trailingPE"))
    de = _n(fi.get("debtToEquity"))
    cr = _n(fi.get("currentRatio"))
    rec = (fi.get("recommendationKey") or "").lower()
    target = _n(fi.get("targetMeanPrice"))
    close = _n(q.get("close"))

    if chg and chg > 0:
        cats.append(f"Positive momentum +{chg:.1f}%")
    if roe and roe > 0.15:
        cats.append(f"Strong ROE {roe * 100:.1f}%")
    if pe and pe < 20:
        cats.append(f"Attractive P/E {pe:.1f}x")
    if de and de < 0.5:
        cats.append("Low leverage")
    if cr and cr > 1.5:
        cats.append("Healthy liquidity")
    if "buy" in rec:
        cats.append("Analyst buy consensus")
    if target and close and (target - close) / close > 0.10:
        cats.append(f"Analyst upside {((target - close) / close * 100):.0f}%")

    return cats[:4]


def _rationale(q: dict, fi: dict, score: float) -> str:
    close = _n(q.get("close"))
    pe = _n(fi.get("trailingPE"))
    roe = _n(fi.get("returnOnEquity"))
    chg = _n(q.get("change_pct"))
    target = _n(fi.get("targetMeanPrice"))
    rec = (fi.get("recommendationKey") or "").replace("_", " ").title()
    sector = fi.get("sector") or "Equity"

    s1 = f"{sector} play"
    if pe:
        s1 += f" trading at {pe:.1f}x P/E"
    if roe:
        s1 += f" with {roe * 100:.1f}% ROE"

    s2 = ""
    if chg is not None:
        s2 += f"Recent 1-day move of {chg:+.2f}%"
    if target and close:
        s2 += f"; analyst target implies {(target - close) / close * 100:.0f}% upside"
    if rec:
        s2 += f" ({rec})"

    return f"{s1}. {s2}." if s2 else f"{s1}."


def _price_target(q: dict, fi: dict) -> float | None:
    close = _n(q.get("close"))
    target = _n(fi.get("targetMeanPrice"))
    if not close:
        return None
    if target and target > close:
        chg = _n(q.get("change_pct")) or 0
        momentum_3m = close * (1 + (chg / 100) * 30)
        return round(target * 0.6 + momentum_3m * 0.4, 2)
    return round(close * 1.08, 2)


def compute_top5() -> dict:
    with engine.connect() as conn:
        def _rows(table: str) -> list[dict]:
            try:
                raw = conn.execute(text(f'SELECT * FROM "{table}"'))
                cols = list(raw.keys())
                return [dict(zip(cols, r)) for r in raw.fetchall()]
            except Exception:
                return []

        quotes_rows = _rows("quotes")
        info_rows = _rows("info")

    if not quotes_rows:
        raise HTTPException(503, detail="No quotes data yet. POST /api/refresh first.")

    quotes_map: dict[str, dict] = {}
    for q in quotes_rows:
        sym = q.get("symbol")
        if not sym:
            continue
        prev = quotes_map.get(sym)
        if prev is None or (q.get("date", "") or "") >= (prev.get("date", "") or ""):
            quotes_map[sym] = q

    info_map: dict[str, dict] = {r["symbol"]: r for r in info_rows if r.get("symbol")}
    scored: list[dict] = []

    for sym, q in quotes_map.items():
        fi = info_map.get(sym, {})
        raw_score = _score_stock(q, fi)
        normalised = min(100, round(raw_score / 1.3, 1))

        scored.append({
            "symbol": sym,
            "sector": fi.get("sector") or "Equity",
            "industry": fi.get("industry") or "",
            "short_name": fi.get("shortName") or sym,
            "close": _n(q.get("close")),
            "change_pct": _n(q.get("change_pct")),
            "volume": q.get("volume"),
            "pe": _n(fi.get("trailingPE")),
            "forward_pe": _n(fi.get("forwardPE")),
            "roe": _n(fi.get("returnOnEquity")),
            "roa": _n(fi.get("returnOnAssets")),
            "market_cap": _n(fi.get("marketCap")),
            "debt_equity": _n(fi.get("debtToEquity")),
            "profit_margin": _n(fi.get("profitMargins")),
            "current_ratio": _n(fi.get("currentRatio")),
            "div_yield": _n(fi.get("dividendYield")),
            "beta": _n(fi.get("beta")),
            "wk52_high": _n(fi.get("fiftyTwoWeekHigh")),
            "wk52_low": _n(fi.get("fiftyTwoWeekLow")),
            "ma50": _n(fi.get("fiftyDayAverage")),
            "ma200": _n(fi.get("twoHundredDayAverage")),
            "analyst_target": _n(fi.get("targetMeanPrice")),
            "rec_key": fi.get("recommendationKey") or "",
            "score": normalised,
            "signal": _signal(raw_score),
            "confidence": int(min(96, max(55, normalised))),
            "target": _price_target(q, fi),
            "rationale": _rationale(q, fi, raw_score),
            "catalysts": _catalysts(q, fi, raw_score),
        })

    top5 = sorted(scored, key=lambda x: x["score"], reverse=True)[:5]

    valid_changes = [s["change_pct"] for s in scored if s["change_pct"] is not None]
    avg_chg = sum(valid_changes) / max(1, len(valid_changes))

    sentiment = "Bullish" if avg_chg > 0.5 else "Cautious" if avg_chg < -0.5 else "Neutral"

    upsides = [
        (s["target"] - s["close"]) / s["close"] * 100
        for s in top5
        if s["target"] and s["close"]
    ]
    avg_upside = round(sum(upsides) / len(upsides), 1) if upsides else 0.0

    betas = [s["beta"] for s in top5 if s["beta"] is not None]
    avg_beta = sum(betas) / max(1, len(betas))
    risk = "High" if avg_beta > 1.3 else "Low" if avg_beta < 0.8 else "Medium"

    sector_count: dict[str, int] = {}
    for s in scored:
        sector_count[s["sector"]] = sector_count.get(s["sector"], 0) + 1

    top_sector = max(sector_count, key=sector_count.get) if sector_count else "Diversified"

    market_summary = {
        "sentiment": sentiment,
        "avg_upside": avg_upside,
        "risk_level": risk,
        "top_sector": top_sector,
        "nifty_outlook": {
            "Bullish": "Positive bias; momentum favours longs.",
            "Neutral": "Range-bound; select stock picking advised.",
            "Cautious": "Broader market weakness; manage risk carefully.",
        }[sentiment],
        "avg_score": round(sum(s["score"] for s in top5) / max(1, len(top5)), 1),
        "last_refresh": _get_last_refresh_date() or "never",
        "stock_universe": len(scored),
    }

    return {"picks": top5, "market_summary": market_summary}


_api_key_header = APIKeyHeader(name="X-Admin-Api-Key", auto_error=False)


def require_api_key(key: str | None = Depends(_api_key_header)) -> None:
    if ADMIN_API_KEY and key != ADMIN_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")


class ColMeta(BaseModel):
    name: str
    data_type: str = ""
    udt_name: str | None = None
    kind: str
    filterable: bool


class TableInfo(BaseModel):
    name: str
    columns: list[ColMeta]


class TablesResponse(BaseModel):
    tables: list[TableInfo]


class RowsResponse(BaseModel):
    table: str
    total: int
    limit: int
    offset: int
    rows: list[dict[str, Any]]


app = FastAPI(
    title="Indian Equity Screener",
    description="Yahoo Finance data + multi-factor Top-5 scoring engine.",
    version="3.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.get("/api/v1/browse/tables", response_model=TablesResponse)
def list_tables(_: None = Depends(require_api_key)):
    return TablesResponse(tables=[TableInfo(**t) for t in get_table_infos()])


@app.get("/api/v1/browse/tables/{table_name}/rows", response_model=RowsResponse)
def get_rows(
    table_name: str,
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
    _: None = Depends(require_api_key),
):
    insp = inspect(engine)

    if table_name not in insp.get_table_names():
        raise HTTPException(404, f"Table '{table_name}' not found.")

    col_kinds = {
        c["name"]: _col_kind(str(c["type"]))
        for c in insp.get_columns(table_name)
    }

    fp = {
        k: v
        for k, v in request.query_params.items()
        if k.startswith(("eq__", "like__", "gte__", "lte__"))
    }

    rows, total = query_rows(db, table_name, col_kinds, limit, offset, fp)
    return RowsResponse(table=table_name, total=total, limit=limit, offset=offset, rows=rows)


@app.get("/api/v1/insights/top5")
def top5_insights(_: None = Depends(require_api_key)):
    return compute_top5()


@app.post("/api/refresh")
def manual_refresh(
    full: bool = Query(default=False),
    _: None = Depends(require_api_key),
):
    if full:
        refresh_all()
        mode = "full"
    else:
        refresh_quotes_only()
        mode = "quotes_only"

    return {
        "status": "ok",
        "mode": mode,
        "ticker_count": len(TICKERS),
        "tickers": TICKERS,
    }


@app.get("/api/tickers")
def list_tickers():
    return {"ticker_count": len(TICKERS), "tickers": TICKERS}


@app.get("/api/health")
def health():
    insp = inspect(engine)
    return {
        "status": "ok",
        "tables": insp.get_table_names(),
        "ticker_count": len(TICKERS),
        "sample_tickers": TICKERS[:10],
        "last_refresh_date": _get_last_refresh_date(),
        "daily_refresh_time_ist": DAILY_REFRESH_TIME,
        "nifty500_url": NIFTY500_URL,
        "startup_refresh_mode": "quotes_only",
    }


@app.on_event("startup")
def on_startup():
    if not _already_refreshed_today():
        log.info("Startup: fetching only quotes for fast startup ...")
        refresh_quotes_only()
    else:
        log.info("Startup: today's data already present - skipping fetch.")

    import pytz

    ist = pytz.timezone("Asia/Kolkata")
    scheduler = BackgroundScheduler(timezone=ist)
    scheduler.add_job(
        refresh_quotes_only,
        trigger="cron",
        hour=_refresh_hour,
        minute=_refresh_minute,
        id="daily_quotes_refresh",
        replace_existing=True,
    )
    scheduler.start()

    log.info("Daily fast quotes refresh scheduled at %s IST.", DAILY_REFRESH_TIME)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)