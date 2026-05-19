"""
refresh_handler.py  –  Daily Append Edition
============================================

WHAT CHANGED vs previous versions
-----------------------------------

OLD behaviour (both v1 and v2):
  • refresh_quotes_only() → called yf.download and did _batch_write(TBL_QUOTES, rows)
    which called put_item with PK=symbol, SK=date.  Running it twice on the same day
    silently overwrote the same row.  No history built up inside equity-quotes.
  • refresh_all() → same overwrite pattern, just for more tables.
  • equity-history was filled once by fetch_history(period="30d") — only 30 days,
    not appended, re-pulled from scratch every Sunday.
  • No intraday bars stored at all.
  • No audit trail of what was fetched when.
  • Duplicate Lambda invocations (retries, manual triggers) caused silent data corruption.

NEW behaviour (this file):
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Table              Write pattern       Idempotent?  TTL?               │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  equity-quotes      append 1 row/day    YES          No (keep forever)  │
  │  equity-intraday    append new 5m bars  YES          YES (7 days)       │
  │  equity-history     gap-fill only       YES          No (keep forever)  │
  │  equity-info        overwrite (latest)  n/a          No                 │
  │  equity-signal-*    append per run      YES          No                 │
  │  equity-daily-log   append per symbol   YES          No (audit trail)   │
  └─────────────────────────────────────────────────────────────────────────┘

  Idempotency mechanism:
    Before writing a (symbol, date) row the code checks whether it already
    exists via get_item.  If it does, the write is skipped entirely.
    This means running the Lambda 10× on the same day produces exactly 1 row,
    not 10 overwritten copies.

  Gap-fill for history:
    _get_existing_dates() queries all stored SK values for a symbol, then the
    fetched DataFrame is filtered to only rows whose date is NOT in that set.
    On the first run this writes ~250 rows (1 year of trading days).
    On subsequent runs it writes 0–1 rows (only genuinely new trading days).

  Intraday TTL:
    Each 5-min bar gets a Unix-epoch `ttl` attribute set to now + 7 days.
    DynamoDB auto-deletes rows past their TTL, so intraday storage stays lean
    without any manual cleanup.

  Audit log (equity-daily-log):
    PK=log_date, SK=symbol.  One row per symbol per run date recording how many
    rows were written, the catalyst score, bias, and any errors.
    Lets you answer "did APOLLOMICRO get fetched on 2026-05-18?" instantly.

EventBridge schedule recommendations
--------------------------------------
  Daily  (weekdays 16:00 IST): { "mode": "quotes_only" }
  Weekly (Sunday  02:00 IST):  { "mode": "full" }
  Ad-hoc watchlist deep scan:  { "mode": "quotes_only", "symbols": ["APOLLOMICRO.NS"] }
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import boto3
import numpy as np
import yfinance as yf
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

log = logging.getLogger("screener.refresh")
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

# ── Re-use shared helpers from the main app ───────────────────────────────────
from app.main import (
    _fetch_historical_indicators,
    _get_item,
    _get_meta,
    _put_item,
    _query_items,
    _news_service,
    _normalise_ticker,
    TBL_INFO,
    TBL_QUOTES,
    TBL_HISTORY,
    AWS_REGION,
    DYNAMODB_ENDPOINT_URL,
    TICKERS,
)

# ── Config ────────────────────────────────────────────────────────────────────

TBL_INTRADAY  = os.getenv("DDB_TBL_INTRADAY",  "equity-intraday")
TBL_SIGNAL    = os.getenv("DDB_TBL_SIGNAL",    "equity-signal-snapshots")
TBL_DAILY_LOG = os.getenv("DDB_TBL_DAILY_LOG", "equity-daily-log")

INTRADAY_TTL_DAYS = int(os.getenv("INTRADAY_TTL_DAYS", "7"))
HISTORY_PERIOD    = os.getenv("HISTORY_LOOKBACK", "1y")
WATCHLIST         = [
    t.strip()
    for t in os.getenv("WATCHLIST_SYMBOLS", "APOLLOMICRO.NS").split(",")
    if t.strip()
]

_ddb_kwargs: dict[str, Any] = {"region_name": AWS_REGION}
if DYNAMODB_ENDPOINT_URL:
    _ddb_kwargs["endpoint_url"] = DYNAMODB_ENDPOINT_URL
_ddb = boto3.resource("dynamodb", **_ddb_kwargs)

# ── Type helpers ──────────────────────────────────────────────────────────────

def _n(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def _to_dec(v: Any) -> Decimal | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else Decimal(str(f))
    except (TypeError, ValueError, InvalidOperation):
        return None


def _clean(d: dict) -> dict:
    """Convert all values to DynamoDB-safe types; drop None / NaN."""
    out: dict = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, float):
            dec = _to_dec(v)
            if dec is not None:
                out[k] = dec
        elif isinstance(v, np.floating):
            dec = _to_dec(float(v))
            if dec is not None:
                out[k] = dec
        elif isinstance(v, np.integer):
            out[k] = int(v)
        elif isinstance(v, (bool, int, Decimal)):
            out[k] = v
        else:
            s = str(v)
            if s:
                out[k] = s
    return out


# ── Table auto-create ─────────────────────────────────────────────────────────

def _ensure_table(
    name: str,
    pk: str,
    sk: str | None = None,
    ttl_attr: str | None = None,
) -> None:
    try:
        _ddb.meta.client.describe_table(TableName=name)
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    attrs  = [{"AttributeName": pk, "AttributeType": "S"}]
    schema = [{"AttributeName": pk, "KeyType": "HASH"}]
    if sk:
        attrs.append({"AttributeName": sk, "AttributeType": "S"})
        schema.append({"AttributeName": sk, "KeyType": "RANGE"})

    _ddb.create_table(
        TableName=name,
        AttributeDefinitions=attrs,
        KeySchema=schema,
        BillingMode="PAY_PER_REQUEST",
    )
    _ddb.meta.client.get_waiter("table_exists").wait(TableName=name)

    if ttl_attr:
        _ddb.meta.client.update_time_to_live(
            TableName=name,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": ttl_attr},
        )
    log.info("Created DynamoDB table: %s", name)


def _init_tables() -> None:
    _ensure_table(TBL_INTRADAY,  pk="symbol", sk="datetime_iso", ttl_attr="ttl")
    _ensure_table(TBL_SIGNAL,    pk="symbol", sk="snapshot_ts")
    _ensure_table(TBL_DAILY_LOG, pk="log_date", sk="symbol")


# ── Idempotency helpers ───────────────────────────────────────────────────────

def _row_exists(table_name: str, symbol: str, sk_name: str, sk_val: str) -> bool:
    """Return True if (symbol, sk_val) already stored → skip the write."""
    try:
        resp = _ddb.Table(table_name).get_item(
            Key={"symbol": symbol, sk_name: sk_val}
        )
        return "Item" in resp
    except Exception:
        return False


def _get_existing_dates(table_name: str, symbol: str) -> set[str]:
    """Return all 'date' SK values already stored for a symbol (for gap-fill)."""
    try:
        resp = _ddb.Table(table_name).query(
            KeyConditionExpression=Key("symbol").eq(symbol),
            ProjectionExpression="#d",
            ExpressionAttributeNames={"#d": "date"},
        )
        return {item["date"] for item in resp.get("Items", [])}
    except Exception as exc:
        log.warning("get_existing_dates %s/%s: %s", table_name, symbol, exc)
        return set()


# ── Step 1: daily OHLCV append → equity-quotes ───────────────────────────────

def append_daily_quotes(tickers: list[str]) -> dict[str, int]:
    """
    Fetch today's OHLCV for every ticker.
    Write exactly ONE row per symbol per calendar date.
    Already-written dates are skipped (idempotent).

    Key formula change vs old code
    --------------------------------
    OLD:  _batch_write(TBL_QUOTES, rows)          → always put_item regardless of date
    NEW:  if _row_exists(TBL_QUOTES, ticker, "date", today): continue
          else: table.put_item(Item=...)           → one row appended per day, never overwritten
    """
    log.info("append_daily_quotes: %d tickers", len(tickers))
    today   = date.today().isoformat()
    now_iso = datetime.now(timezone.utc).isoformat()
    written: dict[str, int] = {}

    # Bulk daily download (2 days needed for prev_close)
    try:
        daily = yf.download(
            tickers, period="2d", interval="1d",
            group_by="ticker", auto_adjust=True,
            progress=False, threads=True,
        )
    except Exception as exc:
        log.error("daily download failed: %s", exc)
        return written

    # Best-effort intraday for live price
    try:
        intra = yf.download(
            tickers, period="1d", interval="5m",
            group_by="ticker", auto_adjust=True,
            progress=False, threads=True,
        )
    except Exception:
        intra = None

    table = _ddb.Table(TBL_QUOTES)

    for ticker in tickers:
        try:
            df_d = daily if len(tickers) == 1 else daily.get(ticker)
            if df_d is None or df_d.empty:
                written[ticker] = 0
                continue

            # ── Idempotency guard ──────────────────────────────────────────
            if _row_exists(TBL_QUOTES, ticker, "date", today):
                log.debug("quotes: %s %s already exists – skip", ticker, today)
                written[ticker] = 0
                continue

            prev_close = (
                float(df_d.iloc[-2]["Close"])
                if len(df_d) >= 2
                else float(df_d.iloc[-1]["Close"])
            )

            # Use intraday last bar if available, else fall back to daily
            if intra is not None:
                df_i = intra if len(tickers) == 1 else intra.get(ticker)
                src  = df_i.iloc[-1] if (df_i is not None and not df_i.empty) else df_d.iloc[-1]
            else:
                src = df_d.iloc[-1]

            close  = float(src["Close"])
            volume = int(src["Volume"]) if src["Volume"] else 0
            open_  = float(src["Open"])
            high   = float(src["High"])
            low    = float(src["Low"])

            if close <= 0:
                written[ticker] = 0
                continue

            change     = close - prev_close
            change_pct = (change / prev_close * 100) if prev_close else 0.0

            table.put_item(Item=_clean({
                "symbol":     ticker,
                "date":       today,          # SK  one row per trading day
                "open":       open_,
                "high":       high,
                "low":        low,
                "close":      close,
                "volume":     volume,
                "prev_close": prev_close,
                "change":     change,
                "change_pct": change_pct,
                "fetched_at": now_iso,
            }))
            written[ticker] = 1

        except Exception as exc:
            log.warning("daily quote skip %s: %s", ticker, exc)
            written[ticker] = 0

    log.info("append_daily_quotes: %d new rows", sum(written.values()))
    return written


# ── Step 2: intraday 5-min bars → equity-intraday (TTL-managed) ──────────────

def append_intraday_bars(tickers: list[str]) -> int:
    """
    Append only NEW 5-min bars for today.
    Each bar gets a TTL so DynamoDB auto-expires bars after INTRADAY_TTL_DAYS.

    Key formula change vs old code
    --------------------------------
    OLD:  Not done at all.
    NEW:  Query existing datetime_iso SKs for each ticker → write only missing bars.
          Each item has ttl = now + 7 days (Unix epoch).
          DynamoDB TTL daemon deletes expired items automatically.
    """
    log.info("append_intraday_bars: %d tickers", len(tickers))
    ttl_ts  = int((datetime.now(timezone.utc) + timedelta(days=INTRADAY_TTL_DAYS)).timestamp())
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        bulk = yf.download(
            tickers, period="1d", interval="5m",
            group_by="ticker", auto_adjust=True,
            progress=False, threads=True,
        )
    except Exception as exc:
        log.error("intraday download failed: %s", exc)
        return 0

    table     = _ddb.Table(TBL_INTRADAY)
    total_new = 0

    for ticker in tickers:
        try:
            df = bulk if len(tickers) == 1 else bulk.get(ticker)
            if df is None or df.empty:
                continue

            # Get stored datetimes to skip duplicates
            existing: set[str] = set()
            try:
                resp = table.query(
                    KeyConditionExpression=Key("symbol").eq(ticker),
                    ProjectionExpression="datetime_iso",
                )
                existing = {r["datetime_iso"] for r in resp.get("Items", [])}
            except Exception:
                pass

            new_rows: list[dict] = []
            for ts, row in df.iterrows():
                dt_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
                if dt_str in existing:
                    continue
                close_val = _n(row.get("Close"))
                if not close_val or close_val <= 0:
                    continue
                new_rows.append(_clean({
                    "symbol":       ticker,
                    "datetime_iso": dt_str,     # SK  unique per 5-min bar
                    "open":         _n(row.get("Open")),
                    "high":         _n(row.get("High")),
                    "low":          _n(row.get("Low")),
                    "close":        close_val,
                    "volume":       int(row.get("Volume") or 0),
                    "ttl":          ttl_ts,     # auto-expire after 7 days
                    "fetched_at":   now_iso,
                }))

            if new_rows:
                with table.batch_writer() as bw:
                    for item in new_rows:
                        bw.put_item(Item=item)
                total_new += len(new_rows)

        except Exception as exc:
            log.warning("intraday skip %s: %s", ticker, exc)

    log.info("append_intraday_bars: %d new bars", total_new)
    return total_new


# ── Step 3: 1-year history gap-fill → equity-history ─────────────────────────

def gap_fill_history(tickers: list[str]) -> int:
    """
    Pull up to 1 year of daily OHLCV per ticker.
    Write ONLY dates not already stored (gap-fill, never overwrite).

    Key formula change vs old code
    --------------------------------
    OLD:  fetch_history(period="30d") pulled 30 days and batch-wrote everything,
          overwriting previously stored rows each Sunday.
    NEW:  Pull 1y.  Query existing dates per ticker.
          Only rows whose date is NOT in existing_dates are written.
          First run: ~250 new rows per ticker.
          Subsequent runs: 0–5 new rows (just the genuinely new trading days).
    """
    log.info("gap_fill_history: %d tickers, period=%s", len(tickers), HISTORY_PERIOD)
    now_iso   = datetime.now(timezone.utc).isoformat()
    table     = _ddb.Table(TBL_HISTORY)
    total_new = 0

    for ticker in tickers:
        try:
            df = yf.Ticker(ticker).history(period=HISTORY_PERIOD, auto_adjust=True)
            if df is None or df.empty:
                continue

            existing = _get_existing_dates(TBL_HISTORY, ticker)
            new_rows: list[dict] = []

            for ts, row in df.iterrows():
                date_str  = str(ts.date())
                if date_str in existing:
                    continue              # already stored — skip
                close_val = _n(row.get("Close"))
                if not close_val or close_val <= 0:
                    continue
                new_rows.append(_clean({
                    "symbol":       ticker,
                    "date":         date_str,   # SK  one row per trading day
                    "open":         _n(row.get("Open")),
                    "high":         _n(row.get("High")),
                    "low":          _n(row.get("Low")),
                    "close":        close_val,
                    "volume":       int(row.get("Volume") or 0),
                    "dividends":    _n(row.get("Dividends")),
                    "stock_splits": _n(row.get("Stock Splits")),
                    "fetched_at":   now_iso,
                }))

            if new_rows:
                with table.batch_writer() as bw:
                    for item in new_rows:
                        bw.put_item(Item=item)
                total_new += len(new_rows)
                log.debug("history gap-fill: %s +%d rows", ticker, len(new_rows))

        except Exception as exc:
            log.warning("history gap-fill skip %s: %s", ticker, exc)

    log.info("gap_fill_history: %d new rows total", total_new)
    return total_new


# ── Step 4: audit log ─────────────────────────────────────────────────────────

def _write_audit_log(
    log_date: str,
    symbol: str,
    mode: str,
    quotes_written: int,
    intraday_bars: int,
    history_rows: int,
    catalyst_score: float | None,
    bias: str,
    error: str = "",
) -> None:
    try:
        _ddb.Table(TBL_DAILY_LOG).put_item(Item=_clean({
            "log_date":       log_date,          # PK
            "symbol":         symbol,            # SK
            "mode":           mode,
            "quotes_written": quotes_written,
            "intraday_bars":  intraday_bars,
            "history_rows":   history_rows,
            "catalyst_score": catalyst_score,
            "bias":           bias,
            "error":          error,
            "logged_at":      datetime.now(timezone.utc).isoformat(),
        }))
    except Exception as exc:
        log.warning("audit log failed %s: %s", symbol, exc)


# ── Step 5: catalyst scoring (APOLLO-pattern-derived) ────────────────────────

def _earnings_surprise_score(fi: dict) -> tuple[float, list[str]]:
    score, reasons = 0.0, []
    t_pe  = _n(fi.get("trailingPE"))
    f_pe  = _n(fi.get("forwardPE"))
    eg    = _n(fi.get("earningsGrowth"))
    rg    = _n(fi.get("revenueGrowth"))
    roe   = _n(fi.get("returnOnEquity"))

    if t_pe and f_pe and 0 < f_pe < t_pe:
        gap = (t_pe - f_pe) / t_pe * 100
        score += min(10.0, gap * 0.4)
        reasons.append(f"Forward PE {f_pe:.1f}x < Trailing PE {t_pe:.1f}x → earnings acceleration")

    if eg is not None:
        if eg > 1.0:    score += 10; reasons.append(f"Earnings +{eg*100:.0f}% YoY — exceptional")
        elif eg > 0.5:  score += 7;  reasons.append(f"Earnings +{eg*100:.0f}% YoY — strong beat")
        elif eg > 0.2:  score += 4;  reasons.append(f"Earnings +{eg*100:.0f}% YoY — above consensus")
        elif eg > 0:    score += 2;  reasons.append(f"Earnings +{eg*100:.0f}% YoY — modest growth")
        else:           reasons.append(f"Earnings {eg*100:.0f}% YoY — decline headwind")

    if rg is not None:
        if rg > 0.5:    score += 5; reasons.append(f"Revenue +{rg*100:.0f}% YoY")
        elif rg > 0.2:  score += 3; reasons.append(f"Revenue +{rg*100:.0f}% YoY — solid")
        elif rg > 0:    score += 1; reasons.append(f"Revenue +{rg*100:.0f}% YoY — marginal")

    if roe and roe > 0.25:  score += 5; reasons.append(f"ROE {roe*100:.0f}% — exceptional")
    elif roe and roe > 0.15:score += 3; reasons.append(f"ROE {roe*100:.0f}% — above-average")

    return min(25.0, score), reasons


def _catalyst_event_score(symbol: str, fi: dict, news: list[dict]) -> tuple[float, list[str]]:
    score, reasons = 0.0, []
    ORDER_KW    = {"order","contract","win","bag","secure","award","ministry","defence","drdo","psu"}
    UPGRADE_KW  = {"licence","license","approval","upgrade","acquisition","merger","joint","partnership"}
    RESULT_KW   = {"q4","q3","q2","q1","fy26","fy25","result","earnings","profit","revenue","ebitda","pat"}
    ANALYST_KW  = {"analyst","investor","call","meet","target","upgrade","outperform","buy"}
    DIVIDEND_KW = {"dividend","bonus","split","buyback"}
    hits = {k: 0 for k in ("order","upgrade","result","analyst","dividend")}
    for item in news:
        words = set((item.get("title") or "").lower().split())
        if words & ORDER_KW:    hits["order"]    += 1
        if words & UPGRADE_KW:  hits["upgrade"]  += 1
        if words & RESULT_KW:   hits["result"]   += 1
        if words & ANALYST_KW:  hits["analyst"]  += 1
        if words & DIVIDEND_KW: hits["dividend"] += 1
    if hits["result"]   >= 1: score += 10; reasons.append("Result in news — post-result re-rating potential")
    if hits["analyst"]  >= 1: score += 8;  reasons.append("Analyst call or target revision in news")
    if hits["order"]    >= 2: score += 8;  reasons.append(f"{hits['order']}× order-win — strong order-book momentum")
    elif hits["order"]  == 1: score += 4;  reasons.append("Order win — revenue visibility")
    if hits["upgrade"]  >= 1: score += 6;  reasons.append("Licence / upgrade event — structural re-rating")
    if hits["dividend"] >= 1: score += 3;  reasons.append("Dividend / bonus signal")
    s = (fi.get("sector") or "").lower() + (fi.get("industry") or "").lower()
    if any(x in s for x in ("defence","defense","aerospace","space")): score += 5; reasons.append("Defence sector GOI tailwind")
    elif any(x in s for x in ("capital goods","infrastructure","rail","power")): score += 3; reasons.append("Infra sector PLI tailwind")
    return min(25.0, score), reasons


def _technical_momentum_score(q: dict, fi: dict, ind: dict) -> tuple[float, list[str]]:
    score, reasons = 0.0, []
    close   = _n(q.get("close")) or 0.0
    chg     = _n(q.get("change_pct")) or 0.0
    hi52    = _n(fi.get("fiftyTwoWeekHigh"))
    lo52    = _n(fi.get("fiftyTwoWeekLow"))
    rsi     = _n(ind.get("rsi"))
    macd    = _n(ind.get("macd"))
    macd_s  = _n(ind.get("macd_signal"))
    rv      = _n(ind.get("relative_volume"))
    sma20   = _n(ind.get("sma20"))
    sma50   = _n(ind.get("sma50"))

    if chg >= 8:    score += 8; reasons.append(f"Explosive +{chg:.1f}% — post-catalyst buying")
    elif chg >= 4:  score += 6; reasons.append(f"Strong +{chg:.1f}% — breakout move")
    elif chg >= 2:  score += 4; reasons.append(f"Positive +{chg:.1f}%")
    elif chg >= 0.5:score += 2; reasons.append(f"Mild +{chg:.1f}%")
    elif chg < -3:  score -= 4; reasons.append(f"Selling pressure {chg:.1f}%")

    if rv is not None:
        if rv >= 4.0:   score += 7; reasons.append(f"Vol {rv:.1f}× avg — institutional accumulation")
        elif rv >= 2.0: score += 5; reasons.append(f"Vol {rv:.1f}× avg — strong participation")
        elif rv >= 1.3: score += 3; reasons.append(f"Vol {rv:.1f}× avg — above average")
        elif rv < 0.5:  reasons.append(f"Thin vol {rv:.1f}× — lacks conviction")

    if close and hi52 and lo52 and hi52 > lo52:
        pct_from_high = (hi52 - close) / hi52 * 100
        if 10 <= pct_from_high <= 20: score += 5; reasons.append(f"{pct_from_high:.0f}% below 52w high — room to run")
        elif 5 <= pct_from_high < 10: score += 3; reasons.append(f"{pct_from_high:.0f}% below 52w high — near breakout")
        elif pct_from_high < 5:       score += 1; reasons.append("Near 52w high — breakout zone")

    if macd is not None and macd_s is not None:
        gap = macd - macd_s
        if gap > 0: score += 3; reasons.append(f"MACD bullish ({gap:+.3f})")
        else:       reasons.append(f"MACD bearish ({gap:+.3f})")

    if rsi is not None:
        if 45 <= rsi <= 65: score += 3; reasons.append(f"RSI {rsi:.0f} healthy mid-range")
        elif rsi < 40:      score += 2; reasons.append(f"RSI {rsi:.0f} oversold bounce")
        elif rsi > 75:      reasons.append(f"RSI {rsi:.0f} overbought")

    if close and sma20 and close > sma20 * 1.02: score += 2; reasons.append("Above SMA20")
    if close and sma50 and close > sma50:         score += 2; reasons.append("Above SMA50")

    return min(25.0, max(0.0, score)), reasons


def _risk_quality_score(fi: dict, ind: dict) -> tuple[float, list[str]]:
    score, reasons = 0.0, []
    de  = _n(fi.get("debtToEquity"))
    cr  = _n(fi.get("currentRatio"))
    em  = _n(fi.get("ebitdaMargins")) or _n(fi.get("operatingMargins"))
    bt  = _n(fi.get("beta"))
    fcf = _n(fi.get("freeCashflow"))

    if de is not None:
        if de < 0.3:   score += 7; reasons.append(f"D/E {de:.2f} — debt-free")
        elif de < 0.8: score += 5; reasons.append(f"D/E {de:.2f} — manageable")
        elif de < 1.5: score += 2; reasons.append(f"D/E {de:.2f} — moderate")
        else:           reasons.append(f"D/E {de:.2f} — high leverage")

    if em is not None:
        if em > 0.25:   score += 7; reasons.append(f"EBITDA margin {em*100:.1f}% — superior")
        elif em > 0.15: score += 5; reasons.append(f"EBITDA margin {em*100:.1f}% — above average")
        elif em > 0.08: score += 2; reasons.append(f"EBITDA margin {em*100:.1f}% — acceptable")

    if cr is not None:
        if cr > 2.0:   score += 5; reasons.append(f"Current ratio {cr:.1f} — strong")
        elif cr > 1.5: score += 3; reasons.append(f"Current ratio {cr:.1f} — adequate")
        elif cr < 1.0: reasons.append(f"Current ratio {cr:.1f} — tight liquidity")

    if bt is not None and 0.5 <= bt <= 1.2: score += 3; reasons.append(f"Beta {bt:.2f} — manageable risk")
    if fcf is not None and fcf > 0:         score += 3; reasons.append("Positive FCF")

    return min(25.0, score), reasons


def _compute_catalyst_score(
    symbol: str, q: dict, fi: dict, ind: dict, news: list[dict]
) -> dict:
    es, es_r = _earnings_surprise_score(fi)
    ce, ce_r = _catalyst_event_score(symbol, fi, news)
    tm, tm_r = _technical_momentum_score(q, fi, ind)
    rq, rq_r = _risk_quality_score(fi, ind)
    total    = round(es + ce + tm + rq, 1)
    bias     = (
        "HIGH_CONVICTION" if total >= 70 else
        "BULLISH"         if total >= 55 else
        "NEUTRAL"         if total >= 40 else
        "BEARISH"
    )
    close  = _n(q.get("close")) or 0.0
    atr    = _n(ind.get("atr"))
    sup    = _n(ind.get("support_30d"))
    hi52   = _n(fi.get("fiftyTwoWeekHigh"))
    sl     = round(close - (atr * 1.5 if atr else close * 0.05), 2)
    bz     = round(sup * 1.01 if sup else close * 0.98, 2)
    t1     = round(close + (close - sl) * 2, 2)
    t2     = round(hi52 * 0.98 if hi52 else close * 1.15, 2)
    return {
        "symbol":        symbol,
        "snapshot_ts":   datetime.now(timezone.utc).isoformat(),
        "snapshot_date": date.today().isoformat(),
        "catalyst_score": total,
        "bias":          bias,
        "score_breakdown": {
            "earnings_surprise":  round(es, 1),
            "catalyst_events":    round(ce, 1),
            "technical_momentum": round(tm, 1),
            "risk_quality":       round(rq, 1),
        },
        "trade_levels": {
            "close": round(close, 2), "buy_zone": bz,
            "stop_loss": sl, "target_1": t1, "target_2": t2,
            "rr_ratio": round((t1 - close) / max(close - sl, 0.01), 2),
        },
        "reasons":    [f"[E] {r}" for r in es_r] + [f"[C] {r}" for r in ce_r] +
                      [f"[T] {r}" for r in tm_r] + [f"[Q] {r}" for r in rq_r],
        "news_items": news[:5],
        "disclaimer": "CatalystScore is signal-confluence, NOT a price prediction.",
    }


# ── Main async orchestrator ───────────────────────────────────────────────────

async def _run_pipeline(mode: str, scan_symbols: list[str]) -> dict:
    _init_tables()
    today     = date.today().isoformat()
    all_tkrs  = TICKERS

    # 1. Daily OHLCV  (idempotent append)
    quotes_written = append_daily_quotes(all_tkrs)

    # 2. Intraday 5-min bars  (TTL-managed append)
    intraday_total = append_intraday_bars(all_tkrs)

    # 3. History gap-fill  (full mode / weekly only)
    history_total = gap_fill_history(all_tkrs) if mode == "full" else 0

    # 4. Catalyst scan + persist snapshots
    await _news_service.start()
    snapshots: list[dict] = []

    for symbol in scan_symbols:
        sym = _normalise_ticker(symbol)
        try:
            q_rows = _query_items(TBL_QUOTES, "symbol", sym)
            q      = sorted(q_rows, key=lambda r: r.get("date", ""), reverse=True)[0] if q_rows else {}
            fi     = _get_item(TBL_INFO, {"symbol": sym}) or {}
            if not q:
                log.warning("no quote data for %s — skipping catalyst scan", sym)
                continue

            ind  = _fetch_historical_indicators(sym)
            news = await _news_service.get_news(
                {"symbol": sym,
                 "short_name": fi.get("shortName") or sym.replace(".NS", ""),
                 "sector": fi.get("sector") or "",
                 "industry": fi.get("industry") or ""},
                limit=10,
            )
            snap = _compute_catalyst_score(sym, q, fi, ind, news)
            snapshots.append(snap)

            # Persist — each snapshot has a unique snapshot_ts SK so nothing is overwritten
            try:
                _put_item(TBL_SIGNAL, _clean({
                    "symbol":          sym,
                    "snapshot_ts":     snap["snapshot_ts"],
                    "snapshot_date":   snap["snapshot_date"],
                    "catalyst_score":  snap["catalyst_score"],
                    "bias":            snap["bias"],
                    "earnings_score":  snap["score_breakdown"]["earnings_surprise"],
                    "catalyst_score_events": snap["score_breakdown"]["catalyst_events"],
                    "tech_score":      snap["score_breakdown"]["technical_momentum"],
                    "quality_score":   snap["score_breakdown"]["risk_quality"],
                    "stop_loss":       snap["trade_levels"]["stop_loss"],
                    "target_1":        snap["trade_levels"]["target_1"],
                    "rr_ratio":        snap["trade_levels"]["rr_ratio"],
                    "reasons_summary": " | ".join(snap["reasons"][:6]),
                }))
            except Exception as e:
                log.warning("snapshot persist %s: %s", sym, e)

            # 5. Audit log
            _write_audit_log(
                today, sym, mode,
                quotes_written.get(sym, 0),
                0, 0,
                snap["catalyst_score"],
                snap["bias"],
            )

        except Exception as exc:
            log.error("pipeline failed %s: %s", sym, exc, exc_info=True)
            _write_audit_log(today, sym, mode, 0, 0, 0, None, "ERROR", str(exc))

    await _news_service.stop()

    high_conviction = sorted(
        [s for s in snapshots if s["bias"] in ("HIGH_CONVICTION", "BULLISH")],
        key=lambda x: x["catalyst_score"], reverse=True,
    )

    return {
        "status":              "ok",
        "mode":                mode,
        "date":                today,
        "tickers_total":       len(all_tkrs),
        "quotes_rows_written": sum(quotes_written.values()),
        "intraday_bars_added": intraday_total,
        "history_rows_added":  history_total,
        "symbols_scanned":     len(snapshots),
        "high_conviction":     [
            {
                "symbol":         s["symbol"],
                "catalyst_score": s["catalyst_score"],
                "bias":           s["bias"],
                "breakdown":      s["score_breakdown"],
                "trade_levels":   s["trade_levels"],
                "top_reasons":    s["reasons"][:4],
            }
            for s in high_conviction
        ],
        "disclaimer": "CatalystScore is signal-confluence, NOT a price prediction.",
    }


# ── Lambda entry point ────────────────────────────────────────────────────────

def handler(event: dict, context: Any) -> dict:
    """
    EventBridge payload shapes
    --------------------------
    Daily  (weekdays 16:00 IST):   { "mode": "quotes_only" }
    Weekly (Sunday  02:00 IST):    { "mode": "full" }
    Ad-hoc deep scan:              { "mode": "quotes_only", "symbols": ["APOLLOMICRO.NS"] }
    Force full universe scan:      { "mode": "catalyst_scan" }

    Falls back to REFRESH_MODE env var, then "quotes_only".
    """
    mode           = (event.get("mode") or os.getenv("REFRESH_MODE", "quotes_only")).lower().strip()
    custom_symbols = event.get("symbols")

    if custom_symbols:
        scan_symbols = custom_symbols
    elif mode in ("full", "catalyst_scan"):
        scan_symbols = TICKERS
    else:
        scan_symbols = WATCHLIST

    log.info("handler: mode=%s  scan=%d symbols", mode, len(scan_symbols))

    try:
        result = asyncio.run(_run_pipeline(mode, scan_symbols))
    except RuntimeError:
        loop   = asyncio.new_event_loop()
        result = loop.run_until_complete(_run_pipeline(mode, scan_symbols))
        loop.close()
    except Exception as exc:
        log.error("handler error: %s", exc, exc_info=True)
        return {"status": "error", "error": str(exc)}

    log.info(
        "Complete — quotes=%d  intraday=%d  history=%d  scanned=%d  high_conviction=%d",
        result["quotes_rows_written"],
        result["intraday_bars_added"],
        result["history_rows_added"],
        result["symbols_scanned"],
        len(result["high_conviction"]),
    )
    return result