# ---------------------------------------------------------------------------
# Yahoo Finance Fetchers with Technical Indicators
# ---------------------------------------------------------------------------

import yfinance as yf
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timezone, timedelta
import logging
import numpy as np

log = logging.getLogger("screener")

log = logging.getLogger("screener")

def fetch_quotes(tickers: list[str]) -> list[dict]:
    log.info("Fetching quotes for %d tickers …", len(tickers))
    now  = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []

    # Fetch daily data for prev_close (adjusted and unadjusted)
    try:
        daily_data = yf.download(
            tickers,
            period="2d",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        daily_data_unadj = yf.download(
            tickers,
            period="2d",
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
        )
    except Exception as exc:
        log.error("yf.download daily failed: %s", exc)
        return rows

    # Fetch intraday data for live price (adjusted and unadjusted)
    try:
        intraday_data = yf.download(
            tickers,
            period="1d",
            interval="5m",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        intraday_data_unadj = yf.download(
            tickers,
            period="1d",
            interval="5m",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
        )
    except Exception as exc:
        log.error("yf.download intraday failed: %s", exc)
        intraday_data = None
        intraday_data_unadj = None

    for ticker in tickers:
        try:
            daily_df = daily_data if len(tickers) == 1 else daily_data.get(ticker)
            if daily_df is None or daily_df.empty:
                continue
            prev_close = float(daily_df.iloc[-2]["Close"]) if len(daily_df) >= 2 else float(daily_df.iloc[-1]["Close"])
            # compute prev_close unadjusted when available
            prev_close_unadj = None
            if daily_data_unadj is not None:
                daily_unadj = daily_data_unadj if len(tickers) == 1 else daily_data_unadj.get(ticker)
                if daily_unadj is not None and not daily_unadj.empty:
                    prev_close_unadj = float(daily_unadj.iloc[-2]["Close"]) if len(daily_unadj) >= 2 else float(daily_unadj.iloc[-1]["Close"])

            # Use intraday for live close if available
            if intraday_data is not None:
                intra_df = intraday_data if len(tickers) == 1 else intraday_data.get(ticker)
                if intra_df is not None and not intra_df.empty:
                    last = intra_df.iloc[-1]
                    # adjusted close (existing behaviour)
                    close = float(last["Close"])
                    volume = last["Volume"]
                    open_ = float(last["Open"])
                    high = float(last["High"])
                    low = float(last["Low"])
                    # try to get unadjusted close from parallel unadjusted frame
                    last_unadj = None
                    if intraday_data_unadj is not None:
                        intra_unadj = intraday_data_unadj if len(tickers) == 1 else intraday_data_unadj.get(ticker)
                        if intra_unadj is not None and not intra_unadj.empty:
                            last_unadj = intra_unadj.iloc[-1]
                    close_unadj = float(last_unadj["Close"]) if last_unadj is not None else None
                else:
                    # Fallback to daily
                    last = daily_df.iloc[-1]
                    close = float(last["Close"])
                    volume = last["Volume"]
                    open_ = float(last["Open"])
                    high = float(last["High"])
                    low = float(last["Low"])
                    # unadjusted fallback from daily unadjusted
                    last_unadj = None
                    if daily_data_unadj is not None:
                        daily_unadj = daily_data_unadj if len(tickers) == 1 else daily_data_unadj.get(ticker)
                        if daily_unadj is not None and not daily_unadj.empty:
                            last_unadj = daily_unadj.iloc[-1]
                    close_unadj = float(last_unadj["Close"]) if last_unadj is not None else None
            else:
                # Fallback to daily
                last = daily_df.iloc[-1]
                close = float(last["Close"])
                volume = last["Volume"]
                open_ = float(last["Open"])
                high = float(last["High"])
                low = float(last["Low"])
                # unadjusted fallback from daily unadjusted
                last_unadj = None
                if daily_data_unadj is not None:
                    daily_unadj = daily_data_unadj if len(tickers) == 1 else daily_data_unadj.get(ticker)
                    if daily_unadj is not None and not daily_unadj.empty:
                        last_unadj = daily_unadj.iloc[-1]
                close_unadj = float(last_unadj["Close"]) if last_unadj is not None else None

            if close <= 0 or volume is None:
                continue
            change = close - prev_close
            pct    = (change / prev_close * 100) if prev_close else 0.0
            rows.append({
                "symbol":     ticker,
                "date":       str(last.name.date()) if hasattr(last.name, "date") else str(last.name),
                "open":       round(open_, 2),
                "high":       round(high, 2),
                "low":        round(low, 2),
                # keep existing semantics: `close` is adjusted close
                "close":      round(close, 2),
                # also expose unadjusted close when available for comparison
                "close_unadjusted": round(close_unadj, 2) if close_unadj is not None else None,
                "volume":     int(volume),
                "prev_close": round(prev_close, 2),
                "prev_close_unadjusted": round(prev_close_unadj, 2) if prev_close_unadj is not None else None,
                "change":     round(change, 2),
                "change_pct": round(pct, 2),
                "fetched_at": now,
            })
        except Exception as exc:
            log.warning("quotes: skip %s – %s", ticker, exc)

    log.info("quotes: %d rows", len(rows))
    return rows


def fetch_historical_indicators(ticker: str, retries: int = 3) -> dict:
    """Fetch historical data and calculate technical indicators with retries."""
    for attempt in range(retries):
        try:
            # Fetch 3 months of daily data for indicators
            data = yf.download(ticker, period="3mo", interval="1d", auto_adjust=True, progress=False)
            if data.empty:
                log.warning(f"No data for {ticker}, attempt {attempt + 1}")
                if attempt < retries - 1:
                    continue
                return {}
            
            # Calculate indicators
            indicators = {}
            
            # RSI (14)
            indicators['rsi'] = ta.rsi(data['Close'], length=14).iloc[-1] if len(data) >= 14 else None
            
            # MACD
            macd = ta.macd(data['Close'], fast=12, slow=26, signal=9)
            indicators['macd'] = macd['MACD_12_26_9'].iloc[-1] if not macd.empty else None
            indicators['macd_signal'] = macd['MACDs_12_26_9'].iloc[-1] if not macd.empty else None
            indicators['macd_hist'] = macd['MACDh_12_26_9'].iloc[-1] if not macd.empty else None
            
            # Moving Averages
            indicators['ema9'] = ta.ema(data['Close'], length=9).iloc[-1] if len(data) >= 9 else None
            indicators['ema20'] = ta.ema(data['Close'], length=20).iloc[-1] if len(data) >= 20 else None
            indicators['sma50'] = ta.sma(data['Close'], length=50).iloc[-1] if len(data) >= 50 else None
            
            # ATR (14)
            indicators['atr'] = ta.atr(data['High'], data['Low'], data['Close'], length=14).iloc[-1] if len(data) >= 14 else None
            
            # VWAP approximation for daily
            data['vwap'] = (data['Close'] * data['Volume']).cumsum() / data['Volume'].cumsum()
            indicators['vwap'] = data['vwap'].iloc[-1]
            
            # Volume metrics
            avg_volume = data['Volume'].tail(20).mean()
            indicators['avg_volume_20'] = avg_volume
            indicators['relative_volume'] = data['Volume'].iloc[-1] / avg_volume if avg_volume > 0 else None
            
            # Momentum (price change over last 5 days)
            if len(data) >= 5:
                indicators['momentum_5d'] = (data['Close'].iloc[-1] - data['Close'].iloc[-5]) / data['Close'].iloc[-5] * 100
            
            # Gap analysis
            if len(data) >= 2:
                prev_close = data['Close'].iloc[-2]
                today_open = data['Open'].iloc[-1]
                indicators['gap_pct'] = (today_open - prev_close) / prev_close * 100 if prev_close > 0 else None
            
            # Support/Resistance (simple: recent highs/lows)
            recent_high = data['High'].tail(10).max()
            recent_low = data['Low'].tail(10).min()
            indicators['support'] = recent_low
            indicators['resistance'] = recent_high
            
            return {k: v for k, v in indicators.items() if v is not None and not (isinstance(v, float) and np.isnan(v))}
        
        except Exception as exc:
            log.warning(f"Failed to fetch indicators for {ticker}, attempt {attempt + 1}: {exc}")
            if attempt < retries - 1:
                continue
            return {}
    
    return {}


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