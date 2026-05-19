# ---------------------------------------------------------------------------
# Scoring / signal helpers with Technical Indicators and Risk Management
# ---------------------------------------------------------------------------

import math
from typing import Any, Dict, Tuple

def _n(v) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def _apply_filters(q: dict, fi: dict, indicators: dict, score: float) -> str | None:
    """Return exclusion reason for stocks failing minimum quality thresholds."""
    avg_volume = _n(indicators.get('avg_volume_20'))
    if avg_volume is not None and avg_volume < 100000:
        return "Insufficient average volume"
    rsi = _n(indicators.get('rsi'))
    if rsi is not None and rsi < 30:
        return "RSI is too low"
    if score < 30:
        return "Score is below minimum threshold"
    return None


def _score_stock(q: dict, fi: dict, indicators: dict) -> Tuple[float, Dict[str, Any]]:
    """Calculate weighted score and risk management metrics."""
    score = 0.0
    reasons = {}
    
    # 20% Momentum
    momentum_score = 0.0
    momentum_5d = _n(indicators.get('momentum_5d'))
    if momentum_5d is not None:
        momentum_score = min(100, max(0, (momentum_5d + 10) * 5))  # Scale -10% to +10% to 0-100
        reasons['momentum'] = f"5-day momentum: {momentum_5d:.1f}%"
    score += 0.20 * momentum_score
    
    # 15% Relative Volume
    volume_score = 0.0
    rel_vol = _n(indicators.get('relative_volume'))
    if rel_vol is not None:
        volume_score = min(100, rel_vol * 50)  # Scale relative volume
        reasons['volume'] = f"Relative volume: {rel_vol:.1f}x"
    score += 0.15 * volume_score
    
    # 15% Breakout Strength
    breakout_score = 0.0
    close = _n(q.get('close'))
    resistance = _n(indicators.get('resistance'))
    support = _n(indicators.get('support'))
    gap_pct = _n(indicators.get('gap_pct'))
    
    if close and resistance and close > resistance * 0.98:  # Near resistance
        breakout_score += 50
        reasons['breakout'] = f"Breaking resistance at {resistance:.1f}"
    if gap_pct and gap_pct > 2:
        breakout_score += 50
        reasons['gap'] = f"Gap up: {gap_pct:.1f}%"
    score += 0.15 * breakout_score
    
    # 10% RSI Quality
    rsi_score = 0.0
    rsi = _n(indicators.get('rsi'))
    if rsi is not None:
        if 40 <= rsi <= 70:  # Optimal range
            rsi_score = 100
        elif rsi > 70:
            rsi_score = 50  # Overbought
        else:
            rsi_score = 30  # Oversold
        reasons['rsi'] = f"RSI: {rsi:.1f}"
    score += 0.10 * rsi_score
    
    # 10% MACD Trend Confirmation
    macd_score = 0.0
    macd = _n(indicators.get('macd'))
    macd_signal = _n(indicators.get('macd_signal'))
    if macd is not None and macd_signal is not None:
        if macd > macd_signal:
            macd_score = 100  # Bullish
            reasons['macd'] = "MACD bullish crossover"
        else:
            macd_score = 30  # Bearish
            reasons['macd'] = "MACD bearish"
    score += 0.10 * macd_score
    
    # 10% Moving Average Alignment
    ma_score = 0.0
    ema9 = _n(indicators.get('ema9'))
    ema20 = _n(indicators.get('ema20'))
    sma50 = _n(indicators.get('sma50'))
    if close and ema9 and ema20 and sma50:
        alignments = 0
        if close > ema9: alignments += 1
        if close > ema20: alignments += 1
        if close > sma50: alignments += 1
        ma_score = (alignments / 3) * 100
        reasons['ma'] = f"MA alignment: {alignments}/3 above"
    score += 0.10 * ma_score
    
    # 10% Volatility/ATR Suitability
    vol_score = 0.0
    atr = _n(indicators.get('atr'))
    if close and atr:
        vol_pct = (atr / close) * 100
        if 1 <= vol_pct <= 3:  # Suitable volatility for swing trading
            vol_score = 100
        elif vol_pct > 3:
            vol_score = 50  # Too volatile
        else:
            vol_score = 30  # Too stable
        reasons['volatility'] = f"ATR: {vol_pct:.1f}% of price"
    score += 0.10 * vol_score
    
    # 5% Liquidity
    liq_score = 0.0
    avg_vol = _n(indicators.get('avg_volume_20'))
    market_cap = _n(fi.get('marketCap'))
    if avg_vol and avg_vol > 100000:  # Minimum volume
        liq_score = 100
        reasons['liquidity'] = f"Avg volume: {avg_vol:,.0f}"
    else:
        liq_score = 0
        reasons['liquidity'] = "Low liquidity"
    score += 0.05 * liq_score
    
    # 5% News/Sentiment
    news_score = 0.0
    rec = (fi.get("recommendationKey") or "").lower()
    if "strong_buy" in rec or "buy" in rec:
        news_score = 100
        reasons['sentiment'] = f"Analyst: {rec}"
    elif "hold" in rec:
        news_score = 50
    else:
        news_score = 0
    score += 0.05 * news_score
    
    # Risk Management Calculations
    risk_metrics = _calculate_risk_metrics(q, fi, indicators)
    
    return round(score, 2), {**reasons, **risk_metrics}


def _calculate_risk_metrics(q: dict, fi: dict, indicators: dict) -> Dict[str, Any]:
    """Calculate risk management metrics."""
    close = _n(q.get('close'))
    if not close:
        return {}
    
    # Buy Zone: Support level or recent low
    support = _n(indicators.get('support'))
    buy_zone = support if support else close * 0.98  # 2% below current
    
    # Stop Loss: Below support or ATR-based
    atr = _n(indicators.get('atr'))
    stop_loss = support * 0.98 if support else (close - (atr * 1.5)) if atr else close * 0.95
    
    # Target: Resistance or upside potential
    resistance = _n(indicators.get('resistance'))
    target = resistance if resistance else close * 1.05  # 5% upside
    
    # Risk/Reward Ratio
    risk = close - stop_loss
    reward = target - close
    rr_ratio = reward / risk if risk > 0 else 0
    
    # Confidence Score (based on score and filters)
    confidence = 50  # Base
    if rr_ratio >= 1.5: confidence += 20
    if _n(indicators.get('relative_volume', 0)) > 1.2: confidence += 10
    if _n(indicators.get('rsi', 50)) > 40 and _n(indicators.get('rsi', 50)) < 70: confidence += 10
    confidence = min(100, confidence)
    
    return {
        'buy_zone': round(buy_zone, 2),
        'stop_loss': round(stop_loss, 2),
        'target': round(target, 2),
        'rr_ratio': round(rr_ratio, 2),
        'confidence': confidence,
        'risk_warning': "High volatility" if atr and (atr/close) > 0.03 else "Monitor closely" if rr_ratio < 2 else "Favorable risk/reward"
    }


def _signal(score: float) -> str:
    if score >= 70: return "Strong Buy"
    if score >= 50: return "Buy"
    if score >= 30: return "Hold"
    return "Avoid"


def _is_btst_candidate(q: dict, fi: dict, indicators: dict, score: float) -> bool:
    """BTST candidate: strong momentum, technical breakout, analyst support, good liquidity."""
    chg = _n(q.get("change_pct"))
    close = _n(q.get("close"))
    open_ = _n(q.get("open"))
    
    # Strong momentum
    momentum = _n(indicators.get('momentum_5d', 0))
    if not (momentum and momentum >= 3.0):
        return False
    
    # Bullish close
    if not (open_ and close and close > open_):
        return False
    
    # Above key MAs
    ema20 = _n(indicators.get('ema20'))
    if not (close and ema20 and close > ema20):
        return False
    
    # Analyst support
    rec = (fi.get("recommendationKey") or "").lower()
    if not ("buy" in rec or "strong" in rec):
        return False
    
    # Good liquidity
    avg_vol = _n(indicators.get('avg_volume_20', 0))
    if avg_vol < 500000:  # Higher threshold for BTST
        return False
    
    # High score
    if score < 60:
        return False
    
    # Suitable volatility
    atr = _n(indicators.get('atr', 0))
    if atr / close > 0.04:  # Not too volatile
        return False
    
    return True


def _catalysts(q: dict, fi: dict, score: float) -> list[str]:
    cats: list[str] = []
    chg      = _n(q.get("change_pct"))
    roe      = _n(fi.get("returnOnEquity"))
    pe       = _n(fi.get("trailingPE"))
    de       = _n(fi.get("debtToEquity"))
    cr       = _n(fi.get("currentRatio"))
    rec      = (fi.get("recommendationKey") or "").lower()
    target   = _n(fi.get("targetMeanPrice"))
    close    = _n(q.get("close"))
    open_    = _n(q.get("open"))
    ma50     = _n(fi.get("fiftyDayAverage"))
    ma200    = _n(fi.get("twoHundredDayAverage"))
    ps       = _n(fi.get("priceToSalesTrailing12Months"))
    num_analysts = _n(fi.get("numberOfAnalystOpinions"))

    if chg and chg >= 2.0:           cats.append(f"Strong momentum +{chg:.1f}%")
    elif chg and chg > 0:            cats.append(f"Momentum +{chg:.1f}%")
    if open_ is not None and close is not None and close > open_:
        cats.append("Bullish close above open")
    if roe and roe > 0.15:           cats.append(f"Strong ROE {roe * 100:.1f}%")
    if pe and pe < 20:               cats.append(f"Attractive P/E {pe:.1f}x")
    if ps is not None and ps < 3:     cats.append(f"Low P/S {ps:.1f}")
    if close and ma50 and close > ma50: cats.append("Above 50-day average")
    if close and ma200 and close > ma200: cats.append("Above 200-day average")
    if de and de < 0.5:              cats.append("Low leverage")
    if cr and cr > 1.5:              cats.append("Healthy liquidity")
    if num_analysts and num_analysts >= 5: cats.append(f"{int(num_analysts)} analyst opinions")
    if "buy" in rec:                cats.append("Analyst buy consensus")
    if target and close and (target - close) / close > 0.10:
        cats.append(f"Analyst upside {((target - close) / close * 100):.0f}%")
    return cats[:5]


def _rationale(q: dict, fi: dict, score: float) -> str:
    close       = _n(q.get("close"))
    pe          = _n(fi.get("trailingPE"))
    roe         = _n(fi.get("returnOnEquity"))
    chg         = _n(q.get("change_pct"))
    target      = _n(fi.get("targetMeanPrice"))
    rec         = (fi.get("recommendationKey") or "").replace("_", " ").title()
    sector      = fi.get("sector") or "Equity"
    ma50        = _n(fi.get("fiftyDayAverage"))
    ma200       = _n(fi.get("twoHundredDayAverage"))

    s1 = f"{sector} play"
    if pe:  s1 += f" trading at {pe:.1f}x P/E"
    if roe: s1 += f" with {roe * 100:.1f}% ROE"

    s2_parts = []
    if chg is not None:
        s2_parts.append(f"recent move of {chg:+.2f}%")
    if close and ma50 and close > ma50:
        s2_parts.append("above the 50-day average")
    if close and ma200 and close > ma200:
        s2_parts.append("above the 200-day average")
    if target and close:
        s2_parts.append(f"analyst target implies {(target - close) / close * 100:.0f}% upside")
    if rec:
        s2_parts.append(f"{rec}")

    s2 = "; ".join(s2_parts)
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