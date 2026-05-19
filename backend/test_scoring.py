import pytest
from scoring import _score_stock, _calculate_risk_metrics, _apply_filters, _signal, _is_btst_candidate, _n


def test_n_function():
    """Test the _n helper function."""
    assert _n(None) is None
    assert _n(5.5) == 5.5
    assert _n(float('nan')) is None
    assert _n(float('inf')) is None


def test_score_stock_basic():
    """Test basic scoring functionality."""
    q = {"close": 100, "change_pct": 2.0}
    fi = {"recommendationKey": "BUY"}
    indicators = {
        'momentum_5d': 3.0,
        'relative_volume': 1.5,
        'rsi': 65,
        'macd': 1.0,
        'macd_signal': 0.8,
        'ema9': 98,
        'ema20': 97,
        'sma50': 95,
        'atr': 2.0,
        'avg_volume_20': 500000,
        'resistance': 105,
        'support': 95
    }
    
    score, details = _score_stock(q, fi, indicators)
    
    assert isinstance(score, float)
    assert 0 <= score <= 100
    assert isinstance(details, dict)
    assert 'buy_zone' in details
    assert 'stop_loss' in details
    assert 'target' in details


def test_calculate_risk_metrics():
    """Test risk metrics calculation."""
    q = {"close": 100}
    fi = {}
    indicators = {'support': 95, 'resistance': 105, 'atr': 2.0}
    
    metrics = _calculate_risk_metrics(q, fi, indicators)
    
    assert 'buy_zone' in metrics
    assert 'stop_loss' in metrics
    assert 'target' in metrics
    assert 'rr_ratio' in metrics
    assert 'confidence' in metrics
    assert 'risk_warning' in metrics


def test_apply_filters():
    """Test filtering logic."""
    # Should pass
    q = {"close": 100}
    fi = {}
    indicators = {'avg_volume_20': 200000, 'rsi': 50, 'atr': 1.0}
    
    reason = _apply_filters(q, fi, indicators, 60)
    assert reason is None
    
    # Should filter: low volume
    indicators_low_vol = {'avg_volume_20': 50000, 'rsi': 50, 'atr': 1.0}
    reason = _apply_filters(q, fi, indicators_low_vol, 60)
    assert reason is not None
    assert "volume" in reason.lower()


def test_signal():
    """Test signal generation."""
    assert _signal(80) == "Strong Buy"
    assert _signal(60) == "Buy"
    assert _signal(40) == "Hold"
    assert _signal(20) == "Avoid"


def test_is_btst_candidate():
    """Test BTST candidate logic."""
    q = {"close": 100, "change_pct": 3.0, "open": 98}
    fi = {"recommendationKey": "BUY"}
    indicators = {
        'momentum_5d': 4.0,
        'ema20': 97,
        'avg_volume_20': 1000000,
        'atr': 1.5
    }
    
    assert _is_btst_candidate(q, fi, indicators, 70) == True
    
    # Should fail: low momentum
    indicators_low = indicators.copy()
    indicators_low['momentum_5d'] = 1.0
    assert _is_btst_candidate(q, fi, indicators_low, 70) == False


def test_empty_data_handling():
    """Test handling of missing or empty data."""
    q = {"close": None}
    fi = {}
    indicators = {}
    
    score, details = _score_stock(q, fi, indicators)
    assert score == 0.0  # Should handle gracefully
    
    metrics = _calculate_risk_metrics(q, fi, indicators)
    assert metrics == {}  # Should return empty dict for invalid data