# ---------------------------------------------------------------------------
# DynamoDB Helpers
# ---------------------------------------------------------------------------

import boto3
from decimal import Decimal, InvalidOperation
import math
from datetime import datetime
from typing import Any
import os

from config import AWS_REGION, TBL_QUOTES, TBL_INFO, TBL_FINANCIALS, TBL_BALANCE_SHEET, TBL_HISTORY, TBL_META, TBL_NEWS_CACHE

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
    print(f"batch_write → {table_name}: {total} items")


def _put_item(table_name: str, item: dict) -> None:
    _tbl(table_name).put_item(Item=_clean_item(item))


def _get_item(table_name: str, key: dict) -> dict | None:
    resp = _tbl(table_name).get_item(Key=key)
    return _from_decimal(resp.get("Item"))


def _query_items(table_name: str, pk_name: str, pk_val: str) -> list[dict]:
    from boto3.dynamodb.conditions import Key
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