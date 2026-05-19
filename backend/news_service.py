# ---------------------------------------------------------------------------
# News Service
# ---------------------------------------------------------------------------

import asyncio
import hashlib
import html
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Callable
import logging

from gnews import GNews
import yfinance as yf

from config import NEWS_CACHE_TTL, NEWS_CIRCUIT_OPEN_S, NEWS_MAX_FAILURES, NEWS_FETCH_TIMEOUT, NEWS_MAX_WORKERS, NEWS_MAX_AGE_HOURS, _RSS_FEEDS

log = logging.getLogger("screener")

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
        items.append(_NewsItem(
            title=title, link=link, publisher=r.get("publisher") or "Google News",
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


@dataclass
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