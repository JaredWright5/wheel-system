import os
import re
import requests
from typing import Any, Dict, List, Optional
from datetime import date
from tenacity import retry, wait_exponential, stop_after_attempt

BASE = "https://financialmodelingprep.com/api/v3"

def _redact_apikey(url: str) -> str:
    return re.sub(r"(apikey=)[^&]+", r"\1REDACTED", url)

class FMPClient:
    def __init__(self, api_key: Optional[str] = None, timeout: int = 25):
        self.api_key = api_key or os.getenv("FMP_API_KEY")
        if not self.api_key:
            raise RuntimeError("Missing FMP_API_KEY")
        self.timeout = timeout

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        params = params or {}
        params["apikey"] = self.api_key
        url = f"{BASE}/{path.lstrip('/')}"
        r = requests.get(url, params=params, timeout=self.timeout)
        if r.status_code >= 400:
            safe_url = _redact_apikey(r.url)
            raise requests.HTTPError(
                f"{r.status_code} {r.reason} for url: {safe_url} | body: {r.text[:300]}",
                response=r
            )
        return r.json()

    @retry(wait=wait_exponential(min=1, max=15), stop=stop_after_attempt(2))
    def profile(self, symbol: str) -> Optional[Dict[str, Any]]:
        data = self._get(f"profile/{symbol}")
        return data[0] if isinstance(data, list) and data else None

    @retry(wait=wait_exponential(min=1, max=15), stop=stop_after_attempt(2))
    def quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        data = self._get(f"quote/{symbol}")
        return data[0] if isinstance(data, list) and data else None

    @retry(wait=wait_exponential(min=1, max=15), stop=stop_after_attempt(2))
    def key_metrics_ttm(self, symbol: str) -> Optional[Dict[str, Any]]:
        data = self._get(f"key-metrics-ttm/{symbol}")
        return data[0] if isinstance(data, list) and data else None

    @retry(wait=wait_exponential(min=1, max=15), stop=stop_after_attempt(2))
    def ratios_ttm(self, symbol: str) -> Optional[Dict[str, Any]]:
        data = self._get(f"ratios-ttm/{symbol}")
        return data[0] if isinstance(data, list) and data else None

    @retry(wait=wait_exponential(min=1, max=15), stop=stop_after_attempt(2))
    def earnings_calendar(self, start: date, end: date) -> List[Dict[str, Any]]:
        return self._get("earning_calendar", params={"from": start.isoformat(), "to": end.isoformat()})

    @retry(wait=wait_exponential(min=1, max=15), stop=stop_after_attempt(2))
    def stock_news(self, symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
        return self._get("stock_news", params={"tickers": symbol, "limit": limit})

def simple_sentiment_score(news_items: List[Dict[str, Any]]) -> float:
    if not news_items:
        return 0.0

    pos = {"beat", "beats", "surge", "soar", "record", "upgrade", "upgraded", "buy", "growth", "strong", "raises", "raise", "profit"}
    neg = {"miss", "misses", "plunge", "drop", "downgrade", "downgraded", "sell", "lawsuit", "probe", "weak", "cuts", "cut", "loss"}

    score = 0
    n = 0
    for it in news_items:
        title = (it.get("title") or "").lower()
        if not title:
            continue
        n += 1
        p = sum(1 for w in pos if w in title)
        m = sum(1 for w in neg if w in title)
        score += (p - m)

    if n == 0:
        return 0.0
    raw = score / max(1, n)
    raw = max(-3, min(3, raw))
    return raw / 3.0
