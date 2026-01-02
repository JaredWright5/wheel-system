"""
FMP Stable Client for Financial Modeling Prep API (Stable endpoints only).
Uses https://financialmodelingprep.com/stable base URL.
"""
import os
import re
from typing import Any, Dict, List, Optional
from datetime import date, timedelta

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from loguru import logger

# Import symbol normalization utility
try:
    from apps.worker.src.utils.symbols import normalize_for_fmp
except ImportError:
    # Fallback if import fails (e.g., in some environments)
    def normalize_for_fmp(symbol: str) -> str:
        """Fallback: return symbol as-is if utils module not available."""
        return symbol

BASE_URL = "https://financialmodelingprep.com/stable"
VERSION = "fmp_stable_v1"


def _redact_apikey(url: str) -> str:
    """Redact API key from URLs in logs."""
    return re.sub(r"(apikey=)[^&]+", r"\1REDACTED", url)


class FMPStableClient:
    """Client for Financial Modeling Prep API using stable endpoints only."""

    def __init__(self, api_key: Optional[str] = None, timeout: int = 30):
        self.api_key = api_key or os.getenv("FMP_API_KEY")
        if not self.api_key:
            raise RuntimeError("Missing FMP_API_KEY environment variable")
        self.timeout = timeout

    def _get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Make GET request to FMP stable API.
        
        Args:
            endpoint: Endpoint path (e.g., "profile" or "company-screener")
            params: Query parameters (apikey will be added automatically)
            
        Returns:
            JSON response data, or None on 404
            
        Raises:
            requests.HTTPError: On HTTP errors (except 404 which returns None)
        """
        params = params or {}
        params["apikey"] = self.api_key

        url = f"{BASE_URL}/{endpoint.lstrip('/')}"
        
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            
            # Don't retry 404s (resource doesn't exist)
            if response.status_code == 404:
                return None
            
            # Raise for other HTTP errors (5xx, 429, etc. will be retried by tenacity)
            response.raise_for_status()
            return response.json()
            
        except requests.HTTPError as e:
            safe_url_full = _redact_apikey(str(e.response.url) if hasattr(e, 'response') and e.response else url)
            raise requests.HTTPError(
                f"FMP Stable API error: {e.response.status_code} {e.response.reason} | "
                f"url: {safe_url_full} | body: {e.response.text[:300]}",
                response=e.response if hasattr(e, 'response') else None,
            ) from e

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        retry=retry_if_exception_type(requests.HTTPError),
    )
    def company_screener(
        self,
        exchange: Optional[str] = None,
        sector: Optional[str] = None,
        industry: Optional[str] = None,
        limit: int = 1000,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Company screener using stable endpoint.
        
        Args:
            exchange: Exchange filter (e.g., "NASDAQ", "NYSE", "AMEX")
            sector: Sector filter
            industry: Industry filter
            limit: Maximum results
            **kwargs: Additional screener parameters
            
        Returns:
            List of company dictionaries
        """
        try:
            params = {"limit": limit}
            if exchange:
                params["exchange"] = exchange
            if sector:
                params["sector"] = sector
            if industry:
                params["industry"] = industry
            params.update(kwargs)
            
            data = self._get("company-screener", params=params)
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return [data]
            return []
        except requests.HTTPError as e:
            logger.warning(f"FMP company_screener failed: {e}")
            return []
        except Exception as e:
            logger.warning(f"FMP company_screener unexpected error: {e}")
            return []

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        retry=retry_if_exception_type(requests.HTTPError),
    )
    def profile(self, symbol: str) -> Dict[str, Any]:
        """
        Get company profile for a single symbol.
        
        Args:
            symbol: Stock symbol (e.g., "AAPL")
            
        Returns:
            Dictionary with profile data, or {} if not found
        """
        original_symbol = symbol
        symbol = normalize_for_fmp(symbol)
        try:
            data = self._get("profile", params={"symbol": symbol})
            if isinstance(data, list) and data:
                return data[0]
            if isinstance(data, dict):
                return data
            return {}
        except requests.HTTPError as e:
            if hasattr(e, 'response') and e.response and e.response.status_code == 404:
                return {}
            logger.warning(f"FMP profile({original_symbol} -> {symbol}) failed: {e}")
            return {}
        except Exception as e:
            logger.warning(f"FMP profile({original_symbol} -> {symbol}) unexpected error: {e}")
            return {}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        retry=retry_if_exception_type(requests.HTTPError),
    )
    def quote(self, symbol: str) -> Dict[str, Any]:
        """
        Get quote for a single symbol.
        
        Args:
            symbol: Stock symbol (e.g., "AAPL")
            
        Returns:
            Dictionary with quote data, or {} if not found
        """
        original_symbol = symbol
        symbol = normalize_for_fmp(symbol)
        try:
            data = self._get("quote", params={"symbol": symbol})
            if isinstance(data, list) and data:
                return data[0]
            if isinstance(data, dict):
                return data
            return {}
        except requests.HTTPError as e:
            if hasattr(e, 'response') and e.response and e.response.status_code == 404:
                return {}
            logger.warning(f"FMP quote({original_symbol} -> {symbol}) failed: {e}")
            return {}
        except Exception as e:
            logger.warning(f"FMP quote({original_symbol} -> {symbol}) unexpected error: {e}")
            return {}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        retry=retry_if_exception_type(requests.HTTPError),
    )
    def key_metrics_ttm(self, symbol: str) -> Dict[str, Any]:
        """
        Get key metrics TTM for a symbol.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dictionary with metrics, or {} if not found
        """
        original_symbol = symbol
        symbol = normalize_for_fmp(symbol)
        try:
            data = self._get("key-metrics-ttm", params={"symbol": symbol})
            if isinstance(data, list) and data:
                return data[0]
            if isinstance(data, dict):
                return data
            return {}
        except requests.HTTPError as e:
            if hasattr(e, 'response') and e.response and e.response.status_code == 404:
                return {}
            logger.warning(f"FMP key_metrics_ttm({original_symbol} -> {symbol}) failed: {e}")
            return {}
        except Exception as e:
            logger.warning(f"FMP key_metrics_ttm({original_symbol} -> {symbol}) unexpected error: {e}")
            return {}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        retry=retry_if_exception_type(requests.HTTPError),
    )
    def ratios_ttm(self, symbol: str) -> Dict[str, Any]:
        """
        Get ratios TTM for a symbol.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dictionary with ratios, or {} if not found
        """
        original_symbol = symbol
        symbol = normalize_for_fmp(symbol)
        try:
            data = self._get("ratios-ttm", params={"symbol": symbol})
            if isinstance(data, list) and data:
                return data[0]
            if isinstance(data, dict):
                return data
            return {}
        except requests.HTTPError as e:
            if hasattr(e, 'response') and e.response and e.response.status_code == 404:
                return {}
            logger.warning(f"FMP ratios_ttm({original_symbol} -> {symbol}) failed: {e}")
            return {}
        except Exception as e:
            logger.warning(f"FMP ratios_ttm({original_symbol} -> {symbol}) unexpected error: {e}")
            return {}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        retry=retry_if_exception_type(requests.HTTPError),
    )
    def stock_news(self, symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get stock news.
        
        Args:
            symbol: Stock symbol
            limit: Maximum number of news items
            
        Returns:
            List of news items, or [] on error
        """
        original_symbol = symbol
        symbol = normalize_for_fmp(symbol)
        try:
            data = self._get("stock-news", params={"tickers": symbol, "limit": limit})
            if isinstance(data, list):
                return data[:limit]
            if isinstance(data, dict):
                return [data]
            return []
        except requests.HTTPError as e:
            if hasattr(e, 'response') and e.response and e.response.status_code == 404:
                return []
            logger.warning(f"FMP stock_news({original_symbol} -> {symbol}) failed: {e}")
            return []
        except Exception as e:
            logger.warning(f"FMP stock_news({original_symbol} -> {symbol}) unexpected error: {e}")
            return []

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        retry=retry_if_exception_type(requests.HTTPError),
    )
    def technical_indicator_rsi(
        self,
        symbol: str,
        period: int = 14,
        interval: str = "1day",
        limit: int = 1
    ) -> Optional[float]:
        """
        Get RSI technical indicator.
        
        Args:
            symbol: Stock symbol
            period: RSI period (default 14)
            interval: Data interval (default "1day")
            limit: Number of data points (default 1 for latest)
            
        Returns:
            RSI value (float) if available, None otherwise
        """
        original_symbol = symbol
        symbol = normalize_for_fmp(symbol)
        try:
            # Convert interval format: "daily" -> "1day", "weekly" -> "1week", etc.
            timeframe_map = {
                "daily": "1day",
                "1day": "1day",
                "weekly": "1week",
                "1week": "1week",
                "monthly": "1month",
                "1month": "1month",
            }
            timeframe = timeframe_map.get(interval.lower(), interval)
            
            # FMP endpoint uses periodLength and timeframe (not period and interval)
            params = {
                "symbol": symbol,
                "periodLength": period,
                "timeframe": timeframe,
            }
            data = self._get("technical-indicators/rsi", params=params)
            
            # Handle various response formats
            if isinstance(data, list) and data:
                latest = data[0]
                if isinstance(latest, dict):
                    # Look for rsi field
                    rsi = latest.get("rsi") or latest.get("RSI") or latest.get("value")
                    if rsi is not None:
                        try:
                            return float(rsi)
                        except (ValueError, TypeError):
                            pass
            if isinstance(data, dict):
                rsi = data.get("rsi") or data.get("RSI") or data.get("value")
                if rsi is not None:
                    try:
                        return float(rsi)
                    except (ValueError, TypeError):
                        pass
            
            return None
        except requests.HTTPError as e:
            # 404 or other errors - return None (indicator not available)
            return None
        except Exception as e:
            logger.debug(f"FMP technical_indicator_rsi({original_symbol} -> {symbol}) error: {e}")
            return None


def simple_sentiment_score(news_items: List[Dict[str, Any]]) -> float:
    """
    Simple sentiment scoring from news items.
    
    Args:
        news_items: List of news item dictionaries with 'title' field
        
    Returns:
        Sentiment score in range [-1.0, 1.0]
    """
    if not news_items:
        return 0.0
    
    pos_words = {"beat", "beats", "surge", "soar", "record", "upgrade", "upgraded", "buy", "growth", "strong", "raises", "raise", "profit"}
    neg_words = {"miss", "misses", "plunge", "drop", "downgrade", "downgraded", "sell", "lawsuit", "probe", "weak", "cuts", "cut", "loss"}
    
    score = 0
    n = 0
    for item in news_items:
        title = (item.get("title") or item.get("summary") or "").lower()
        if not title:
            continue
        n += 1
        pos_count = sum(1 for w in pos_words if w in title)
        neg_count = sum(1 for w in neg_words if w in title)
        score += (pos_count - neg_count)
    
    if n == 0:
        return 0.0
    
    raw = score / max(1, n)
    raw = max(-3, min(3, raw))
    return raw / 3.0

