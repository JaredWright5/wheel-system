"""
FMP Stable Client for Financial Modeling Prep API (Stable endpoints only).
Uses https://financialmodelingprep.com/stable base URL.
"""
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import date, timedelta

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from loguru import logger

BASE_URL = "https://financialmodelingprep.com/stable"
VERSION = "fmp_stable_v1"


def _redact_apikey(url: str) -> str:
    """Redact API key from URLs in logs."""
    return re.sub(r"(apikey=)[^&]+", r"\1REDACTED", url)


def _normalize_symbol_for_fmp(symbol: str) -> str:
    """
    Normalize symbol for FMP API requests.
    
    Behavior:
    - Strip whitespace
    - Uppercase
    - Replace "." with "-" (e.g., BRK.B -> BRK-B)
    
    Args:
        symbol: Stock symbol (e.g., "BRK.B" or "AAPL")
        
    Returns:
        Normalized symbol (e.g., "BRK-B" or "AAPL")
    """
    if not symbol:
        return symbol
    normalized = symbol.strip().upper()
    # Replace "." with "-" for class shares (e.g., BRK.B -> BRK-B)
    normalized = normalized.replace(".", "-")
    return normalized


class FMPStableClient:
    """Client for Financial Modeling Prep API using stable endpoints only."""

    def __init__(self, api_key: Optional[str] = None, timeout: int = 30):
        self.api_key = api_key or os.getenv("FMP_API_KEY")
        if not self.api_key:
            raise RuntimeError("Missing FMP_API_KEY environment variable")
        self.timeout = timeout
        # Cache for 402-blocked endpoint+symbol combinations
        # Set of tuples: (endpoint_name, normalized_symbol)
        self._blocked: Set[Tuple[str, str]] = set()

    def _is_blocked(self, endpoint: str, normalized_symbol: str) -> bool:
        """
        Check if endpoint+symbol combination is blocked (returned 402 previously).
        
        Args:
            endpoint: Endpoint name (e.g., "quote", "financial-scores")
            normalized_symbol: Normalized symbol (e.g., "BRK-B")
            
        Returns:
            True if blocked, False otherwise
        """
        return (endpoint, normalized_symbol) in self._blocked

    def _mark_blocked(self, endpoint: str, normalized_symbol: str) -> None:
        """
        Mark endpoint+symbol combination as blocked (returned 402).
        
        Args:
            endpoint: Endpoint name
            normalized_symbol: Normalized symbol
        """
        self._blocked.add((endpoint, normalized_symbol))

    def _get_json(
        self,
        endpoint: str,
        params: Dict[str, Any],
        endpoint_name: str,
        symbol_original: str,
    ) -> Tuple[Any, Dict[str, Any]]:
        """
        Generic helper to fetch JSON from FMP API with diagnostics.
        
        Args:
            endpoint: Endpoint path (e.g., "quote", "financial-scores")
            params: Query parameters (apikey will be added automatically)
            endpoint_name: Endpoint name for logging/blocking (e.g., "quote")
            symbol_original: Original symbol for logging
            
        Returns:
            Tuple of (data, meta) where:
            - data: Parsed JSON response (or None if error)
            - meta: Dict with {"ok": bool, "status": int|None, "error_type": str}
              error_type in {"blocked_402", "http_error", "empty", "parse_error", "ok"}
        """
        normalized_symbol = _normalize_symbol_for_fmp(symbol_original)
        
        # Check blocked cache
        if self._is_blocked(endpoint_name, normalized_symbol):
            logger.debug(f"FMP {endpoint_name}({symbol_original} -> {normalized_symbol}): skipping (402-blocked)")
            return None, {"ok": False, "status": None, "error_type": "blocked_402"}
        
        # Prepare request
        request_params = params.copy()
        request_params["apikey"] = self.api_key
        url = f"{BASE_URL}/{endpoint.lstrip('/')}"
        
        try:
            response = requests.get(url, params=request_params, timeout=self.timeout)
            status_code = response.status_code
            
            # Handle 402 Payment Required
            if status_code == 402:
                self._mark_blocked(endpoint_name, normalized_symbol)
                logger.warning(
                    f"FMP {endpoint_name}({symbol_original} -> {normalized_symbol}): "
                    f"402 Payment Required (subscription tier) - blocking future calls"
                )
                return None, {"ok": False, "status": 402, "error_type": "blocked_402"}
            
            # Handle 404
            if status_code == 404:
                return None, {"ok": False, "status": 404, "error_type": "empty"}
            
            # Raise for other HTTP errors
            response.raise_for_status()
            
            # Parse JSON
            try:
                data = response.json()
                # Check if response is empty
                if data is None or (isinstance(data, list) and len(data) == 0) or (isinstance(data, dict) and len(data) == 0):
                    return None, {"ok": False, "status": status_code, "error_type": "empty"}
                return data, {"ok": True, "status": status_code, "error_type": "ok"}
            except (ValueError, TypeError) as parse_err:
                logger.warning(f"FMP {endpoint_name}({symbol_original} -> {normalized_symbol}): JSON parse error: {parse_err}")
                return None, {"ok": False, "status": status_code, "error_type": "parse_error"}
                
        except requests.HTTPError as e:
            status_code = e.response.status_code if hasattr(e, 'response') and e.response else None
            # Handle 402 in exception path
            if status_code == 402:
                self._mark_blocked(endpoint_name, normalized_symbol)
                logger.warning(
                    f"FMP {endpoint_name}({symbol_original} -> {normalized_symbol}): "
                    f"402 Payment Required (subscription tier) - blocking future calls"
                )
                return None, {"ok": False, "status": 402, "error_type": "blocked_402"}
            logger.warning(f"FMP {endpoint_name}({symbol_original} -> {normalized_symbol}): HTTP error {status_code}: {e}")
            return None, {"ok": False, "status": status_code, "error_type": "http_error"}
        except Exception as e:
            logger.warning(f"FMP {endpoint_name}({symbol_original} -> {normalized_symbol}): unexpected error: {e}")
            return None, {"ok": False, "status": None, "error_type": "http_error"}

    def _get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        check_blocked: bool = False,
        normalized_symbol: Optional[str] = None,
    ) -> Any:
        """
        Make GET request to FMP stable API (legacy method, use _get_json for new code).
        
        Args:
            endpoint: Endpoint path (e.g., "profile" or "company-screener")
            params: Query parameters (apikey will be added automatically)
            check_blocked: If True, check blocked cache before making request
            normalized_symbol: Normalized symbol (required if check_blocked=True)
            
        Returns:
            JSON response data, or None on 404
            
        Raises:
            requests.HTTPError: On HTTP errors (except 404 which returns None, 402 handled specially)
        """
        # Check blocked cache if requested
        if check_blocked and normalized_symbol:
            if self._is_blocked(endpoint, normalized_symbol):
                logger.debug(f"FMP {endpoint}({normalized_symbol}): skipping (402-blocked)")
                return None
        
        params = params or {}
        params["apikey"] = self.api_key

        url = f"{BASE_URL}/{endpoint.lstrip('/')}"
        
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            
            # Handle 402 Payment Required (subscription tier)
            if response.status_code == 402:
                if check_blocked and normalized_symbol:
                    # Mark as blocked and log warning once
                    self._mark_blocked(endpoint, normalized_symbol)
                    logger.warning(
                        f"FMP {endpoint}({normalized_symbol}): 402 Payment Required (subscription tier) - "
                        f"blocking future calls for this endpoint+symbol"
                    )
                else:
                    # For non-symbol endpoints, just log warning
                    logger.warning(f"FMP {endpoint}: 402 Payment Required (subscription tier)")
                return None
            
            # Don't retry 404s (resource doesn't exist)
            if response.status_code == 404:
                return None
            
            # Raise for other HTTP errors (5xx, 429, etc. will be retried by tenacity)
            response.raise_for_status()
            return response.json()
            
        except requests.HTTPError as e:
            # Handle 402 in exception path (if not caught above)
            if hasattr(e, 'response') and e.response and e.response.status_code == 402:
                if check_blocked and normalized_symbol:
                    self._mark_blocked(endpoint, normalized_symbol)
                    logger.warning(
                        f"FMP {endpoint}({normalized_symbol}): 402 Payment Required (subscription tier) - "
                        f"blocking future calls for this endpoint+symbol"
                    )
                else:
                    logger.warning(f"FMP {endpoint}: 402 Payment Required (subscription tier)")
                return None
            
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
        normalized_symbol = _normalize_symbol_for_fmp(symbol)
        try:
            data = self._get("profile", params={"symbol": normalized_symbol}, check_blocked=True, normalized_symbol=normalized_symbol)
            if data is None:
                return {}
            if isinstance(data, list) and data:
                return data[0]
            if isinstance(data, dict):
                return data
            return {}
        except requests.HTTPError as e:
            if hasattr(e, 'response') and e.response and e.response.status_code == 404:
                return {}
            logger.warning(f"FMP profile({original_symbol} -> {normalized_symbol}) failed: {e}")
            return {}
        except Exception as e:
            logger.warning(f"FMP profile({original_symbol} -> {normalized_symbol}) unexpected error: {e}")
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
        normalized_symbol = _normalize_symbol_for_fmp(symbol)
        try:
            data = self._get("quote", params={"symbol": normalized_symbol}, check_blocked=True, normalized_symbol=normalized_symbol)
            if data is None:
                return {}
            if isinstance(data, list) and data:
                return data[0]
            if isinstance(data, dict):
                return data
            return {}
        except requests.HTTPError as e:
            if hasattr(e, 'response') and e.response and e.response.status_code == 404:
                return {}
            logger.warning(f"FMP quote({original_symbol} -> {normalized_symbol}) failed: {e}")
            return {}
        except Exception as e:
            logger.warning(f"FMP quote({original_symbol} -> {normalized_symbol}) unexpected error: {e}")
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
        normalized_symbol = _normalize_symbol_for_fmp(symbol)
        try:
            data = self._get("key-metrics-ttm", params={"symbol": normalized_symbol}, check_blocked=True, normalized_symbol=normalized_symbol)
            if data is None:
                return {}
            if isinstance(data, list) and data:
                return data[0]
            if isinstance(data, dict):
                return data
            return {}
        except requests.HTTPError as e:
            if hasattr(e, 'response') and e.response and e.response.status_code == 404:
                return {}
            logger.warning(f"FMP key_metrics_ttm({original_symbol} -> {normalized_symbol}) failed: {e}")
            return {}
        except Exception as e:
            logger.warning(f"FMP key_metrics_ttm({original_symbol} -> {normalized_symbol}) unexpected error: {e}")
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
        normalized_symbol = _normalize_symbol_for_fmp(symbol)
        try:
            data = self._get("ratios-ttm", params={"symbol": normalized_symbol}, check_blocked=True, normalized_symbol=normalized_symbol)
            if data is None:
                return {}
            if isinstance(data, list) and data:
                return data[0]
            if isinstance(data, dict):
                return data
            return {}
        except requests.HTTPError as e:
            if hasattr(e, 'response') and e.response and e.response.status_code == 404:
                return {}
            logger.warning(f"FMP ratios_ttm({original_symbol} -> {normalized_symbol}) failed: {e}")
            return {}
        except Exception as e:
            logger.warning(f"FMP ratios_ttm({original_symbol} -> {normalized_symbol}) unexpected error: {e}")
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
        normalized_symbol = _normalize_symbol_for_fmp(symbol)
        try:
            data = self._get("stock-news", params={"tickers": normalized_symbol, "limit": limit}, check_blocked=True, normalized_symbol=normalized_symbol)
            if data is None:
                return []
            if isinstance(data, list):
                return data[:limit]
            if isinstance(data, dict):
                return [data]
            return []
        except requests.HTTPError as e:
            if hasattr(e, 'response') and e.response and e.response.status_code == 404:
                return []
            logger.warning(f"FMP stock_news({original_symbol} -> {normalized_symbol}) failed: {e}")
            return []
        except Exception as e:
            logger.warning(f"FMP stock_news({original_symbol} -> {normalized_symbol}) unexpected error: {e}")
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
        
        Calls: GET /stable/technical-indicators/rsi
        Params: symbol=<normalized>, periodLength=<int>, timeframe=<string like "1day">
        
        Robustly handles multiple response formats:
        - {"rsi": 53.2}
        - [{"rsi": 53.2, "date": "..."}]
        - [{"value": 53.2, ...}]
        
        Args:
            symbol: Stock symbol
            period: RSI period (default 14) -> maps to periodLength
            interval: Data interval (default "1day") -> maps to timeframe
            limit: Number of data points (default 1 for latest)
            
        Returns:
            RSI value (float) if available, None otherwise
        """
        rsi_value, _ = self.technical_indicator_rsi_with_meta(symbol, period, interval, limit)
        return rsi_value

    def technical_indicator_rsi_with_meta(
        self,
        symbol: str,
        period: int = 14,
        interval: str = "1day",
        limit: int = 1
    ) -> Tuple[Optional[float], Dict[str, Any]]:
        """
        Get RSI technical indicator with diagnostics metadata.
        
        Args:
            symbol: Stock symbol
            period: RSI period (default 14) -> maps to periodLength
            interval: Data interval (default "1day") -> maps to timeframe
            limit: Number of data points (default 1 for latest)
            
        Returns:
            Tuple of (rsi_value, meta) where:
            - rsi_value: RSI value (float) if available, None otherwise
            - meta: Dict with {"ok": bool, "status": int|None, "error_type": str}
        """
        original_symbol = symbol
        normalized_symbol = _normalize_symbol_for_fmp(symbol)
        
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
                "symbol": normalized_symbol,
                "periodLength": period,
                "timeframe": timeframe,
            }
            
            data, meta = self._get_json("technical-indicators/rsi", params, "rsi", original_symbol)
            
            if data is None:
                return None, meta
            
            # Handle various response formats
            # Format 1: {"rsi": 53.2} or {"RSI": 53.2} or {"value": 53.2}
            if isinstance(data, dict):
                rsi = data.get("rsi") or data.get("RSI") or data.get("value")
                if rsi is not None:
                    try:
                        return float(rsi), meta
                    except (ValueError, TypeError):
                        pass
            
            # Format 2: [{"rsi": 53.2, "date": "..."}, ...] or [{"value": 53.2, ...}, ...]
            if isinstance(data, list) and data:
                # If list has "date" field, find most recent; otherwise take first element
                if len(data) > 0:
                    # Check if any element has "date" field
                    has_dates = any(isinstance(item, dict) and "date" in item for item in data)
                    
                    if has_dates:
                        # Sort by date descending and take most recent
                        try:
                            sorted_data = sorted(
                                [item for item in data if isinstance(item, dict) and "date" in item],
                                key=lambda x: x.get("date", ""),
                                reverse=True
                            )
                            if sorted_data:
                                latest = sorted_data[0]
                            else:
                                latest = data[0]
                        except Exception:
                            # Fallback to first element if sorting fails
                            latest = data[0]
                    else:
                        # No dates, take first element
                        latest = data[0]
                    
                    if isinstance(latest, dict):
                        rsi = latest.get("rsi") or latest.get("RSI") or latest.get("value")
                        if rsi is not None:
                            try:
                                return float(rsi), meta
                            except (ValueError, TypeError):
                                pass
            
            # Data exists but couldn't parse RSI value
            return None, {"ok": False, "status": meta.get("status"), "error_type": "parse_error"}
        except Exception as e:
            logger.debug(f"FMP technical_indicator_rsi({original_symbol} -> {normalized_symbol}) error: {e}")
            return None, {"ok": False, "status": None, "error_type": "parse_error"}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        retry=retry_if_exception_type(requests.HTTPError),
    )
    def financial_scores(self, symbol: str) -> Dict[str, Any]:
        """
        Get financial scores (Piotroski score, Altman Z-Score, etc.) for a symbol.
        
        Args:
            symbol: Stock symbol (e.g., "AAPL")
            
        Returns:
            Dictionary with financial scores, or {} if not found or on error
        """
        original_symbol = symbol
        normalized_symbol = _normalize_symbol_for_fmp(symbol)
        
        params = {"symbol": normalized_symbol}
        data, meta = self._get_json("financial-scores", params, "financial-scores", original_symbol)
        
        if data is None:
            return {}
        
        # Normalize to dict
        if isinstance(data, list) and data:
            return data[0]
        if isinstance(data, dict):
            return data
        return {}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        retry=retry_if_exception_type(requests.HTTPError),
    )
    def financial_statement_growth(
        self,
        symbol: str,
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Get financial statement growth data for a symbol.
        
        Calls: GET /stable/financial-growth
        Params: symbol=<normalized>
        
        Args:
            symbol: Stock symbol (e.g., "AAPL")
            limit: Maximum number of periods to return (default 5)
            
        Returns:
            List of growth records (most recent first), or [] if not found or on error
        """
        growth_data, _ = self.financial_statement_growth_with_meta(symbol, limit)
        return growth_data

    def financial_statement_growth_with_meta(
        self,
        symbol: str,
        limit: int = 5
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Get financial statement growth data with diagnostics metadata.
        
        Args:
            symbol: Stock symbol (e.g., "AAPL")
            limit: Maximum number of periods to return (default 5)
            
        Returns:
            Tuple of (growth_list, meta) where:
            - growth_list: List of growth records, or [] if not found
            - meta: Dict with {"ok": bool, "status": int|None, "error_type": str}
        """
        original_symbol = symbol
        normalized_symbol = _normalize_symbol_for_fmp(symbol)
        
        # Use correct endpoint: /stable/financial-growth (not financial-statement-growth)
        params = {"symbol": normalized_symbol}
        data, meta = self._get_json("financial-growth", params, "financial-growth", original_symbol)
        
        if data is None:
            return [], meta
        
        # Normalize to list
        if isinstance(data, list):
            # Return most recent records (limit to <= limit)
            result = data[:limit]
            return result, meta
        elif isinstance(data, dict):
            # Single record as dict -> convert to list
            return [data], meta
        
        return [], meta


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
