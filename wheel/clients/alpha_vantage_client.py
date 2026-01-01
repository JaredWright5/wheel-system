"""
Alpha Vantage API client for technical indicators (RSI).
Includes throttling to respect rate limits (~5 requests/minute).
"""
import os
import time
from typing import Optional
from datetime import datetime

import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

BASE_URL = "https://www.alphavantage.co/query"
VERSION = "alpha_vantage_v1"


class AlphaVantageClient:
    """Client for Alpha Vantage API, focused on technical indicators with throttling."""

    def __init__(self, api_key: Optional[str] = None, timeout: int = 30, requests_per_minute: float = 5.0):
        self.api_key = api_key or os.getenv("ALPHAVANTAGE_API_KEY")
        if not self.api_key:
            raise RuntimeError("Missing ALPHAVANTAGE_API_KEY environment variable")
        self.timeout = timeout
        self.requests_per_minute = requests_per_minute
        self.min_interval_seconds = 60.0 / requests_per_minute
        
        # Throttling state
        self._last_request_time: float = 0.0

    def _throttle(self):
        """Simple throttle: ensure minimum interval between requests."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self.min_interval_seconds:
            sleep_time = self.min_interval_seconds - elapsed
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=2, max=20),
        retry=retry_if_exception_type(requests.HTTPError),
    )
    def get_rsi(
        self,
        symbol: str,
        interval: str = "daily",
        period: int = 14,
    ) -> Optional[float]:
        """
        Get RSI technical indicator for a symbol.
        
        Args:
            symbol: Stock symbol (e.g., "AAPL")
            interval: Data interval (1min, 5min, 15min, 30min, 60min, daily, weekly, monthly)
            period: Number of data points used to calculate RSI (default 14)
            
        Returns:
            RSI value (float) if available, None otherwise
        """
        # Throttle requests to respect rate limits
        self._throttle()
        
        try:
            params = {
                "function": "RSI",
                "symbol": symbol,
                "interval": interval,
                "time_period": period,
                "series_type": "close",
                "apikey": self.api_key,
            }
            
            response = requests.get(BASE_URL, params=params, timeout=self.timeout)
            
            # Alpha Vantage returns 200 even for errors, check response content
            if response.status_code != 200:
                logger.warning(f"Alpha Vantage RSI({symbol}) HTTP error: {response.status_code}")
                return None
            
            data = response.json()
            
            # Check for API errors in response
            if "Error Message" in data:
                logger.warning(f"Alpha Vantage RSI({symbol}) error: {data['Error Message']}")
                return None
            
            if "Note" in data:
                # Rate limit message
                logger.warning(f"Alpha Vantage RSI({symbol}) rate limited: {data['Note'][:200]}")
                return None
            
            # Extract RSI from Technical Analysis: RSI data
            rsi_data = data.get("Technical Analysis: RSI", {})
            if not rsi_data:
                logger.debug(f"Alpha Vantage RSI({symbol}): no RSI data in response")
                return None
            
            # Get the most recent RSI value (keys are timestamps, sorted descending)
            # Format: "2024-01-01" or "2024-01-01 12:00:00"
            sorted_keys = sorted(rsi_data.keys(), reverse=True)
            if not sorted_keys:
                return None
            
            latest = rsi_data[sorted_keys[0]]
            rsi_str = latest.get("RSI")
            
            if rsi_str is None:
                return None
            
            try:
                rsi_value = float(rsi_str)
                return rsi_value
            except (ValueError, TypeError):
                logger.warning(f"Alpha Vantage RSI({symbol}): invalid RSI value '{rsi_str}'")
                return None
                
        except requests.HTTPError as e:
            logger.warning(f"Alpha Vantage RSI({symbol}) HTTP error: {e}")
            return None
        except Exception as e:
            logger.warning(f"Alpha Vantage RSI({symbol}) unexpected error: {e}")
            return None

