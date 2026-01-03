import os
import time
from typing import Any, Dict, Optional

import requests
from loguru import logger


class SchwabAuthError(RuntimeError):
    pass


def _normalize_symbol_for_chain(symbol: str) -> str:
    """
    Normalize symbol for Schwab option chain requests.
    
    Behavior:
    - Strip whitespace
    - Uppercase
    - Replace "." with "/" (share-class format): BRK.B -> BRK/B, BF.B -> BF/B
    
    Args:
        symbol: Stock symbol (e.g., "BRK.B" or "AAPL")
        
    Returns:
        Normalized symbol (e.g., "BRK/B" or "AAPL")
    """
    if not symbol:
        return symbol
    normalized = symbol.strip().upper()
    # Replace "." with "/" for share-class format (BRK.B -> BRK/B)
    normalized = normalized.replace(".", "/")
    return normalized


class SchwabMarketDataClient:
    """
    Minimal Schwab Market Data client for v1.
    - Uses ACCESS_TOKEN if provided
    - Otherwise refreshes using REFRESH_TOKEN via OAuth token endpoint
    """

    BASE_URL = "https://api.schwabapi.com"
    TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"

    def __init__(self) -> None:
        self.client_id = os.getenv("SCHWAB_CLIENT_ID")
        self.client_secret = os.getenv("SCHWAB_CLIENT_SECRET")

        self.access_token = os.getenv("SCHWAB_ACCESS_TOKEN")
        self.refresh_token = os.getenv("SCHWAB_REFRESH_TOKEN")

        # Optional: cache token in-memory for a run
        self._access_token_cached: Optional[str] = None
        self._access_token_expiry_epoch: Optional[float] = None

        # Parse symbol aliases from env var
        # Format: "BRK.B=BRK/B,BF.B=BF/B"
        self._symbol_aliases: Dict[str, str] = {}
        aliases_str = os.getenv("SCHWAB_CHAIN_SYMBOL_ALIASES", "")
        if aliases_str:
            for pair in aliases_str.split(","):
                pair = pair.strip()
                if "=" in pair:
                    original, alias = pair.split("=", 1)
                    self._symbol_aliases[original.strip().upper()] = alias.strip().upper()

        if not (self.access_token or self.refresh_token):
            raise SchwabAuthError(
                "Missing SCHWAB_ACCESS_TOKEN and SCHWAB_REFRESH_TOKEN. "
                "Provide SCHWAB_REFRESH_TOKEN (recommended) so we can refresh automatically."
            )

        if self.refresh_token and not (self.client_id and self.client_secret):
            raise SchwabAuthError(
                "Missing SCHWAB_CLIENT_ID or SCHWAB_CLIENT_SECRET (required for refresh token flow)."
            )

    def _get_bearer_token(self) -> str:
        # If user provided a fixed access token, use it
        if self.access_token:
            return self.access_token

        # If we have a cached token and it's not close to expiring, reuse
        now = time.time()
        if (
            self._access_token_cached
            and self._access_token_expiry_epoch
            and now < (self._access_token_expiry_epoch - 30)
        ):
            return self._access_token_cached

        # Otherwise refresh
        assert self.refresh_token is not None
        assert self.client_id is not None
        assert self.client_secret is not None

        logger.info("Refreshing Schwab access token using refresh token...")

        # Schwab token endpoint uses Basic auth with client_id:client_secret
        r = requests.post(
            self.TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            },
            auth=(self.client_id, self.client_secret),
            timeout=30,
        )

        if r.status_code != 200:
            raise SchwabAuthError(f"Token refresh failed: {r.status_code} {r.reason} | body={r.text[:500]}")

        payload = r.json()
        token = payload.get("access_token")
        expires_in = payload.get("expires_in", 1800)

        if not token:
            raise SchwabAuthError(f"Token refresh response missing access_token. body={str(payload)[:500]}")

        self._access_token_cached = token
        self._access_token_expiry_epoch = time.time() + int(expires_in)

        return token

    def _request(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None) -> Any:
        token = self._get_bearer_token()
        url = f"{self.BASE_URL}{path}"

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

        r = requests.request(method, url, headers=headers, params=params, timeout=30)

        if r.status_code >= 400:
            logger.error(f"Schwab API error: {r.status_code} {r.reason} | url={url} | body={r.text[:500]}")
            r.raise_for_status()

        # Some Schwab endpoints can return empty body on 200
        if not r.text:
            return None

        return r.json()

    def get_option_chain(self, symbol: str, **kwargs) -> dict:
        """
        Schwab option chain endpoint with robust parameter handling and symbol normalization.
        
        Uses minimal known-good parameter set first, with fallback on 400 errors.
        Normalizes symbols and applies aliases before making requests.
        
        Args:
            symbol: Stock symbol (e.g., "AAPL", "BRK.B")
            **kwargs: Optional parameters (for future extensibility)
            
        Returns:
            Dictionary with option chain data, or {} if empty/no data, or error dict with "_error_type": "invalid_symbol" if symbol is invalid
        """
        symbol_original = symbol
        
        # Apply alias if present (case-insensitive match)
        symbol_to_use = symbol_original.strip().upper()
        if symbol_to_use in self._symbol_aliases:
            symbol_to_use = self._symbol_aliases[symbol_to_use]
            logger.debug(f"Symbol alias applied: {symbol_original} -> {symbol_to_use}")
        
        # Normalize symbol for Schwab (BRK.B -> BRK/B)
        symbol_request = _normalize_symbol_for_chain(symbol_to_use)
        
        if symbol_request != symbol_original:
            logger.debug(f"Symbol normalized: {symbol_original} -> {symbol_request}")
        
        token = self._get_bearer_token()
        url = f"{self.BASE_URL}/marketdata/v1/chains"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        
        # Known-good minimal parameter set
        params = {
            "symbol": symbol_request,
            "includeUnderlyingQuote": "true",
            "strategy": "SINGLE",
            "contractType": "PUT",
        }
        
        # Apply any additional kwargs
        params.update(kwargs)
        
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            
            if r.status_code == 400:
                # Log warning with params (safe; not secret)
                logger.warning(
                    f"Schwab option chain 400 error for {symbol_original} (request: {symbol_request}), retrying with minimal params. "
                    f"Original params: {params}"
                )
                
                # Retry with minimal fallback params
                fallback_params = {
                    "symbol": symbol_request,
                    "includeUnderlyingQuote": "true",
                }
                
                r = requests.get(url, headers=headers, params=fallback_params, timeout=30)
                
                if r.status_code == 400:
                    # Fallback also failed - likely invalid symbol
                    error_body = r.text[:500]
                    logger.debug(
                        f"Schwab option chain 400 error for {symbol_original} (request: {symbol_request}) even with minimal params. "
                        f"Treating as invalid symbol."
                    )
                    # Return error dict instead of raising
                    return {
                        "_error_type": "invalid_symbol",
                        "_symbol_request": symbol_request,
                        "_symbol_original": symbol_original,
                        "_body": error_body,
                    }
            
            if r.status_code >= 400:
                logger.error(f"Schwab API error: {r.status_code} {r.reason} | url={url} | body={r.text[:500]}")
                r.raise_for_status()
            
            # Handle empty response
            if not r.text:
                logger.debug(f"Schwab option chain empty response for {symbol_original} (request: {symbol_request})")
                return {}
            
            data = r.json()
            
            # Check if response has any option data
            if isinstance(data, dict):
                # Check for common option data keys
                has_data = any(
                    key in data 
                    for key in ("putExpDateMap", "callExpDateMap", "expirations", "expirationDates", "puts", "calls")
                )
                if not has_data:
                    logger.debug(f"Schwab option chain response for {symbol_original} (request: {symbol_request}) has no option data (keys: {list(data.keys())[:10]})")
                    return {}
            
            return data if isinstance(data, dict) else {}
            
        except requests.HTTPError as e:
            # Re-raise HTTP errors (already logged above)
            raise
        except Exception as e:
            logger.error(f"Schwab option chain unexpected error for {symbol_original} (request: {symbol_request}): {e}")
            raise
