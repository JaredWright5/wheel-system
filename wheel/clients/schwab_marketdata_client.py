import os
import time
from typing import Any, Dict, Optional

import requests
from loguru import logger


class SchwabAuthError(RuntimeError):
    pass


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

    def get_option_chain(self, symbol: str, *, contract_type: str = "PUT", strike_count: int = 50) -> Any:
        """
        Schwab option chain endpoint.
        We keep params minimal + robust.
        """
        # Common query params (best-effort; Schwab params may evolve)
        params = {
            "symbol": symbol,
            "contractType": contract_type,  # "PUT" for CSPs
            "strikeCount": strike_count,
            # You can optionally add these later:
            # "includeQuotes": "TRUE",
            # "strategy": "SINGLE",
        }
        return self._request("GET", "/marketdata/v1/chains", params=params)

