from __future__ import annotations

import base64
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests
from loguru import logger


@dataclass
class SchwabConfig:
    client_id: str
    client_secret: str
    refresh_token: str
    account_id: Optional[str] = None


class SchwabClient:
    """
    Minimal Schwab API client:
    - refresh_token -> access_token
    - GET endpoints used for read-only validation and tracking
    """

    OAUTH_TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"
    TRADER_BASE = "https://api.schwabapi.com/trader/v1"

    def __init__(self, config: SchwabConfig, timeout_s: int = 20):
        self.cfg = config
        self.timeout_s = timeout_s
        self._access_token: Optional[str] = None
        self._access_token_expiry_epoch: float = 0.0

    @classmethod
    def from_env(cls) -> "SchwabClient":
        client_id = os.environ.get("SCHWAB_CLIENT_ID", "").strip()
        client_secret = os.environ.get("SCHWAB_CLIENT_SECRET", "").strip()
        refresh_token = os.environ.get("SCHWAB_REFRESH_TOKEN", "").strip()
        account_id = os.environ.get("SCHWAB_ACCOUNT_ID", "").strip() or None

        missing = [k for k, v in {
            "SCHWAB_CLIENT_ID": client_id,
            "SCHWAB_CLIENT_SECRET": client_secret,
            "SCHWAB_REFRESH_TOKEN": refresh_token,
        }.items() if not v]

        if missing:
            raise RuntimeError(f"Missing required env vars: {missing}")

        return cls(SchwabConfig(
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            account_id=account_id,
        ))

    def _basic_auth_header(self) -> str:
        raw = f"{self.cfg.client_id}:{self.cfg.client_secret}".encode()
        return "Basic " + base64.b64encode(raw).decode()

    def refresh_access_token(self) -> str:
        """
        Refresh token -> access token.
        Stores access token in memory with an expiry buffer.
        """
        headers = {
            "Authorization": self._basic_auth_header(),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.cfg.refresh_token,
        }

        r = requests.post(self.OAUTH_TOKEN_URL, headers=headers, data=data, timeout=self.timeout_s)
        if r.status_code >= 400:
            # Don't log secrets/tokens
            logger.error(f"Schwab token refresh failed: {r.status_code} {r.reason} | body={r.text[:300]}")
            r.raise_for_status()

        payload = r.json()
        token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 1800))

        if not token:
            raise RuntimeError(f"Token refresh response missing access_token: {payload}")

        # store with a 60s safety buffer
        self._access_token = token
        self._access_token_expiry_epoch = time.time() + max(0, expires_in - 60)
        return token

    def access_token(self) -> str:
        if self._access_token and time.time() < self._access_token_expiry_epoch:
            return self._access_token
        return self.refresh_access_token()

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.TRADER_BASE}{path}"
        token = self.access_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

        # Simple retry for rate limit / intermittent
        for attempt in range(1, 4):
            r = requests.request(method, url, headers=headers, params=params, timeout=self.timeout_s)

            if r.status_code == 401 and attempt < 3:
                # token expired/invalid -> refresh and retry
                logger.warning("Schwab 401; refreshing token and retrying...")
                self.refresh_access_token()
                headers["Authorization"] = f"Bearer {self._access_token}"
                continue

            if r.status_code == 429 and attempt < 3:
                backoff = attempt * 2
                logger.warning(f"Schwab 429 rate limit; sleeping {backoff}s and retrying...")
                time.sleep(backoff)
                continue

            if r.status_code >= 400:
                logger.error(f"Schwab API error: {r.status_code} {r.reason} | url={url} | body={r.text[:300]}")
                r.raise_for_status()

            if not r.text:
                return None
            return r.json()

        raise RuntimeError("Schwab request failed after retries")

    # --- Read-only endpoints we need for v1 tracking ---

    def get_accounts(self) -> Any:
        return self._request("GET", "/accounts")

    def get_account(self, account_id: str, fields: str = "positions") -> Any:
        # fields can include positions/orders; we'll keep minimal
        return self._request("GET", f"/accounts/{account_id}", params={"fields": fields})

    def get_orders(self, account_id: str, from_date: str, to_date: str) -> Any:
        return self._request("GET", f"/accounts/{account_id}/orders", params={"fromEnteredTime": from_date, "toEnteredTime": to_date})

    def get_transactions(self, account_id: str, start_date: str, end_date: str) -> Any:
        return self._request("GET", f"/accounts/{account_id}/transactions", params={"startDate": start_date, "endDate": end_date})

