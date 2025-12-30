from __future__ import annotations

from datetime import datetime, timezone, timedelta
from loguru import logger

from wheel.clients.schwab_client import SchwabClient


def main():
    logger.info("Starting Schwab smoketest...")
    schwab = SchwabClient.from_env()

    accounts = schwab.get_accounts()
    logger.info(f"Accounts response type={type(accounts)}")

    # Schwab Trader API uses account HASH in the URL path
    account_hash = schwab.resolve_account_hash()
    acct = schwab.get_account(account_hash, fields="positions")
    logger.info(f"Account fetched OK. account_hash={account_hash}")

    # Simple position count
    positions = []
    try:
        # payload shapes vary; be defensive
        if isinstance(acct, dict):
            # possible nesting
            positions = (
                acct.get("securitiesAccount", {})
                .get("positions", [])
                or acct.get("positions", [])
                or []
            )
    except Exception:
        positions = []

    logger.info(f"Positions count: {len(positions)}")

    # Recent orders sanity test (last 7 days)
    now = datetime.now(timezone.utc)
    frm = (now - timedelta(days=7)).isoformat()
    to = now.isoformat()

    try:
        orders = schwab.get_orders(account_hash, frm, to)
        if orders is None:
            orders = []
        logger.info(f"Orders (last 7d) count: {len(orders) if isinstance(orders, list) else 'non-list'}")
    except Exception as e:
        logger.warning(f"Orders endpoint check failed (non-fatal): {e}")

    logger.info("✅ Schwab smoketest success.")


if __name__ == "__main__":
    main()

