from __future__ import annotations

from datetime import datetime, timezone, timedelta
from loguru import logger

from wheel.clients.schwab_client import SchwabClient


def main():
    logger.info("Starting Schwab smoketest...")
    schwab = SchwabClient.from_env()

    accounts = schwab.get_accounts()
    logger.info(f"Accounts response type={type(accounts)}")

    # Pick account id: env var first, else first account in list
    account_id = schwab.cfg.account_id
    if not account_id:
        # Schwab often returns a list of account objects; try to find an id
        if isinstance(accounts, list) and accounts:
            account_id = accounts[0].get("accountNumber") or accounts[0].get("hashValue") or accounts[0].get("accountId")
        if not account_id:
            raise RuntimeError("Could not determine SCHWAB_ACCOUNT_ID from /accounts response. Set SCHWAB_ACCOUNT_ID in env.")

    acct = schwab.get_account(account_id, fields="positions")
    logger.info(f"Account fetched OK. account_id={account_id}")

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
        orders = schwab.get_orders(account_id, frm, to)
        if orders is None:
            orders = []
        logger.info(f"Orders (last 7d) count: {len(orders) if isinstance(orders, list) else 'non-list'}")
    except Exception as e:
        logger.warning(f"Orders endpoint check failed (non-fatal): {e}")

    logger.info("✅ Schwab smoketest success.")


if __name__ == "__main__":
    main()

