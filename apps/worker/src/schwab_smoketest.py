from loguru import logger
from wheel.clients.schwab_client import SchwabClient

def main():
    logger.info("Starting Schwab smoketest...")

    schwab = SchwabClient.from_env()

    accounts = schwab.get_accounts()
    logger.info(f"Accounts response type={type(accounts)}")

    # ✅ Use account HASH (hashValue) for Trader API endpoints
    account_hash = schwab.resolve_account_hash(accounts)
    logger.info(f"Resolved account_hash={account_hash}")

    acct = schwab.get_account(account_hash, fields="positions")
    logger.info(f"✅ Account OK. keys={list(acct.keys()) if isinstance(acct, dict) else type(acct)}")

if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()

