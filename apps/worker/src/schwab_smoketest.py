from loguru import logger
from wheel.clients.schwab_client import SchwabClient

def main():
    logger.info("Starting Schwab smoketest...")
    schwab = SchwabClient.from_env()

    # Single-account mode: SchwabClient resolves and caches account hashValue internally
    acct = schwab.get_account(fields="positions")

    logger.info("✅ Schwab smoketest success.")
    if isinstance(acct, dict):
        logger.info(f"Account keys: {list(acct.keys())}")

if __name__ == "__main__":
    main()
