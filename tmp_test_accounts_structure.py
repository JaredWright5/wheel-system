from loguru import logger
from wheel.clients.schwab_client import SchwabClient

def main():
    logger.info("Starting Schwab smoketest...")
    schwab = SchwabClient.from_env()

    accounts = schwab.get_accounts()   # the actual method name
    logger.info(f"Accounts response type={type(accounts)} len={len(accounts) if isinstance(accounts, list) else 'N/A'}")
    logger.info(f"Accounts[0] keys={list(accounts[0].keys()) if isinstance(accounts, list) and accounts else None}")
    logger.info(f"Accounts[0] sample={accounts[0] if isinstance(accounts, list) and accounts else None}")

if __name__ == "__main__":
    main()

