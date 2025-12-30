from loguru import logger
from wheel.clients.broker_snapshot import snapshot_schwab_account

def main():
    logger.info("Starting daily tracker...")
    
    # Snapshot Schwab account (positions, balances)
    snapshot_schwab_account()
    
    logger.info("daily_tracker completed.")

if __name__ == "__main__":
    main()

