from dotenv import load_dotenv
from loguru import logger
from wheel.clients.broker_snapshot import snapshot_schwab_account

# Load environment variables from .env.local
load_dotenv(".env.local")

def main():
    logger.info("Starting daily tracker...")
    result = snapshot_schwab_account()
    logger.info(f"daily_tracker completed: {result}")

if __name__ == "__main__":
    main()
