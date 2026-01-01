from datetime import datetime, timezone
from loguru import logger

from wheel.clients.supabase_client import insert_row

def main():
    logger.info("Starting Supabase smoketest...")
    row = insert_row("screening_runs", {
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "universe_size": 0,
        "notes": "SMOKETEST"
    })
    logger.info(f"Insert response: {row}")
    if not row.get("run_id"):
        raise RuntimeError("Smoketest failed: no run_id returned from Supabase insert.")
    logger.info("✅ Supabase smoketest success.")

if __name__ == "__main__":
    main()

