from dotenv import load_dotenv
from loguru import logger

from wheel.clients.supabase_client import get_supabase

# Load environment variables from .env.local
load_dotenv(".env.local")


def main():
    logger.info("Starting database smoketest...")
    
    try:
        sb = get_supabase()
        
        # Simple query: select one row from screening_runs
        res = (
            sb.table("screening_runs")
            .select("run_id, run_ts, universe_size")
            .limit(1)
            .execute()
        )
        
        data = res.data or []
        if data:
            logger.info(f"✅ Database smoketest success. Found {len(data)} row(s)")
            logger.info(f"Sample row: run_id={data[0].get('run_id')}, run_ts={data[0].get('run_ts')}")
        else:
            logger.warning("✅ Database connection OK, but no rows found in screening_runs (table may be empty)")
        
    except Exception as e:
        logger.exception("❌ Database smoketest failed")
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import sys
        sys.exit(1)

