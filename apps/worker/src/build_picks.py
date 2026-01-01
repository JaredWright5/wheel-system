from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from loguru import logger

from wheel.clients.supabase_client import get_supabase, upsert_rows

# Load environment variables from .env.local
load_dotenv(".env.local")

TOP_N = int(__import__("os").getenv("PICKS_TOP_N", "25"))


def _latest_run_id() -> Optional[str]:
    sb = get_supabase()
    res = (
        sb.table("screening_runs")
        .select("run_id, run_ts")
        .order("run_ts", desc=True)
        .limit(1)
        .execute()
    )
    data = res.data or []
    if not data:
        return None
    return data[0]["run_id"]


def _fetch_candidates(run_id: str) -> List[Dict[str, Any]]:
    sb = get_supabase()

    # For your universe size (~168), one page is enough.
    res = (
        sb.table("screening_candidates")
        .select("*")
        .eq("run_id", run_id)
        .limit(2000)
        .execute()
    )
    return res.data or []


def main():
    logger.info("Starting build_picks...")

    run_id = _latest_run_id()
    if not run_id:
        raise RuntimeError("No rows found in screening_runs. Run weekly_screener first.")

    cands = _fetch_candidates(run_id)
    if not cands:
        raise RuntimeError(f"No rows found in screening_candidates for run_id={run_id}")

    # Sort by score desc, nulls last
    def sort_key(r: Dict[str, Any]):
        s = r.get("score")
        return (-float(s) if s is not None else 10**12)

    cands_sorted = sorted(cands, key=sort_key)
    picks = cands_sorted[:TOP_N]

    now_ts = datetime.now(timezone.utc).isoformat()

    pick_rows: List[Dict[str, Any]] = []
    for rank, c in enumerate(picks, start=1):
        # Keep this v1 simple: CSP action only.
        pick_rows.append(
            {
                "run_id": run_id,
                "ticker": c.get("ticker"),
                "action": "CSP",

                # placeholders for options-engine v2
                "dte": None,
                "target_delta": None,
                "strike": None,
                "premium": None,
                "annualized_yield": None,

                # helpful columns for dashboards
                "score": c.get("score"),
                "rank": rank,
                "price": c.get("price"),
                "iv": c.get("iv"),
                "iv_rank": c.get("iv_rank"),
                "beta": c.get("beta"),
                "rsi": c.get("rsi"),
                "earn_in_days": c.get("earn_in_days"),
                "sentiment_score": c.get("sentiment_score"),

                # full snapshot (so we can debug + iterate)
                "pick_metrics": c.get("metrics") or c,

                "created_at": now_ts,
                "updated_at": now_ts,
            }
        )

    # Dedupe protection: composite unique per run/ticker/action
    # (Matches the pattern you already use in supabase_client.py)
    logger.info(f"Upserting screening_picks: {len(pick_rows)} for run_id={run_id}")
    upsert_rows("screening_picks", pick_rows, keys=["run_id", "ticker", "action"])
    logger.info("✅ screening_picks upserted successfully")
    logger.info("build_picks complete.")


if __name__ == "__main__":
    main()

