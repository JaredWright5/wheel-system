from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from loguru import logger

from wheel.clients.schwab_client import SchwabClient
from wheel.clients.supabase_client import get_supabase, insert_row, upsert_rows


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def snapshot_schwab_account() -> Dict[str, Any]:
    """
    Pull account + positions from Schwab and write:
      - screening_runs row (notes='DAILY_TRACKER')
      - account_snapshots (1 row)
      - position_snapshots (N rows)
    """
    sb = get_supabase()
    schwab = SchwabClient.from_env()

    run_ts = _utc_now_iso()
    account_hash = schwab._resolve_account_hash()

    logger.info(f"Schwab snapshot: account_hash={account_hash}")

    # 1) Create a run row we can link everything to
    run_row = insert_row(
        "screening_runs",
        {
            "run_ts": run_ts,
            "universe_size": 0,
            "notes": "DAILY_TRACKER",
        },
    )
    run_id = run_row["run_id"]
    logger.info(f"Created run_id={run_id}")

    # 2) Pull account with positions
    acct = schwab.get_account(fields="positions")  # returns dict
    raw = acct if isinstance(acct, dict) else {"data": acct}

    # Schwab payloads vary; we do best-effort extraction
    balances = {}
    # common places we've seen balances live
    for key in ["securitiesAccount", "account"]:
        if isinstance(raw.get(key), dict):
            balances = raw.get(key).get("currentBalances") or raw.get(key).get("balances") or {}
            if balances:
                break

    account_snapshot = {
        "run_id": run_id,
        "run_ts": run_ts,
        "account_hash": account_hash,
        "net_liquidation": balances.get("liquidationValue") or balances.get("netLiquidation"),
        "cash": balances.get("cashBalance") or balances.get("cashAvailableForTrading"),
        "buying_power": balances.get("buyingPower"),
        "maintenance_requirement": balances.get("maintenanceRequirement"),
        "raw": raw,
    }
    insert_row("account_snapshots", account_snapshot)
    logger.info("Inserted account_snapshots row")

    # 3) Positions
    positions: List[Dict[str, Any]] = []
    sec = raw.get("securitiesAccount") if isinstance(raw.get("securitiesAccount"), dict) else raw
    if isinstance(sec, dict):
        positions = sec.get("positions") or []

    pos_rows = []
    for p in positions:
        if not isinstance(p, dict):
            continue
        instr = p.get("instrument") or {}
        symbol = instr.get("symbol") or instr.get("underlyingSymbol") or instr.get("description") or "UNKNOWN"
        asset_type = instr.get("assetType")

        pos_rows.append(
            {
                "run_id": run_id,
                "run_ts": run_ts,
                "account_hash": account_hash,
                "symbol": symbol,
                "asset_type": asset_type,
                "quantity": p.get("longQuantity") or p.get("shortQuantity") or p.get("quantity"),
                "average_price": p.get("averagePrice") or p.get("averageLongPrice") or p.get("averageShortPrice"),
                "market_value": p.get("marketValue"),
                "day_pnl": p.get("currentDayProfitLoss"),
                "total_pnl": p.get("currentDayProfitLossPercentage"),  # placeholder; varies by payload
                "raw": p,
            }
        )

    # avoid Supabase "ON CONFLICT DO UPDATE cannot affect row twice"
    # by deduping symbols within this payload
    seen = set()
    deduped = []
    for r in pos_rows:
        k = (r["run_id"], r["symbol"])
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)

    if deduped:
        upsert_rows("position_snapshots", deduped)
        logger.info(f"Upserted position_snapshots: {len(deduped)}")
    else:
        logger.info("No positions found (position_snapshots skipped)")

    return {"run_id": run_id, "run_ts": run_ts, "account_hash": account_hash, "positions": len(deduped)}
