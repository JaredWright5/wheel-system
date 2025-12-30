from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from loguru import logger

from wheel.clients.schwab_client import SchwabClient
from wheel.clients.supabase_client import get_supabase


def snapshot_schwab_account(account_id: Optional[str] = None) -> Dict[str, Any]:
    schwab = SchwabClient.from_env()
    sb = get_supabase()

    acct = schwab.get_account(fields="positions")
    logger.info("Account fetched OK.")

    # defensive parse
    raw = acct
    sec = acct.get("securitiesAccount", acct) if isinstance(acct, dict) else {}
    positions = sec.get("positions", []) if isinstance(sec, dict) else []

    # balances shape varies; keep best-effort
    cash = None
    net_liq = None
    try:
        balances = sec.get("currentBalances", {}) or sec.get("initialBalances", {}) or {}
        cash = balances.get("cashBalance") or balances.get("cashAvailableForTrading")
        net_liq = balances.get("liquidationValue") or balances.get("netLiquidation")
    except Exception:
        pass

    # Get account hash for storage
    account_hash = schwab._resolve_account_hash()
    
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "source": "schwab",
        "account_id": str(account_hash),
        "cash": cash,
        "net_liquidation": net_liq,
        "positions": positions,
        "raw": raw,
    }

    res = sb.table("broker_snapshots").insert(row).execute()
    logger.info(f"broker_snapshots inserted: {res.data[0].get('id') if res.data else 'ok'}")
    return row

