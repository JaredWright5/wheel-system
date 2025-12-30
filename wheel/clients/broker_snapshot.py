from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from loguru import logger

from wheel.clients.schwab_client import SchwabClient
from wheel.clients.supabase_client import get_supabase


def snapshot_schwab_account(account_id: Optional[str] = None) -> Dict[str, Any]:
    schwab = SchwabClient.from_env()
    sb = get_supabase()

    # determine account_id
    if not account_id:
        if schwab.cfg.account_id:
            account_id = schwab.cfg.account_id
        else:
            accts = schwab.get_accounts()
            if isinstance(accts, list) and accts:
                account_id = accts[0].get("accountNumber") or accts[0].get("hashValue") or accts[0].get("accountId")

    if not account_id:
        raise RuntimeError("No Schwab account_id found. Set SCHWAB_ACCOUNT_ID env var.")

    acct = schwab.get_account(account_id, fields="positions")

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

    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "source": "schwab",
        "account_id": str(account_id),
        "cash": cash,
        "net_liquidation": net_liq,
        "positions": positions,
        "raw": raw,
    }

    res = sb.table("broker_snapshots").insert(row).execute()
    logger.info(f"broker_snapshots inserted: {res.data[0].get('id') if res.data else 'ok'}")
    return row

