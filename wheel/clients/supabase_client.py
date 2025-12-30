import os
from typing import Any, Dict, List, Optional
from supabase import create_client, Client

def get_supabase() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)

def _raise_if_error(res, context: str) -> None:
    # supabase-py returns an object with .data and .error
    err = getattr(res, "error", None)
    if err:
        raise RuntimeError(f"Supabase error during {context}: {err}")

def upsert_rows(table: str, rows: List[Dict[str, Any]], *, key: Optional[str] = None) -> None:
    """Upsert rows into Supabase, safely deduping within the batch by conflict key.

    Postgres error 21000 happens when the same unique key appears twice in ONE upsert call.
    We dedupe in-Python to ensure each constrained key appears once per request.
    """
    if not rows:
        return None

    # sensible defaults
    if key is None:
        if table == "tickers":
            key = "ticker"
        elif table in ("screening_candidates", "screening_picks"):
            key = "id"
        else:
            key = "id"

    deduped = {}
    for r in rows:
        k = r.get(key)
        if k is None:
            # if key missing, keep row but don't use it for dedupe
            # (these rows may fail server-side; better to catch upstream later)
            k = f"__missing__{len(deduped)}"
        deduped[k] = r  # last one wins

    payload = list(deduped.values())
    sb = get_supabase()
    res = sb.table(table).upsert(payload).execute()
    _raise_if_error(res, f"upsert_rows({table})")

def insert_row(table: str, row: Dict[str, Any]) -> Dict[str, Any]:
    sb = get_supabase()
    res = sb.table(table).insert(row).execute()
    _raise_if_error(res, f"insert_row({table})")
    data = res.data or []
    return data[0] if data else {}

def update_rows(table: str, match: Dict[str, Any], values: Dict[str, Any]) -> None:
    sb = get_supabase()
    q = sb.table(table).update(values)
    for k, v in match.items():
        q = q.eq(k, v)
    res = q.execute()
    _raise_if_error(res, f"update_rows({table})")
