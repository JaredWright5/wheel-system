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

def upsert_rows(table: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    sb = get_supabase()
    res = sb.table(table).upsert(rows).execute()
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
