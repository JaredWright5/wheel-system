import os
from typing import Any, Dict, List, Optional
from supabase import create_client, Client

def get_supabase() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)

def upsert_rows(table: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    sb = get_supabase()
    sb.table(table).upsert(rows).execute()

def insert_row(table: str, row: Dict[str, Any]) -> Dict[str, Any]:
    sb = get_supabase()
    res = sb.table(table).insert(row).execute()
    data = res.data or []
    return data[0] if data else {}

def fetch_one(table: str, filters: Dict[str, Any], select: str="*") -> Optional[Dict[str, Any]]:
    sb = get_supabase()
    q = sb.table(table).select(select)
    for k, v in filters.items():
        q = q.eq(k, v)
    res = q.limit(1).execute()
    data = res.data or []
    return data[0] if data else None

