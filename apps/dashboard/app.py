"""
Dashboard web application for wheel system.
Displays screening runs, candidates, and picks.
"""
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv
from loguru import logger
import json
import os

from wheel.clients.supabase_client import get_supabase, select_all

# Load environment variables
load_dotenv(".env.local", override=False)

app = FastAPI()
templates = Jinja2Templates(directory="apps/dashboard/templates")

# Initialize Supabase client
try:
    sb = get_supabase()
except Exception as e:
    logger.error(f"Failed to initialize Supabase: {e}")
    sb = None


def _safe_select(table_or_view: str, limit: int = 100) -> tuple[list, bool]:
    """
    Safely select from a table or view.
    Returns (data, has_error) where has_error is True if an exception occurred.
    """
    if not sb:
        return [], True
    
    try:
        data = select_all(table_or_view, limit=limit)
        return (data, False)
    except Exception as e:
        logger.error(f"Error selecting from {table_or_view}: {e}")
        return ([], True)


def _parse_trade_card(pick: dict) -> dict:
    """
    Safely parse pick_metrics.trade_card from a pick row.
    Returns empty dict if parsing fails.
    """
    if not pick:
        return {}
    
    try:
        pick_metrics = pick.get("pick_metrics")
        if isinstance(pick_metrics, str):
            pick_metrics = json.loads(pick_metrics)
        
        if isinstance(pick_metrics, dict):
            trade_card = pick_metrics.get("trade_card", {})
            if isinstance(trade_card, dict):
                return trade_card
    except (json.JSONDecodeError, TypeError, AttributeError) as e:
        logger.debug(f"Failed to parse trade_card: {e}")
    
    return {}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Dashboard home page showing summary and latest data."""
    # Fetch latest run history
    runs, runs_error = _safe_select("v_run_history", limit=10)
    
    # Fetch latest top 25 candidates
    candidates, candidates_error = _safe_select("v_latest_run_top25_candidates", limit=25)
    
    # Fetch latest CSP picks (limit to top few for home page)
    csp_picks, csp_error = _safe_select("v_latest_run_csp_picks", limit=5)
    
    # Fetch latest CC picks (limit to top few for home page)
    cc_picks, cc_error = _safe_select("v_latest_run_cc_picks", limit=5)
    
    # Fetch best CSP pick for latest run
    best_csp_data, best_csp_error = _safe_select("v_latest_run_best_csp_pick", limit=1)
    best_csp_pick = best_csp_data[0] if best_csp_data else None
    best_trade_card = _parse_trade_card(best_csp_pick) if best_csp_pick else {}
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "runs": runs,
            "runs_error": runs_error,
            "candidates": candidates,
            "candidates_error": candidates_error,
            "csp_picks": csp_picks,
            "csp_error": csp_error,
            "cc_picks": cc_picks,
            "cc_error": cc_error,
            "best_csp_pick": best_csp_pick,
            "best_csp_error": best_csp_error,
            "best_trade_card": best_trade_card,
        },
    )


@app.get("/runs", response_class=HTMLResponse)
async def runs(request: Request):
    """Run history page."""
    runs, runs_error = _safe_select("v_run_history", limit=100)
    
    return templates.TemplateResponse(
        "runs.html",
        {
            "request": request,
            "runs": runs,
            "runs_error": runs_error,
        },
    )


@app.get("/candidates", response_class=HTMLResponse)
async def candidates(request: Request):
    """Latest candidates page."""
    candidates, candidates_error = _safe_select("v_latest_run_top25_candidates", limit=25)
    
    return templates.TemplateResponse(
        "candidates.html",
        {
            "request": request,
            "candidates": candidates,
            "candidates_error": candidates_error,
        },
    )


@app.get("/picks", response_class=HTMLResponse)
async def picks(request: Request, mode: str = "best"):
    """Picks page - shows CSP and CC picks."""
    # Validate mode
    if mode not in ("best", "all"):
        mode = "best"
    
    # Fetch CSP picks based on mode
    if mode == "best":
        # Fetch best CSP pick only
        best_csp_data, csp_error = _safe_select("v_latest_run_best_csp_pick", limit=1)
        csp_picks = best_csp_data if best_csp_data else []
    else:
        # Fetch all CSP picks
        csp_picks, csp_error = _safe_select("v_latest_run_csp_picks", limit=100)
    
    # Fetch CC picks (unchanged)
    cc_picks, cc_error = _safe_select("v_latest_run_cc_picks", limit=100)
    
    # Parse trade_card for best pick if in best mode
    best_trade_card = {}
    if mode == "best" and csp_picks:
        best_trade_card = _parse_trade_card(csp_picks[0])
    
    return templates.TemplateResponse(
        "picks.html",
        {
            "request": request,
            "csp_picks": csp_picks,
            "csp_error": csp_error,
            "cc_picks": cc_picks,
            "cc_error": cc_error,
            "mode": mode,
            "best_trade_card": best_trade_card,
        },
    )


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok", "supabase": sb is not None}
