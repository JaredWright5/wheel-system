"""
Dashboard v1: FastAPI web app for viewing screening results
"""
import os
from typing import List, Dict, Any

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from loguru import logger

from wheel.clients.supabase_client import select_all

# Load environment variables from .env.local if it exists (for local dev)
# On Render, environment variables are provided directly
env_file = Path(".env.local")
if env_file.exists():
    from dotenv import load_dotenv
    load_dotenv(".env.local")

app = FastAPI(title="Wheel System Dashboard v1")

# Mount static files
app.mount("/static", StaticFiles(directory="apps/dashboard/static"), name="static")

# Templates
templates = Jinja2Templates(directory="apps/dashboard/templates")


def _safe_select(view_name: str, limit: int = 100) -> tuple[List[Dict[str, Any]], bool]:
    """
    Safe select that returns (data, has_error) tuple.
    has_error is True if the query failed (view doesn't exist or other error).
    """
    try:
        data = select_all(view_name, limit)
        return data, False
    except Exception as e:
        # Log the full error for debugging
        error_msg = str(e)
        logger.error(f"Failed to query {view_name}: {error_msg}")
        # Check if it's a "relation does not exist" error (view missing)
        if "does not exist" in error_msg.lower() or "relation" in error_msg.lower():
            logger.error(f"View {view_name} does not exist in database. Run migrations!")
        return [], True


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    """Home page: summary with latest candidates, CSP picks, and CC picks."""
    candidates = _safe_select("v_latest_run_top25_candidates", limit=25)
    csp_picks = _safe_select("v_latest_run_csp_picks", limit=50)
    cc_picks = _safe_select("v_latest_run_cc_picks", limit=50)
    runs = _safe_select("v_run_history", limit=5)  # Latest 5 runs for summary
    
    # Get latest run info
    latest_run = runs[0] if runs else None
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "candidates": candidates,
            "csp_picks": csp_picks,
            "cc_picks": cc_picks,
            "latest_run": latest_run,
            "candidates_error": len(candidates) == 0 and len(runs) > 0,
            "csp_error": len(csp_picks) == 0 and len(runs) > 0,
            "cc_error": len(cc_picks) == 0 and len(runs) > 0,
        },
    )


@app.get("/runs", response_class=HTMLResponse)
def runs(request: Request):
    """Run history page."""
    runs_data, has_error = _safe_select("v_run_history", limit=200)
    
    return templates.TemplateResponse(
        "runs.html",
        {
            "request": request,
            "runs": runs_data,
            "has_error": has_error,
        },
    )


@app.get("/candidates", response_class=HTMLResponse)
def candidates(request: Request):
    """Latest top 25 candidates page."""
    candidates_data, has_error = _safe_select("v_latest_run_top25_candidates", limit=25)
    
    return templates.TemplateResponse(
        "candidates.html",
        {
            "request": request,
            "candidates": candidates_data,
            "has_error": has_error,
        },
    )


@app.get("/picks", response_class=HTMLResponse)
def picks(request: Request):
    """Picks page: CSP and CC picks."""
    csp_picks, csp_error = _safe_select("v_latest_run_csp_picks", limit=50)
    cc_picks, cc_error = _safe_select("v_latest_run_cc_picks", limit=50)
    
    return templates.TemplateResponse(
        "picks.html",
        {
            "request": request,
            "csp_picks": csp_picks,
            "cc_picks": cc_picks,
            "csp_error": csp_error,
            "cc_error": cc_error,
        },
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

