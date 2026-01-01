from __future__ import annotations

from datetime import datetime, timezone, date
from typing import Any, Dict, List, Optional, Tuple
import os
from dotenv import load_dotenv
from loguru import logger

from wheel.clients.supabase_client import get_supabase
from wheel.clients.schwab_marketdata_client import SchwabMarketDataClient

# Load environment variables from .env.local
load_dotenv(".env.local")


# ---------- Configuration ----------

# Allow env override of run_id for reruns
RUN_ID = os.getenv("RUN_ID")  # None = use latest

# Number of candidates to process (default 25)
PICKS_N = int(os.getenv("PICKS_N", "25"))

# DTE constraints for weeklies (primary window)
MIN_DTE = int(os.getenv("MIN_DTE", "4"))
MAX_DTE = int(os.getenv("MAX_DTE", "10"))

# Fallback DTE windows
FALLBACK_MAX_DTE_1 = int(os.getenv("FALLBACK_MAX_DTE_1", "14"))
FALLBACK_MIN_DTE_2 = int(os.getenv("FALLBACK_MIN_DTE_2", "1"))
FALLBACK_MAX_DTE_2 = int(os.getenv("FALLBACK_MAX_DTE_2", "21"))


# ---------- Helpers ----------

def _safe_float(x, default=None):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _find_expiration_in_window(expirations: List[date], min_dte: int, max_dte: int) -> Optional[date]:
    """
    Find the nearest expiration in the given DTE window.
    Returns the expiration date if found, None otherwise.
    """
    today = datetime.now(timezone.utc).date()
    future = sorted([d for d in expirations if d > today])
    
    # Find first expiration within DTE range
    for exp in future:
        dte = (exp - today).days
        if min_dte <= dte <= max_dte:
            return exp
    
    return None


def _pick_expiration_tiered(expirations: List[date]) -> Tuple[Optional[date], Optional[str]]:
    """
    Tiered expiration selection:
    1. Try [MIN_DTE, MAX_DTE] (primary weekly window)
    2. Try [MIN_DTE, FALLBACK_MAX_DTE_1] (extended weekly window)
    3. Try [FALLBACK_MIN_DTE_2, FALLBACK_MAX_DTE_2] (fallback window)
    
    Returns (expiration_date, window_name) or (None, None) if no match
    """
    # Primary window: [MIN_DTE, MAX_DTE]
    exp = _find_expiration_in_window(expirations, MIN_DTE, MAX_DTE)
    if exp:
        return exp, "primary"
    
    # Fallback 1: [MIN_DTE, FALLBACK_MAX_DTE_1]
    exp = _find_expiration_in_window(expirations, MIN_DTE, FALLBACK_MAX_DTE_1)
    if exp:
        return exp, "fallback1"
    
    # Fallback 2: [FALLBACK_MIN_DTE_2, FALLBACK_MAX_DTE_2]
    exp = _find_expiration_in_window(expirations, FALLBACK_MIN_DTE_2, FALLBACK_MAX_DTE_2)
    if exp:
        return exp, "fallback2"
    
    return None, None


def _parse_expirations_from_chain(chain: Any) -> List[date]:
    """
    Parse expiration dates from Schwab option chain.
    Supports TD-style putExpDateMap keys like "2026-01-02:4"
    """
    expirations: List[date] = []

    if not chain:
        return expirations

    # TD-style maps (Schwab uses this format)
    for key in ("putExpDateMap", "callExpDateMap"):
        m = chain.get(key) if isinstance(chain, dict) else None
        if isinstance(m, dict):
            for k in m.keys():
                # "YYYY-MM-DD:##"
                try:
                    ds = k.split(":")[0]
                    expirations.append(date.fromisoformat(ds))
                except Exception:
                    pass

    # If Schwab ever returns explicit expiration list
    exp_list = None
    if isinstance(chain, dict):
        exp_list = chain.get("expirations") or chain.get("expirationDates")
    if isinstance(exp_list, list):
        for item in exp_list:
            try:
                expirations.append(date.fromisoformat(str(item)[:10]))
            except Exception:
                pass

    # Dedup
    return sorted(list(set(expirations)))


def _extract_put_options_for_exp(chain: Dict[str, Any], exp: date) -> List[Dict[str, Any]]:
    """
    Return a flat list of PUT option entries for a specific expiration date.
    Supports TD-style exp-date maps.
    """
    results: List[Dict[str, Any]] = []
    exp_key_prefix = exp.isoformat()

    put_map = chain.get("putExpDateMap") if isinstance(chain, dict) else None
    if not isinstance(put_map, dict):
        return results

    # Find matching expKey like "YYYY-MM-DD:7"
    for exp_key, strikes_map in put_map.items():
        if not isinstance(exp_key, str) or not exp_key.startswith(exp_key_prefix):
            continue
        if not isinstance(strikes_map, dict):
            continue

        for strike_str, opt_list in strikes_map.items():
            if not isinstance(opt_list, list):
                continue
            for opt in opt_list:
                if isinstance(opt, dict):
                    # attach strike
                    opt = dict(opt)
                    opt["strike"] = _safe_float(opt.get("strike") or strike_str)
                    results.append(opt)

    return results


def _choose_best_put_in_delta_band(
    options: List[Dict[str, Any]],
    *,
    target_delta_low: float = 0.20,
    target_delta_high: float = 0.30,
    expiration: date,
) -> Optional[Dict[str, Any]]:
    """
    Choose the best PUT option in the target delta band that maximizes annualized yield.
    
    Requirements:
    - abs(delta) in [target_delta_low, target_delta_high]
    - bid > 0
    - Maximizes annualized_yield = (premium / strike) * (365 / dte)
    """
    if not options:
        return None

    today = datetime.now(timezone.utc).date()
    dte = (expiration - today).days

    candidates: List[Dict[str, Any]] = []

    for o in options:
        # delta might be negative for puts; use abs(delta)
        d = _safe_float(o.get("delta"))
        if d is None:
            continue
        abs_delta = abs(d)

        # Check delta band
        if not (target_delta_low <= abs_delta <= target_delta_high):
            continue

        # Check bid > 0
        bid = _safe_float(o.get("bid"), 0.0) or 0.0
        if bid <= 0:
            continue

        # Premium estimate (prefer mark, fallback to mid, then last)
        ask = _safe_float(o.get("ask"), 0.0) or 0.0
        mark = _safe_float(o.get("mark"))
        last = _safe_float(o.get("last"))

        if mark is None and bid and ask:
            mark = (bid + ask) / 2.0
        if mark is None:
            mark = last if last is not None else None
        if mark is None or mark <= 0:
            continue

        strike = _safe_float(o.get("strike"))
        if strike is None or strike <= 0:
            continue

        # Calculate annualized yield: (premium / strike) * (365 / dte)
        premium_yield = mark / strike
        annualized_yield = premium_yield * (365.0 / float(dte))

        candidates.append({
            **o,
            "_abs_delta": abs_delta,
            "_premium": mark,
            "_annualized_yield": annualized_yield,
        })

    if not candidates:
        return None

    # Sort by annualized_yield descending (highest first)
    candidates.sort(key=lambda x: x["_annualized_yield"], reverse=True)
    return candidates[0]


def _annualized_yield(premium: float, strike: float, dte: int) -> Optional[float]:
    """Calculate annualized yield: (premium / strike) * (365 / dte)"""
    if premium is None or strike is None or dte is None:
        return None
    if strike <= 0 or dte <= 0:
        return None
    premium_yield = premium / strike
    return premium_yield * (365.0 / float(dte))


# ---------- Main ----------

def main() -> None:
    logger.info(
        f"Starting build_csp_picks (PICKS_N={PICKS_N}, "
        f"primary=[{MIN_DTE},{MAX_DTE}], "
        f"fallback1=[{MIN_DTE},{FALLBACK_MAX_DTE_1}], "
        f"fallback2=[{FALLBACK_MIN_DTE_2},{FALLBACK_MAX_DTE_2}])..."
    )

    sb = get_supabase()

    # 1) Determine run_id (env override or latest)
    if RUN_ID:
        run_id = RUN_ID
        logger.info(f"Using RUN_ID from env: {run_id}")
    else:
        runs = (
            sb.table("screening_runs")
            .select("run_id, run_ts")
            .order("run_ts", desc=True)
            .limit(1)
            .execute()
            .data
            or []
        )
        if not runs:
            raise RuntimeError("No screening_runs found. Run weekly_screener first.")
        run_id = runs[0]["run_id"]
        logger.info(f"Using latest run_id: {run_id}")

    # 2) Get top candidates for that run
    cands = (
        sb.table("screening_candidates")
        .select("*")
        .eq("run_id", run_id)
        .order("score", desc=True)
        .limit(PICKS_N)
        .execute()
        .data
        or []
    )
    if not cands:
        raise RuntimeError(f"No screening_candidates found for run_id={run_id}")

    logger.info(f"Processing {len(cands)} candidates for run_id={run_id}")

    md = SchwabMarketDataClient()

    pick_rows: List[Dict[str, Any]] = []
    skipped_no_chain = 0
    skipped_no_exp = 0
    skipped_no_deltas = 0
    skipped_no_puts_in_band = 0

    for i, c in enumerate(cands, start=1):
        ticker = c.get("ticker")
        if not ticker:
            continue

        try:
            # Fetch option chain
            chain = md.get_option_chain(ticker, contract_type="PUT", strike_count=80)
            if not chain:
                skipped_no_chain += 1
                logger.warning(f"{ticker}: no option chain returned")
                continue

            # Parse expirations and pick expiration using tiered strategy
            expirations = _parse_expirations_from_chain(chain)
            if not expirations:
                skipped_no_exp += 1
                logger.warning(f"{ticker}: no expirations found in chain")
                continue

            # Try tiered expiration selection
            exp, window_used = _pick_expiration_tiered(expirations)
            if not exp:
                skipped_no_exp += 1
                # Log available expirations with their DTEs
                today = datetime.now(timezone.utc).date()
                exp_dtes = [(e, (e - today).days) for e in expirations if e > today]
                exp_str = ", ".join([f"{e.isoformat()}(dte={dte})" for e, dte in sorted(exp_dtes, key=lambda x: x[1])])
                logger.warning(
                    f"{ticker}: no expiration in any window "
                    f"(primary=[{MIN_DTE},{MAX_DTE}], "
                    f"fallback1=[{MIN_DTE},{FALLBACK_MAX_DTE_1}], "
                    f"fallback2=[{FALLBACK_MIN_DTE_2},{FALLBACK_MAX_DTE_2}]). "
                    f"Available: {exp_str}"
                )
                continue

            # Extract PUT options for this expiration
            puts = _extract_put_options_for_exp(chain, exp)
            if not puts:
                skipped_no_deltas += 1
                logger.warning(f"{ticker}: no PUTs extracted for exp={exp}")
                continue

            # Choose best PUT in delta band [0.20, 0.30]
            best = _choose_best_put_in_delta_band(
                puts,
                target_delta_low=0.20,
                target_delta_high=0.30,
                expiration=exp,
            )
            if not best:
                skipped_no_puts_in_band += 1
                logger.warning(f"{ticker}: no PUTs in delta band [0.20, 0.30] with bid>0")
                continue

            # Extract values
            strike = _safe_float(best.get("strike"))
            premium = _safe_float(best.get("_premium"))
            abs_delta = _safe_float(best.get("_abs_delta"))
            delta = _safe_float(best.get("delta"))  # Original delta (may be negative)
            bid = _safe_float(best.get("bid"), 0.0) or 0.0
            today = datetime.now(timezone.utc).date()
            dte = (exp - today).days
            ann_yld = _safe_float(best.get("_annualized_yield"))

            # Log successful pick with window used
            logger.info(
                f"{ticker}: pick created | window={window_used} | "
                f"exp={exp.isoformat()} | dte={dte} | strike={strike} | "
                f"bid={bid:.2f} | delta={delta:.3f} | yield={ann_yld:.2%}"
            )

            pick_rows.append({
                "run_id": run_id,
                "ticker": ticker,
                "action": "CSP",
                "dte": dte,
                "target_delta": abs_delta,  # Store abs(delta) as target_delta
                "expiration": exp.isoformat(),  # Store as date string
                "strike": strike,
                "premium": premium,
                "annualized_yield": ann_yld,
                "delta": delta,  # Store original delta (may be negative)
                # Carry-through fields from screening_candidates
                "score": c.get("score"),
                "rank": c.get("rank") or i,
                "price": c.get("price"),
                "iv": c.get("iv"),
                "iv_rank": c.get("iv_rank"),
                "beta": c.get("beta"),
                "rsi": c.get("rsi"),
                "earn_in_days": c.get("earn_in_days"),
                "sentiment_score": c.get("sentiment_score"),
                "pick_metrics": {
                    "expiration": exp.isoformat(),
                    "window_used": window_used,
                    "chain_raw_sample": {
                        "underlyingPrice": chain.get("underlyingPrice") if isinstance(chain, dict) else None,
                    },
                    "option_selected": {
                        "strike": strike,
                        "mark": premium,
                        "delta": delta,
                        "bid": best.get("bid"),
                        "ask": best.get("ask"),
                        "openInterest": best.get("openInterest"),
                        "volume": best.get("totalVolume") or best.get("volume"),
                        "inTheMoney": best.get("inTheMoney"),
                        "symbol": best.get("symbol"),
                        "abs_delta": abs_delta,
                        "dte": dte,
                    },
                },
            })

        except Exception as e:
            skipped_no_chain += 1
            logger.exception(f"{ticker}: failed to build CSP pick: {e}")

    # Log summary
    logger.info(
        f"Pick generation summary: "
        f"processed={len(cands)}, "
        f"created={len(pick_rows)}, "
        f"skipped_no_chain={skipped_no_chain}, "
        f"skipped_no_exp={skipped_no_exp}, "
        f"skipped_no_deltas={skipped_no_deltas}, "
        f"skipped_no_puts_in_band={skipped_no_puts_in_band}"
    )

    if not pick_rows:
        raise RuntimeError("No CSP picks were generated. Check chain parsing / auth / delta availability.")

    # 3) Delete existing CSP picks for this run_id, then insert new ones
    # (This ensures idempotent reruns)
    logger.info(f"Deleting existing CSP picks for run_id={run_id}")
    delete_res = (
        sb.table("screening_picks")
        .delete()
        .eq("run_id", run_id)
        .eq("action", "CSP")
        .execute()
    )

    # 4) Insert new picks (batch insert)
    logger.info(f"Inserting {len(pick_rows)} screening_picks rows...")
    insert_res = sb.table("screening_picks").insert(pick_rows).execute()
    
    # Check for errors
    if hasattr(insert_res, "error") and insert_res.error:
        raise RuntimeError(f"Supabase error inserting picks: {insert_res.error}")
    
    logger.info(f"✅ build_csp_picks complete. Created {len(pick_rows)} CSP picks for run_id={run_id}")


if __name__ == "__main__":
    main()
