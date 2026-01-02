"""
Build CSP (Cash-Secured Put) picks from screening candidates.
Uses WheelRules for consistent configuration and applies earnings exclusion.
Enhanced with diagnostic logging for pick generation failures.
"""
from __future__ import annotations

from datetime import datetime, timezone, date, timedelta
from typing import Any, Dict, List, Optional, Tuple
import os
from dotenv import load_dotenv
from loguru import logger

from wheel.clients.supabase_client import get_supabase, upsert_rows
from wheel.clients.schwab_marketdata_client import SchwabMarketDataClient
from apps.worker.src.config.wheel_rules import (
    load_wheel_rules,
    earnings_ok,
    find_expiration_in_window,
    is_within_dte_window,
)

# Load environment variables from .env.local
load_dotenv(".env.local")


# ---------- Configuration ----------

# Allow env override of run_id for reruns
RUN_ID = os.getenv("RUN_ID")  # None = use latest

# Number of candidates to process (default 25)
PICKS_N = int(os.getenv("PICKS_N", "25"))

# Liquidity/spread filters (configurable via env)
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "2.5"))  # Max spread as % of mid price
MIN_OPEN_INTEREST = int(os.getenv("MIN_OPEN_INTEREST", "50"))  # Minimum open interest


# ---------- Helpers ----------

def _safe_float(x, default=None):
    """Safely convert to float, returning default on error."""
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


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


def _check_liquidity(option: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Check if option meets liquidity requirements.
    
    Returns:
        (is_valid, reason_if_invalid)
    """
    bid = _safe_float(option.get("bid"), 0.0) or 0.0
    ask = _safe_float(option.get("ask"), 0.0) or 0.0
    
    # Require non-null bid/ask
    if bid <= 0 or ask <= 0:
        return False, "missing_bid_ask"
    
    # Calculate spread
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return False, "invalid_mid"
    
    spread = ask - bid
    spread_pct = (spread / mid) * 100.0
    
    if spread_pct > MAX_SPREAD_PCT:
        return False, f"spread_too_wide_{spread_pct:.1f}%"
    
    # Check open interest
    oi = _safe_float(option.get("openInterest"), 0.0) or 0.0
    if oi < MIN_OPEN_INTEREST:
        return False, f"low_oi_{int(oi)}"
    
    return True, None


def _count_put_contracts_diagnostics(
    options: List[Dict[str, Any]],
    target_delta_low: float,
    target_delta_high: float,
) -> Dict[str, int]:
    """
    Count PUT contracts at each filtering stage for diagnostics.
    
    Returns:
        Dictionary with counts: puts_total, delta_present, in_delta, bid_ok, spread_ok, oi_ok
    """
    counts = {
        "puts_total": len(options),
        "delta_present": 0,
        "in_delta": 0,
        "bid_ok": 0,
        "spread_ok": 0,
        "oi_ok": 0,
    }
    
    for o in options:
        # Count delta present
        d = _safe_float(o.get("delta"))
        if d is not None:
            counts["delta_present"] += 1
            abs_delta = abs(d)
            
            # Count in delta band
            if target_delta_low <= abs_delta <= target_delta_high:
                counts["in_delta"] += 1
        
        # Count bid > 0
        bid = _safe_float(o.get("bid"), 0.0) or 0.0
        if bid > 0:
            counts["bid_ok"] += 1
            
            # Count spread OK (only if bid > 0)
            ask = _safe_float(o.get("ask"), 0.0) or 0.0
            if ask > 0:
                mid = (bid + ask) / 2.0
                if mid > 0:
                    spread = ask - bid
                    spread_pct = (spread / mid) * 100.0
                    if spread_pct <= MAX_SPREAD_PCT:
                        counts["spread_ok"] += 1
                        
                        # Count OI OK (only if spread OK)
                        oi = _safe_float(o.get("openInterest"), 0.0) or 0.0
                        if oi >= MIN_OPEN_INTEREST:
                            counts["oi_ok"] += 1
    
    return counts


def _choose_best_put_in_delta_band(
    options: List[Dict[str, Any]],
    *,
    target_delta_low: float,
    target_delta_high: float,
    expiration: date,
    rules,
) -> Optional[Dict[str, Any]]:
    """
    Choose the best PUT option in the target delta band that maximizes annualized yield.
    
    Requirements:
    - abs(delta) in [target_delta_low, target_delta_high]
    - Passes liquidity checks (bid>0, spread, OI)
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

        # Check liquidity
        is_liquid, liquidity_reason = _check_liquidity(o)
        if not is_liquid:
            continue  # Skip but don't log here (too verbose)

        # Premium estimate (prefer mark, fallback to mid, then last)
        bid = _safe_float(o.get("bid"), 0.0) or 0.0
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
            "_liquidity_ok": True,
        })

    if not candidates:
        return None

    # Sort by annualized_yield descending (highest first)
    candidates.sort(key=lambda x: x["_annualized_yield"], reverse=True)
    return candidates[0]


def _pick_expiration_with_fallback(
    expirations: List[date],
    rules,
    now: Optional[date] = None,
) -> Tuple[Optional[date], Optional[str]]:
    """
    Pick expiration using primary window, with fallback if allowed.
    
    Returns:
        (expiration_date, window_name) or (None, None) if no match
    """
    if now is None:
        now = datetime.now(timezone.utc).date()
    
    # Primary window: [DTE_MIN_PRIMARY, DTE_MAX_PRIMARY]
    exp = find_expiration_in_window(
        expirations,
        min_dte=rules.dte_min_primary,
        max_dte=rules.dte_max_primary,
        now=now,
    )
    if exp:
        return exp, "primary"
    
    # Fallback window (if allowed)
    if rules.allow_fallback_dte:
        exp = find_expiration_in_window(
            expirations,
            min_dte=rules.dte_min_fallback,
            max_dte=rules.dte_max_fallback,
            now=now,
        )
        if exp:
            return exp, "fallback"
    
    return None, None


# ---------- Main ----------

def main() -> None:
    # Load wheel rules
    rules = load_wheel_rules()
    logger.info(
        f"Wheel rules in effect: "
        f"CSP delta=[{rules.csp_delta_min:.2f}, {rules.csp_delta_max:.2f}], "
        f"DTE primary=[{rules.dte_min_primary}, {rules.dte_max_primary}], "
        f"DTE fallback=[{rules.dte_min_fallback}, {rules.dte_max_fallback}] "
        f"(allow_fallback={rules.allow_fallback_dte}), "
        f"earnings_avoid_days={rules.earnings_avoid_days}, "
        f"liquidity: max_spread={MAX_SPREAD_PCT}%, min_oi={MIN_OPEN_INTEREST}"
    )

    sb = get_supabase()

    # 1) Determine run_id (env override or latest successful screening run)
    if RUN_ID:
        run_id = RUN_ID
        logger.info(f"Using RUN_ID from env: {run_id}")
    else:
        # Get latest successful screening run (exclude daily tracker runs)
        runs = (
            sb.table("screening_runs")
            .select("run_id, run_ts, status, notes")
            .eq("status", "success")
            .neq("notes", "DAILY_TRACKER")
            .order("run_ts", desc=True)
            .limit(1)
            .execute()
            .data
            or []
        )
        if not runs:
            raise RuntimeError("No successful screening_runs found. Run weekly_screener first.")
        run_id = runs[0]["run_id"]
        logger.info(f"Using latest successful screening run_id: {run_id} (run_ts={runs[0].get('run_ts')})")

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
        # Provide more helpful error message
        run_check = (
            sb.table("screening_runs")
            .select("status, notes, candidates_count, run_ts")
            .eq("run_id", run_id)
            .execute()
            .data
        )
        if run_check:
            run_info = run_check[0]
            raise RuntimeError(
                f"No screening_candidates found for run_id={run_id}. "
                f"Run status: {run_info.get('status')}, notes: {run_info.get('notes')}, "
                f"candidates_count: {run_info.get('candidates_count')}, run_ts: {run_info.get('run_ts')}. "
                f"Make sure weekly_screener completed successfully for this run."
            )
        else:
            raise RuntimeError(f"Run_id {run_id} not found in screening_runs table.")

    logger.info(f"Processing {len(cands)} candidates for run_id={run_id}")

    md = SchwabMarketDataClient()

    pick_rows: List[Dict[str, Any]] = []
    
    # Skip counters by reason
    skipped_earnings_blocked = 0
    skipped_no_chain = 0
    skipped_no_contract_in_dte = 0
    skipped_delta_missing = 0
    skipped_delta_out_of_band = 0
    skipped_bid_zero = 0
    skipped_spread = 0
    skipped_open_interest = 0

    now = datetime.now(timezone.utc).date()

    for i, c in enumerate(cands, start=1):
        ticker = c.get("ticker")
        if not ticker:
            continue

        try:
            # Check earnings exclusion
            earn_in_days = c.get("earn_in_days")
            if earn_in_days is not None:
                # Calculate earnings date (if we have days until earnings)
                earnings_date = now + timedelta(days=earn_in_days) if earn_in_days > 0 else None
            else:
                # Try to parse from metrics if available
                metrics = c.get("metrics") or {}
                earn_in_days_from_metrics = metrics.get("earnings_in_days")
                if earn_in_days_from_metrics is not None:
                    earnings_date = now + timedelta(days=earn_in_days_from_metrics) if earn_in_days_from_metrics > 0 else None
                else:
                    earnings_date = None
            
            # Apply earnings exclusion
            if not earnings_ok(earnings_date, now=now, avoid_days=rules.earnings_avoid_days):
                skipped_earnings_blocked += 1
                logger.warning(
                    f"{ticker}: skipped (earnings in {earn_in_days} days, "
                    f"within avoid_days={rules.earnings_avoid_days})"
                )
                continue
            elif earnings_date is None:
                logger.debug(f"{ticker}: earnings date unknown (not excluded)")

            # Fetch option chain
            chain = md.get_option_chain(ticker, contract_type="PUT", strike_count=80)
            if not chain:
                skipped_no_chain += 1
                logger.warning(f"{ticker}: no option chain returned")
                continue

            # Parse expirations and pick expiration using tiered strategy
            expirations = _parse_expirations_from_chain(chain)
            if not expirations:
                skipped_no_contract_in_dte += 1
                logger.warning(f"{ticker}: no expirations found in chain")
                continue

            # Try expiration selection with fallback
            exp, window_used = _pick_expiration_with_fallback(expirations, rules, now=now)
            if not exp:
                skipped_no_contract_in_dte += 1
                # Log available expirations with their DTEs
                exp_dtes = [(e, (e - now).days) for e in expirations if e > now]
                exp_str = ", ".join([f"{e.isoformat()}(dte={dte})" for e, dte in sorted(exp_dtes, key=lambda x: x[1])])
                logger.warning(
                    f"{ticker}: no expiration in DTE windows "
                    f"(primary=[{rules.dte_min_primary},{rules.dte_max_primary}], "
                    f"fallback=[{rules.dte_min_fallback},{rules.dte_max_fallback}], "
                    f"allow_fallback={rules.allow_fallback_dte}). "
                    f"Available: {exp_str}"
                )
                continue

            # Extract PUT options for this expiration
            puts = _extract_put_options_for_exp(chain, exp)
            if not puts:
                skipped_delta_missing += 1  # No contracts at all
                logger.warning(f"{ticker}: no PUTs extracted for exp={exp}")
                continue

            # Count diagnostics BEFORE filtering
            diag_counts = _count_put_contracts_diagnostics(
                puts,
                target_delta_low=rules.csp_delta_min,
                target_delta_high=rules.csp_delta_max,
            )

            # Choose best PUT in delta band [CSP_DELTA_MIN, CSP_DELTA_MAX]
            best = _choose_best_put_in_delta_band(
                puts,
                target_delta_low=rules.csp_delta_min,
                target_delta_high=rules.csp_delta_max,
                expiration=exp,
                rules=rules,
            )
            if not best:
                # Determine which filter failed based on diagnostics
                if diag_counts["delta_present"] == 0:
                    skipped_delta_missing += 1
                    skip_reason = "delta missing from Schwab"
                elif diag_counts["in_delta"] == 0:
                    skipped_delta_out_of_band += 1
                    skip_reason = "delta out of band"
                elif diag_counts["bid_ok"] == 0:
                    skipped_bid_zero += 1
                    skip_reason = "no bid > 0"
                elif diag_counts["spread_ok"] == 0:
                    skipped_spread += 1
                    skip_reason = "spread too wide"
                elif diag_counts["oi_ok"] == 0:
                    skipped_open_interest += 1
                    skip_reason = "low open interest"
                else:
                    # Fallback to generic delta skip if diagnostics don't clearly indicate
                    skipped_delta_out_of_band += 1
                    skip_reason = "other"
                
                # Log ONE warning line with diagnostics
                log_msg = (
                    f"{ticker}: no pick | "
                    f"puts_total={diag_counts['puts_total']} "
                    f"delta_present={diag_counts['delta_present']} "
                    f"in_delta={diag_counts['in_delta']} "
                    f"bid_ok={diag_counts['bid_ok']} "
                    f"spread_ok={diag_counts['spread_ok']} "
                    f"oi_ok={diag_counts['oi_ok']}"
                )
                if diag_counts["delta_present"] == 0:
                    log_msg += " (delta missing from Schwab)"
                else:
                    log_msg += f" | reason={skip_reason}"
                
                logger.warning(log_msg)
                continue

            # Extract values
            strike = _safe_float(best.get("strike"))
            premium = _safe_float(best.get("_premium"))
            abs_delta = _safe_float(best.get("_abs_delta"))
            delta = _safe_float(best.get("delta"))  # Original delta (may be negative)
            bid = _safe_float(best.get("bid"), 0.0) or 0.0
            ask = _safe_float(best.get("ask"), 0.0) or 0.0
            dte = (exp - now).days
            ann_yld = _safe_float(best.get("_annualized_yield"))

            # Log successful pick with window used
            logger.info(
                f"{ticker}: pick created | window={window_used} | "
                f"exp={exp.isoformat()} | dte={dte} | strike={strike} | "
                f"bid={bid:.2f} | delta={delta:.3f} | yield={ann_yld:.2%}"
            )

            # Get RSI period/interval from candidate metrics or use defaults
            candidate_metrics = c.get("metrics") or {}
            rsi_period = candidate_metrics.get("rsi_period") or rules.rsi_period
            rsi_interval = candidate_metrics.get("rsi_interval") or rules.rsi_interval

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
                    "rule_context": {
                        "used_dte_window": window_used,
                        "earnings_avoid_days": rules.earnings_avoid_days,
                        "delta_band": [rules.csp_delta_min, rules.csp_delta_max],
                        "rsi_period": rsi_period,
                        "rsi_interval": rsi_interval,
                    },
                    "expiration": exp.isoformat(),
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

    # Log summary with skip counts by reason
    logger.info(
        f"Pick generation summary: "
        f"processed={len(cands)}, "
        f"created={len(pick_rows)}, "
        f"skipped_earnings_blocked={skipped_earnings_blocked}, "
        f"skipped_no_chain={skipped_no_chain}, "
        f"skipped_no_contract_in_dte={skipped_no_contract_in_dte}, "
        f"skipped_delta_missing={skipped_delta_missing}, "
        f"skipped_delta_out_of_band={skipped_delta_out_of_band}, "
        f"skipped_bid_zero={skipped_bid_zero}, "
        f"skipped_spread={skipped_spread}, "
        f"skipped_open_interest={skipped_open_interest}"
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
