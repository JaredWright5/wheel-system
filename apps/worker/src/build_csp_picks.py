"""
Build CSP (Cash-Secured Put) picks from screening candidates.
Uses WheelRules for consistent configuration and applies earnings exclusion.
Enhanced with diagnostic logging for pick generation failures.
"""
from __future__ import annotations

from datetime import datetime, timezone, date, timedelta
from typing import Any, Dict, List, Optional, Tuple
import os
import math
from dotenv import load_dotenv
from loguru import logger

from wheel.clients.supabase_client import get_supabase, upsert_rows
from wheel.clients.schwab_marketdata_client import SchwabMarketDataClient
from apps.worker.src.config.wheel_rules import (
    load_wheel_rules,
    find_expiration_in_window,
    is_within_dte_window,
    spread_ok,
)

# Load environment variables from .env.local
load_dotenv(".env.local")


# ---------- Configuration ----------

# Allow env override of run_id for reruns
RUN_ID = os.getenv("RUN_ID")  # None = use latest

# Number of candidates to process (default 25)
PICKS_N = int(os.getenv("PICKS_N", "25"))

# Maximum annualized yield (as decimal, e.g., 3.0 = 300%)
MAX_ANNUALIZED_YIELD = float(os.getenv("MAX_ANNUALIZED_YIELD", "3.0"))


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


def _check_liquidity(option: Dict[str, Any], rules) -> Tuple[bool, Optional[str]]:
    """
    Check if option meets liquidity requirements using WheelRules.
    
    Returns:
        (is_valid, reason_if_invalid)
    """
    bid = _safe_float(option.get("bid"), 0.0) or 0.0
    ask = _safe_float(option.get("ask"), 0.0) or 0.0
    
    # Require bid >= MIN_BID
    if bid < rules.min_bid:
        return False, f"bid_below_min_{bid:.2f}"
    
    # Require non-null ask
    if ask <= 0:
        return False, "missing_ask"
    
    # Check spread using wheel_rules.spread_ok()
    if not spread_ok(
        bid=bid,
        ask=ask,
        max_spread_pct=rules.max_spread_pct,
        max_abs_low=rules.max_abs_spread_low_premium,
        max_abs_high=rules.max_abs_spread_high_premium,
    ):
        # Determine specific failure reason for logging
        mid = (bid + ask) / 2.0
        abs_spread = ask - bid
        pct_spread = (abs_spread / mid) * 100.0 if mid > 0 else 0.0
        max_abs = rules.max_abs_spread_low_premium if mid < 1.00 else rules.max_abs_spread_high_premium
        
        if pct_spread > rules.max_spread_pct:
            return False, f"spread_pct_fail_{pct_spread:.1f}%"
        elif abs_spread > max_abs:
            return False, f"spread_abs_fail_{abs_spread:.2f}"
        else:
            return False, "spread_fail"
    
    # Check open interest
    oi = _safe_float(option.get("openInterest"), 0.0) or 0.0
    if oi < rules.min_open_interest:
        return False, f"low_oi_{int(oi)}"
    
    return True, None


def _count_put_contracts_diagnostics(
    options: List[Dict[str, Any]],
    target_delta_low: float,
    target_delta_high: float,
    rules,
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
        
        # Count bid >= MIN_BID
        bid = _safe_float(o.get("bid"), 0.0) or 0.0
        if bid >= rules.min_bid:
            counts["bid_ok"] += 1
            
            # Count spread OK (only if bid >= MIN_BID)
            ask = _safe_float(o.get("ask"), 0.0) or 0.0
            if ask > 0:
                if spread_ok(
                    bid=bid,
                    ask=ask,
                    max_spread_pct=rules.max_spread_pct,
                    max_abs_low=rules.max_abs_spread_low_premium,
                    max_abs_high=rules.max_abs_spread_high_premium,
                ):
                    counts["spread_ok"] += 1
                    
                    # Count OI OK (only if spread OK)
                    oi = _safe_float(o.get("openInterest"), 0.0) or 0.0
                    if oi >= rules.min_open_interest:
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
    - Passes liquidity checks (bid >= MIN_BID, spread_ok, OI >= MIN_OPEN_INTEREST)
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
        is_liquid, liquidity_reason = _check_liquidity(o, rules)
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

        # Quote sanity checks
        if ask < bid:
            continue  # Invalid: ask < bid
        mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else mark
        if mid <= 0:
            continue  # Invalid: mid must be > 0
        abs_spread = ask - bid
        if abs_spread <= 0:
            continue  # Invalid: spread must be > 0

        strike = _safe_float(o.get("strike"))
        if strike is None or strike <= 0:
            continue

        # Calculate annualized yield: (premium / strike) * (365 / dte)
        premium_yield = mark / strike
        annualized_yield = premium_yield * (365.0 / float(dte))

        # Yield sanity check
        if annualized_yield > MAX_ANNUALIZED_YIELD:
            continue  # Reject contracts with unrealistic yields

        # Calculate spread percentage
        spread_pct = (abs_spread / mid) * 100.0 if mid > 0 else 0.0

        # Get open interest for scoring
        oi = _safe_float(o.get("openInterest"), 0.0) or 0.0

        # Calculate contract_score:
        # + annualized_yield (primary)
        # - spread_pct * 10 (penalize wide spreads)
        # - (1 / sqrt(open_interest + 1)) * 5 (penalize low OI)
        # + abs(delta) * 10 (prefer closer to 0.30 than 0.20 within band)
        contract_score = (
            annualized_yield
            - (spread_pct * 10.0)
            - ((1.0 / math.sqrt(oi + 1.0)) * 5.0)
            + (abs_delta * 10.0)
        )

        candidates.append({
            **o,
            "_abs_delta": abs_delta,
            "_premium": mark,
            "_annualized_yield": annualized_yield,
            "_mid": mid,
            "_spread_abs": abs_spread,
            "_spread_pct": spread_pct,
            "_contract_score": contract_score,
            "_liquidity_ok": True,
        })

    if not candidates:
        return None

    # Sort by contract_score descending (highest first)
    candidates.sort(key=lambda x: x["_contract_score"], reverse=True)
    return candidates[0]


def _determine_skip_reason(diag_counts: Dict[str, int], rules) -> str:
    """
    Determine skip reason string from diagnostics.
    
    Returns:
        Reason string like "delta out of band", "spread failed", etc.
    """
    if diag_counts["delta_present"] == 0:
        return "delta missing from Schwab"
    elif diag_counts["in_delta"] == 0:
        return "delta out of band"
    elif diag_counts["bid_ok"] == 0:
        return f"bid < ${rules.min_bid:.2f}"
    elif diag_counts["spread_ok"] == 0:
        return "spread failed (pct or abs)"
    elif diag_counts["oi_ok"] == 0:
        return f"oi < {rules.min_open_interest}"
    else:
        return "other"


def attempt_window(
    window_name: str,
    min_dte: int,
    max_dte: int,
    chain: Dict[str, Any],
    expirations: List[date],
    rules,
    now: date,
) -> Tuple[Optional[Dict[str, Any]], Optional[date], Dict[str, Any]]:
    """
    Attempt to find a valid PUT contract in a given DTE window.
    
    Args:
        window_name: Name of window for logging (e.g., "primary", "fallback")
        min_dte: Minimum DTE for the window
        max_dte: Maximum DTE for the window
        chain: Option chain from Schwab
        expirations: List of available expiration dates
        rules: WheelRules instance
        now: Current date (UTC)
        
    Returns:
        (best_contract or None, expiration_date or None, diagnostics_dict)
        
    Diagnostics dict includes:
        - puts_total, delta_present, in_delta, bid_ok, spread_ok, oi_ok
        - reason: skip reason string if no pick (or None if pick found)
    """
    # Find expiration in window
    exp = find_expiration_in_window(expirations, min_dte=min_dte, max_dte=max_dte, now=now)
    if not exp:
        return None, None, {
            "puts_total": 0,
            "delta_present": 0,
            "in_delta": 0,
            "bid_ok": 0,
            "spread_ok": 0,
            "oi_ok": 0,
            "reason": "no expiration in window",
        }
    
    # Extract PUT options for this expiration
    puts = _extract_put_options_for_exp(chain, exp)
    if not puts:
        return None, None, {
            "puts_total": 0,
            "delta_present": 0,
            "in_delta": 0,
            "bid_ok": 0,
            "spread_ok": 0,
            "oi_ok": 0,
            "reason": "no PUTs extracted",
        }
    
    # Count diagnostics BEFORE filtering
    diag_counts = _count_put_contracts_diagnostics(
        puts,
        target_delta_low=rules.csp_delta_min,
        target_delta_high=rules.csp_delta_max,
        rules=rules,
    )
    
    # Try to find best PUT in delta band
    best = _choose_best_put_in_delta_band(
        puts,
        target_delta_low=rules.csp_delta_min,
        target_delta_high=rules.csp_delta_max,
        expiration=exp,
        rules=rules,
    )
    
    if best:
        diag_counts["reason"] = None
    else:
        diag_counts["reason"] = _determine_skip_reason(diag_counts, rules)
    
    return best, exp, diag_counts


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
        f"liquidity: max_spread_pct={rules.max_spread_pct}%, "
        f"min_bid=${rules.min_bid:.2f}, min_oi={rules.min_open_interest}, "
        f"max_abs_spread_low=${rules.max_abs_spread_low_premium:.2f}, "
        f"max_abs_spread_high=${rules.max_abs_spread_high_premium:.2f}"
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
    
    # Earnings tracking
    earnings_known = 0
    earnings_unknown = 0

    now = datetime.now(timezone.utc).date()

    for i, c in enumerate(cands, start=1):
        ticker = c.get("ticker")
        if not ticker:
            continue

        try:
            # Load earnings_in_days from column or metrics JSON
            earnings_in_days = c.get("earn_in_days")
            if earnings_in_days is None:
                # Try to load from metrics JSON
                metrics = c.get("metrics") or {}
                earnings_in_days = metrics.get("earnings_in_days")
            
            # Track earnings statistics
            if earnings_in_days is not None:
                earnings_known += 1
            else:
                earnings_unknown += 1
            
            # Apply earnings exclusion: skip if earnings_in_days <= EARNINGS_AVOID_DAYS
            if earnings_in_days is not None and earnings_in_days <= rules.earnings_avoid_days:
                skipped_earnings_blocked += 1
                logger.warning(
                    f"{ticker}: blocked by earnings | earnings_in_days={earnings_in_days} avoid_days={rules.earnings_avoid_days}"
                )
                continue

            # Fetch option chain
            chain = md.get_option_chain(ticker, contract_type="PUT", strike_count=80)
            if not chain:
                skipped_no_chain += 1
                logger.warning(f"{ticker}: no option chain returned")
                continue

            # Parse expirations
            expirations = _parse_expirations_from_chain(chain)
            if not expirations:
                skipped_no_contract_in_dte += 1
                logger.warning(f"{ticker}: no expirations found in chain")
                continue

            # Try primary window first
            best, exp, diag_primary = attempt_window(
                window_name="primary",
                min_dte=rules.dte_min_primary,
                max_dte=rules.dte_max_primary,
                chain=chain,
                expirations=expirations,
                rules=rules,
                now=now,
            )
            
            window_used = None
            fallback_attempted = False
            
            if best:
                # Success in primary window
                window_used = "primary"
            else:
                # No pick in primary - check if we should try fallback
                should_try_fallback = (
                    rules.allow_fallback_dte and
                    (diag_primary["in_delta"] == 0 or 
                     (diag_primary["in_delta"] > 0 and diag_primary["spread_ok"] == 0))
                )
                
                if should_try_fallback:
                    fallback_attempted = True
                    logger.info(f"{ticker}: attempting fallback due to primary liquidity failure")
                    
                    # Try fallback window
                    best_fallback, exp_fallback, diag_fallback = attempt_window(
                        window_name="fallback",
                        min_dte=rules.dte_min_fallback,
                        max_dte=rules.dte_max_fallback,
                        chain=chain,
                        expirations=expirations,
                        rules=rules,
                        now=now,
                    )
                    
                    if best_fallback:
                        # Success in fallback window
                        best = best_fallback
                        exp = exp_fallback
                        window_used = "fallback"
                    else:
                        # Fallback also failed - will use primary diagnostics for logging
                        pass

            # If no pick was created, log diagnostics
            if not best:
                # Use primary diagnostics if fallback wasn't attempted, otherwise use fallback
                diag_to_log = diag_fallback if fallback_attempted else diag_primary
                
                # Determine which filter failed based on diagnostics
                skip_reason = diag_to_log.get("reason", "other")
                
                if skip_reason == "delta missing from Schwab":
                    skipped_delta_missing += 1
                elif skip_reason == "delta out of band":
                    skipped_delta_out_of_band += 1
                elif "bid <" in skip_reason:
                    skipped_bid_zero += 1
                elif "spread" in skip_reason:
                    skipped_spread += 1
                elif "oi <" in skip_reason:
                    skipped_open_interest += 1
                else:
                    skipped_delta_out_of_band += 1  # Default fallback
                
                # Log ONE warning line with diagnostics
                log_msg = (
                    f"{ticker}: no pick | "
                    f"puts_total={diag_to_log['puts_total']} "
                    f"delta_present={diag_to_log['delta_present']} "
                    f"in_delta={diag_to_log['in_delta']} "
                    f"bid_ok={diag_to_log['bid_ok']} "
                    f"spread_ok={diag_to_log['spread_ok']} "
                    f"oi_ok={diag_to_log['oi_ok']} "
                    f"fallback_attempted={fallback_attempted}"
                )
                
                if diag_to_log["delta_present"] == 0:
                    log_msg += " (delta missing from Schwab)"
                else:
                    log_msg += f" | reason={skip_reason}"
                
                # If fallback was attempted, include fallback diagnostics
                if fallback_attempted:
                    log_msg += (
                        f" | fallback: puts_total={diag_fallback['puts_total']} "
                        f"in_delta={diag_fallback['in_delta']} spread_ok={diag_fallback['spread_ok']} "
                        f"reason={diag_fallback.get('reason', 'unknown')}"
                    )
                
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
            mid = _safe_float(best.get("_mid")) or ((bid + ask) / 2.0 if (bid > 0 and ask > 0) else premium)
            spread_abs = _safe_float(best.get("_spread_abs")) or (ask - bid if (ask > bid) else 0.0)
            spread_pct = _safe_float(best.get("_spread_pct")) or ((spread_abs / mid) * 100.0 if mid > 0 else 0.0)
            contract_score = _safe_float(best.get("_contract_score"))

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
                "earn_in_days": earnings_in_days,  # Use the loaded value
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
                        "mid": mid,
                        "spread_abs": spread_abs,
                        "spread_pct": spread_pct,
                        "annualized_yield": ann_yld,
                        "contract_score": contract_score,
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
        f"earnings_known={earnings_known}, earnings_unknown={earnings_unknown}, "
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
