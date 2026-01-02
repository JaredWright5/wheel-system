"""
Build CC (Covered Call) picks from Schwab positions.
Uses WheelRules for consistent configuration and applies earnings exclusion.
"""
from __future__ import annotations

from datetime import datetime, timezone, date, timedelta
from typing import Any, Dict, List, Optional, Tuple
import os
from dotenv import load_dotenv
from loguru import logger

from wheel.clients.supabase_client import get_supabase
from wheel.clients.schwab_client import SchwabClient
from wheel.clients.schwab_marketdata_client import SchwabMarketDataClient
from wheel.clients.fmp_stable_client import FMPStableClient
from apps.worker.src.config.wheel_rules import (
    load_wheel_rules,
    earnings_ok,
    find_expiration_in_window,
)

# Load environment variables from .env.local
load_dotenv(".env.local")


# ---------- Configuration ----------

# Allow env override of run_id for reruns
RUN_ID = os.getenv("RUN_ID")  # None = use latest

# Number of positions to process (default 25)
CC_PICKS_N = int(os.getenv("CC_PICKS_N", "25"))

# Liquidity/spread filters (configurable via env)
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "2.5"))  # Max spread as % of mid price
MIN_OPEN_INTEREST = int(os.getenv("MIN_OPEN_INTEREST", "50"))  # Minimum open interest

# Allow ITM calls (default False - prefer OTM)
ALLOW_ITM_CALLS = os.getenv("ALLOW_ITM_CALLS", "false").lower() == "true"

# Test mode: comma-separated tickers (e.g., "AAPL,MSFT")
CC_TEST_TICKERS = os.getenv("CC_TEST_TICKERS", "").strip()
CC_TEST_MODE = bool(CC_TEST_TICKERS)


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
    Supports TD-style callExpDateMap keys like "2026-01-02:4"
    """
    expirations: List[date] = []

    if not chain:
        return expirations

    # TD-style maps (Schwab uses this format)
    for key in ("callExpDateMap", "putExpDateMap"):
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


def _extract_call_options_for_exp(chain: Dict[str, Any], exp: date) -> List[Dict[str, Any]]:
    """
    Return a flat list of CALL option entries for a specific expiration date.
    Supports TD-style exp-date maps.
    """
    results: List[Dict[str, Any]] = []
    exp_key_prefix = exp.isoformat()

    call_map = chain.get("callExpDateMap") if isinstance(chain, dict) else None
    if not isinstance(call_map, dict):
        return results

    # Find matching expKey like "YYYY-MM-DD:7"
    for exp_key, strikes_map in call_map.items():
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


def _choose_best_call_in_delta_band(
    options: List[Dict[str, Any]],
    *,
    target_delta_low: float,
    target_delta_high: float,
    current_price: float,
    expiration: date,
    rules,
) -> Optional[Dict[str, Any]]:
    """
    Choose the best CALL option in the target delta band that maximizes annualized yield.
    
    Requirements:
    - delta in [target_delta_low, target_delta_high] (calls have positive delta)
    - Passes liquidity checks (bid>0, spread, OI)
    - Prefer OTM calls (strike >= current_price) unless ALLOW_ITM_CALLS=true
    - Maximizes annualized_yield = (premium / strike) * (365 / dte)
    """
    if not options:
        return None

    today = datetime.now(timezone.utc).date()
    dte = (expiration - today).days

    candidates: List[Dict[str, Any]] = []

    for o in options:
        # delta for calls is positive
        delta = _safe_float(o.get("delta"))
        if delta is None:
            continue

        # Check delta band (positive for calls)
        if not (target_delta_low <= delta <= target_delta_high):
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
        
        # ITM/OTM check: prefer OTM calls unless explicitly allowed
        if not ALLOW_ITM_CALLS and strike < current_price:
            # ITM call - skip (prefer OTM)
            continue

        # Calculate annualized yield: (premium / strike) * (365 / dte)
        premium_yield = mark / strike
        annualized_yield = premium_yield * (365.0 / float(dte))

        candidates.append({
            **o,
            "_delta": delta,
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


def _get_earnings_date_for_ticker(
    ticker: str,
    candidate: Optional[Dict[str, Any]],
    sb: Any,
    fmp: Optional[FMPStableClient],
    now: date,
) -> Optional[date]:
    """
    Get earnings date for a ticker, preferring cached candidate data, then FMP.
    
    Returns:
        Earnings date if found, None otherwise
    """
    # 1) Try from candidate data (most reliable - from weekly_screener)
    if candidate:
        earn_in_days = candidate.get("earn_in_days")
        if earn_in_days is not None and earn_in_days > 0:
            return now + timedelta(days=earn_in_days)
        
        # Try from candidate metrics
        metrics = candidate.get("metrics") or {}
        earn_in_days_from_metrics = metrics.get("earnings_in_days")
        if earn_in_days_from_metrics is not None and earn_in_days_from_metrics > 0:
            return now + timedelta(days=earn_in_days_from_metrics)
    
    # 2) Try from tickers table (if we store it there)
    try:
        ticker_row = (
            sb.table("tickers")
            .select("ticker, metrics")
            .eq("ticker", ticker)
            .limit(1)
            .execute()
            .data
        )
        
        if ticker_row and ticker_row[0].get("metrics"):
            metrics = ticker_row[0]["metrics"]
            if isinstance(metrics, dict):
                earn_in_days = metrics.get("earnings_in_days")
                if earn_in_days is not None and earn_in_days > 0:
                    return now + timedelta(days=earn_in_days)
    except Exception:
        pass
    
    # 3) Fallback to FMP if available (but don't block if unavailable)
    if fmp:
        try:
            profile = fmp.profile(ticker) or {}
            # FMP profile may have earnings date fields (varies by subscription)
            # This is best-effort
            earnings_date_str = profile.get("lastDivDate") or profile.get("nextEarningsDate")
            if earnings_date_str:
                try:
                    if isinstance(earnings_date_str, str):
                        if "T" in earnings_date_str:
                            earnings_date = datetime.fromisoformat(earnings_date_str.replace("Z", "+00:00")).date()
                        else:
                            earnings_date = date.fromisoformat(earnings_date_str[:10])
                        if earnings_date > now:
                            return earnings_date
                except Exception:
                    pass
        except Exception:
            pass
    
    return None


# ---------- Main ----------

def main() -> None:
    # Load wheel rules
    rules = load_wheel_rules()
    mode = "test" if CC_TEST_MODE else "positions"
    logger.info(
        f"Wheel rules in effect: "
        f"CC delta=[{rules.cc_delta_min:.2f}, {rules.cc_delta_max:.2f}], "
        f"DTE primary=[{rules.dte_min_primary}, {rules.dte_max_primary}], "
        f"DTE fallback=[{rules.dte_min_fallback}, {rules.dte_max_fallback}] "
        f"(allow_fallback={rules.allow_fallback_dte}), "
        f"earnings_avoid_days={rules.earnings_avoid_days}, "
        f"liquidity: max_spread={MAX_SPREAD_PCT}%, min_oi={MIN_OPEN_INTEREST}, "
        f"allow_itm_calls={ALLOW_ITM_CALLS}"
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

    # 2) Get eligible positions (test mode or real positions)
    eligible_positions: List[Dict[str, Any]] = []
    
    if CC_TEST_MODE:
        # Test mode: use CC_TEST_TICKERS as synthetic positions
        test_tickers = [t.strip().upper() for t in CC_TEST_TICKERS.split(",") if t.strip()]
        logger.info(f"TEST MODE: Using test tickers: {test_tickers}")
        
        for ticker in test_tickers:
            eligible_positions.append({
                "symbol": ticker,
                "quantity": 100,  # Synthetic quantity
                "current_price": None,  # Will be fetched from chain
                "asset_type": "EQUITY",
                "raw_position": None,
            })
        
        logger.info(f"TEST MODE: Created {len(eligible_positions)} synthetic positions")
    else:
        # Normal mode: fetch Schwab positions
        logger.info("Fetching Schwab account positions...")
        schwab = SchwabClient.from_env()
        acct = schwab.get_account(fields="positions")
        
        # Parse positions from account response
        positions: List[Dict[str, Any]] = []
        sec = acct.get("securitiesAccount") if isinstance(acct, dict) else acct
        if isinstance(sec, dict):
            positions = sec.get("positions") or []
        
        logger.info(f"Found {len(positions)} total positions")
        
        # Filter eligible positions (equities, long, quantity >= 100)
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            
            instr = pos.get("instrument") or {}
            asset_type = instr.get("assetType")
            symbol = instr.get("symbol") or instr.get("underlyingSymbol") or ""
            
            # Only equities
            if asset_type not in ("EQUITY", "STOCK"):
                continue
            
            # Get quantity (long only)
            quantity = _safe_float(pos.get("longQuantity") or pos.get("quantity"), 0.0) or 0.0
            if quantity < 100:
                continue
            
            # Get current price
            current_price = _safe_float(pos.get("averagePrice") or pos.get("lastPrice"))
            if current_price is None:
                # Try to get from marketValue / quantity
                market_value = _safe_float(pos.get("marketValue"))
                if market_value and quantity:
                    current_price = market_value / quantity
                else:
                    current_price = _safe_float(instr.get("lastPrice"))
            
            eligible_positions.append({
                "symbol": symbol,
                "quantity": quantity,
                "current_price": current_price,
                "asset_type": asset_type,
                "raw_position": pos,
            })
        
        logger.info(f"Found {len(eligible_positions)} eligible positions (equity, long, >=100 shares)")
    
    # Limit to CC_PICKS_N
    eligible_positions = eligible_positions[:CC_PICKS_N]
    
    if not eligible_positions:
        logger.warning(f"No eligible positions found (mode={mode})")
        return
    
    # 3) Get candidate data for these tickers (for copying fields and earnings lookup)
    ticker_symbols = [p["symbol"] for p in eligible_positions]
    candidates_map: Dict[str, Dict[str, Any]] = {}
    if ticker_symbols:
        try:
            cands = (
                sb.table("screening_candidates")
                .select("*")
                .eq("run_id", run_id)
                .in_("ticker", ticker_symbols)
                .execute()
                .data
                or []
            )
            for c in cands:
                candidates_map[c.get("ticker")] = c
        except Exception as e:
            logger.warning(f"Could not fetch candidate data: {e}")
    
    # 4) Initialize FMP client for earnings lookup fallback (optional)
    fmp: Optional[FMPStableClient] = None
    try:
        fmp = FMPStableClient()
    except Exception as e:
        logger.debug(f"FMP client not available for earnings lookup: {e}")
    
    # 5) Fetch option chains and build picks
    md = SchwabMarketDataClient()
    
    pick_rows: List[Dict[str, Any]] = []
    
    # Skip counters by reason
    skipped_earnings_blocked = 0
    skipped_no_chain = 0
    skipped_no_contract_in_dte = 0
    skipped_no_contract_in_delta = 0
    skipped_liquidity_failed = 0
    skipped_no_shares = 0
    
    now = datetime.now(timezone.utc).date()

    for pos in eligible_positions:
        ticker = pos["symbol"]
        quantity = pos["quantity"]
        current_price = pos.get("current_price")
        
        if not ticker:
            continue
        
        if quantity < 100:
            skipped_no_shares += 1
            continue
        
        try:
            # Check earnings exclusion
            candidate = candidates_map.get(ticker)
            earnings_date = _get_earnings_date_for_ticker(ticker, candidate, sb, fmp, now)
            
            if not earnings_ok(earnings_date, now=now, avoid_days=rules.earnings_avoid_days):
                skipped_earnings_blocked += 1
                earn_in_days = (earnings_date - now).days if earnings_date else None
                logger.warning(
                    f"{ticker}: skipped (earnings in {earn_in_days} days, "
                    f"within avoid_days={rules.earnings_avoid_days})"
                )
                continue
            elif earnings_date is None:
                logger.debug(f"{ticker}: earnings date unknown (not excluded)")
            
            # Fetch option chain (CALLS)
            chain = md.get_option_chain(ticker, contract_type="CALL", strike_count=80)
            if not chain:
                skipped_no_chain += 1
                logger.warning(f"{ticker}: no option chain returned")
                continue
            
            # Get underlying price from chain if current_price is missing (especially in test mode)
            if current_price is None:
                current_price = _safe_float(chain.get("underlyingPrice") if isinstance(chain, dict) else None)
                if current_price is None:
                    skipped_no_contract_in_delta += 1
                    logger.warning(f"{ticker}: cannot determine current price for OTM filter")
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
            
            # Extract CALL options for this expiration
            calls = _extract_call_options_for_exp(chain, exp)
            if not calls:
                skipped_no_contract_in_delta += 1
                logger.warning(f"{ticker}: no CALLs extracted for exp={exp}")
                continue
            
            # Choose best CALL in delta band [CC_DELTA_MIN, CC_DELTA_MAX]
            best = _choose_best_call_in_delta_band(
                calls,
                target_delta_low=rules.cc_delta_min,
                target_delta_high=rules.cc_delta_max,
                current_price=current_price,
                expiration=exp,
                rules=rules,
            )
            if not best:
                skipped_no_contract_in_delta += 1
                logger.warning(
                    f"{ticker}: no CALLs in delta band "
                    f"[{rules.cc_delta_min:.2f}, {rules.cc_delta_max:.2f}] "
                    f"with liquidity (bid>0, spread<{MAX_SPREAD_PCT}%, oi>={MIN_OPEN_INTEREST}) "
                    f"and OTM (allow_itm={ALLOW_ITM_CALLS})"
                )
                continue
            
            # Extract values
            strike = _safe_float(best.get("strike"))
            premium = _safe_float(best.get("_premium"))
            delta = _safe_float(best.get("_delta"))
            bid = _safe_float(best.get("bid"), 0.0) or 0.0
            ask = _safe_float(best.get("ask"), 0.0) or 0.0
            dte = (exp - now).days
            ann_yld = _safe_float(best.get("_annualized_yield"))
            
            # Log successful pick with window used
            logger.info(
                f"{ticker}: CC pick created | window={window_used} | "
                f"exp={exp.isoformat()} | dte={dte} | strike={strike} | "
                f"bid={bid:.2f} | delta={delta:.3f} | yield={ann_yld:.2%} | "
                f"shares={quantity} | mode={mode}"
            )
            
            # Get RSI period/interval from candidate metrics or use defaults
            candidate_metrics = candidate.get("metrics") if candidate else {}
            rsi_period = candidate_metrics.get("rsi_period") if isinstance(candidate_metrics, dict) else None
            rsi_interval = candidate_metrics.get("rsi_interval") if isinstance(candidate_metrics, dict) else None
            if not rsi_period:
                rsi_period = rules.rsi_period
            if not rsi_interval:
                rsi_interval = rules.rsi_interval
            
            pick_rows.append({
                "run_id": run_id,
                "ticker": ticker,
                "action": "CC",
                "dte": dte,
                "target_delta": delta,  # Store delta for calls (positive)
                "expiration": exp.isoformat(),
                "strike": strike,
                "premium": premium,
                "annualized_yield": ann_yld,
                "delta": delta,
                # Carry-through fields from screening_candidates (if available)
                "score": candidate.get("score") if candidate else None,
                "rank": candidate.get("rank") if candidate else None,
                "price": candidate.get("price") if candidate else current_price,
                "iv": candidate.get("iv") if candidate else None,
                "iv_rank": candidate.get("iv_rank") if candidate else None,
                "beta": candidate.get("beta") if candidate else None,
                "rsi": candidate.get("rsi") if candidate else None,
                "earn_in_days": candidate.get("earn_in_days") if candidate else None,
                "sentiment_score": candidate.get("sentiment_score") if candidate else None,
                "pick_metrics": {
                    "rule_context": {
                        "used_dte_window": window_used,
                        "earnings_avoid_days": rules.earnings_avoid_days,
                        "delta_band": [rules.cc_delta_min, rules.cc_delta_max],
                        "rsi_period": rsi_period,
                        "rsi_interval": rsi_interval,
                        "allow_itm_calls": ALLOW_ITM_CALLS,
                    },
                    "expiration": exp.isoformat(),
                    "quantity": quantity,
                    "current_price": current_price,
                    "mode": mode,
                    "earnings_date": earnings_date.isoformat() if earnings_date else None,
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
                        "dte": dte,
                    },
                },
            })
            
        except Exception as e:
            skipped_no_chain += 1
            logger.exception(f"{ticker}: failed to build CC pick: {e}")
    
    # Log summary with skip counts by reason
    logger.info(
        f"CC pick generation summary (mode={mode}): "
        f"processed_positions={len(eligible_positions)}, "
        f"created={len(pick_rows)}, "
        f"skipped_earnings_blocked={skipped_earnings_blocked}, "
        f"skipped_no_chain={skipped_no_chain}, "
        f"skipped_no_contract_in_dte={skipped_no_contract_in_dte}, "
        f"skipped_no_contract_in_delta={skipped_no_contract_in_delta}, "
        f"skipped_liquidity_failed={skipped_liquidity_failed}, "
        f"skipped_no_shares={skipped_no_shares}"
    )
    
    # Log top-10 summary of selected contracts
    if pick_rows:
        top10 = sorted(pick_rows, key=lambda x: x.get("annualized_yield") or 0.0, reverse=True)[:10]
        logger.info("Top 10 CC picks by annualized yield:")
        for i, pick in enumerate(top10, start=1):
            ticker = pick.get("ticker")
            yield_pct = pick.get("annualized_yield", 0.0) * 100.0
            strike = pick.get("strike")
            dte = pick.get("dte")
            delta = pick.get("delta")
            logger.info(
                f"  {i}. {ticker}: {yield_pct:.2f}% yield | "
                f"strike={strike} | dte={dte} | delta={delta:.3f}"
            )
    
    if not pick_rows:
        logger.warning(f"No CC picks were generated (mode={mode}).")
        return
    
    # 6) Delete existing CC picks for this run_id, then insert new ones
    logger.info(f"Deleting existing CC picks for run_id={run_id}")
    delete_res = (
        sb.table("screening_picks")
        .delete()
        .eq("run_id", run_id)
        .eq("action", "CC")
        .execute()
    )
    
    # 7) Insert new picks (batch insert)
    logger.info(f"Inserting {len(pick_rows)} screening_picks rows...")
    insert_res = sb.table("screening_picks").insert(pick_rows).execute()
    
    # Check for errors
    if hasattr(insert_res, "error") and insert_res.error:
        raise RuntimeError(f"Supabase error inserting picks: {insert_res.error}")
    
    logger.info(f"✅ build_cc_picks complete. Created {len(pick_rows)} CC picks for run_id={run_id} (mode={mode})")


if __name__ == "__main__":
    main()
