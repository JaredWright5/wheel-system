from __future__ import annotations

from datetime import datetime, timezone, date, timedelta
from typing import Any, Dict, List, Optional, Tuple
import os
from dotenv import load_dotenv
from loguru import logger

from wheel.clients.supabase_client import get_supabase
from wheel.clients.schwab_client import SchwabClient
from wheel.clients.schwab_marketdata_client import SchwabMarketDataClient

# Load environment variables from .env.local
load_dotenv(".env.local")


# ---------- Configuration ----------

# Allow env override of run_id for reruns
RUN_ID = os.getenv("RUN_ID")  # None = use latest

# Number of positions to process (default 25)
CC_PICKS_N = int(os.getenv("CC_PICKS_N", "25"))

# DTE constraints for weeklies (primary window)
MIN_DTE = int(os.getenv("MIN_DTE", "4"))
MAX_DTE = int(os.getenv("MAX_DTE", "10"))

# Fallback DTE windows
FALLBACK_MAX_DTE_1 = int(os.getenv("FALLBACK_MAX_DTE_1", "14"))
FALLBACK_MIN_DTE_2 = int(os.getenv("FALLBACK_MIN_DTE_2", "1"))
FALLBACK_MAX_DTE_2 = int(os.getenv("FALLBACK_MAX_DTE_2", "21"))

# Delta band for covered calls
DELTA_MIN = float(os.getenv("DELTA_MIN", "0.20"))
DELTA_MAX = float(os.getenv("DELTA_MAX", "0.30"))

# Ex-dividend guardrail
EXDIV_SKIP_DAYS = int(os.getenv("EXDIV_SKIP_DAYS", "2"))

# Test mode: comma-separated tickers (e.g., "AAPL,MSFT")
CC_TEST_TICKERS = os.getenv("CC_TEST_TICKERS", "").strip()
CC_TEST_MODE = bool(CC_TEST_TICKERS)


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


def _choose_best_call_in_delta_band(
    options: List[Dict[str, Any]],
    *,
    target_delta_low: float = 0.20,
    target_delta_high: float = 0.30,
    current_price: float,
    expiration: date,
) -> Optional[Dict[str, Any]]:
    """
    Choose the best CALL option in the target delta band that maximizes annualized yield.
    
    Requirements:
    - delta in [target_delta_low, target_delta_high] (calls have positive delta)
    - bid > 0
    - Prefer OTM calls (strike >= current_price)
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

        # Check bid > 0
        bid = _safe_float(o.get("bid"), 0.0) or 0.0
        if bid <= 0:
            continue

        # Prefer OTM calls (strike >= current_price)
        strike = _safe_float(o.get("strike"))
        if strike is None or strike <= 0:
            continue
        
        if strike < current_price:
            # ITM call - skip (prefer OTM)
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

        # Calculate annualized yield: (premium / strike) * (365 / dte)
        premium_yield = mark / strike
        annualized_yield = premium_yield * (365.0 / float(dte))

        candidates.append({
            **o,
            "_delta": delta,
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


def _check_exdiv_guardrail(ticker: str, sb: Any) -> Tuple[bool, Optional[str]]:
    """
    Check if ticker has ex-dividend date within EXDIV_SKIP_DAYS.
    Returns (should_skip, exdiv_date_str or None)
    """
    today = datetime.now(timezone.utc).date()
    cutoff_date = today + timedelta(days=EXDIV_SKIP_DAYS)
    
    try:
        # Try to get ex-div date from tickers table (if we store it)
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
                exdiv_str = metrics.get("exDividendDate") or metrics.get("ex_dividend_date")
                if exdiv_str:
                    try:
                        exdiv_date = date.fromisoformat(str(exdiv_str)[:10])
                        if today <= exdiv_date <= cutoff_date:
                            return True, exdiv_date.isoformat()
                    except Exception:
                        pass
    except Exception:
        # If we can't determine ex-div, proceed but enforce OTM strictly
        pass
    
    return False, None


# ---------- Main ----------

def main() -> None:
    mode = "test" if CC_TEST_MODE else "positions"
    logger.info(
        f"Starting build_cc_picks (mode={mode}, CC_PICKS_N={CC_PICKS_N}, "
        f"primary=[{MIN_DTE},{MAX_DTE}], "
        f"fallback1=[{MIN_DTE},{FALLBACK_MAX_DTE_1}], "
        f"fallback2=[{FALLBACK_MIN_DTE_2},{FALLBACK_MAX_DTE_2}], "
        f"delta=[{DELTA_MIN},{DELTA_MAX}])..."
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
    
    # 3) Get candidate data for these tickers (for copying fields)
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
    
    # 4) Fetch option chains and build picks
    md = SchwabMarketDataClient()
    
    pick_rows: List[Dict[str, Any]] = []
    skipped_no_chain = 0
    skipped_no_exp = 0
    skipped_no_calls_in_band = 0
    skipped_exdiv = 0
    skipped_no_shares = 0
    
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
            # Check ex-div guardrail
            should_skip_exdiv, exdiv_date = _check_exdiv_guardrail(ticker, sb)
            if should_skip_exdiv:
                skipped_exdiv += 1
                logger.warning(f"{ticker}: skipping due to ex-div date {exdiv_date} within {EXDIV_SKIP_DAYS} days")
                continue
            
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
                    skipped_no_calls_in_band += 1
                    logger.warning(f"{ticker}: cannot determine current price for OTM filter")
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
            
            # Extract CALL options for this expiration
            calls = _extract_call_options_for_exp(chain, exp)
            if not calls:
                skipped_no_calls_in_band += 1
                logger.warning(f"{ticker}: no CALLs extracted for exp={exp}")
                continue
            
            # Choose best CALL in delta band [DELTA_MIN, DELTA_MAX]
            best = _choose_best_call_in_delta_band(
                calls,
                target_delta_low=DELTA_MIN,
                target_delta_high=DELTA_MAX,
                current_price=current_price,
                expiration=exp,
            )
            if not best:
                skipped_no_calls_in_band += 1
                logger.warning(f"{ticker}: no CALLs in delta band [{DELTA_MIN}, {DELTA_MAX}] with bid>0 and OTM")
                continue
            
            # Extract values
            strike = _safe_float(best.get("strike"))
            premium = _safe_float(best.get("_premium"))
            delta = _safe_float(best.get("_delta"))
            bid = _safe_float(best.get("bid"), 0.0) or 0.0
            today = datetime.now(timezone.utc).date()
            dte = (exp - today).days
            ann_yld = _safe_float(best.get("_annualized_yield"))
            
            # Get candidate data for this ticker (if available)
            candidate = candidates_map.get(ticker, {})
            
            # Log successful pick with window used
            logger.info(
                f"{ticker}: CC pick created | window={window_used} | "
                f"exp={exp.isoformat()} | dte={dte} | strike={strike} | "
                f"bid={bid:.2f} | delta={delta:.3f} | yield={ann_yld:.2%} | "
                f"shares={quantity} | mode={mode}"
            )
            
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
                "score": candidate.get("score"),
                "rank": candidate.get("rank"),
                "price": candidate.get("price") or current_price,
                "iv": candidate.get("iv"),
                "iv_rank": candidate.get("iv_rank"),
                "beta": candidate.get("beta"),
                "rsi": candidate.get("rsi"),
                "earn_in_days": candidate.get("earn_in_days"),
                "sentiment_score": candidate.get("sentiment_score"),
                "pick_metrics": {
                    "expiration": exp.isoformat(),
                    "window_used": window_used,
                    "quantity": quantity,
                    "current_price": current_price,
                    "mode": mode,
                    "exdiv_checked": exdiv_date is not None,
                    "exdiv_date": exdiv_date,
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
    
    # Log summary
    logger.info(
        f"CC pick generation summary (mode={mode}): "
        f"processed_positions={len(eligible_positions)}, "
        f"eligible_positions={len(eligible_positions)}, "
        f"created={len(pick_rows)}, "
        f"skipped_no_chain={skipped_no_chain}, "
        f"skipped_no_exp={skipped_no_exp}, "
        f"skipped_no_calls_in_band={skipped_no_calls_in_band}, "
        f"skipped_exdiv={skipped_exdiv}, "
        f"skipped_no_shares={skipped_no_shares}"
    )
    
    if not pick_rows:
        logger.warning(f"No CC picks were generated (mode={mode}).")
        return
    
    # 5) Delete existing CC picks for this run_id, then insert new ones
    logger.info(f"Deleting existing CC picks for run_id={run_id}")
    delete_res = (
        sb.table("screening_picks")
        .delete()
        .eq("run_id", run_id)
        .eq("action", "CC")
        .execute()
    )
    
    # 6) Insert new picks (batch insert)
    logger.info(f"Inserting {len(pick_rows)} screening_picks rows...")
    insert_res = sb.table("screening_picks").insert(pick_rows).execute()
    
    # Check for errors
    if hasattr(insert_res, "error") and insert_res.error:
        raise RuntimeError(f"Supabase error inserting picks: {insert_res.error}")
    
    logger.info(f"✅ build_cc_picks complete. Created {len(pick_rows)} CC picks for run_id={run_id} (mode={mode})")


if __name__ == "__main__":
    main()
