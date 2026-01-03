"""
IV Snapshot Worker: Fetches IV from Schwab option chains and caches in Supabase.
Runs daily to populate iv_snapshots table for use by weekly_screener.
"""
from __future__ import annotations

from datetime import datetime, timezone, date, timedelta
from zoneinfo import ZoneInfo
import csv
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dotenv import load_dotenv
from loguru import logger
import os
import time
import statistics

# Load environment variables
load_dotenv(".env.local")

from wheel.clients.schwab_marketdata_client import SchwabMarketDataClient
from wheel.clients.supabase_client import get_supabase, upsert_rows
from apps.worker.src.config.wheel_rules import load_wheel_rules


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    """Safely convert to float, returning default on error."""
    try:
        if x is None:
            return default
        return float(x)
    except (ValueError, TypeError):
        return default


def load_universe_csv(path: str) -> List[Dict[str, Any]]:
    """Load universe from CSV file."""
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"Universe CSV not found: {path}")
    out: List[Dict[str, Any]] = []
    with p.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym = (row.get("symbol") or "").strip()
            if sym:
                out.append({"symbol": sym, "name": sym, "exchange": None})
    return out


def _extract_underlying_price(chain: Dict[str, Any]) -> Tuple[Optional[float], Dict[str, Any]]:
    """
    Robustly extract underlying price from option chain response.
    
    Returns:
        (price, diagnostics_dict) where diagnostics includes keys_seen, has_puts, has_calls
    """
    diagnostics = {
        "keys_seen": [],
        "has_puts": False,
        "has_calls": False,
    }
    
    if not isinstance(chain, dict):
        return None, diagnostics
    
    diagnostics["keys_seen"] = list(chain.keys())[:20]  # First 20 keys for logging
    
    # Priority order for underlying price extraction
    # 1) resp.get("underlyingPrice")
    price = chain.get("underlyingPrice")
    if price is not None:
        price_float = _safe_float(price)
        if price_float and price_float > 0:
            return price_float, diagnostics
    
    # 2) resp.get("underlying", {}).get("last")
    underlying = chain.get("underlying")
    if isinstance(underlying, dict):
        price = underlying.get("last")
        if price is not None:
            price_float = _safe_float(price)
            if price_float and price_float > 0:
                return price_float, diagnostics
        
        # 3) resp.get("underlying", {}).get("mark")
        price = underlying.get("mark")
        if price is not None:
            price_float = _safe_float(price)
            if price_float and price_float > 0:
                return price_float, diagnostics
        
        # 4) resp.get("underlying", {}).get("quote", {}).get("lastPrice")
        quote = underlying.get("quote")
        if isinstance(quote, dict):
            price = quote.get("lastPrice")
            if price is not None:
                price_float = _safe_float(price)
                if price_float and price_float > 0:
                    return price_float, diagnostics
            
            # 5) resp.get("underlying", {}).get("quote", {}).get("mark")
            price = quote.get("mark")
            if price is not None:
                price_float = _safe_float(price)
                if price_float and price_float > 0:
                    return price_float, diagnostics
    
    # 6) Last resort: infer from option strikes (median strike as proxy)
    # Check if we have PUT options
    put_map = chain.get("putExpDateMap")
    if isinstance(put_map, dict):
        diagnostics["has_puts"] = True
        strikes = []
        for strikes_map in put_map.values():
            if isinstance(strikes_map, dict):
                for strike_str, opt_list in strikes_map.items():
                    if isinstance(opt_list, list) and opt_list:
                        strike = _safe_float(strike_str)
                        if strike and strike > 0:
                            strikes.append(strike)
        
        if strikes:
            median_strike = statistics.median(strikes)
            logger.debug(f"Inferred underlying price from median PUT strike: {median_strike:.2f}")
            return median_strike, diagnostics
    
    # Check calls as well (for diagnostics)
    call_map = chain.get("callExpDateMap")
    if isinstance(call_map, dict):
        diagnostics["has_calls"] = True
    
    return None, diagnostics


def _parse_expirations_from_chain(chain: Dict[str, Any]) -> List[date]:
    """
    Parse expiration dates from Schwab option chain.
    Supports TD-style putExpDateMap keys like "2026-01-02:4"
    """
    expirations: List[date] = []

    if not isinstance(chain, dict):
        return expirations

    # TD-style maps (Schwab uses this format)
    for key in ("putExpDateMap", "callExpDateMap"):
        m = chain.get(key)
        if isinstance(m, dict):
            for k in m.keys():
                # "YYYY-MM-DD:##"
                try:
                    ds = k.split(":")[0]
                    expirations.append(date.fromisoformat(ds))
                except Exception:
                    pass

    # If Schwab ever returns explicit expiration list
    exp_list = chain.get("expirations") or chain.get("expirationDates")
    if isinstance(exp_list, list):
        for item in exp_list:
            try:
                expirations.append(date.fromisoformat(str(item)[:10]))
            except Exception:
                pass

    # Dedup
    return sorted(list(set(expirations)))


def _calculate_dte(exp_date: date, asof_date: date) -> int:
    """Calculate days to expiration."""
    return (exp_date - asof_date).days


def _find_expiration_in_window(
    expirations: List[date],
    asof_date: date,
    dte_min: int,
    dte_max: int,
) -> Optional[date]:
    """
    Find expiration date within DTE window.
    
    Returns:
        Expiration date if found, None otherwise
    """
    for exp in expirations:
        dte = _calculate_dte(exp, asof_date)
        if dte_min <= dte <= dte_max:
            return exp
    return None


def _extract_put_options_for_exp(chain: Dict[str, Any], exp: date) -> List[Dict[str, Any]]:
    """
    Robustly extract PUT option contracts for a specific expiration date.
    Supports TD-style exp-date maps and flat lists.
    """
    results: List[Dict[str, Any]] = []
    exp_key_prefix = exp.isoformat()

    # Try TD-style putExpDateMap structure
    put_map = chain.get("putExpDateMap")
    if isinstance(put_map, dict):
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
                        # Create a copy and ensure strike is set
                        opt_copy = dict(opt)
                        strike = _safe_float(opt_copy.get("strikePrice") or opt_copy.get("strike") or strike_str)
                        if strike is not None:
                            opt_copy["strike"] = strike
                            # Ensure expirationDate is set
                            if "expirationDate" not in opt_copy:
                                opt_copy["expirationDate"] = exp.isoformat()
                            results.append(opt_copy)
    
    # Try flat list structure (if Schwab returns puts as a list)
    puts_list = chain.get("puts")
    if isinstance(puts_list, list):
        for opt in puts_list:
            if not isinstance(opt, dict):
                continue
            exp_date_str = opt.get("expirationDate") or opt.get("expDate") or opt.get("expiration")
            if exp_date_str:
                try:
                    opt_exp = date.fromisoformat(str(exp_date_str)[:10])
                    if opt_exp == exp:
                        opt_copy = dict(opt)
                        strike = _safe_float(opt_copy.get("strikePrice") or opt_copy.get("strike"))
                        if strike is not None:
                            opt_copy["strike"] = strike
                            results.append(opt_copy)
                except Exception:
                    pass

    return results


def _find_atm_put(
    options: List[Dict[str, Any]],
    underlying_price: float,
) -> Optional[Dict[str, Any]]:
    """
    Find the PUT option with strike closest to underlying price (ATM).
    
    Args:
        options: List of PUT option dictionaries
        underlying_price: Current underlying stock price
        
    Returns:
        Option dictionary with closest strike, or None if no options
    """
    if not options or underlying_price <= 0:
        return None
    
    best_option = None
    best_diff = float('inf')
    
    for opt in options:
        strike = _safe_float(opt.get("strike") or opt.get("strikePrice"))
        if strike is None:
            continue
        
        diff = abs(strike - underlying_price)
        if diff < best_diff:
            best_diff = diff
            best_option = opt
    
    return best_option


def _extract_iv_from_contract(contract: Dict[str, Any]) -> Optional[float]:
    """
    Extract IV from option contract, handling percent normalization.
    
    If IV appears as percent (e.g., 25), normalize to 0.25 ONLY if value > 3 (heuristic).
    
    Returns:
        IV as decimal (0.25 = 25%), or None if missing/invalid
    """
    # Try multiple field names
    iv = (
        contract.get("impliedVolatility") or
        contract.get("volatility") or
        contract.get("iv") or
        contract.get("impliedVol")
    )
    
    if iv is None:
        return None
    
    iv_float = _safe_float(iv)
    if iv_float is None:
        return None
    
    # Normalize if value > 3 (likely a percent like 25 instead of 0.25)
    if iv_float > 3.0:
        iv_float = iv_float / 100.0
    
    if iv_float <= 0:
        return None
    
    return iv_float


def main() -> None:
    try:
        # Load wheel rules for DTE windows
        rules = load_wheel_rules()
        logger.info(
            f"IV Snapshot: DTE primary=[{rules.dte_min_primary}, {rules.dte_max_primary}], "
            f"DTE fallback=[{rules.dte_min_fallback}, {rules.dte_max_fallback}]"
        )
        
        # Environment variables
        IV_LOOKBACK_DAYS = int(os.getenv("WHEEL_IV_LOOKBACK_DAYS", "252"))
        IV_ATM_METHOD = os.getenv("WHEEL_IV_ATM_METHOD", "nearest_put")
        MAX_SYMBOLS = int(os.getenv("WHEEL_IV_SNAPSHOT_MAX_SYMBOLS", "0")) or None  # 0 means no limit
        
        # Get today's date in America/Los_Angeles timezone
        la_tz = ZoneInfo("America/Los_Angeles")
        asof_date = datetime.now(la_tz).date()
        logger.info(f"IV Snapshot run for asof_date={asof_date.isoformat()}")
        
        # Load universe
        universe = load_universe_csv("data/universe_us.csv")
        if MAX_SYMBOLS:
            universe = universe[:MAX_SYMBOLS]
            logger.info(f"Limited to first {MAX_SYMBOLS} symbols for testing")
        
        logger.info(f"Universe size: {len(universe)}")
        
        # Initialize clients
        schwab = SchwabMarketDataClient()
        sb = get_supabase()
        
        # Statistics
        processed = 0
        inserted = 0
        updated = 0
        skipped_no_chain = 0
        skipped_no_exp = 0
        skipped_no_iv = 0
        skipped_no_underlying = 0
        errors = 0
        
        # Process each symbol
        for item in universe:
            symbol = item.get("symbol")
            if not symbol:
                continue
            
            processed += 1
            
            try:
                # Rate limiting: sleep between requests
                if processed > 1:
                    time.sleep(0.5)  # 500ms between requests
                
                # Fetch option chain
                chain = schwab.get_option_chain(symbol)
                
                if not chain or not isinstance(chain, dict):
                    skipped_no_chain += 1
                    logger.debug(f"{symbol}: no option chain")
                    continue
                
                # Robustly extract underlying price
                underlying_price, price_diag = _extract_underlying_price(chain)
                if underlying_price is None or underlying_price <= 0:
                    skipped_no_underlying += 1
                    keys_seen = ",".join(price_diag.get("keys_seen", [])[:10])
                    has_puts = price_diag.get("has_puts", False)
                    has_calls = price_diag.get("has_calls", False)
                    logger.debug(
                        f"{symbol}: missing underlying price "
                        f"(keys_seen={keys_seen}, has_puts={has_puts}, has_calls={has_calls})"
                    )
                    continue
                
                # Parse expirations
                expirations = _parse_expirations_from_chain(chain)
                if not expirations:
                    skipped_no_exp += 1
                    logger.debug(f"{symbol}: no expirations found")
                    continue
                
                # Find expiration in DTE window (primary first, then fallback)
                target_exp = _find_expiration_in_window(
                    expirations,
                    asof_date,
                    rules.dte_min_primary,
                    rules.dte_max_primary,
                )
                
                if not target_exp and rules.allow_fallback_dte:
                    target_exp = _find_expiration_in_window(
                        expirations,
                        asof_date,
                        rules.dte_min_fallback,
                        rules.dte_max_fallback,
                    )
                
                if not target_exp:
                    skipped_no_exp += 1
                    logger.debug(f"{symbol}: no expiration in DTE window")
                    continue
                
                dte = _calculate_dte(target_exp, asof_date)
                
                # Extract PUT options for target expiration
                put_options = _extract_put_options_for_exp(chain, target_exp)
                if not put_options:
                    skipped_no_iv += 1
                    logger.debug(f"{symbol}: no PUT options for expiration {target_exp}")
                    continue
                
                # Find ATM PUT (nearest strike to underlying price)
                atm_put = _find_atm_put(put_options, underlying_price)
                if not atm_put:
                    skipped_no_iv += 1
                    logger.debug(f"{symbol}: no ATM PUT found")
                    continue
                
                # Extract IV with normalization
                iv = _extract_iv_from_contract(atm_put)
                if iv is None:
                    skipped_no_iv += 1
                    logger.debug(f"{symbol}: missing or zero IV")
                    continue
                
                strike = _safe_float(atm_put.get("strike") or atm_put.get("strikePrice"))
                if strike is None:
                    skipped_no_iv += 1
                    logger.debug(f"{symbol}: missing strike")
                    continue
                
                # Prepare row for upsert
                row = {
                    "symbol": symbol,
                    "asof_date": asof_date.isoformat(),
                    "exp_date": target_exp.isoformat(),
                    "dte": dte,
                    "strike": float(strike),
                    "underlying_price": float(underlying_price),
                    "iv": float(iv),
                    "source": "schwab",
                }
                
                # Check if row already exists
                existing = sb.table("iv_snapshots").select("id").eq("symbol", symbol).eq("asof_date", asof_date.isoformat()).limit(1).execute()
                
                if existing.data and len(existing.data) > 0:
                    # Update existing
                    sb.table("iv_snapshots").update(row).eq("symbol", symbol).eq("asof_date", asof_date.isoformat()).execute()
                    updated += 1
                    logger.debug(f"{symbol}: updated IV={iv:.2%} strike={strike:.2f} dte={dte}")
                else:
                    # Insert new
                    upsert_rows("iv_snapshots", [row], keys=["symbol", "asof_date"])
                    inserted += 1
                    logger.debug(f"{symbol}: inserted IV={iv:.2%} strike={strike:.2f} dte={dte}")
                
            except Exception as e:
                errors += 1
                error_msg = str(e)[:200]  # Truncate long error messages
                logger.warning(f"{symbol}: error during IV snapshot: {error_msg}")
                continue
        
        # Summary log
        logger.info(
            f"IV Snapshot complete: processed={processed}, inserted={inserted}, updated={updated}, "
            f"skipped_no_chain={skipped_no_chain}, skipped_no_exp={skipped_no_exp}, "
            f"skipped_no_iv={skipped_no_iv}, skipped_no_underlying={skipped_no_underlying}, errors={errors}"
        )
        
    except Exception as e:
        logger.exception(f"IV Snapshot failed: {e}")
        raise


if __name__ == "__main__":
    main()
