"""
IV Snapshot Worker: Fetches IV from Schwab option chains and caches in Supabase.
Runs daily to populate iv_snapshots table for use by weekly_screener.
"""
from __future__ import annotations

from datetime import datetime, timezone, date, timedelta
import csv
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dotenv import load_dotenv
from loguru import logger
import os
import time

# Load environment variables
load_dotenv(".env.local")

from wheel.clients.schwab_marketdata_client import SchwabMarketDataClient
from wheel.clients.supabase_client import get_supabase, upsert_rows
from apps.worker.src.config.wheel_rules import load_wheel_rules
import pytz


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
        strike = _safe_float(opt.get("strike"))
        if strike is None:
            continue
        
        diff = abs(strike - underlying_price)
        if diff < best_diff:
            best_diff = diff
            best_option = opt
    
    return best_option


def _extract_underlying_price(chain: Dict[str, Any]) -> Optional[float]:
    """
    Extract underlying price from option chain.
    Tries multiple field names.
    """
    if not isinstance(chain, dict):
        return None
    
    # Try common field names
    price = (
        chain.get("underlyingPrice") or
        chain.get("underlying_price") or
        chain.get("underlying") or
        chain.get("quote", {}).get("lastPrice") if isinstance(chain.get("quote"), dict) else None
    )
    
    return _safe_float(price)


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
        la_tz = pytz.timezone("America/Los_Angeles")
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
                chain = schwab.get_option_chain(symbol, contract_type="PUT", strike_count=50)
                
                if not chain or not isinstance(chain, dict):
                    skipped_no_chain += 1
                    logger.debug(f"{symbol}: no option chain")
                    continue
                
                # Extract underlying price
                underlying_price = _extract_underlying_price(chain)
                if underlying_price is None or underlying_price <= 0:
                    skipped_no_chain += 1
                    logger.debug(f"{symbol}: missing underlying price")
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
                
                # Extract IV
                iv = _safe_float(atm_put.get("impliedVolatility") or atm_put.get("volatility") or atm_put.get("iv"))
                if iv is None or iv <= 0:
                    skipped_no_iv += 1
                    logger.debug(f"{symbol}: missing or zero IV")
                    continue
                
                strike = _safe_float(atm_put.get("strike"))
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
                logger.warning(f"{symbol}: error during IV snapshot: {e}")
                continue
        
        # Summary log
        logger.info(
            f"IV Snapshot complete: processed={processed}, inserted={inserted}, updated={updated}, "
            f"skipped_no_chain={skipped_no_chain}, skipped_no_exp={skipped_no_exp}, "
            f"skipped_no_iv={skipped_no_iv}, errors={errors}"
        )
        
    except Exception as e:
        logger.exception(f"IV Snapshot failed: {e}")
        raise


if __name__ == "__main__":
    main()

