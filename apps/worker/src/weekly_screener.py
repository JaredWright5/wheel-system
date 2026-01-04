"""
Weekly Screener v2: FMP Stable Universe + Enrichment
Uses FMP stable endpoints for universe building, fundamentals, technicals, and sentiment.
Integrates with WheelRules for configurable trading parameters.
Includes Fundamentals v1 enrichment with financial scores and growth data.
Includes IV enrichment from Schwab option chain snapshots.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta, date
import csv
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dotenv import load_dotenv
from loguru import logger
import os
import math
import statistics

# Load environment variables
load_dotenv(".env.local")
BUILD_SHA = os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or "local"

from wheel.clients.fmp_stable_client import FMPStableClient, simple_sentiment_score
from wheel.clients.supabase_client import insert_row, upsert_rows, update_rows, get_supabase
from apps.worker.src.config.wheel_rules import load_wheel_rules
from apps.worker.src.utils.symbols import normalize_equity_symbol, to_universe_symbol


@dataclass
class Candidate:
    ticker: str
    name: str
    sector: Optional[str]
    industry: Optional[str]
    market_cap: Optional[int]
    price: Optional[float]
    beta: Optional[float]
    rsi: Optional[float]
    next_earnings_date: Optional[date]
    earnings_in_days: Optional[int]
    earnings_source: str
    fundamentals_score: int
    sentiment_score: int
    trend_score: int
    technical_score: int
    wheel_score: int
    reasons: Dict[str, Any]
    features: Dict[str, Any]


def clamp_int(x: float, lo: int, hi: int) -> int:
    """Clamp value to integer range."""
    return max(lo, min(hi, int(round(x))))


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


def build_universe_fmp_stable(
    client: FMPStableClient,
    min_price: float = 5.0,
    min_market_cap: int = 2_000_000_000,
    min_avg_volume: Optional[int] = 1_000_000,
) -> List[Dict[str, Any]]:
    """
    Build universe using FMP stable company screener.
    
    Args:
        client: FMP stable client
        min_price: Minimum price filter
        min_market_cap: Minimum market cap filter
        min_avg_volume: Minimum average volume filter (optional)
        
    Returns:
        List of company dictionaries with symbol, name, exchange
    """
    logger.info(f"Building universe from FMP stable company screener (min_price={min_price}, min_mcap={min_market_cap})")
    
    # Fetch from major US exchanges
    # Note: Using smaller limits to avoid excessive API calls during enrichment
    # Each company requires ~6 API calls (profile, quote, ratios, metrics, RSI, news)
    all_companies = []
    exchanges = ["NYSE", "NASDAQ", "AMEX"]
    limit_per_exchange = 500  # Reasonable limit: 500 * 3 = 1500 companies max
    
    for exchange in exchanges:
        try:
            logger.info(f"  Fetching {exchange} (limit={limit_per_exchange})...")
            companies = client.company_screener(exchange=exchange, limit=limit_per_exchange)
            logger.info(f"  {exchange}: {len(companies)} companies fetched")
            all_companies.extend(companies)
        except Exception as e:
            logger.warning(f"  {exchange}: failed to fetch ({e}), continuing")
            continue
    
    logger.info(f"Total companies fetched: {len(all_companies)} (will be filtered further)")
    
    # Filter and dedupe
    seen_symbols = set()
    filtered = []
    
    for company in all_companies:
        symbol = company.get("symbol") or company.get("Symbol")
        if not symbol or symbol in seen_symbols:
            continue
        
        # Apply filters (best effort - fields may vary)
        price = company.get("price") or company.get("Price")
        market_cap = company.get("marketCap") or company.get("MarketCap") or company.get("mktCap")
        avg_volume = company.get("avgVolume") or company.get("AvgVolume") or company.get("averageVolume")
        
        if price and price < min_price:
            continue
        if market_cap and market_cap < min_market_cap:
            continue
        if min_avg_volume and avg_volume and avg_volume < min_avg_volume:
            continue
        
        seen_symbols.add(symbol)
        filtered.append({
            "symbol": symbol,
            "name": company.get("companyName") or company.get("name") or symbol,
            "exchange": exchange if "exchange" in company else None,
        })
    
    logger.info(f"Universe after filters: {len(filtered)} companies")
    return filtered


def fetch_earnings_calendar_range(
    client: FMPStableClient,
    start_date: date,
    end_date: date,
    universe_symbols: set,
) -> Dict[str, date]:
    """
    Fetch earnings calendar for a date range and map to universe symbols.
    
    Args:
        client: FMP stable client
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        universe_symbols: Set of symbols in our universe (for symbol matching)
        
    Returns:
        Dictionary mapping universe symbol -> earliest upcoming earnings date
    """
    CHUNK_DAYS = 7  # Configurable chunk size (7 days per chunk)
    now = datetime.now(timezone.utc).date()
    earnings_map: Dict[str, date] = {}
    all_events: List[Dict[str, Any]] = []
    
    # Build canonical universe symbol set for matching (once, before chunking)
    universe_canon = {normalize_equity_symbol(s) for s in universe_symbols}
    
    # Build reverse mapping: canonical -> original (for getting original symbol after matching)
    canonical_to_original: Dict[str, str] = {}
    for orig_sym in universe_symbols:
        canon_sym = normalize_equity_symbol(orig_sym)
        canonical_to_original[canon_sym] = orig_sym
    
    # Generate date chunks (7-day windows)
    chunks = []
    chunk_start = start_date
    while chunk_start <= end_date:
        chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS - 1), end_date)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end + timedelta(days=1)
    
    logger.info(f"Fetching earnings calendar in {len(chunks)} chunks of {CHUNK_DAYS} days each (range: {start_date.isoformat()} to {end_date.isoformat()})")
    
    # Fetch each chunk
    for chunk_idx, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        try:
            params = {
                "from": chunk_start.isoformat(),
                "to": chunk_end.isoformat(),
            }
            
            earnings_cal = client._get("earnings-calendar", params=params)
            
            if not earnings_cal:
                logger.warning(f"Chunk {chunk_idx}/{len(chunks)} [{chunk_start.isoformat()} to {chunk_end.isoformat()}]: empty response")
                continue
            
            if not isinstance(earnings_cal, list):
                logger.warning(f"Chunk {chunk_idx}/{len(chunks)} [{chunk_start.isoformat()} to {chunk_end.isoformat()}]: non-list response (type={type(earnings_cal)})")
                if isinstance(earnings_cal, dict):
                    sample = str(earnings_cal)[:200]
                    logger.debug(f"Sample response (first 200 chars): {sample}")
                continue
            
            chunk_event_count = len(earnings_cal)
            logger.info(f"Chunk {chunk_idx}/{len(chunks)} [{chunk_start.isoformat()} to {chunk_end.isoformat()}]: {chunk_event_count} events")
            all_events.extend(earnings_cal)
            
        except Exception as e:
            # Check if it's a 4xx error (402, 403, etc. - subscription/permission issues)
            error_str = str(e)
            if "402" in error_str or "403" in error_str or "4" in error_str[:3]:
                body_sample = error_str[-300:] if len(error_str) > 300 else error_str
                logger.error(
                    f"Chunk {chunk_idx}/{len(chunks)} [{chunk_start.isoformat()} to {chunk_end.isoformat()}]: "
                    f"4xx error (subscription/permission issue). Error: {body_sample}. Continuing..."
                )
            else:
                logger.warning(
                    f"Chunk {chunk_idx}/{len(chunks)} [{chunk_start.isoformat()} to {chunk_end.isoformat()}]: "
                    f"fetch failed: {e}. Continuing..."
                )
            continue
    
    # Log total events collected
    total_events_collected = len(all_events)
    logger.info(f"Total events collected across all chunks: {total_events_collected}")
    
    # Compute min/max earnings dates in collected events (for debugging)
    earnings_dates_seen: List[date] = []
    for item in all_events:
        if not isinstance(item, dict):
            continue
        earnings_date_str = (
            item.get("date") or
            item.get("Date") or
            item.get("earningsDate") or
            item.get("EarningsDate") or
            item.get("reportDate") or
            item.get("ReportDate") or
            None
        )
        if earnings_date_str:
            try:
                if isinstance(earnings_date_str, str):
                    if "T" in earnings_date_str:
                        earnings_date = datetime.fromisoformat(earnings_date_str.replace("Z", "+00:00")).date()
                    else:
                        earnings_date = date.fromisoformat(earnings_date_str[:10])
                    if earnings_date >= now:
                        earnings_dates_seen.append(earnings_date)
            except Exception:
                pass
    
    if earnings_dates_seen:
        min_earnings_date = min(earnings_dates_seen)
        max_earnings_date = max(earnings_dates_seen)
        logger.info(f"Earnings dates range in collected events: {min_earnings_date.isoformat()} to {max_earnings_date.isoformat()}")
    else:
        logger.warning("No valid future earnings dates found in collected events")
    
    # Parse earnings rows and map to universe symbols
    for item in all_events:
        if not isinstance(item, dict):
            continue
        
        # Extract symbol (try multiple field names)
        row_symbol = (
            item.get("symbol") or
            item.get("Symbol") or
            item.get("ticker") or
            item.get("Ticker") or
            None
        )
        if not row_symbol:
            continue
        
        # Extract earnings date (try multiple field names)
        earnings_date_str = (
            item.get("date") or
            item.get("Date") or
            item.get("earningsDate") or
            item.get("EarningsDate") or
            item.get("reportDate") or
            item.get("ReportDate") or
            None
        )
        if not earnings_date_str:
            continue
        
        # Parse date
        try:
            if isinstance(earnings_date_str, str):
                if "T" in earnings_date_str:
                    earnings_date = datetime.fromisoformat(earnings_date_str.replace("Z", "+00:00")).date()
                else:
                    earnings_date = date.fromisoformat(earnings_date_str[:10])
            else:
                continue
        except Exception:
            continue
        
        # Only consider future dates
        if earnings_date < now:
            continue
        
        # Convert provider symbol to canonical universe format
        event_sym = to_universe_symbol(row_symbol)
        event_sym = normalize_equity_symbol(event_sym)
        
        # Only map if event_sym is in canonical universe
        if event_sym not in universe_canon:
            continue
        
        # Find original universe symbol using reverse mapping
        universe_symbol = canonical_to_original.get(event_sym)
        if not universe_symbol:
            continue
        
        # Store earliest earnings date per universe symbol
        if universe_symbol not in earnings_map or earnings_date < earnings_map[universe_symbol]:
            earnings_map[universe_symbol] = earnings_date
    
    mapped_count = len(earnings_map)
    logger.info(f"Earnings calendar mapped to {mapped_count} unique symbols from universe (mapped_to_universe_count={mapped_count})")
    
    # Calculate earnings_known/unknown for universe
    earnings_known_count = mapped_count
    earnings_unknown_count = len(universe_symbols) - mapped_count
    logger.info(f"Universe earnings coverage: known={earnings_known_count}, unknown={earnings_unknown_count}")
    
    # Log sample of unmapped universe symbols (up to 15)
    unmapped_symbols = []
    for sym in universe_symbols:
        if sym not in earnings_map:
            unmapped_symbols.append(sym)
            if len(unmapped_symbols) >= 15:
                break
    
    if unmapped_symbols:
        total_unmapped = len([s for s in universe_symbols if s not in earnings_map])
        logger.info(f"Sample of unmapped universe symbols (showing up to 15 of {total_unmapped} total): {unmapped_symbols}")
    
    return earnings_map


def calculate_earnings_in_days(earnings_date: Optional[date], now: Optional[date] = None) -> Optional[int]:
    """
    Calculate days until earnings from earnings date.
    
    Args:
        earnings_date: Earnings date (date object) or None
        now: Reference date (defaults to today UTC)
        
    Returns:
        Days until earnings (int) if earnings_date is valid and in future, None otherwise
    """
    if earnings_date is None:
        return None
    
    if now is None:
        now = datetime.now(timezone.utc).date()
    
    # Only return positive days (future earnings)
    if earnings_date > now:
        return (earnings_date - now).days
    else:
        return None  # Earnings in past or today


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    """Safely convert to float, returning default on error."""
    try:
        if x is None:
            return default
        return float(x)
    except (ValueError, TypeError):
        return default


def batch_fetch_iv_snapshots(
    symbols: List[str],
    lookback_days: int = 252,
    min_points: int = 20,
) -> Dict[str, Dict[str, Any]]:
    """
    Batch fetch IV snapshots for all symbols, then compute metrics per symbol.
    
    Args:
        symbols: List of stock symbols
        lookback_days: Number of days to look back for historical series (default 252)
        min_points: Minimum number of data points required for rank/percentile/zscore (default 20)
        
    Returns:
        Dictionary mapping symbol -> IV metrics dict (same format as get_iv_from_snapshots)
    """
    result: Dict[str, Dict[str, Any]] = {}
    
    if not symbols:
        return result
    
    try:
        sb = get_supabase()
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()
        
        # Fetch all IV snapshots for all symbols in lookback window (batched query)
        # Supabase .in_() supports up to ~1000 items, so we chunk if needed
        CHUNK_SIZE = 500
        all_rows: List[Dict[str, Any]] = []
        
        for i in range(0, len(symbols), CHUNK_SIZE):
            chunk_symbols = symbols[i:i + CHUNK_SIZE]
            try:
                res = sb.table("iv_snapshots").select("*").in_("symbol", chunk_symbols).gte("asof_date", cutoff_date.isoformat()).order("asof_date", desc=False).execute()
                if res.data:
                    all_rows.extend(res.data)
            except Exception as e:
                logger.warning(f"Error fetching IV snapshot chunk {i//CHUNK_SIZE + 1}: {e}")
                continue
        
        # Group by symbol and compute metrics
        by_symbol: Dict[str, List[Dict[str, Any]]] = {}
        for row in all_rows:
            symbol = row.get("symbol")
            if symbol:
                if symbol not in by_symbol:
                    by_symbol[symbol] = []
                by_symbol[symbol].append(row)
        
        # For each symbol, find latest snapshot and compute metrics
        for symbol in symbols:
            rows = by_symbol.get(symbol, [])
            if not rows:
                continue
            
            # Find latest snapshot (most recent asof_date)
            latest = max(rows, key=lambda r: r.get("asof_date", ""))
            iv_current = _safe_float(latest.get("iv"))
            
            if iv_current is None or iv_current <= 0:
                continue
            
            asof_date = latest.get("asof_date")
            exp_date = latest.get("exp_date")
            dte = latest.get("dte")
            atm_strike = _safe_float(latest.get("strike"))
            
            # Extract IV series (all valid IV values in lookback window)
            iv_series = []
            for row in rows:
                iv_val = _safe_float(row.get("iv"))
                if iv_val is not None and iv_val > 0:
                    iv_series.append(iv_val)
            
            # Compute metrics if we have enough data points
            iv_rank = None
            iv_percentile = None
            iv_zscore = None
            
            if len(iv_series) >= min_points:
                iv_min = min(iv_series)
                iv_max = max(iv_series)
                iv_mean = statistics.mean(iv_series)
                iv_std = statistics.stdev(iv_series) if len(iv_series) > 1 else 0.0
                
                # IV Rank: (current - min) / (max - min) * 100
                if iv_max > iv_min:
                    iv_rank = ((iv_current - iv_min) / (iv_max - iv_min)) * 100.0
                    iv_rank = max(0.0, min(100.0, iv_rank))  # Clamp to [0, 100]
                
                # IV Percentile: percentile rank of current within series
                below_count = sum(1 for v in iv_series if v < iv_current)
                iv_percentile = (below_count / len(iv_series)) * 100.0
                
                # IV Z-Score: (current - mean) / std
                if iv_std > 0:
                    iv_zscore = (iv_current - iv_mean) / iv_std
            
            result[symbol] = {
                "current": iv_current,
                "rank": iv_rank,
                "percentile": iv_percentile,
                "zscore": iv_zscore,
                "asof_date": asof_date,
                "exp_date": exp_date,
                "dte": dte,
                "atm_strike": atm_strike,
            }
        
    except Exception as e:
        logger.warning(f"Error in batch_fetch_iv_snapshots: {e}")
    
    return result


def score_volatility_regime(iv_data: Optional[Dict[str, Any]]) -> float:
    """
    Compute volatility regime bonus for scoring.
    
    Returns:
        Bonus points (0-15 total):
        - IV Rank bonus: 0-10 points (higher rank => higher bonus)
        - Mean reversion bonus: 0-5 points (if zscore > 1, add small bonus)
    """
    if iv_data is None:
        return 0.0
    
    bonus = 0.0
    
    # IV Rank bonus: 0-10 points
    iv_rank = iv_data.get("rank")
    if iv_rank is not None:
        # Scale: rank 0% = 0 points, rank 100% = 10 points
        bonus += (iv_rank / 100.0) * 10.0
    
    # Mean reversion bonus: 0-5 points (if zscore > 1)
    iv_zscore = iv_data.get("zscore")
    if iv_zscore is not None and iv_zscore > 1.0:
        # Scale: zscore 1.0 = 0 points, zscore 3.0+ = 5 points
        z_bonus = min(5.0, (iv_zscore - 1.0) / 2.0 * 5.0)
        bonus += z_bonus
    
    return bonus


def score_fundamentals(
    ratios: Dict[str, Any],
    metrics: Dict[str, Any],
    financial_scores: Dict[str, Any],
    financial_growth: List[Dict[str, Any]]
) -> Tuple[int, Dict[str, Any]]:
    """
    Score fundamentals based on profitability, leverage, valuation, growth, and quality.
    
    Expanded scoring model:
    - Profitability (0-25): netProfitMargin, operatingProfitMargin, returnOnEquity/returnOnAssets
    - Balance sheet / leverage (0-20): debtEquity, interestCoverage, currentRatio
    - Valuation sanity (0-15): PE, priceToFreeCashFlows, enterpriseValueMultiple
    - Growth (0-20): revenueGrowth, epsGrowth, freeCashFlowGrowth from financial statement growth
    - Quality / distress (0-20): Piotroski score and Altman Z-Score from financial_scores
    
    If a metric is missing, skip it and reweight within that sub-bucket.
    
    Returns:
        (score 0-100, breakdown dict with subcomponent scores and notes)
    """
    breakdown: Dict[str, Any] = {
        "profitability": 0.0,
        "leverage": 0.0,
        "valuation": 0.0,
        "growth": 0.0,
        "quality": 0.0,
        "notes": []
    }
    
    total_score = 0.0
    total_weight = 0.0
    
    # --- Profitability (0-25 points) ---
    profitability_score = 0.0
    profitability_weight = 0.0
    
    npm = _safe_float(ratios.get("netProfitMarginTTM") or ratios.get("netProfitMargin"))
    if npm is not None:
        # Normalize: -10% to 30% -> 0 to 1
        normalized = (min(max(npm, -0.10), 0.30) + 0.10) / 0.40
        profitability_score += normalized * 10.0
        profitability_weight += 10.0
    
    opm = _safe_float(ratios.get("operatingProfitMarginTTM") or ratios.get("operatingProfitMargin"))
    if opm is not None:
        # Normalize: -10% to 35% -> 0 to 1
        normalized = (min(max(opm, -0.10), 0.35) + 0.10) / 0.45
        profitability_score += normalized * 8.0
        profitability_weight += 8.0
    
    roe = _safe_float(ratios.get("returnOnEquityTTM") or ratios.get("returnOnEquity"))
    roa = _safe_float(ratios.get("returnOnAssetsTTM") or ratios.get("returnOnAssets"))
    if roe is not None:
        # Normalize: 0% to 35% -> 0 to 1
        normalized = min(max(roe, 0.0), 0.35) / 0.35
        profitability_score += normalized * 7.0
        profitability_weight += 7.0
    elif roa is not None:
        # Fallback to ROA if ROE missing
        normalized = min(max(roa, 0.0), 0.20) / 0.20
        profitability_score += normalized * 7.0
        profitability_weight += 7.0
    
    if profitability_weight > 0:
        profitability_final = (profitability_score / profitability_weight) * 25.0
        breakdown["profitability"] = profitability_final
        total_score += profitability_final
        total_weight += 25.0
    else:
        breakdown["notes"].append("profitability: no data")
    
    # --- Balance sheet / leverage (0-20 points) ---
    leverage_score = 0.0
    leverage_weight = 0.0
    
    de = _safe_float(ratios.get("debtEquityRatioTTM") or ratios.get("debtEquityRatio"))
    if de is not None and de >= 0:
        # Lower is better: 0-1.0 = 1.0, 1.0-2.5 = linear decay, >2.5 = 0.2
        if de <= 1.0:
            v = 1.0
        elif de <= 2.5:
            v = 1.0 - (de - 1.0) / 1.5 * 0.7
        else:
            v = 0.2
        leverage_score += v * 8.0
        leverage_weight += 8.0
    
    interest_coverage = _safe_float(ratios.get("interestCoverageTTM") or ratios.get("interestCoverage"))
    if interest_coverage is not None and interest_coverage > 0:
        # Higher is better: >= 5 = 1.0, 2-5 = linear, <2 = 0.3
        if interest_coverage >= 5.0:
            v = 1.0
        elif interest_coverage >= 2.0:
            v = 0.3 + (interest_coverage - 2.0) / 3.0 * 0.7
        else:
            v = 0.3
        leverage_score += v * 6.0
        leverage_weight += 6.0
    
    current_ratio = _safe_float(ratios.get("currentRatioTTM") or ratios.get("currentRatio"))
    if current_ratio is not None and current_ratio > 0:
        # Ideal: 1.5-3.0 = 1.0, outside range = lower score
        if 1.5 <= current_ratio <= 3.0:
            v = 1.0
        elif current_ratio < 1.5:
            v = current_ratio / 1.5
        else:
            v = max(0.5, 1.0 - (current_ratio - 3.0) / 3.0)
        leverage_score += v * 6.0
        leverage_weight += 6.0
    
    if leverage_weight > 0:
        leverage_final = (leverage_score / leverage_weight) * 20.0
        breakdown["leverage"] = leverage_final
        total_score += leverage_final
        total_weight += 20.0
    else:
        breakdown["notes"].append("leverage: no data")
    
    # --- Valuation sanity (0-15 points) ---
    valuation_score = 0.0
    valuation_weight = 0.0
    
    pe = _safe_float(ratios.get("peRatioTTM") or ratios.get("peRatio") or metrics.get("peRatioTTM"))
    if pe is not None and pe > 0:
        # Lower is better: <= 25 = 1.0, 25-40 = linear decay, >40 = 0.2
        if pe <= 25:
            v = 1.0
        elif pe <= 40:
            v = 1.0 - (pe - 25) / 15 * 0.6
        else:
            v = 0.2
        valuation_score += v * 5.0
        valuation_weight += 5.0
    
    price_to_fcf = _safe_float(metrics.get("priceToFreeCashFlowsTTM") or metrics.get("priceToFreeCashFlows"))
    if price_to_fcf is not None and price_to_fcf > 0:
        # Lower is better: <= 20 = 1.0, 20-40 = linear decay, >40 = 0.3
        if price_to_fcf <= 20:
            v = 1.0
        elif price_to_fcf <= 40:
            v = 1.0 - (price_to_fcf - 20) / 20 * 0.5
        else:
            v = 0.3
        valuation_score += v * 5.0
        valuation_weight += 5.0
    
    ev_multiple = _safe_float(metrics.get("enterpriseValueMultipleTTM") or metrics.get("enterpriseValueMultiple"))
    if ev_multiple is not None and ev_multiple > 0:
        # Lower is better: <= 15 = 1.0, 15-30 = linear decay, >30 = 0.3
        if ev_multiple <= 15:
            v = 1.0
        elif ev_multiple <= 30:
            v = 1.0 - (ev_multiple - 15) / 15 * 0.5
        else:
            v = 0.3
        valuation_score += v * 5.0
        valuation_weight += 5.0
    
    if valuation_weight > 0:
        valuation_final = (valuation_score / valuation_weight) * 15.0
        breakdown["valuation"] = valuation_final
        total_score += valuation_final
        total_weight += 15.0
    else:
        breakdown["notes"].append("valuation: no data")
    
    # --- Growth (0-20 points) ---
    growth_score = 0.0
    growth_weight = 0.0
    
    # Use most recent growth record (first in list if sorted by date desc)
    growth_record = financial_growth[0] if financial_growth and isinstance(financial_growth[0], dict) else {}
    
    revenue_growth = _safe_float(growth_record.get("revenueGrowth") or growth_record.get("revenueGrowthRate"))
    if revenue_growth is not None:
        # Normalize: -20% to 30% -> 0 to 1
        normalized = (min(max(revenue_growth, -0.20), 0.30) + 0.20) / 0.50
        growth_score += normalized * 7.0
        growth_weight += 7.0
    
    eps_growth = _safe_float(growth_record.get("epsGrowth") or growth_record.get("epsGrowthRate"))
    if eps_growth is not None:
        # Normalize: -30% to 40% -> 0 to 1
        normalized = (min(max(eps_growth, -0.30), 0.40) + 0.30) / 0.70
        growth_score += normalized * 7.0
        growth_weight += 7.0
    
    fcf_growth = _safe_float(growth_record.get("freeCashFlowGrowth") or growth_record.get("freeCashFlowGrowthRate"))
    if fcf_growth is not None:
        # Normalize: -50% to 50% -> 0 to 1
        normalized = (min(max(fcf_growth, -0.50), 0.50) + 0.50) / 1.0
        growth_score += normalized * 6.0
        growth_weight += 6.0
    
    if growth_weight > 0:
        growth_final = (growth_score / growth_weight) * 20.0
        breakdown["growth"] = growth_final
        total_score += growth_final
        total_weight += 20.0
    else:
        breakdown["notes"].append("growth: no data")
    
    # --- Quality / distress (0-20 points) ---
    quality_score = 0.0
    quality_weight = 0.0
    
    piotroski = _safe_float(financial_scores.get("piotroskiScore") or financial_scores.get("piotroski"))
    if piotroski is not None:
        # Piotroski: 0-9 scale, normalize to 0-1
        normalized = min(max(piotroski, 0.0), 9.0) / 9.0
        quality_score += normalized * 10.0
        quality_weight += 10.0
    
    altman_z = _safe_float(financial_scores.get("altmanZScore") or financial_scores.get("altmanZ"))
    if altman_z is not None:
        # Altman Z: <1.8 = distress, 1.8-2.99 = gray, >=3 = safe
        # Score: >=3 = 1.0, 2.5-3 = 0.8, 1.8-2.5 = 0.5, <1.8 = 0.2
        if altman_z >= 3.0:
            v = 1.0
        elif altman_z >= 2.5:
            v = 0.8
        elif altman_z >= 1.8:
            v = 0.5
        else:
            v = 0.2
        quality_score += v * 10.0
        quality_weight += 10.0
    
    if quality_weight > 0:
        quality_final = (quality_score / quality_weight) * 20.0
        breakdown["quality"] = quality_final
        total_score += quality_final
        total_weight += 20.0
    else:
        breakdown["notes"].append("quality: no data")
    
    # Final score: normalize to 0-100 if we have any weight
    if total_weight > 0:
        final_score = (total_score / total_weight) * 100.0
    else:
        # No data at all - return neutral score
        final_score = 40.0
        breakdown["notes"].append("fundamentals: no data available")
    
    return clamp_int(final_score, 0, 100), breakdown


def score_sentiment(sent: float) -> int:
    """Convert sentiment float [-1, 1] to score [0, 100]."""
    return clamp_int((sent + 1) * 50, 0, 100)


def score_trend_proxy(quote: Dict[str, Any]) -> tuple[int, Dict[str, Any]]:
    """Score based on 52-week price position."""
    price = quote.get("price")
    low = quote.get("yearLow") or quote.get("yearLow52Week")
    high = quote.get("yearHigh") or quote.get("yearHigh52Week")
    
    feats: Dict[str, Any] = {"price": price, "yearLow": low, "yearHigh": high}
    
    if price is None or low is None or high is None or high == low:
        return 50, feats
    
    pos = (price - low) / (high - low)  # 0..1
    # Prefer middle-ish of 52w range (avoid extremes)
    score = 100 * (1 - min(abs(pos - 0.5) / 0.5, 1))
    feats["pos_52w"] = pos
    return clamp_int(score, 0, 100), feats


def get_rsi_from_cache(
    ticker: str,
    interval: str,
    period: int,
    max_age_hours: int = 24,
) -> Optional[float]:
    """
    Get RSI from Supabase cache (rsi_snapshots table).
    Prefers today's snapshot, else falls back to latest within max_age_hours.
    
    Args:
        ticker: Stock symbol
        interval: RSI interval (e.g., "1day")
        period: RSI period (e.g., 14)
        max_age_hours: Maximum age in hours for cached RSI (default 24)
        
    Returns:
        RSI value (float) if found, None otherwise
    """
    try:
        sb = get_supabase()
        today = datetime.now(timezone.utc).date()
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        
        # Try to get today's snapshot first
        res = sb.table("rsi_snapshots").select("rsi, as_of_date").eq("ticker", ticker).eq("interval", interval).eq("period", period).eq("as_of_date", today.isoformat()).limit(1).execute()
        
        if res.data and len(res.data) > 0:
            rsi_val = res.data[0].get("rsi")
            if rsi_val is not None:
                return float(rsi_val)
        
        # Fall back to latest snapshot within max_age_hours
        res = sb.table("rsi_snapshots").select("rsi, as_of_date, created_at").eq("ticker", ticker).eq("interval", interval).eq("period", period).gte("created_at", cutoff_time.isoformat()).order("as_of_date", desc=True).limit(1).execute()
        
        if res.data and len(res.data) > 0:
            rsi_val = res.data[0].get("rsi")
            if rsi_val is not None:
                return float(rsi_val)
        
        return None
    except Exception as e:
        logger.debug(f"Error fetching RSI from cache for {ticker}: {e}")
        return None


def score_technical(rsi: Optional[float], iv_data: Optional[Dict[str, Any]] = None) -> int:
    """
    Score based on RSI (technical sanity) with IV volatility bonus integrated.
    Prefer RSI in reasonable range (30-70).
    Missing RSI is treated as neutral (50 points base).
    IV bonus adds 0-10 points based on IV rank, or 2 points if IV present but rank unavailable.
    
    Args:
        rsi: RSI value (float or None)
        iv_data: IV metrics dict with "rank" and "current" keys (optional)
        
    Returns:
        Score 0-100 (100 = ideal RSI + high IV rank, 50 = neutral/missing, 0 = extreme)
    """
    # Base RSI score (0-100, defaults to 50 if missing)
    if rsi is None:
        base_score = 50  # Neutral if missing (soft score, no penalty)
    else:
        # Ideal range: 30-70 (score = 100)
        # Outside range: score decreases
        if 30 <= rsi <= 70:
            base_score = 100
        elif rsi < 30:
            # Oversold: score decreases linearly from 100 at 30 to 0 at 0
            base_score = clamp_int(100 * (rsi / 30.0), 0, 100)
        else:
            # Overbought: score decreases linearly from 100 at 70 to 0 at 100
            base_score = clamp_int(100 * ((100 - rsi) / 30.0), 0, 100)
    
    # Add IV bonus (0-10 points) as part of technical score
    iv_bonus = 0.0
    if iv_data is not None:
        iv_rank = iv_data.get("rank")
        iv_current = iv_data.get("current")
        
        if iv_rank is not None:
            # IV Rank bonus: 0-10 points (higher rank => higher bonus)
            iv_bonus = (iv_rank / 100.0) * 10.0
        elif iv_current is not None:
            # Small "IV present" credit if rank unavailable
            iv_bonus = 2.0
    
    # Combine: base_score is 0-100, iv_bonus is 0-10, result should be 0-100
    # Scale iv_bonus to fit within the 0-100 range (add directly, cap at 100)
    final_score = clamp_int(base_score + iv_bonus, 0, 100)
    
    return final_score


def main() -> None:
    run_id: Optional[str] = None
    
    try:
        # Load wheel rules configuration
        rules = load_wheel_rules()
        logger.info(
            f"Wheel rules in effect: "
            f"CSP delta=[{rules.csp_delta_min:.2f}, {rules.csp_delta_max:.2f}], "
            f"CC delta=[{rules.cc_delta_min:.2f}, {rules.cc_delta_max:.2f}], "
            f"DTE primary=[{rules.dte_min_primary}, {rules.dte_max_primary}], "
            f"DTE fallback=[{rules.dte_min_fallback}, {rules.dte_max_fallback}], "
            f"earnings_avoid_days={rules.earnings_avoid_days}, "
            f"RSI(period={rules.rsi_period}, interval={rules.rsi_interval})"
        )
        
        # Initialize FMP stable client
        fmp = FMPStableClient()
        logger.info(f"Build SHA: {BUILD_SHA}")
        logger.info(f"FMP Stable Client version: {fmp.__class__.__module__}")
        
        # Environment variables for universe source and filters
        universe_source = os.getenv("UNIVERSE_SOURCE", "csv").lower()  # Default to CSV for now
        MIN_PRICE = float(os.getenv("MIN_PRICE", "5.0"))
        MIN_MARKET_CAP = int(os.getenv("MIN_MARKET_CAP", "2000000000"))  # $2B default
        MIN_AVG_VOLUME = int(os.getenv("MIN_AVG_VOLUME", "1000000")) if os.getenv("MIN_AVG_VOLUME") else None
        RSI_MAX_AGE_HOURS = int(os.getenv("RSI_MAX_AGE_HOURS", "24"))
        IV_LOOKBACK_DAYS = int(os.getenv("WHEEL_IV_LOOKBACK_DAYS", "252"))
        IV_MIN_POINTS = int(os.getenv("WHEEL_IV_MIN_POINTS", "20"))
        
        # Build universe
        if universe_source == "csv":
            universe = load_universe_csv("data/universe_us.csv")
            logger.info(f"Universe size from CSV: {len(universe)} (source=csv)")
        else:
            universe = build_universe_fmp_stable(fmp, MIN_PRICE, MIN_MARKET_CAP, MIN_AVG_VOLUME)
            logger.info(f"Universe size from FMP stable: {len(universe)} (source=fmp_stable)")
        
        # Fetch earnings calendar data for date range
        now = datetime.now(timezone.utc).date()
        start_date = now
        end_date = now + timedelta(days=90)
        universe_symbols = set(item.get("symbol") for item in universe if item.get("symbol"))
        
        logger.info(f"Fetching earnings calendar for date range: {start_date.isoformat()} to {end_date.isoformat()}")
        earnings_map = fetch_earnings_calendar_range(fmp, start_date, end_date, universe_symbols)
        
        # Batch fetch IV snapshots for all symbols
        logger.info(f"Batch fetching IV snapshots for {len(universe)} symbols (lookback_days={IV_LOOKBACK_DAYS}, min_points={IV_MIN_POINTS})")
        iv_cache = batch_fetch_iv_snapshots(
            list(universe_symbols),
            lookback_days=IV_LOOKBACK_DAYS,
            min_points=IV_MIN_POINTS
        )
        logger.info(f"IV snapshots fetched for {len(iv_cache)} symbols")
        
        # Insert run row with status='running'
        run_row = insert_row("screening_runs", {
            "run_ts": datetime.now(timezone.utc).isoformat(),
            "universe_size": len(universe),
            "status": "running",
            "build_sha": BUILD_SHA,
            "notes": f"STARTED: weekly screener running (universe_source={universe_source})"
        })
        run_id = run_row.get("run_id")
        if not run_id:
            raise RuntimeError("Failed to create screening run (missing run_id)")
        
        logger.info(f"Screening run started: run_id={run_id}")
        
        # Process candidates
        candidates: List[Candidate] = []
        ticker_rows: List[Dict[str, Any]] = []
        
        # Statistics
        prof_missing = 0
        quote_missing = 0
        price_missing = 0
        mcap_missing = 0
        mcap_filtered = 0
        price_filtered = 0
        rsi_missing = 0
        earnings_known = 0
        earnings_unknown = 0
        passed_all_filters = 0
        financial_scores_missing = 0
        financial_growth_missing = 0
        fundamentals_scores: List[int] = []
        # Error type counters for diagnostics
        rsi_empty = 0
        rsi_http_error = 0
        rsi_blocked_402 = 0
        rsi_parse_error = 0
        growth_empty = 0
        growth_http_error = 0
        growth_blocked_402 = 0
        growth_parse_error = 0
        # IV statistics
        iv_missing = 0
        iv_rank_available = 0
        iv_percentile_available = 0
        iv_zscore_available = 0

        for item in universe:
            t = item.get("symbol")
            if not t:
                continue
            
            try:
                # Get earnings data from pre-fetched map
                next_earnings_date = earnings_map.get(t)
                earnings_in_days = calculate_earnings_in_days(next_earnings_date, now=now)
                earnings_source = "fmp_calendar_range" if next_earnings_date is not None else "unknown"
                
                # Track earnings statistics
                if next_earnings_date is not None:
                    earnings_known += 1
                else:
                    earnings_unknown += 1
                
                # Fetch data from FMP stable
                profile = fmp.profile(t) or {}
                quote = fmp.quote(t) or {}
                ratios = fmp.ratios_ttm(t) or {}
                metrics = fmp.key_metrics_ttm(t) or {}
                news = fmp.stock_news(t, limit=50)
                
                # Fetch new fundamental datasets with diagnostics
                financial_scores_data = fmp.financial_scores(t) or {}
                financial_growth_data, growth_meta = fmp.financial_statement_growth_with_meta(t, limit=5)
                
                # Track missing datasets and error types
                if not financial_scores_data:
                    financial_scores_missing += 1
                if not financial_growth_data:
                    financial_growth_missing += 1
                    # Track growth error type
                    error_type = growth_meta.get("error_type", "empty")
                    if error_type == "empty":
                        growth_empty += 1
                    elif error_type == "http_error":
                        growth_http_error += 1
                    elif error_type == "blocked_402":
                        growth_blocked_402 += 1
                    elif error_type == "parse_error":
                        growth_parse_error += 1
                
                # Get RSI from FMP with diagnostics (primary source)
                rsi_value, rsi_meta = fmp.technical_indicator_rsi_with_meta(
                    t,
                    period=rules.rsi_period,
                    interval=rules.rsi_interval
                )
                
                # Fallback to Supabase cache if FMP failed
                if rsi_value is None:
                    rsi_value = get_rsi_from_cache(
                        t,
                        interval=rules.rsi_interval,
                        period=rules.rsi_period,
                        max_age_hours=RSI_MAX_AGE_HOURS
                    )
                    # If cache also failed, track error type from FMP
                    if rsi_value is None:
                        error_type = rsi_meta.get("error_type", "empty")
                        if error_type == "empty":
                            rsi_empty += 1
                        elif error_type == "http_error":
                            rsi_http_error += 1
                        elif error_type == "blocked_402":
                            rsi_blocked_402 += 1
                        elif error_type == "parse_error":
                            rsi_parse_error += 1
                
                rsi = rsi_value
                
                # Get IV data from batch cache
                iv_data = iv_cache.get(t)
                if iv_data is None or iv_data.get("current") is None:
                    iv_missing += 1
                else:
                    # Track IV metrics availability
                    if iv_data.get("rank") is not None:
                        iv_rank_available += 1
                    if iv_data.get("percentile") is not None:
                        iv_percentile_available += 1
                    if iv_data.get("zscore") is not None:
                        iv_zscore_available += 1
                
                # Track missing data
                if not profile:
                    prof_missing += 1
                if not quote:
                    quote_missing += 1
                
                # Extract key fields
                price = quote.get("price")
                market_cap = profile.get("mktCap") or quote.get("marketCap") or profile.get("marketCap")
                beta = profile.get("beta") or quote.get("beta")
                
                # Filter: price required
                if price is None or price <= 0:
                    price_missing += 1
                    continue
                
                # Filter: market cap
                if market_cap is None:
                    mcap_missing += 1
                    continue
                if market_cap < MIN_MARKET_CAP:
                    mcap_filtered += 1
                    continue
                
                # Filter: price minimum
                if price < MIN_PRICE:
                    price_filtered += 1
                    continue
                
                # RSI is NOT a hard filter - track missing but don't exclude
                if rsi is None:
                    rsi_missing += 1
                    # Continue processing (RSI missing is OK, treated as neutral in scoring)
                
                # IV is NOT a hard filter - track missing but don't exclude
                # Continue processing (IV missing is OK, no bonus applied)
                
                # Earnings exclusion is NOT applied here (happens in pick builders)
                # We just track and store the data
                
                passed_all_filters += 1
                
                # Compute sentiment
                sent = simple_sentiment_score(news)
                sent_score = score_sentiment(sent)
                
                # Compute scores with expanded fundamentals model
                f_score, f_breakdown = score_fundamentals(ratios, metrics, financial_scores_data, financial_growth_data)
                fundamentals_scores.append(f_score)
                trend_score, t_feats = score_trend_proxy(quote)
                tech_score = score_technical(rsi_value, iv_data=iv_data)  # RSI + IV bonus integrated (10% weight)
                
                # Composite wheel score (weighted) - same top-level weights
                # IV bonus is now integrated into tech_score
                wheel_score = clamp_int(
                    0.50 * f_score +      # Fundamentals: 50%
                    0.20 * sent_score +   # Sentiment: 20%
                    0.20 * trend_score +  # Trend: 20%
                    0.10 * tech_score,    # Technical (RSI + IV): 10% - soft score, missing RSI/IV = neutral
                    0, 100
                )
                
                # Extract company info
                name = profile.get("companyName") or profile.get("name") or item.get("name") or t
                sector = profile.get("sector")
                industry = profile.get("industry") or profile.get("subSector")
                exchange = profile.get("exchangeShortName") or profile.get("exchange") or item.get("exchange")
                
                reasons = {
                    "market_cap_min": MIN_MARKET_CAP,
                    "price_min": MIN_PRICE,
                    "rsi_period": rules.rsi_period,
                    "rsi_interval": rules.rsi_interval,
                    "rsi_missing": rsi is None,
                    "iv_missing": iv_data is None or iv_data.get("current") is None,
                    "iv_lookback_days": IV_LOOKBACK_DAYS,
                    "iv_min_points": IV_MIN_POINTS,
                    "notes": "IV sourced from Schwab option chain snapshots; weekly_screener does not require IV to run"
                }
                
                # Build features dict with raw data (store all datasets in metrics)
                # Store RSI as {"value": float|None, "period": int, "interval": str}
                # Store IV as {"current": float|None, "rank": float|None, "percentile": float|None, "zscore": float|None, ...}
                features = {
                    "profile": profile,
                    "quote": quote,
                    "ratios_ttm": ratios,
                    "key_metrics_ttm": metrics,
                    "financial_scores": financial_scores_data,
                    "financial_growth": financial_growth_data[:5] if financial_growth_data else [],  # Limit to 5 records
                    "rsi": {
                        "value": rsi,
                        "period": rules.rsi_period,
                        "interval": rules.rsi_interval,
                    },
                    "iv": iv_data if iv_data else {
                        "current": None,
                        "rank": None,
                        "percentile": None,
                        "zscore": None,
                        "asof_date": None,
                        "exp_date": None,
                        "dte": None,
                        "atm_strike": None,
                    },
                    "next_earnings_date": next_earnings_date.isoformat() if next_earnings_date else None,
                    "earnings_in_days": earnings_in_days,
                    "earnings_source": earnings_source,
                    "news_count": len(news),
                    "sentiment_raw": sent,
                    "fundamentals_breakdown": f_breakdown,  # Store breakdown in features
                    "trend": t_feats,
                }
                
                candidates.append(Candidate(
                    ticker=t,
                    name=name,
                    sector=sector,
                    industry=industry,
                    market_cap=int(market_cap),
                    price=float(price),
                    beta=float(beta) if beta is not None else None,
                    rsi=rsi_value,
                    next_earnings_date=next_earnings_date,
                    earnings_in_days=earnings_in_days,
                    earnings_source=earnings_source,
                    fundamentals_score=f_score,
                    sentiment_score=sent_score,
                    trend_score=trend_score,
                    technical_score=tech_score,
                    wheel_score=wheel_score,
                    reasons=reasons,
                    features=features
                ))
                
                # Build ticker row
                ticker_rows.append({
                    "ticker": t,
                    "name": name,
                    "exchange": exchange,
                    "sector": sector,
                    "industry": industry,
                    "market_cap": int(market_cap),
                    "currency": profile.get("currency") or "USD",
                    "is_active": True,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
                
            except Exception as e:
                logger.warning(f"{t}: error during processing: {e}")
                continue
        
        # Log filter statistics
        logger.info(
            f"Filter stats: prof_missing={prof_missing}, quote_missing={quote_missing}, "
            f"price_missing={price_missing}, mcap_missing={mcap_missing}, "
            f"mcap_filtered={mcap_filtered}, price_filtered={price_filtered}, "
            f"rsi_missing={rsi_missing}, earnings_known={earnings_known}, "
            f"earnings_unknown={earnings_unknown}, passed_all_filters={passed_all_filters}"
        )
        
        # Log fundamentals enrichment statistics with error type breakdown
        logger.info(
            f"Fundamentals enrichment: financial_scores_missing={financial_scores_missing}, "
            f"financial_growth_missing={financial_growth_missing} "
            f"(empty={growth_empty}, http_error={growth_http_error}, blocked_402={growth_blocked_402}, parse_error={growth_parse_error})"
        )
        
        # Log RSI diagnostics
        logger.info(
            f"RSI diagnostics: rsi_missing={rsi_missing} "
            f"(empty={rsi_empty}, http_error={rsi_http_error}, blocked_402={rsi_blocked_402}, parse_error={rsi_parse_error})"
        )
        
        # Log IV enrichment statistics
        iv_current_known = passed_all_filters - iv_missing
        iv_rank_unknown = iv_current_known - iv_rank_available
        logger.info(
            f"IV enrichment: iv_current_known={iv_current_known} unknown={iv_missing}; "
            f"iv_rank_known={iv_rank_available} unknown={iv_rank_unknown} "
            f"(min_points={IV_MIN_POINTS}, lookback_days={IV_LOOKBACK_DAYS})"
        )
        
        # Log fundamentals score distribution
        if fundamentals_scores:
            fundamentals_scores_sorted = sorted(fundamentals_scores)
            n = len(fundamentals_scores_sorted)
            min_score = fundamentals_scores_sorted[0]
            median_score = fundamentals_scores_sorted[n // 2] if n > 0 else 0
            max_score = fundamentals_scores_sorted[-1]
            logger.info(
                f"Fundamentals score distribution: min={min_score}, median={median_score}, max={max_score}, "
                f"n={n}"
            )
        
        # Upsert tickers
        logger.info(f"Upserting tickers: {len(ticker_rows)}")
        upsert_rows("tickers", ticker_rows)
        logger.info("Tickers upserted")
        
        # Sort by wheel score
        candidates.sort(key=lambda c: c.wheel_score, reverse=True)
        logger.info(f"Candidates after filters: {len(candidates)}")
        
        # Write screening_candidates rows
        cand_rows: List[Dict[str, Any]] = []
        for i, c in enumerate(candidates, start=1):
            # Extract fundamentals breakdown from features
            fundamentals_breakdown = c.features.get("fundamentals_breakdown", {})
            iv_data = c.features.get("iv", {})
            
            metrics_json = {
                "wheel_score": c.wheel_score,
                "fundamentals_score": c.fundamentals_score,
                "fundamentals_breakdown": fundamentals_breakdown,  # Store breakdown in metrics
                "sentiment_score": c.sentiment_score,
                "trend_score": c.trend_score,
                "technical_score": c.technical_score,
                "rsi_period": rules.rsi_period,
                "rsi_interval": rules.rsi_interval,
                "next_earnings_date": c.next_earnings_date.isoformat() if c.next_earnings_date else None,
                "earnings_in_days": c.earnings_in_days,
                "earnings_source": c.earnings_source,
                "reasons": c.reasons,
                # Store all raw datasets in metrics
                "profile": c.features.get("profile"),
                "quote": c.features.get("quote"),
                "ratios_ttm": c.features.get("ratios_ttm"),
                "key_metrics_ttm": c.features.get("key_metrics_ttm"),
                "financial_scores": c.features.get("financial_scores"),
                "financial_growth": c.features.get("financial_growth"),
                "rsi": c.features.get("rsi"),  # Stored as {"value": float|None, "period": int, "interval": str}
                "iv": iv_data,  # Stored as {"current": float|None, "rank": float|None, "percentile": float|None, "zscore": float|None, ...}
                "sentiment": c.features.get("sentiment_raw"),
            }
            
            # Build row - try explicit columns first, fallback to metadata JSON
            row = {
                "run_id": run_id,
                "ticker": c.ticker,
                "score": int(c.wheel_score),
                "rank": i,
                "price": c.price,
                "market_cap": c.market_cap,
                "sector": c.sector,
                "industry": c.industry,
                "iv": iv_data.get("current") if iv_data else None,  # Store current IV in explicit column if available
                "iv_rank": iv_data.get("rank") if iv_data else None,  # Store IV rank in explicit column if available
                "beta": c.beta,
                "rsi": c.rsi,
                "sentiment_score": c.sentiment_score,
                "metrics": metrics_json,  # Always store in JSONB for redundancy
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            
            # Add explicit column if it exists in schema (earn_in_days is the column name)
            # If column doesn't exist, it will be stored in metrics JSON
            if c.earnings_in_days is not None:
                row["earn_in_days"] = c.earnings_in_days
            
            cand_rows.append(row)
        
        logger.info(f"Upserting screening_candidates: {len(cand_rows)}")
        upsert_rows("screening_candidates", cand_rows, keys=["run_id", "ticker"])
        logger.info("screening_candidates upserted")
        
        # Maintain approved universe (Top 40)
        top40 = candidates[:40]
        approved_rows: List[Dict[str, Any]] = []
        for i, c in enumerate(top40, start=1):
            approved_rows.append({
                "ticker": c.ticker,
                "approved": True,
                "last_run_id": run_id,
                "last_run_ts": datetime.now(timezone.utc).isoformat(),
                "last_rank": i,
                "last_score": c.wheel_score,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
        
        logger.info(f"Upserting approved_universe: {len(approved_rows)}")
        upsert_rows("approved_universe", approved_rows)
        logger.info("approved_universe upserted")
        
        # Update run with success status
        candidates_count = len(candidates)
        update_rows("screening_runs", {"run_id": run_id}, {
            "status": "success",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "candidates_count": candidates_count,
            "picks_count": 0,
            "notes": f"OK: candidates written (source={universe_source}, earnings_known={earnings_known}, earnings_unknown={earnings_unknown}, iv_missing={iv_missing})"
        })
        
        logger.info(
            f"Run complete. run_id={run_id} | status=success | "
            f"candidates={candidates_count} | picks=0 | "
            f"earnings_known={earnings_known} earnings_unknown={earnings_unknown} | "
            f"iv_missing={iv_missing} iv_rank_available={iv_rank_available}"
        )
        
    except Exception as e:
        # Update run with failed status
        error_msg = str(e)[:800]
        finished_at = datetime.now(timezone.utc).isoformat()
        
        if run_id:
            try:
                update_rows("screening_runs", {"run_id": run_id}, {
                    "status": "failed",
                    "finished_at": finished_at,
                    "error": error_msg
                })
                logger.error(f"Run failed. run_id={run_id} | status=failed | error={error_msg[:100]}...")
            except Exception as update_err:
                logger.exception(f"Failed to update run status to 'failed': {update_err}")
        
        raise


if __name__ == "__main__":
    main()
