"""
Weekly Screener v2: FMP Stable Universe + Enrichment
Uses FMP stable endpoints for universe building, fundamentals, technicals, and sentiment.
Integrates with WheelRules for configurable trading parameters.
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


def score_fundamentals(
    ratios: Dict[str, Any],
    metrics: Dict[str, Any]
) -> tuple[int, Dict[str, Any]]:
    """
    Score fundamentals based on profitability, leverage, growth, valuation.
    
    Returns:
        (score 0-100, features dict)
    """
    feats: Dict[str, Any] = {}
    
    # Get ratios/metrics
    npm = ratios.get("netProfitMarginTTM") or ratios.get("netProfitMargin")
    opm = ratios.get("operatingProfitMarginTTM") or ratios.get("operatingProfitMargin")
    roe = ratios.get("returnOnEquityTTM") or ratios.get("returnOnEquity")
    pe = ratios.get("peRatioTTM") or ratios.get("peRatio") or metrics.get("peRatioTTM")
    de = ratios.get("debtEquityRatioTTM") or ratios.get("debtEquityRatio")
    
    feats.update({"npm": npm, "opm": opm, "roe": roe, "pe": pe, "de": de})
    
    s = 0.0
    w = 0.0
    
    # Profitability (0-70 points)
    if npm is not None:
        s += (min(max(npm, -0.10), 0.30) + 0.10) / 0.40 * 25
        w += 25
    if opm is not None:
        s += (min(max(opm, -0.10), 0.35) + 0.10) / 0.45 * 20
        w += 20
    if roe is not None:
        s += min(max(roe, 0.0), 0.35) / 0.35 * 25
        w += 25
    
    # Valuation (0-20 points)
    if pe is not None and pe > 0:
        if pe <= 25:
            v = 1.0
        elif pe <= 40:
            v = 1.0 - (pe - 25) / 15 * 0.6
        else:
            v = 0.2
        s += v * 20
        w += 20
    
    # Leverage (0-10 points)
    if de is not None and de >= 0:
        if de <= 1.0:
            v = 1.0
        elif de <= 2.5:
            v = 1.0 - (de - 1.0) / 1.5 * 0.7
        else:
            v = 0.2
        s += v * 10
        w += 10
    
    if w == 0:
        return 40, feats
    
    score = (s / w) * 100
    return clamp_int(score, 0, 100), feats


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


def score_technical(rsi: Optional[float]) -> int:
    """
    Score based on RSI (technical sanity).
    Prefer RSI in reasonable range (30-70).
    Missing RSI is treated as neutral (50 points).
    
    Returns:
        Score 0-100 (100 = ideal RSI, 50 = neutral/missing, 0 = extreme)
    """
    if rsi is None:
        return 50  # Neutral if missing (soft score, no penalty)
    
    # Ideal range: 30-70 (score = 100)
    # Outside range: score decreases
    if 30 <= rsi <= 70:
        return 100
    elif rsi < 30:
        # Oversold: score decreases linearly from 100 at 30 to 0 at 0
        return clamp_int(100 * (rsi / 30.0), 0, 100)
    else:
        # Overbought: score decreases linearly from 100 at 70 to 0 at 100
        return clamp_int(100 * ((100 - rsi) / 30.0), 0, 100)


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
                
                # Get RSI from Supabase cache using wheel rules parameters
                rsi = get_rsi_from_cache(
                    t,
                    interval=rules.rsi_interval,
                    period=rules.rsi_period,
                    max_age_hours=RSI_MAX_AGE_HOURS
                )
                
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
                
                # Earnings exclusion is NOT applied here (happens in pick builders)
                # We just track and store the data
                
                passed_all_filters += 1
                
                # Compute sentiment
                sent = simple_sentiment_score(news)
                sent_score = score_sentiment(sent)
                
                # Compute scores
                f_score, f_feats = score_fundamentals(ratios, metrics)
                trend_score, t_feats = score_trend_proxy(quote)
                tech_score = score_technical(rsi)  # RSI contributes as soft score (10% weight)
                
                # Composite wheel score (weighted)
                wheel_score = clamp_int(
                    0.50 * f_score +      # Fundamentals: 50%
                    0.20 * sent_score +   # Sentiment: 20%
                    0.20 * trend_score +  # Trend: 20%
                    0.10 * tech_score,    # Technical (RSI): 10% - soft score, missing RSI = neutral
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
                    "notes": "IV sourced from Schwab in pick builder; weekly_screener does not require IV to run"
                }
                
                # Build features dict with raw data
                features = {
                    "profile": profile,
                    "quote": quote,
                    "ratios": ratios,
                    "metrics": metrics,
                    "rsi": rsi,
                    "rsi_period": rules.rsi_period,
                    "rsi_interval": rules.rsi_interval,
                    "next_earnings_date": next_earnings_date.isoformat() if next_earnings_date else None,
                    "earnings_in_days": earnings_in_days,
                    "earnings_source": earnings_source,
                    "news_count": len(news),
                    "sentiment_raw": sent,
                    "fundamentals": f_feats,
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
                    rsi=rsi,
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
            metrics_json = {
                "wheel_score": c.wheel_score,
                "fundamentals_score": c.fundamentals_score,
                "sentiment_score": c.sentiment_score,
                "trend_score": c.trend_score,
                "technical_score": c.technical_score,
                "rsi_period": rules.rsi_period,
                "rsi_interval": rules.rsi_interval,
                "next_earnings_date": c.next_earnings_date.isoformat() if c.next_earnings_date else None,
                "earnings_in_days": c.earnings_in_days,
                "earnings_source": c.earnings_source,
                "reasons": c.reasons,
                "features": c.features,  # Full raw data dump
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
                "iv": None,  # IV sourced from Schwab in pick builder
                "iv_rank": None,  # IV rank computed from Schwab + history
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
            "notes": f"OK: candidates written (source={universe_source}, earnings_known={earnings_known}, earnings_unknown={earnings_unknown})"
        })
        
        logger.info(
            f"Run complete. run_id={run_id} | status=success | "
            f"candidates={candidates_count} | picks=0 | "
            f"earnings_known={earnings_known} earnings_unknown={earnings_unknown}"
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
