"""
RSI Snapshot Worker: Fetches RSI from Alpha Vantage and caches in Supabase.
Runs daily to populate rsi_snapshots table for use by weekly_screener.
"""
from __future__ import annotations

from datetime import datetime, timezone, date, timedelta
import csv
from pathlib import Path
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
from loguru import logger
import os

# Load environment variables
load_dotenv(".env.local")

from wheel.clients.alpha_vantage_client import AlphaVantageClient
from wheel.clients.fmp_stable_client import FMPStableClient
from wheel.clients.supabase_client import get_supabase, upsert_rows


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
    (Same implementation as weekly_screener.py)
    """
    logger.info(f"Building universe from FMP stable company screener (min_price={min_price}, min_mcap={min_market_cap})")
    
    all_companies = []
    exchanges = ["NYSE", "NASDAQ", "AMEX"]
    limit_per_exchange = 500
    
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


def main() -> None:
    try:
        # Initialize clients
        av = AlphaVantageClient()
        logger.info("Alpha Vantage client initialized")
        
        # Environment variables
        universe_source = os.getenv("UNIVERSE_SOURCE", "csv").lower()
        RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
        RSI_INTERVAL = os.getenv("RSI_INTERVAL", "daily")
        MIN_PRICE = float(os.getenv("MIN_PRICE", "5.0"))
        MIN_MARKET_CAP = int(os.getenv("MIN_MARKET_CAP", "2000000000"))
        MIN_AVG_VOLUME = int(os.getenv("MIN_AVG_VOLUME", "1000000")) if os.getenv("MIN_AVG_VOLUME") else None
        
        # Build universe (same logic as weekly_screener)
        if universe_source == "csv":
            universe = load_universe_csv("data/universe_us.csv")
            logger.info(f"Universe size from CSV: {len(universe)} (source=csv)")
        else:
            fmp = FMPStableClient()
            universe = build_universe_fmp_stable(fmp, MIN_PRICE, MIN_MARKET_CAP, MIN_AVG_VOLUME)
            logger.info(f"Universe size from FMP stable: {len(universe)} (source=fmp_stable)")
        
        # Get today's date (UTC)
        today = date.today()
        logger.info(f"Fetching RSI for {len(universe)} tickers (as_of_date={today}, interval={RSI_INTERVAL}, period={RSI_PERIOD})")
        
        # Check existing cache for today
        sb = get_supabase()
        existing = sb.table("rsi_snapshots").select("ticker").eq("as_of_date", today.isoformat()).eq("interval", RSI_INTERVAL).eq("period", RSI_PERIOD).execute()
        existing_tickers = {row["ticker"] for row in (existing.data or [])}
        logger.info(f"Found {len(existing_tickers)} tickers already cached for today")
        
        # Statistics
        fetched_ok = 0
        fetched_missing = 0
        skipped_due_to_cache = 0
        inserted = 0
        rate_limited = 0
        stopped_due_to_limit = False
        
        # Alpha Vantage free tier: 25 requests per day
        # Process max 24 per day to stay under limit (with some buffer)
        MAX_REQUESTS_PER_DAY = 24
        
        # Fetch RSI for each ticker
        rows_to_upsert: List[Dict[str, Any]] = []
        
        for item in universe:
            ticker = item.get("symbol")
            if not ticker:
                continue
            
            # Skip if already cached for today
            if ticker in existing_tickers:
                skipped_due_to_cache += 1
                continue
            
            # Stop if we've hit the daily request limit
            if fetched_ok + fetched_missing >= MAX_REQUESTS_PER_DAY:
                stopped_due_to_limit = True
                logger.info(f"Reached daily API limit ({MAX_REQUESTS_PER_DAY} requests). Stopping. Remaining tickers will be processed on subsequent days.")
                break
            
            try:
                # Fetch RSI from Alpha Vantage
                rsi = av.get_rsi(ticker, interval=RSI_INTERVAL, period=RSI_PERIOD)
                
                if rsi is not None:
                    fetched_ok += 1
                    rows_to_upsert.append({
                        "ticker": ticker,
                        "as_of_date": today.isoformat(),
                        "interval": RSI_INTERVAL,
                        "period": RSI_PERIOD,
                        "rsi": float(rsi),
                        "source": "alpha_vantage",
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    })
                else:
                    fetched_missing += 1
                    # Still insert a row with NULL RSI to mark we tried
                    rows_to_upsert.append({
                        "ticker": ticker,
                        "as_of_date": today.isoformat(),
                        "interval": RSI_INTERVAL,
                        "period": RSI_PERIOD,
                        "rsi": None,
                        "source": "alpha_vantage",
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    })
                    
            except RuntimeError as e:
                # Check if this is a rate limit error
                error_str = str(e).lower()
                if "rate limit" in error_str or "25 requests per day" in error_str:
                    stopped_due_to_limit = True
                    rate_limited += 1
                    logger.warning(f"{ticker}: Alpha Vantage rate limit hit. Stopping after {fetched_ok + fetched_missing} requests.")
                    break
                # Other runtime errors
                logger.warning(f"{ticker}: error fetching RSI: {e}")
                fetched_missing += 1
                continue
            except Exception as e:
                logger.warning(f"{ticker}: unexpected error fetching RSI: {e}")
                fetched_missing += 1
                continue
            
            # Batch insert every 50 rows to avoid large transactions
            if len(rows_to_upsert) >= 50:
                try:
                    upsert_rows("rsi_snapshots", rows_to_upsert, keys=["ticker", "as_of_date", "interval", "period"])
                    inserted += len(rows_to_upsert)
                    logger.info(f"Upserted {len(rows_to_upsert)} RSI snapshots (total inserted: {inserted})")
                    rows_to_upsert = []
                except Exception as e:
                    logger.error(f"Error upserting RSI snapshots batch: {e}")
                    rows_to_upsert = []
        
        # Insert remaining rows
        if rows_to_upsert:
            try:
                upsert_rows("rsi_snapshots", rows_to_upsert, keys=["ticker", "as_of_date", "interval", "period"])
                inserted += len(rows_to_upsert)
            except Exception as e:
                logger.error(f"Error upserting final RSI snapshots batch: {e}")
        
        # Log summary
        logger.info(
            f"RSI snapshot complete: "
            f"fetched_ok={fetched_ok}, fetched_missing={fetched_missing}, "
            f"skipped_due_to_cache={skipped_due_to_cache}, rate_limited={rate_limited}, inserted={inserted}"
        )
        if stopped_due_to_limit:
            remaining = len(universe) - skipped_due_to_cache - fetched_ok - fetched_missing - rate_limited
            logger.info(
                f"Note: Stopped due to Alpha Vantage daily limit ({MAX_REQUESTS_PER_DAY} requests/day). "
                f"Processed {fetched_ok + fetched_missing} tickers today. "
                f"Approximately {remaining} tickers remaining. "
                f"Remaining tickers will be processed on subsequent days. "
                f"Consider upgrading to Alpha Vantage premium (75+ requests/day) for faster processing."
            )
        
    except Exception as e:
        logger.exception(f"RSI snapshot failed: {e}")
        raise


if __name__ == "__main__":
    main()

