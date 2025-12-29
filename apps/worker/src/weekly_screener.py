from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional

from loguru import logger

from wheel.clients.fmp_client import FMPClient, simple_sentiment_score
from wheel.clients.supabase_client import insert_row, upsert_rows, update_rows


@dataclass
class Candidate:
    ticker: str
    name: str
    sector: Optional[str]
    industry: Optional[str]
    market_cap: Optional[int]
    fundamentals_score: int
    sentiment_score: int
    trend_score: int
    events_score: int
    wheel_score: int
    gates_passed: bool
    reasons: Dict[str, Any]
    features: Dict[str, Any]


def clamp_int(x: float, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(round(x))))


def score_fundamentals(profile: Dict[str, Any], ratios: Dict[str, Any], km: Dict[str, Any]) -> (int, Dict[str, Any]):
    feats = {}

    npm = ratios.get("netProfitMarginTTM")
    opm = ratios.get("operatingProfitMarginTTM")
    roe = ratios.get("returnOnEquityTTM")
    pe = ratios.get("peRatioTTM")
    pfcf = ratios.get("priceToFreeCashFlowsRatioTTM")
    de = ratios.get("debtEquityRatioTTM")

    feats.update({"npm": npm, "opm": opm, "roe": roe, "pe": pe, "pfcf": pfcf, "de": de})

    s = 0.0
    w = 0.0

    if npm is not None:
        s += (min(max(npm, -0.10), 0.30) + 0.10) / 0.40 * 25
        w += 25
    if opm is not None:
        s += (min(max(opm, -0.10), 0.35) + 0.10) / 0.45 * 20
        w += 20
    if roe is not None:
        s += min(max(roe, 0.0), 0.35) / 0.35 * 25
        w += 25
    if pe is not None and pe > 0:
        if pe <= 25:
            v = 1.0
        elif pe <= 40:
            v = 1.0 - (pe - 25) / 15 * 0.6
        else:
            v = 0.2
        s += v * 20
        w += 20
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
    return clamp_int((sent + 1) * 50, 0, 100)


def score_trend_proxy(quote: Dict[str, Any]) -> (int, Dict[str, Any]):
    price = quote.get("price")
    low = quote.get("yearLow")
    high = quote.get("yearHigh")

    feats = {"price": price, "yearLow": low, "yearHigh": high}

    if price is None or low is None or high is None or high == low:
        return 50, feats

    pos = (price - low) / (high - low)
    score = 100 * (1 - min(abs(pos - 0.5) / 0.5, 1))
    return clamp_int(score, 0, 100), {**feats, "pos_52w": pos}


def main():
    fmp = FMPClient()

    universe = fmp.sp500_constituents()
    logger.info(f"Universe size from FMP S&P 500: {len(universe)}")

    start = date.today()
    end = start + timedelta(days=10)
    earn = fmp.earnings_calendar(start, end)
    earnings_tickers = {e.get("symbol") for e in earn if e.get("symbol")}
    logger.info(f"Earnings filtered tickers: {len(earnings_tickers)}")
    logger.info(f"Earnings in window {start}..{end}: {len(earnings_tickers)} tickers")

    run_row = insert_row("screening_runs", {
        "run_ts": datetime.utcnow().isoformat(),
        "universe_size": len(universe),
        "notes": "v1 fundamentals-only (premium/liquidity pending Schwab); dashboard-first"
    })
    run_id = run_row.get("run_id")
    if not run_id:
        raise RuntimeError("Failed to create screening run (missing run_id)")

    candidates: List[Candidate] = []

    mcap_pass = 0
    prof_missing = 0
    quote_missing = 0

    ticker_rows: List[Dict[str, Any]] = []

    MIN_MARKET_CAP = 20_000_000_000  # $20B

    for item in universe:
        t = item.get("symbol")
        if not t:
            continue

        if t in earnings_tickers:
            continue

        profile = fmp.profile(t) or {}
        quote = fmp.quote(t) or {}
        ratios = fmp.ratios_ttm(t) or {}
        km = fmp.key_metrics_ttm(t) or {}

        if not profile:
            prof_missing += 1
        if not quote:
            quote_missing += 1

        mcap = profile.get("mktCap") or quote.get("marketCap")
        if mcap is None or mcap < MIN_MARKET_CAP:
            continue
        mcap_pass += 1

        news = fmp.stock_news(t, limit=40)
        sent = simple_sentiment_score(news)
        sent_score = score_sentiment(sent)

        f_score, f_feats = score_fundamentals(profile, ratios, km)
        trend_score, t_feats = score_trend_proxy(quote)

        events_score = 100

        wheel_score = clamp_int(0.55 * f_score + 0.15 * sent_score + 0.30 * trend_score, 0, 100)

        name = profile.get("companyName") or item.get("name") or t
        sector = profile.get("sector") or item.get("sector")
        industry = profile.get("industry") or item.get("subSector")

        reasons = {
            "earnings_in_window": False,
            "market_cap_min": MIN_MARKET_CAP,
            "notes": "Options premium/liquidity gates will apply once Schwab options are integrated."
        }

        features = {
            "fundamentals": f_feats,
            "trend": t_feats,
            "sentiment": {"score": sent, "sent_score": sent_score},
            "market_cap": mcap,
            "sector": sector,
            "industry": industry,
        }

        candidates.append(Candidate(
            ticker=t,
            name=name,
            sector=sector,
            industry=industry,
            market_cap=int(mcap) if mcap is not None else None,
            fundamentals_score=f_score,
            sentiment_score=sent_score,
            trend_score=trend_score,
            events_score=events_score,
            wheel_score=wheel_score,
            gates_passed=True,
            reasons=reasons,
            features=features
        ))

        ticker_rows.append({
            "ticker": t,
            "name": name,
            "exchange": profile.get("exchangeShortName") or profile.get("exchange"),
            "sector": sector,
            "industry": industry,
            "market_cap": int(mcap) if mcap is not None else None,
            "currency": profile.get("currency") or "USD",
            "is_active": True,
            "updated_at": datetime.utcnow().isoformat(),
        })

    logger.info(f"Upserting tickers: {len(ticker_rows)}")
upsert_rows("tickers", ticker_rows)
logger.info("Tickers upserted")

    # Sort by wheel score (desc)
    candidates.sort(key=lambda c: c.wheel_score, reverse=True)
    logger.info(f"Candidates after filters: {len(candidates)}")

    # Write wheel_candidates rows
    cand_rows = []
    for c in candidates:
        cand_rows.append({
            "run_id": run_id,
            "ticker": c.ticker,
            "wheel_score": c.wheel_score,
            "score_premium": 0,
            "score_liquidity": 0,
            "score_fundamentals": c.fundamentals_score,
            "score_trend": c.trend_score,
            "score_events": c.events_score,
            "gates_passed": c.gates_passed,
            "reasons": c.reasons,
            "features": c.features,
            "created_at": datetime.utcnow().isoformat(),
        })

    logger.info(f"Upserting wheel_candidates: {len(cand_rows)}")
    upsert_rows("wheel_candidates", cand_rows)
    logger.info("wheel_candidates upserted")

    # Maintain approved universe (Top 40) for stability week-to-week
    top40 = candidates[:40]
    approved_rows = []
    for i, c in enumerate(top40, start=1):
        approved_rows.append({
            "ticker": c.ticker,
            "approved": True,
            "last_run_id": run_id,
            "last_run_ts": datetime.utcnow().isoformat(),
            "last_rank": i,
            "last_score": c.wheel_score,
            "updated_at": datetime.utcnow().isoformat(),
        })
    logger.info(f"Upserting approved_universe: {len(approved_rows)}")
    upsert_rows("approved_universe", approved_rows)
    logger.info("approved_universe upserted")

    update_rows("screening_runs", {"run_id": run_id}, {"notes": "OK: candidates + approved written"})

    logger.info(f"Run complete. run_id={run_id} | candidates={len(candidates)}")
    logger.info("View results in Supabase: screening_runs + wheel_candidates")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Weekly screener failed")
        raise
