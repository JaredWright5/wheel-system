"""
Build CSP (Cash-Secured Put) picks from screening candidates.
Uses WheelRules for consistent configuration and applies earnings exclusion.
Enhanced with diagnostic logging for pick generation failures.
Includes dynamic portfolio budget fetching from Schwab and portfolio selection.
"""
from __future__ import annotations

from datetime import datetime as dt, timezone, date, timedelta
from typing import Any, Dict, List, Optional, Tuple
import os
import math
from dotenv import load_dotenv
from loguru import logger

from wheel.clients.supabase_client import get_supabase, upsert_rows
from wheel.clients.schwab_marketdata_client import SchwabMarketDataClient
from wheel.clients.schwab_client import SchwabClient
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

# Target-based pick generation parameters
CSP_MAX_CANDIDATES_TO_SCAN = int(os.getenv("CSP_MAX_CANDIDATES_TO_SCAN", "100"))
CSP_TARGET_PICKS = int(os.getenv("CSP_TARGET_PICKS", "10"))

# Maximum annualized yield (as decimal, e.g., 3.0 = 300%)
MAX_ANNUALIZED_YIELD = float(os.getenv("MAX_ANNUALIZED_YIELD", "3.0"))

# Portfolio budget and selection parameters
WHEEL_CSP_MAX_TRADES = int(os.getenv("WHEEL_CSP_MAX_TRADES", "4"))
WHEEL_CSP_MIN_CASH_BUFFER_PCT = float(os.getenv("WHEEL_CSP_MIN_CASH_BUFFER_PCT", "0.10"))
WHEEL_CSP_MAX_CASH_PER_TRADE = float(os.getenv("WHEEL_CSP_MAX_CASH_PER_TRADE", "25000.0"))
WHEEL_CASH_EQUIVALENT_SYMBOLS = os.getenv("WHEEL_CASH_EQUIVALENT_SYMBOLS", "SWVXX").strip()
DEFAULT_PORTFOLIO_CASH = 50000.0  # Fallback if Schwab fetch fails

# Portfolio selection quality floor
WHEEL_MIN_TOTAL_SCORE = float(os.getenv("WHEEL_MIN_TOTAL_SCORE", "0.0"))
WHEEL_ALLOW_NEGATIVE_SELECTION = os.getenv("WHEEL_ALLOW_NEGATIVE_SELECTION", "false").lower() in ("true", "1", "yes")

# Scoring mode: "balanced" (default) or "quality_first"
SCORE_MODE = os.getenv("WHEEL_SCORE_MODE", "balanced").lower()
if SCORE_MODE not in ("balanced", "quality_first"):
    logger.warning(f"Invalid WHEEL_SCORE_MODE={SCORE_MODE}, using 'balanced'")
    SCORE_MODE = "balanced"


# ---------- Helpers ----------

def build_why_this_trade(*, symbol: str, pick: dict, metrics: dict, rules: Any) -> dict:
    """
    Build a structured explanation for why this CSP pick was generated.
    
    Args:
        symbol: Stock symbol
        pick: Pick row dict (from pick_rows)
        metrics: Candidate metrics dict (from screening_candidates.metrics)
        rules: WheelRules instance
        
    Returns:
        JSON-serializable dict with:
        - headline: str
        - bullets: list[str]
        - score_breakdown: dict
        - risk_notes: list[str]
    """
    pick_metrics = pick.get("pick_metrics", {})
    option_selected = pick_metrics.get("option_selected", {})
    metadata = pick_metrics.get("metadata", {})
    underlying_breakdown = option_selected.get("underlying_breakdown", {})
    score_components = metadata.get("score_components", {})
    rule_context = pick_metrics.get("rule_context", {})
    
    # Extract key values
    delta = pick.get("delta", 0.0)
    abs_delta = abs(delta)
    dte = pick.get("dte", 0)
    strike = pick.get("strike", 0.0)
    bid = option_selected.get("bid", 0.0)
    ask = option_selected.get("ask", 0.0)
    mid = option_selected.get("mid", 0.0)
    spread_abs = option_selected.get("spread_abs", 0.0)
    spread_pct = option_selected.get("spread_pct", 0.0)
    oi = option_selected.get("openInterest", 0)
    ann_yld = pick.get("annualized_yield", 0.0)
    window_used = metadata.get("used_dte_window", "primary")
    required_cash_net = metadata.get("required_cash_net", 0.0)
    
    # Extract fundamentals
    fund_score = metadata.get("fund_score")
    if fund_score is None:
        fund_score = underlying_breakdown.get("fundamentals_score")
    
    # Extract RSI
    rsi_raw = pick.get("rsi")
    rsi_value = None
    if isinstance(rsi_raw, dict):
        rsi_value = rsi_raw.get("value")
    elif isinstance(rsi_raw, (int, float)):
        rsi_value = rsi_raw
    
    # Extract IV
    iv_current = pick.get("iv")
    iv_rank = pick.get("iv_rank")
    
    # Extract earnings
    earn_in_days = pick.get("earn_in_days")
    earnings_avoid_days = rule_context.get("earnings_avoid_days", rules.earnings_avoid_days)
    
    # Extract liquidity flags
    liquidity = metadata.get("liquidity", {})
    min_bid_ok = liquidity.get("min_bid_ok", False)
    spread_ok_status = liquidity.get("spread_ok", False)
    oi_ok = liquidity.get("oi_ok", False)
    
    # Extract financial scores submetrics
    financial_scores = metrics.get("financial_scores", {})
    piotroski_score = financial_scores.get("piotroskiScore")
    altman_z_score = financial_scores.get("altmanZScore")
    
    # Build headline
    headline = f"CSP {symbol} ${strike:.2f} {dte}DTE: {ann_yld:.1%} yield, delta={abs_delta:.2f}"
    
    # Build bullets
    bullets = []
    
    # 1) Strategy fit
    delta_band = rule_context.get("delta_band", [rules.csp_delta_min, rules.csp_delta_max])
    bullets.append(
        f"CSP in target delta band (abs delta {abs_delta:.3f} in [{delta_band[0]:.2f}, {delta_band[1]:.2f}])"
    )
    if window_used == "primary":
        bullets.append(
            f"DTE in primary window ({rules.dte_min_primary}-{rules.dte_max_primary} days)"
        )
    elif window_used == "fallback":
        bullets.append(
            f"DTE in fallback window ({rules.dte_min_fallback}-{rules.dte_max_fallback} days)"
        )
    else:
        bullets.append(f"DTE: {dte} days")
    
    # 2) Premium & pricing
    credit_str = f"${bid:.2f}" if bid > 0 else "N/A"
    bullets.append(f"Credit (bid) = {credit_str}, annualized yield = {ann_yld:.2%}")
    
    # 3) Liquidity
    liquidity_parts = []
    if min_bid_ok:
        liquidity_parts.append(f"bid >= ${rules.min_bid:.2f}")
    if spread_ok_status:
        liquidity_parts.append(f"spread {spread_pct:.1f}% (abs ${spread_abs:.2f})")
    if oi_ok:
        liquidity_parts.append(f"OI >= {rules.min_open_interest}")
    if liquidity_parts:
        bullets.append(f"Liquidity: {'; '.join(liquidity_parts)} (OI={oi})")
    else:
        bullets.append(f"Liquidity: spread {spread_pct:.1f}%, OI={oi}")
    
    # 4) Fundamentals
    if fund_score is not None:
        fund_diff = fund_score - 50.0
        if fund_diff > 10:
            fund_desc = "strong"
        elif fund_diff > 0:
            fund_desc = "above neutral"
        elif fund_diff > -10:
            fund_desc = "near neutral"
        else:
            fund_desc = "below neutral"
        bullets.append(f"Fundamentals score: {fund_score:.1f}/100 ({fund_desc}, neutral=50)")
        
        # Add financial scores submetrics if present
        strong_metrics = []
        if piotroski_score is not None:
            strong_metrics.append(f"Piotroski={piotroski_score:.1f}")
        if altman_z_score is not None:
            strong_metrics.append(f"Altman Z={altman_z_score:.2f}")
        if strong_metrics:
            bullets.append(f"Financial quality: {', '.join(strong_metrics)}")
    else:
        bullets.append("Fundamentals score: N/A")
    
    # 5) Technicals (RSI)
    if rsi_value is not None:
        if rsi_value < 35:
            rsi_desc = "oversold-ish"
        elif rsi_value < 55:
            rsi_desc = "neutral"
        elif rsi_value < 70:
            rsi_desc = "strong"
        else:
            rsi_desc = "overbought-ish"
        bullets.append(f"RSI: {rsi_value:.1f} ({rsi_desc})")
    else:
        bullets.append("RSI: N/A")
    
    # 6) Volatility
    if iv_current is not None:
        bullets.append(f"IV: {iv_current:.2%}")
        if iv_rank is not None:
            bullets.append(f"IV Rank: {iv_rank:.1f}/100")
        else:
            bullets.append("IV Rank: not available yet (insufficient history)")
    else:
        bullets.append("IV: N/A")
    
    # 7) Earnings
    if earn_in_days is not None:
        try:
            earn_days_int = int(earn_in_days)
            if earn_days_int >= earnings_avoid_days:
                bullets.append(
                    f"Earnings avoided: next earnings in {earn_days_int} days (threshold={earnings_avoid_days})"
                )
            else:
                bullets.append(
                    f"Earnings: next in {earn_days_int} days (within avoid window, but pick generated)"
                )
        except (ValueError, TypeError):
            bullets.append("Earnings date unknown (treated as allowed)")
    else:
        bullets.append("Earnings date unknown (treated as allowed)")
    
    # Build score breakdown
    total_score = metadata.get("total_score") or metadata.get("chosen_total_score", 0.0)
    contract_score = option_selected.get("contract_score", 0.0)
    underlying_bonus = option_selected.get("underlying_bonus", 0.0)
    fundamentals_bonus = underlying_breakdown.get("fundamentals_bonus", 0.0)
    rsi_bonus = underlying_breakdown.get("rsi_bonus", 0.0)
    iv_bonus = underlying_breakdown.get("iv_bonus", 0.0)
    mr_bonus = underlying_breakdown.get("mr_bonus", 0.0)
    fundamentals_penalty = underlying_breakdown.get("fundamentals_penalty", 0.0)
    score_mode = metadata.get("score_mode", SCORE_MODE)
    
    # Get fundamentals component (mode-specific)
    if score_mode == "quality_first":
        fundamentals_component = score_components.get("fundamentals_component", 0.0)
        score_breakdown = {
            "total_score": total_score,
            "contract_score": contract_score,
            "underlying_bonus": underlying_bonus,
            "fundamentals_component": fundamentals_component,
            "rsi_bonus": rsi_bonus,
            "iv_bonus": iv_bonus,
            "mean_reversion_bonus": mr_bonus,
            "fundamentals_penalty": fundamentals_penalty,
            "score_mode": score_mode,
        }
    else:
        score_breakdown = {
            "total_score": total_score,
            "contract_score": contract_score,
            "underlying_bonus": underlying_bonus,
            "fundamentals_bonus": fundamentals_bonus,
            "rsi_bonus": rsi_bonus,
            "iv_bonus": iv_bonus,
            "mean_reversion_bonus": mr_bonus,
            "fundamentals_penalty": fundamentals_penalty,
            "score_mode": score_mode,
        }
    
    # Build risk notes
    risk_notes = []
    
    # Assignment risk
    risk_notes.append(f"Assignment risk: willing to own 100 shares at ${strike:.2f}")
    
    # Concentration risk
    if required_cash_net > 0:
        risk_notes.append(
            f"Concentration risk: high notional vs account size (required_cash_net=${required_cash_net:,.2f})"
        )
    
    # High premium risk
    if ann_yld > 0.60:
        risk_notes.append("High premium can imply higher tail risk / volatility")
    
    # Wide spread risk
    if spread_pct > 0.10:
        risk_notes.append("Wide spread: be careful with limit orders")
    
    # Share class duplication (only if duplicate rule triggered - we can't detect this here, so omit)
    # This would require tracking which picks were skipped due to duplicate exposure
    
    return {
        "headline": headline,
        "bullets": bullets,
        "score_breakdown": score_breakdown,
        "risk_notes": risk_notes,
    }


def normalize_exposure_symbol(symbol: str) -> str:
    """
    Normalize symbol to prevent duplicate economic exposure.
    
    Maps share classes to a canonical form to avoid duplicate picks for the same company:
    - GOOG, GOOGL -> GOOGL (Alphabet class shares)
    - BRK.A, BRK.B -> BRK.B (Berkshire Hathaway, wheel-friendly)
    - BF.A, BF.B -> BF.B (Brown-Forman class shares)
    - Other symbols are normalized using standard equity normalization
    
    Args:
        symbol: Stock symbol (e.g., "GOOG", "GOOGL", "BRK.A", "BRK-B")
        
    Returns:
        Canonical exposure symbol (e.g., "GOOGL", "BRK.B", "AAPL")
        
    Examples:
        >>> normalize_exposure_symbol("GOOG")
        'GOOGL'
        >>> normalize_exposure_symbol("GOOGL")
        'GOOGL'
        >>> normalize_exposure_symbol("BRK.A")
        'BRK.B'
        >>> normalize_exposure_symbol("BRK-B")
        'BRK.B'
        >>> normalize_exposure_symbol("AAPL")
        'AAPL'
    """
    if not symbol:
        return symbol
    
    # Strip and uppercase
    normalized = symbol.strip().upper()
    
    # Handle specific share class mappings
    # Alphabet: GOOG -> GOOGL
    if normalized == "GOOG":
        return "GOOGL"
    
    # Berkshire Hathaway: BRK.A -> BRK.B (wheel-friendly)
    if normalized in ("BRK.A", "BRK-A"):
        return "BRK.B"
    
    # Brown-Forman: BF.A -> BF.B
    if normalized in ("BF.A", "BF-A"):
        return "BF.B"
    
    # For other symbols, use standard normalization (handles BRK.B, BF.B, etc.)
    # Replace "/" with "." (rare edge cases)
    normalized = normalized.replace("/", ".")
    
    # Replace "-" with "." for known class share tickers (BRK, BF)
    if "-" in normalized:
        ticker_base = normalized.split("-")[0]
        if ticker_base in ("BRK", "BF"):
            normalized = normalized.replace("-", ".")
    
    return normalized


def _safe_float(x, default=None):
    """Safely convert to float, returning default on error."""
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _extract_fundamentals_score(metrics: dict) -> Optional[float]:
    """
    Returns fundamentals score in 0-100 range if present, else None.
    Supports multiple historical key names to avoid breaking.
    """
    if not isinstance(metrics, dict):
        return None
    # Preferred keys (newest first)
    for path in [
        ("fundamentals", "score_total"),
        ("fundamentals_score_total",),
        ("fundamentals_score",),
        ("financial_scores", "score"),
        ("financial_scores", "value"),
    ]:
        cur = metrics
        ok = True
        for k in path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False
                break
        if ok:
            try:
                v = float(cur)
                # sanity clamp
                if v < 0:
                    v = 0.0
                if v > 100:
                    v = 100.0
                return v
            except Exception:
                pass
    return None


def compute_underlying_bonus(c: Dict[str, Any]) -> Tuple[float, Dict[str, float]]:
    """
    Compute underlying bonus from candidate metrics (fundamentals, RSI, IV).
    
    Returns:
        (underlying_bonus, breakdown_dict)
        - underlying_bonus: bounded bonus (-10 to +25)
        - breakdown_dict: {"fundamentals_bonus": float, "rsi_bonus": float, "iv_bonus": float, "mr_bonus": float, "fundamentals_penalty": float}
    """
    metrics = c.get("metrics") or {}
    breakdown = {
        "fundamentals_bonus": 0.0,
        "rsi_bonus": 0.0,
        "iv_bonus": 0.0,
        "mr_bonus": 0.0,
        "fundamentals_penalty": 0.0,
    }
    
    # Extract fund_score (treat None as 50 for penalty calculation)
    fund_score_raw = metrics.get("fundamentals_score") or metrics.get("fundamentals_score_total")
    fund_score = _safe_float(fund_score_raw, 50.0) if fund_score_raw is not None else 50.0
    
    # 1) Fundamentals bonus (0 to +15)
    if fund_score_raw is not None:
        fund_score_float = _safe_float(fund_score_raw, 50.0)
        # fundamentals_bonus = clamp((fund_score - 50) / 50, 0, 1) * 15
        # So 50 => 0, 100 => +15
        normalized = max(0.0, min(1.0, (fund_score_float - 50.0) / 50.0))
        breakdown["fundamentals_bonus"] = normalized * 15.0
    
    # 1b) Fundamentals penalty (soft penalty for low quality)
    # if fund_score >= 60: penalty = 0
    # if 55 <= fund_score < 60: penalty = -2
    # if 45 <= fund_score < 55: penalty = -6
    # if fund_score < 45: penalty = -12
    if fund_score >= 60:
        breakdown["fundamentals_penalty"] = 0.0
    elif fund_score >= 55:
        breakdown["fundamentals_penalty"] = -2.0
    elif fund_score >= 45:
        breakdown["fundamentals_penalty"] = -6.0
    else:
        breakdown["fundamentals_penalty"] = -12.0
    
    # 2) RSI mean reversion bonus (-5 to +5)
    rsi_data = metrics.get("rsi") or {}
    rsi_value = rsi_data.get("value") if isinstance(rsi_data, dict) else (rsi_data if isinstance(rsi_data, (int, float)) else None)
    rsi = _safe_float(rsi_value)
    
    if rsi is None:
        breakdown["rsi_bonus"] = 0.0
    else:
        if rsi <= 35:
            breakdown["rsi_bonus"] = 5.0
        elif rsi < 50:
            breakdown["rsi_bonus"] = 2.0
        elif rsi <= 65:
            breakdown["rsi_bonus"] = 0.0
        elif rsi < 75:
            breakdown["rsi_bonus"] = -2.0
        else:  # rsi >= 75
            breakdown["rsi_bonus"] = -5.0
    
    # 3) IV regime bonus (0 to +10)
    iv_data = metrics.get("iv") or {}
    iv_rank = _safe_float(iv_data.get("rank") if isinstance(iv_data, dict) else None)
    iv_current = _safe_float(iv_data.get("current") if isinstance(iv_data, dict) else None)
    
    if iv_rank is not None:
        # iv_bonus = (iv_rank / 100) * 10
        breakdown["iv_bonus"] = (iv_rank / 100.0) * 10.0
    elif iv_current is not None:
        # Minimal bonus until rank history exists (reduced from 2.0 to 0.5)
        breakdown["iv_bonus"] = 0.5
    else:
        breakdown["iv_bonus"] = 0.0
    
    # 4) Mean reversion kicker (0 to +5)
    iv_zscore = _safe_float(iv_data.get("zscore") if isinstance(iv_data, dict) else None)
    if iv_zscore is not None and iv_zscore >= 1.0 and rsi is not None and rsi <= 40:
        breakdown["mr_bonus"] = 5.0
    else:
        breakdown["mr_bonus"] = 0.0
    
    # Total bonus: cap at +25, floor at -10
    # Include fundamentals_penalty in the sum
    total_bonus = (
        breakdown["fundamentals_bonus"] +
        breakdown["rsi_bonus"] +
        breakdown["iv_bonus"] +
        breakdown["mr_bonus"] +
        breakdown["fundamentals_penalty"]
    )
    underlying_bonus = max(-10.0, min(25.0, total_bonus))
    
    return underlying_bonus, breakdown


def compute_total_score(
    contract_score: float,
    liquidity_bonus: float,
    underlying_bonus: float,
    underlying_breakdown: Dict[str, float],
    fund_score: Optional[float],
    score_mode: str,
) -> Tuple[float, Dict[str, Any]]:
    """
    Compute total_score based on scoring mode.
    
    Args:
        contract_score: Option contract score (unchanged)
        liquidity_bonus: Liquidity bonus (unchanged)
        underlying_bonus: Underlying bonus (for balanced mode)
        underlying_breakdown: Breakdown dict with fund/rsi/iv/mr bonuses
        fund_score: Fundamentals score (0-100, for quality_first mode)
        score_mode: "balanced" or "quality_first"
        
    Returns:
        (total_score, components_dict)
        components_dict includes: fundamentals_component, contract_component, tacticals_component (for quality_first)
        or contract_score, liquidity_bonus, underlying_bonus (for balanced)
    """
    if score_mode == "quality_first":
        # fundamentals_component = (fund_score - 50) * 0.8
        # So 50->0, 80->+24, 100->+40, 30->-16
        if fund_score is not None:
            fundamentals_component = (fund_score - 50.0) * 0.8
        else:
            fundamentals_component = 0.0
        
        # contract_component = contract_score (unchanged)
        contract_component = contract_score
        
        # tacticals_component = (rsi_bonus + iv_bonus + mr_bonus) capped at +10, floored at -5
        tacticals_raw = (
            underlying_breakdown.get("rsi_bonus", 0.0) +
            underlying_breakdown.get("iv_bonus", 0.0) +
            underlying_breakdown.get("mr_bonus", 0.0)
        )
        tacticals_component = max(-5.0, min(10.0, tacticals_raw))
        
        # total_score = 0.60 * fundamentals_component + 0.25 * contract_component + 0.15 * tacticals_component
        total_score = (
            0.60 * fundamentals_component +
            0.25 * contract_component +
            0.15 * tacticals_component
        )
        
        return total_score, {
            "fundamentals_component": fundamentals_component,
            "contract_component": contract_component,
            "tacticals_component": tacticals_component,
        }
    else:
        # balanced mode (default): total_score = contract_score + liquidity_bonus + underlying_bonus
        total_score = contract_score + liquidity_bonus + underlying_bonus
        return total_score, {
            "contract_score": contract_score,
            "liquidity_bonus": liquidity_bonus,
            "underlying_bonus": underlying_bonus,
        }


def _fetch_portfolio_budget_from_schwab() -> Tuple[float, str]:
    """
    Fetch portfolio cash budget from Schwab account balances with robust schema handling.
    
    Handles nested structures (securitiesAccount.currentBalances, etc.) and tries multiple
    balance buckets and cash field names in priority order.
    
    Returns:
        (cash_amount, source_string)
        source_string format: "schwab:{balance_bucket}.{key}" or "fallback:reason"
    """
    try:
        schwab = SchwabClient.from_env()
        response = schwab.get_account()
        
        if not isinstance(response, dict):
            logger.warning("Schwab account response is not a dict, using fallback budget")
            return DEFAULT_PORTFOLIO_CASH, "fallback:invalid_response"
        
        # Normalize response into "account" dict
        account = None
        if isinstance(response.get("securitiesAccount"), dict):
            account = response["securitiesAccount"]
        elif isinstance(response.get("account"), dict):
            account = response["account"]
        else:
            # Use raw response as account
            account = response
        
        if not isinstance(account, dict):
            logger.warning("Schwab account response missing account dict, using fallback budget")
            return DEFAULT_PORTFOLIO_CASH, "fallback:no_account"
        
        # Gather candidate balance dicts in priority order
        balance_buckets = [
            ("currentBalances", account.get("currentBalances")),
            ("projectedBalances", account.get("projectedBalances")),
            ("initialBalances", account.get("initialBalances")),
        ]
        
        # Cash/buying power field names in priority order
        # For CSPs, prioritize true cash capacity; buyingPower may include margin
        cash_field_names = [
            "cashAvailableForTrading",
            "availableFundsForTrading",
            "availableFunds",
            "cashBalance",
            "totalCash",
            "optionBuyingPower",
            "buyingPower",
        ]
        
        # Try each balance bucket
        for balance_bucket_name, balance_dict in balance_buckets:
            if not isinstance(balance_dict, dict):
                continue
            
            # Try each cash field name in priority order
            for field_name in cash_field_names:
                cash_value = _safe_float(balance_dict.get(field_name))
                if cash_value is not None and cash_value > 0:
                    source = f"schwab:{balance_bucket_name}.{field_name}"
                    logger.info(f"Portfolio budget from Schwab: {source} = ${cash_value:,.2f}")
                    return cash_value, source
        
        # No cash value found - log structural info for debugging (no sensitive values)
        top_level_keys = list(response.keys()) if isinstance(response, dict) else []
        securities_account_keys = []
        balance_keys_by_bucket = {}
        
        if isinstance(response.get("securitiesAccount"), dict):
            securities_account_keys = list(response["securitiesAccount"].keys())
        
        for balance_bucket_name, balance_dict in balance_buckets:
            if isinstance(balance_dict, dict):
                balance_keys_by_bucket[balance_bucket_name] = list(balance_dict.keys())
        
        logger.warning(
            f"Schwab account response missing all cash fields. "
            f"Top-level keys: {top_level_keys}, "
            f"securitiesAccount keys: {securities_account_keys}, "
            f"balance keys: {balance_keys_by_bucket}. "
            f"Using fallback budget."
        )
        return DEFAULT_PORTFOLIO_CASH, "fallback:no_cash_fields"
        
    except Exception as e:
        logger.warning(f"Failed to fetch portfolio budget from Schwab: {e}. Using fallback budget.")
        return DEFAULT_PORTFOLIO_CASH, "fallback:exception"


def _fetch_cash_equivalents_value() -> float:
    """
    Fetch market value of cash-equivalent positions (e.g., money market funds) from Schwab.
    
    Returns:
        Sum of marketValue for positions matching cash equivalent symbols (case-insensitive)
        Returns 0.0 if fetch fails or no matching positions found
    """
    try:
        # Parse allowlist (comma-separated, case-insensitive)
        if not WHEEL_CASH_EQUIVALENT_SYMBOLS:
            return 0.0
        
        allowlist = {s.strip().upper() for s in WHEEL_CASH_EQUIVALENT_SYMBOLS.split(",") if s.strip()}
        if not allowlist:
            return 0.0
        
        schwab = SchwabClient.from_env()
        positions_response = schwab.get_positions()
        
        if not isinstance(positions_response, dict):
            logger.warning("Schwab positions response is not a dict, skipping cash equivalents")
            return 0.0
        
        # Extract positions from common locations
        positions = []
        if isinstance(positions_response.get("securitiesAccount"), dict):
            positions = positions_response["securitiesAccount"].get("positions") or []
        elif isinstance(positions_response.get("positions"), list):
            positions = positions_response["positions"]
        
        if not isinstance(positions, list):
            logger.warning("Schwab positions response missing positions list, skipping cash equivalents")
            return 0.0
        
        # Sum market value for matching positions
        cash_equiv_value = 0.0
        matched_symbols = []
        
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            
            # Get symbol (case-insensitive match)
            instrument = pos.get("instrument") or {}
            symbol = (instrument.get("symbol") or "").strip().upper()
            if not symbol or symbol not in allowlist:
                continue
            
            # Check asset type (MUTUAL_FUND or similar)
            asset_type = (instrument.get("assetType") or "").upper()
            if "MUTUAL_FUND" not in asset_type and "FUND" not in asset_type:
                continue
            
            # Check quantity > 0
            quantity = _safe_float(pos.get("longQuantity") or pos.get("quantity"), 0.0) or 0.0
            if quantity <= 0:
                continue
            
            # Get market value
            market_value = _safe_float(pos.get("marketValue"), 0.0) or 0.0
            if market_value > 0:
                cash_equiv_value += market_value
                matched_symbols.append(symbol)
        
        if cash_equiv_value > 0:
            logger.info(
                f"Cash equivalents: symbols={sorted(set(matched_symbols))} "
                f"value=${cash_equiv_value:,.2f} (added to budget)"
            )
        
        return cash_equiv_value
        
    except Exception as e:
        logger.warning(f"Failed to fetch cash equivalents from Schwab: {e}. Proceeding with balances cash only.")
        return 0.0


def _determine_portfolio_budget() -> Tuple[float, str]:
    """
    Determine portfolio cash budget from env var or Schwab.
    
    Priority:
    1. WHEEL_CSP_PORTFOLIO_CASH env var (if set)
    2. Schwab account balances
    3. Default fallback
    
    Returns:
        (cash_amount, source_string)
    """
    env_cash = os.getenv("WHEEL_CSP_PORTFOLIO_CASH", "").strip()
    if env_cash:
        try:
            cash = float(env_cash)
            if cash > 0:
                logger.info(f"Portfolio budget from env: WHEEL_CSP_PORTFOLIO_CASH = ${cash:,.2f}")
                return cash, "env"
            else:
                logger.warning(f"WHEEL_CSP_PORTFOLIO_CASH is non-positive ({cash}), fetching from Schwab")
        except ValueError:
            logger.warning(f"WHEEL_CSP_PORTFOLIO_CASH is not a valid float ({env_cash}), fetching from Schwab")
    
    # Fetch from Schwab
    return _fetch_portfolio_budget_from_schwab()


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


def _check_liquidity(option: Dict[str, Any], rules) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
    """
    Check if option meets liquidity requirements using WheelRules.
    
    Returns:
        (is_valid, reason_if_invalid, spread_details)
        spread_details is None if spread check fails early, otherwise contains spread info
    """
    bid = _safe_float(option.get("bid"), 0.0) or 0.0
    ask = _safe_float(option.get("ask"), 0.0) or 0.0
    
    # Require bid >= MIN_CREDIT (stricter than MIN_BID, so this is the effective check)
    if bid < rules.min_credit:
        return False, f"bid_below_min_credit_{bid:.2f}", None
    
    # Require non-null ask
    if ask <= 0:
        return False, "missing_ask", None
    
    # Check spread using wheel_rules.spread_ok() - new signature returns (ok, details)
    spread_ok_result, spread_details = spread_ok(bid=bid, ask=ask, rules=rules)
    if not spread_ok_result:
        # Determine specific failure reason for logging
        if spread_details and spread_details.get("mid") is not None:
            mid = spread_details["mid"]
            abs_spread = spread_details["spread_abs"]
            pct_spread = spread_details["spread_pct"]
            abs_cap = spread_details["abs_cap_used"]
            
            if pct_spread > rules.max_spread_pct:
                return False, f"spread_pct_fail_{pct_spread:.1f}%", spread_details
            elif abs_spread > abs_cap:
                return False, f"spread_abs_fail_{abs_spread:.2f}", spread_details
            else:
                return False, "spread_fail", spread_details
        else:
            return False, "spread_fail", spread_details
    
    # Check open interest
    oi = _safe_float(option.get("openInterest"), 0.0) or 0.0
    if oi < rules.min_open_interest:
        return False, f"low_oi_{int(oi)}", spread_details
    
    return True, None, spread_details


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
        
        # Count bid >= MIN_CREDIT (stricter than MIN_BID)
        bid = _safe_float(o.get("bid"), 0.0) or 0.0
        if bid >= rules.min_credit:
            counts["bid_ok"] += 1
            
            # Count spread OK (only if bid >= MIN_CREDIT)
            ask = _safe_float(o.get("ask"), 0.0) or 0.0
            if ask > 0:
                spread_ok_result, _ = spread_ok(bid=bid, ask=ask, rules=rules)
                if spread_ok_result:
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
    Choose the best PUT option in the target delta band using contract_score.
    
    Requirements:
    - abs(delta) in [target_delta_low, target_delta_high]
    - Passes liquidity checks (bid >= MIN_CREDIT, spread_ok, OI >= MIN_OPEN_INTEREST)
    - Maximizes contract_score
    """
    if not options:
        return None

    today = dt.now(timezone.utc).date()
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

        # Check liquidity (this also validates bid/ask and returns spread_details)
        is_liquid, liquidity_reason, spread_details = _check_liquidity(o, rules)
        if not is_liquid:
            continue  # Skip but don't log here (too verbose)
        
        # Extract bid/ask for premium calculation
        bid = _safe_float(o.get("bid"), 0.0) or 0.0
        ask = _safe_float(o.get("ask"), 0.0) or 0.0
        
        # Use spread_details as single source of truth for spread values
        if not spread_details or spread_details.get("mid") is None:
            continue  # Invalid spread_details
        mid = spread_details["mid"]
        spread_abs = spread_details["spread_abs"]
        spread_pct = spread_details["spread_pct"]

        # Premium estimate (prefer mark, fallback to mid, then last)
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

        # Yield sanity check
        if annualized_yield > MAX_ANNUALIZED_YIELD:
            continue  # Reject contracts with unrealistic yields

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
            "_spread_abs": spread_abs,  # From spread_details (single source of truth)
            "_spread_pct": spread_pct,  # From spread_details (single source of truth)
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
        return f"bid < ${rules.min_credit:.2f}"
    elif diag_counts["spread_ok"] == 0:
        return "spread failed (pct or abs)"
    elif diag_counts["oi_ok"] == 0:
        return f"oi < {rules.min_open_interest}"
    else:
        return "other"


def _find_best_in_delta_contract(puts: List[Dict[str, Any]], target_delta_low: float, target_delta_high: float, expiration: date, rules) -> Optional[Dict[str, Any]]:
    """
    Find the contract in delta band with the tightest spread, even if it fails liquidity checks.
    Used for diagnostic logging when spread checks fail.
    
    Returns:
        Contract dict with spread info, or None if no contracts in delta band
    """
    best_in_delta: Optional[Dict[str, Any]] = None
    best_spread_pct = float('inf')
    
    for o in puts:
        # Check delta band
        d = _safe_float(o.get("delta"))
        if d is None:
            continue
        abs_delta = abs(d)
        if not (target_delta_low <= abs_delta <= target_delta_high):
            continue
        
        # Get bid/ask for spread evaluation
        bid = _safe_float(o.get("bid"), 0.0) or 0.0
        ask = _safe_float(o.get("ask"), 0.0) or 0.0
        if bid <= 0 or ask <= 0 or ask < bid:
            continue
        
        # Use spread_ok() as single source of truth for spread values
        _, spread_details = spread_ok(bid=bid, ask=ask, rules=rules)
        if not spread_details or spread_details.get("mid") is None:
            continue  # Invalid spread_details
        
        # Extract spread values from spread_details
        mid = spread_details["mid"]
        spread_abs = spread_details["spread_abs"]
        spread_pct = spread_details["spread_pct"]
        abs_cap_used = spread_details.get("abs_cap_used")
        
        # Track the contract with tightest spread
        if spread_pct < best_spread_pct:
            best_spread_pct = spread_pct
            strike = _safe_float(o.get("strike"))
            best_in_delta = {
                "spread_pct": spread_pct,
                "spread_abs": spread_abs,
                "bid": bid,
                "ask": ask,
                "strike": strike,
                "exp": expiration.isoformat(),
                "delta": d,
                "abs_cap_used": abs_cap_used,
            }
    
    return best_in_delta


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
        - best_in_delta: dict with best in-delta contract info (if spread failed)
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
        # If spread failed, track the best in-delta contract for logging
        if "spread" in diag_counts.get("reason", "").lower():
            best_in_delta = _find_best_in_delta_contract(puts, rules.csp_delta_min, rules.csp_delta_max, exp, rules)
            if best_in_delta:
                diag_counts["best_in_delta"] = best_in_delta
    
    return best, exp, diag_counts


# ---------- Main ----------

def main() -> None:
    # Load wheel rules
    rules = load_wheel_rules()
    logger.info(f"Scoring mode: {SCORE_MODE}")
    logger.info(
        f"Selection rule: min_total_score={WHEEL_MIN_TOTAL_SCORE:.2f}, allow_negative_selection={WHEEL_ALLOW_NEGATIVE_SELECTION}"
    )
    logger.info(
        f"Wheel rules in effect: "
        f"CSP delta=[{rules.csp_delta_min:.2f}, {rules.csp_delta_max:.2f}], "
        f"DTE primary=[{rules.dte_min_primary}, {rules.dte_max_primary}], "
        f"DTE fallback=[{rules.dte_min_fallback}, {rules.dte_max_fallback}] "
        f"(allow_fallback={rules.allow_fallback_dte}), "
        f"earnings_avoid_days={rules.earnings_avoid_days}, "
        f"liquidity: max_spread_pct={rules.max_spread_pct}%, "
        f"min_bid=${rules.min_bid:.2f}, min_credit=${rules.min_credit:.2f}, min_oi={rules.min_open_interest}, "
        f"spread_tiers: Tier1(mid<${rules.SPREAD_TIER_1_MAX_MID:.2f})=${rules.SPREAD_TIER_1_MAX_ABS:.2f}, "
        f"Tier2(mid<${rules.SPREAD_TIER_2_MAX_MID:.2f})=${rules.SPREAD_TIER_2_MAX_ABS:.2f}, "
        f"Tier3(mid<${rules.SPREAD_TIER_3_MAX_MID:.2f})=${rules.SPREAD_TIER_3_MAX_ABS:.2f}, "
        f"Tier4(mid>=${rules.SPREAD_TIER_3_MAX_MID:.2f})=${rules.SPREAD_TIER_4_MAX_ABS:.2f}"
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

    # 2) Get top candidates for that run (up to max scan limit)
    cands = (
        sb.table("screening_candidates")
        .select("*")
        .eq("run_id", run_id)
        .order("score", desc=True)
        .limit(CSP_MAX_CANDIDATES_TO_SCAN)
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

    logger.info(
        f"Target-based pick generation: scanning up to {len(cands)} candidates "
        f"(max_scan={CSP_MAX_CANDIDATES_TO_SCAN}) to create {CSP_TARGET_PICKS} picks for run_id={run_id}"
    )

    md = SchwabMarketDataClient()

    pick_rows: List[Dict[str, Any]] = []
    
    # Track seen exposure symbols to prevent duplicates
    seen_exposure: set = set()
    
    # Skip counters by reason
    skipped_earnings_blocked = 0
    skipped_no_chain = 0
    skipped_no_contract_in_dte = 0
    skipped_delta_missing = 0
    skipped_delta_out_of_band = 0
    skipped_bid_zero = 0
    skipped_credit_too_low = 0
    skipped_spread = 0
    skipped_open_interest = 0
    skipped_duplicate_exposure = 0
    
    # Earnings tracking
    earnings_known = 0
    earnings_unknown = 0

    now = dt.now(timezone.utc).date()

    scanned_candidates = 0
    for i, c in enumerate(cands, start=1):
        # Stop early if we've reached the target number of picks
        if len(pick_rows) >= CSP_TARGET_PICKS:
            logger.info(
                f"Target reached: created {len(pick_rows)} picks (target={CSP_TARGET_PICKS}). "
                f"Stopping scan after {scanned_candidates} candidates."
            )
            break
        
        ticker = c.get("ticker")
        if not ticker:
            continue

        scanned_candidates += 1

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

            # Compute underlying_bonus once per candidate (used in scoring and comparison)
            underlying_bonus, underlying_breakdown = compute_underlying_bonus(c)
            
            # Extract fund_score from candidate metrics (used in scoring)
            candidate_metrics = c.get("metrics") or {}
            fund_score = _extract_fundamentals_score(candidate_metrics)

            # Try primary window first
            best_primary, exp_primary, diag_primary = attempt_window(
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
            best = best_primary
            exp = exp_primary
            selection_reason = None
            primary_comparison_data = None
            fallback_comparison_data = None
            
            # If primary succeeded and fallback is allowed, also try fallback to compare
            if best_primary and rules.allow_fallback_dte:
                fallback_attempted = True
                logger.info(f"{ticker}: primary succeeded, trying fallback for comparison")
                
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
                    # Both windows succeeded - compare using total_score (includes underlying_bonus)
                    # underlying_bonus already computed above (same for both windows)
                    # Extract primary comparison data
                    strike_primary = _safe_float(best_primary.get("strike"))
                    delta_primary = _safe_float(best_primary.get("delta"))
                    bid_primary = _safe_float(best_primary.get("bid"), 0.0) or 0.0
                    spread_pct_primary = _safe_float(best_primary.get("_spread_pct"), 0.0) or 0.0
                    oi_primary = _safe_float(best_primary.get("openInterest"), 0.0) or 0.0
                    liquidity_bonus_primary = max(0.0, (0.05 - spread_pct_primary) * 100.0)
                    contract_score_primary = _safe_float(best_primary.get("_contract_score"), 0.0) or 0.0
                    # Use fund_score or 50.0 (neutral) if None for scoring
                    fund_score_for_scoring = fund_score if fund_score is not None else 50.0
                    total_score_primary, _ = compute_total_score(
                        contract_score_primary,
                        liquidity_bonus_primary,
                        underlying_bonus,
                        underlying_breakdown,
                        fund_score_for_scoring,
                        SCORE_MODE,
                    )
                    
                    # Extract fallback comparison data
                    strike_fallback = _safe_float(best_fallback.get("strike"))
                    delta_fallback = _safe_float(best_fallback.get("delta"))
                    bid_fallback = _safe_float(best_fallback.get("bid"), 0.0) or 0.0
                    spread_pct_fallback = _safe_float(best_fallback.get("_spread_pct"), 0.0) or 0.0
                    oi_fallback = _safe_float(best_fallback.get("openInterest"), 0.0) or 0.0
                    liquidity_bonus_fallback = max(0.0, (0.05 - spread_pct_fallback) * 100.0)
                    contract_score_fallback = _safe_float(best_fallback.get("_contract_score"), 0.0) or 0.0
                    total_score_fallback, _ = compute_total_score(
                        contract_score_fallback,
                        liquidity_bonus_fallback,
                        underlying_bonus,
                        underlying_breakdown,
                        fund_score_for_scoring,
                        SCORE_MODE,
                    )
                    
                    # Store comparison data (underlying_bonus computed above)
                    primary_comparison_data = {
                        "contract_score": contract_score_primary,
                        "total_score": total_score_primary,
                        "underlying_bonus": underlying_bonus,
                        "exp": exp_primary.isoformat(),
                        "strike": strike_primary,
                        "delta": delta_primary,
                        "bid": bid_primary,
                        "spread_pct": spread_pct_primary,
                        "oi": oi_primary,
                    }
                    fallback_comparison_data = {
                        "contract_score": contract_score_fallback,
                        "total_score": total_score_fallback,
                        "underlying_bonus": underlying_bonus,
                        "exp": exp_fallback.isoformat(),
                        "strike": strike_fallback,
                        "delta": delta_fallback,
                        "bid": bid_fallback,
                        "spread_pct": spread_pct_fallback,
                        "oi": oi_fallback,
                    }
                    
                    # Choose the one with higher total_score
                    if total_score_fallback > total_score_primary:
                        best = best_fallback
                        exp = exp_fallback
                        window_used = "fallback"
                        selection_reason = "fallback_better_score"
                        if SCORE_MODE == "quality_first":
                            logger.info(
                                f"{ticker}: fallback selected | "
                                f"primary_total={total_score_primary:.4f} fallback_total={total_score_fallback:.4f}"
                            )
                        else:
                            logger.info(
                                f"{ticker}: fallback selected | "
                                f"primary_total={total_score_primary:.4f} (contract={contract_score_primary:.4f} + liq={liquidity_bonus_primary:.4f} + underlying={underlying_bonus:.4f}) | "
                                f"fallback_total={total_score_fallback:.4f} (contract={contract_score_fallback:.4f} + liq={liquidity_bonus_fallback:.4f} + underlying={underlying_bonus:.4f})"
                            )
                    else:
                        window_used = "primary"
                        selection_reason = "primary_better_score"
                        if SCORE_MODE == "quality_first":
                            logger.info(
                                f"{ticker}: primary selected | "
                                f"primary_total={total_score_primary:.4f} fallback_total={total_score_fallback:.4f}"
                            )
                        else:
                            logger.info(
                                f"{ticker}: primary selected | "
                                f"primary_total={total_score_primary:.4f} (contract={contract_score_primary:.4f} + liq={liquidity_bonus_primary:.4f} + underlying={underlying_bonus:.4f}) | "
                                f"fallback_total={total_score_fallback:.4f} (contract={contract_score_fallback:.4f} + liq={liquidity_bonus_fallback:.4f} + underlying={underlying_bonus:.4f})"
                            )
                else:
                    # Primary succeeded, fallback failed - use primary
                    window_used = "primary"
                    selection_reason = "primary_only"
            elif best_primary:
                # Primary succeeded, fallback not allowed or not attempted
                window_used = "primary"
                selection_reason = "primary_only"
            else:
                # Primary failed - check if we should try fallback
                should_try_fallback = (
                    rules.allow_fallback_dte and
                    (diag_primary["in_delta"] == 0 or 
                     (diag_primary["in_delta"] > 0 and diag_primary["spread_ok"] == 0))
                )
                
                if should_try_fallback:
                    fallback_attempted = True
                    logger.info(f"{ticker}: attempting fallback due to primary failure")
                    
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
                        selection_reason = "fallback_only"
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
                elif "bid <" in skip_reason or "bid_below_min_credit" in skip_reason:
                    skipped_credit_too_low += 1
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
                    # If spread failed, include best in-delta contract details
                    if "spread" in skip_reason.lower() and "best_in_delta" in diag_to_log:
                        best_in_delta = diag_to_log["best_in_delta"]
                        abs_cap_used = best_in_delta.get('abs_cap_used')
                        log_msg += (
                            f" | best_in_delta: exp={best_in_delta.get('exp')} "
                            f"strike={best_in_delta.get('strike')} "
                            f"delta={best_in_delta.get('delta', 0):.3f} "
                            f"bid={best_in_delta.get('bid', 0):.2f} ask={best_in_delta.get('ask', 0):.2f} "
                            f"spread_pct={best_in_delta.get('spread_pct', 0):.2f}% "
                            f"spread_abs=${best_in_delta.get('spread_abs', 0):.2f}"
                        )
                        if abs_cap_used is not None:
                            log_msg += f" abs_cap=${abs_cap_used:.2f}"
                
                # If fallback was attempted, include fallback diagnostics
                if fallback_attempted:
                    log_msg += (
                        f" | fallback: puts_total={diag_fallback['puts_total']} "
                        f"in_delta={diag_fallback['in_delta']} spread_ok={diag_fallback['spread_ok']} "
                        f"reason={diag_fallback.get('reason', 'unknown')}"
                    )
                
                logger.warning(log_msg)
                continue

            # Extract values from winning contract
            strike = _safe_float(best.get("strike"))
            premium = _safe_float(best.get("_premium"))
            abs_delta = _safe_float(best.get("_abs_delta"))
            delta = _safe_float(best.get("delta"))  # Original delta (may be negative)
            bid = _safe_float(best.get("bid"), 0.0) or 0.0
            ask = _safe_float(best.get("ask"), 0.0) or 0.0
            dte = (exp - now).days
            ann_yld = _safe_float(best.get("_annualized_yield"))
            contract_score = _safe_float(best.get("_contract_score"))
            oi = _safe_float(best.get("openInterest"), 0.0) or 0.0

            # Use spread_ok() as single source of truth for spread values
            # Always call spread_ok() when we have valid bid/ask (contracts that pass all checks should have this)
            min_bid_ok = bid >= rules.min_bid
            spread_ok_status = False
            spread_details_for_metadata = None
            if bid > 0 and ask > 0:
                spread_ok_status, spread_details_for_metadata = spread_ok(bid=bid, ask=ask, rules=rules)
            
            # Extract spread values from spread_details (single source of truth)
            if spread_details_for_metadata and spread_details_for_metadata.get("mid") is not None:
                mid = spread_details_for_metadata["mid"]
                spread_abs = spread_details_for_metadata["spread_abs"]
                spread_pct = spread_details_for_metadata["spread_pct"]
                abs_cap_used = spread_details_for_metadata.get("abs_cap_used")
            else:
                # This should not happen for valid contracts - log error and use stored values as last resort
                logger.error(f"{ticker}: spread_details missing from spread_ok() - using stored values from contract")
                mid = _safe_float(best.get("_mid")) or ((bid + ask) / 2.0 if (bid > 0 and ask > 0) else premium)
                spread_abs = _safe_float(best.get("_spread_abs")) or 0.0
                spread_pct = _safe_float(best.get("_spread_pct")) or 0.0
                abs_cap_used = None

            oi_ok = oi >= rules.min_open_interest

            # Ensure contract_score exists (should always be present from _choose_best_put_in_delta_band)
            if contract_score is None:
                logger.warning(f"{ticker}: contract_score missing from best contract, calculating...")
                contract_score = (
                    ann_yld
                    - (spread_pct * 10.0)
                    - ((1.0 / math.sqrt(oi + 1.0)) * 5.0)
                    + (abs_delta * 10.0)
                )

            # Calculate liquidity bonus
            liquidity_bonus = max(0.0, (0.05 - spread_pct) * 100.0)
            
            # Compute total_score based on scoring mode
            # underlying_bonus and fund_score already computed above (before window selection)
            # Use fund_score or 50.0 (neutral) if None for scoring
            fund_score_for_scoring = fund_score if fund_score is not None else 50.0
            total_score, score_components = compute_total_score(
                contract_score,
                liquidity_bonus,
                underlying_bonus,
                underlying_breakdown,
                fund_score_for_scoring,
                SCORE_MODE,
            )

            # Calculate required cash for CSP
            required_cash = strike * 100.0  # Full assignment value
            required_cash_net = required_cash - (bid * 100.0)  # Net cash after premium received

            # Log successful pick with total_score breakdown
            if SCORE_MODE == "quality_first":
                fund_comp = score_components.get("fundamentals_component", 0.0)
                contract_comp = score_components.get("contract_component", 0.0)
                tacticals_comp = score_components.get("tacticals_component", 0.0)
                fund_score_display = f"{fund_score:.1f}" if fund_score is not None else "NA"
                logger.info(
                    f"{ticker}: pick created | window={window_used} | "
                    f"exp={exp.isoformat()} | dte={dte} | strike={strike} | "
                    f"bid={bid:.2f} | delta={delta:.3f} | yield={ann_yld:.2%} | "
                    f"total_score={total_score:.4f} | "
                    f"fund_score={fund_score_display} | "
                    f"components: fund={fund_comp:.2f} contract={contract_comp:.4f} tacticals={tacticals_comp:.2f} | "
                    f"required_cash_net=${required_cash_net:,.2f}"
                )
            else:
                logger.info(
                    f"{ticker}: pick created | window={window_used} | "
                    f"exp={exp.isoformat()} | dte={dte} | strike={strike} | "
                    f"bid={bid:.2f} | delta={delta:.3f} | yield={ann_yld:.2%} | "
                    f"total_score={total_score:.4f} (contract={contract_score:.4f} + underlying={underlying_bonus:.4f} "
                    f"[fund={underlying_breakdown['fundamentals_bonus']:.2f} rsi={underlying_breakdown['rsi_bonus']:.2f} "
                    f"iv={underlying_breakdown['iv_bonus']:.2f} mr={underlying_breakdown['mr_bonus']:.2f} pen={underlying_breakdown.get('fundamentals_penalty', 0.0):.2f}]) | "
                    f"required_cash_net=${required_cash_net:,.2f}"
                )

            # Get RSI period/interval from candidate metrics or use defaults
            # candidate_metrics already defined above (when extracting fund_score)
            rsi_period = candidate_metrics.get("rsi_period") or rules.rsi_period
            rsi_interval = candidate_metrics.get("rsi_interval") or rules.rsi_interval

            # Build metadata object as specified
            metadata = {
                "contract_score": contract_score,
                "total_score": total_score,
                "score_mode": SCORE_MODE,
                "score_components": score_components,  # Mode-specific score components
                "underlying_bonus": underlying_bonus,
                "underlying_breakdown": underlying_breakdown,
                "fund_score": fund_score,
                "contract_details": {
                    "annualized_yield": ann_yld,  # yield
                    "spread_pct": spread_pct,
                    "open_interest": oi,  # oi
                    "delta_abs": abs_delta,
                    "dte": dte,
                },
                "liquidity": {
                    "min_bid_ok": min_bid_ok,
                    "spread_ok": spread_ok_status,
                    "oi_ok": oi_ok,
                },
                "used_dte_window": window_used,
                "chosen_contract_score": contract_score,  # Alias for backward compatibility
                "chosen_total_score": total_score,  # Alias for backward compatibility
                "chosen_liquidity_bonus": liquidity_bonus,
                "abs_cap_used": abs_cap_used,  # Store the tiered absolute spread cap that was applied
                "required_cash": required_cash,  # Full assignment value (strike * 100)
                "required_cash_net": required_cash_net,  # Net cash after premium received
            }
            
            # Add comparison data if available
            if primary_comparison_data is not None or fallback_comparison_data is not None:
                metadata["window_comparison"] = {}
                if primary_comparison_data is not None:
                    metadata["window_comparison"]["primary"] = primary_comparison_data
                if fallback_comparison_data is not None:
                    metadata["window_comparison"]["fallback"] = fallback_comparison_data
                if selection_reason:
                    metadata["window_comparison"]["selection_reason"] = selection_reason

            # Check for duplicate economic exposure
            exposure_key = normalize_exposure_symbol(ticker)
            if exposure_key in seen_exposure:
                skipped_duplicate_exposure += 1
                logger.warning(f"{ticker}: skipped due to duplicate exposure ({exposure_key})")
                continue
            
            # Add to seen set and accept the pick
            seen_exposure.add(exposure_key)

            # Build pick row
            pick_row = {
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
                    "metadata": metadata,
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
                        "abs_cap_used": abs_cap_used,  # Tiered absolute spread cap applied
                        "annualized_yield": ann_yld,
                        "contract_score": contract_score,
                        "total_score": total_score,
                        "liquidity_bonus": liquidity_bonus,
                        "underlying_bonus": underlying_bonus,
                        "underlying_breakdown": underlying_breakdown,
                        "openInterest": best.get("openInterest"),
                        "volume": best.get("totalVolume") or best.get("volume"),
                        "inTheMoney": best.get("inTheMoney"),
                        "symbol": best.get("symbol"),
                        "abs_delta": abs_delta,
                        "dte": dte,
                    },
                },
            }
            
            # Build and attach WHY_THIS_TRADE explainer
            # Initialize trade_card if it doesn't exist (will be populated later for selected picks)
            if "trade_card" not in pick_row["pick_metrics"]:
                pick_row["pick_metrics"]["trade_card"] = {}
            
            # Build explainer using candidate metrics
            candidate_metrics = c.get("metrics", {}) if isinstance(c.get("metrics"), dict) else {}
            why_this_trade = build_why_this_trade(
                symbol=ticker,
                pick=pick_row,
                metrics=candidate_metrics,
                rules=rules,
            )
            pick_row["pick_metrics"]["trade_card"]["why_this_trade"] = why_this_trade
            
            pick_rows.append(pick_row)

        except Exception as e:
            skipped_no_chain += 1
            logger.exception(f"{ticker}: failed to build CSP pick: {e}")

    # Log summary with skip counts by reason
    logger.info(
        f"Pick generation summary: "
        f"scanned_candidates={scanned_candidates}, "
        f"target_picks={CSP_TARGET_PICKS}, "
        f"created={len(pick_rows)}, "
        f"skipped_earnings_blocked={skipped_earnings_blocked}, "
        f"earnings_known={earnings_known}, earnings_unknown={earnings_unknown}, "
        f"skipped_no_chain={skipped_no_chain}, "
        f"skipped_no_contract_in_dte={skipped_no_contract_in_dte}, "
        f"skipped_delta_missing={skipped_delta_missing}, "
        f"skipped_delta_out_of_band={skipped_delta_out_of_band}, "
        f"skipped_bid_zero={skipped_bid_zero}, "
        f"skipped_credit_too_low={skipped_credit_too_low}, "
        f"skipped_spread={skipped_spread}, "
        f"skipped_open_interest={skipped_open_interest}, "
        f"skipped_duplicate_exposure={skipped_duplicate_exposure}"
    )

    if not pick_rows:
        logger.warning(
            f"No CSP picks were generated after scanning {scanned_candidates} candidates. "
            f"Check chain parsing / auth / delta availability."
        )
        # Don't raise error - allow the run to complete with 0 picks for visibility
        return

    # 2) Log WHY_THIS_TRADE explainers for all generated picks
    logger.info("=" * 80)
    logger.info("WHY_THIS_TRADE (Generated Picks):")
    logger.info("=" * 80)
    for pick in pick_rows:
        ticker = pick.get("ticker", "UNKNOWN")
        trade_card = pick.get("pick_metrics", {}).get("trade_card", {})
        why_this_trade = trade_card.get("why_this_trade", {})
        
        if why_this_trade:
            headline = why_this_trade.get("headline", f"{ticker}: No explainer available")
            bullets = why_this_trade.get("bullets", [])
            
            logger.info(f"WHY_THIS_TRADE: {ticker} — {headline}")
            # Log up to 6 bullets to keep logs readable
            for bullet in bullets[:6]:
                logger.info(f" - {bullet}")
            if len(bullets) > 6:
                logger.info(f" - ... and {len(bullets) - 6} more bullets")
        else:
            logger.warning(f"{ticker}: WHY_THIS_TRADE explainer missing")
    logger.info("=" * 80)

    # 3) Compute display_score (percentile rank of chosen_total_score) for each pick
    if pick_rows:
        # Extract chosen_total_score from metadata for each pick
        scores_with_indices = []
        for idx, pick in enumerate(pick_rows):
            metadata = pick.get("pick_metrics", {}).get("metadata", {})
            chosen_total_score = metadata.get("chosen_total_score")
            if chosen_total_score is not None:
                scores_with_indices.append((idx, chosen_total_score))
        
        if scores_with_indices:
            # Sort by chosen_total_score ascending (lowest first, highest last)
            scores_with_indices.sort(key=lambda x: x[1])
            n = len(scores_with_indices)
            
            # Compute percentile rank for each pick (best = 100, worst = 0)
            # For n=1, assign score of 100 (or 50, but 100 makes more sense for "best")
            if n == 1:
                idx = scores_with_indices[0][0]
                pick_rows[idx]["pick_metrics"]["metadata"]["display_score"] = 100.0
            else:
                # Map to percentile: position 0 gets 0, position n-1 gets 100
                # percentile = (position / (n - 1)) * 100
                for position, (idx, _) in enumerate(scores_with_indices):
                    percentile = (position / (n - 1)) * 100.0
                    pick_rows[idx]["pick_metrics"]["metadata"]["display_score"] = percentile
        
        # Log picks with display_score
        logger.info("Picks with display_score (percentile rank):")
        for pick in pick_rows:
            ticker = pick.get("ticker")
            metadata = pick.get("pick_metrics", {}).get("metadata", {})
            display_score = metadata.get("display_score")
            chosen_total_score = metadata.get("chosen_total_score")
            exp = pick.get("expiration")
            strike = pick.get("strike")
            delta = pick.get("delta")
            ann_yld = pick.get("annualized_yield")
            if display_score is not None:
                logger.info(
                    f"  {ticker}: display_score={display_score:.1f} | "
                    f"exp={exp} | strike={strike} | delta={delta:.3f} | "
                    f"yield={ann_yld:.2%} | total_score={chosen_total_score:.4f}"
                )

    # Portfolio budget determination and selection
    portfolio_budget_cash, budget_source = _determine_portfolio_budget()
    
    # Add cash equivalents if budget is from Schwab (not env var)
    cash_equiv_value = 0.0
    if budget_source.startswith("schwab:"):
        cash_equiv_value = _fetch_cash_equivalents_value()
    
    # Effective budget includes cash equivalents
    effective_budget_cash = portfolio_budget_cash + cash_equiv_value
    
    cash_buffer_pct = WHEEL_CSP_MIN_CASH_BUFFER_PCT
    allocatable_cash = effective_budget_cash * (1.0 - cash_buffer_pct)
    
    logger.info(
        f"Portfolio budget: source={budget_source}, "
        f"cash_budget=${effective_budget_cash:,.2f}, "
        f"buffer_pct={cash_buffer_pct:.1%}, "
        f"allocatable_cash=${allocatable_cash:,.2f}, "
        f"max_trades={WHEEL_CSP_MAX_TRADES}"
    )
    
    # Portfolio selection: select picks that fit within budget
    # Sort picks by total_score (descending) to prioritize best picks
    picks_with_scores = []
    for idx, pick in enumerate(pick_rows):
        metadata = pick.get("pick_metrics", {}).get("metadata", {})
        total_score = metadata.get("total_score") or metadata.get("chosen_total_score") or 0.0
        required_cash_net = metadata.get("required_cash_net", 0.0)
        picks_with_scores.append((idx, total_score, required_cash_net))
    
    # Sort by total_score descending (best first)
    picks_with_scores.sort(key=lambda x: x[1], reverse=True)
    
    # Apply quality floor filter (unless explicitly overridden)
    if WHEEL_ALLOW_NEGATIVE_SELECTION:
        eligible_for_selection = picks_with_scores
    else:
        eligible_for_selection = [
            (idx, total_score, required_cash_net)
            for idx, total_score, required_cash_net in picks_with_scores
            if total_score >= WHEEL_MIN_TOTAL_SCORE
        ]
    
    total_candidates = len(pick_rows)
    eligible_by_score = len(eligible_for_selection)
    skipped_due_to_score_floor = total_candidates - eligible_by_score
    
    selected_indices = set()
    running_allocated_net = 0.0
    selection_rank = 0
    skipped_portfolio_trade_too_large = 0
    
    for idx, total_score, required_cash_net in eligible_for_selection:
        # Check if we've hit max trades limit
        if len(selected_indices) >= WHEEL_CSP_MAX_TRADES:
            break
        
        # Get required_cash from stored metadata
        pick_metadata = pick_rows[idx].get("pick_metrics", {}).get("metadata", {})
        required_cash = pick_metadata.get("required_cash", 0.0)
        
        # Check if trade size exceeds max cash per trade limit
        skipped_due_to_trade_size = required_cash_net > WHEEL_CSP_MAX_CASH_PER_TRADE
        if skipped_due_to_trade_size:
            skipped_portfolio_trade_too_large += 1
        
        # Check if this pick fits within remaining allocatable cash and trade size limit
        if not skipped_due_to_trade_size and running_allocated_net + required_cash_net <= allocatable_cash:
            selected_indices.add(idx)
            running_allocated_net += required_cash_net
            selection_rank += 1
            # Store selection rank in metadata
            pick_rows[idx]["pick_metrics"]["metadata"]["portfolio"] = {
                "budget_source": budget_source,
                "cash_budget": effective_budget_cash,
                "cash_buffer_pct": cash_buffer_pct,
                "allocatable_cash": allocatable_cash,
                "required_cash": required_cash,
                "required_cash_net": required_cash_net,
                "max_cash_per_trade": WHEEL_CSP_MAX_CASH_PER_TRADE,
                "skipped_due_to_trade_size": skipped_due_to_trade_size,
                "budget_components": {
                    "balances_cash": portfolio_budget_cash,
                    "cash_equivalents": cash_equiv_value,
                },
                "selected": True,
                "selection_rank": selection_rank,
                "running_allocated_net": running_allocated_net,
            }
        else:
            # Pick doesn't fit - mark as not selected
            pick_rows[idx]["pick_metrics"]["metadata"]["portfolio"] = {
                "budget_source": budget_source,
                "cash_budget": effective_budget_cash,
                "cash_buffer_pct": cash_buffer_pct,
                "allocatable_cash": allocatable_cash,
                "required_cash": required_cash,
                "required_cash_net": required_cash_net,
                "max_cash_per_trade": WHEEL_CSP_MAX_CASH_PER_TRADE,
                "skipped_due_to_trade_size": skipped_due_to_trade_size,
                "budget_components": {
                    "balances_cash": portfolio_budget_cash,
                    "cash_equivalents": cash_equiv_value,
                },
                "selected": False,
                "selection_rank": None,
                "running_allocated_net": None,
            }
    
    # Log portfolio selection summary
    selected_count = len(selected_indices)
    logger.info(
        f"Portfolio selection: {selected_count}/{len(pick_rows)} picks selected, "
        f"total_candidates={total_candidates}, eligible_by_score={eligible_by_score}, "
        f"skipped_due_to_score_floor={skipped_due_to_score_floor}, "
        f"allocated=${running_allocated_net:,.2f} / ${allocatable_cash:,.2f} allocatable "
        f"(${effective_budget_cash:,.2f} budget with {cash_buffer_pct:.1%} buffer), "
        f"skipped_trade_too_large={skipped_portfolio_trade_too_large}"
    )
    
    if eligible_by_score == 0 and not WHEEL_ALLOW_NEGATIVE_SELECTION:
        logger.warning(
            f"No picks meet min_total_score threshold ({WHEEL_MIN_TOTAL_SCORE:.2f}); selected=0. "
            f"Set WHEEL_ALLOW_NEGATIVE_SELECTION=true to override."
        )

    # 2b) Build trade cards for selected picks
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    
    for idx in sorted(selected_indices):
        pick = pick_rows[idx]
        pick_metrics = pick.get("pick_metrics", {})
        option_selected = pick_metrics.get("option_selected", {})
        metadata = pick_metrics.get("metadata", {})
        portfolio_info = metadata.get("portfolio", {})
        underlying_breakdown = option_selected.get("underlying_breakdown", {})
        score_components = metadata.get("score_components", {})
        
        # Extract earnings date if available (compute from earn_in_days)
        earn_in_days = pick.get("earn_in_days")
        earnings_date = None
        days_to_earnings = None
        if earn_in_days is not None:
            try:
                earn_days_int = int(earn_in_days)
                if earn_days_int >= 0:
                    # Approximate earnings date (today + days)
                    today = dt.now(ZoneInfo("America/Los_Angeles")).date()
                    earnings_date = (today + timedelta(days=earn_days_int)).isoformat()
                    days_to_earnings = earn_days_int
            except (ValueError, TypeError):
                pass
        
        # Extract RSI value (may be dict or float)
        rsi_value = pick.get("rsi")
        if isinstance(rsi_value, dict):
            rsi_value = rsi_value.get("value")
        
        # Extract fundamentals score from metadata
        fundamentals_score = None
        if metadata:
            # Try multiple paths
            fund_score_raw = metadata.get("fundamentals_score") or metadata.get("fundamentals_score_total")
            if fund_score_raw is None:
                # Try from underlying_breakdown
                fund_score_raw = underlying_breakdown.get("fundamentals_score")
            if fund_score_raw is not None:
                try:
                    fundamentals_score = float(fund_score_raw)
                except (ValueError, TypeError):
                    pass
        
        # Build trade card
        trade_card = {
            "symbol": pick.get("ticker"),
            "action": "SELL_TO_OPEN",
            "type": "CSP",
            "exp_date": pick.get("expiration"),  # ISO format date string
            "dte": pick.get("dte"),
            "strike": pick.get("strike"),
            "delta": pick.get("delta"),
            "bid": option_selected.get("bid"),
            "ask": option_selected.get("ask"),
            "mid": option_selected.get("mid"),
            "spread_abs": option_selected.get("spread_abs"),
            "spread_pct": option_selected.get("spread_pct"),
            "credit_assumed": option_selected.get("bid"),  # Use bid as credit
            "required_cash_net": portfolio_info.get("required_cash_net"),
            "yield_annualized": pick.get("annualized_yield"),
            "earnings_date": earnings_date,
            "days_to_earnings": days_to_earnings,
            "fundamentals_score": fundamentals_score,
            "rsi": rsi_value,
            "iv_current": pick.get("iv"),
            "iv_rank": pick.get("iv_rank"),
            "score_breakdown": {
                "total_score": metadata.get("total_score") or metadata.get("chosen_total_score"),
                "contract_score": option_selected.get("contract_score"),
                "underlying_bonus": option_selected.get("underlying_bonus"),
                "fundamentals_bonus": underlying_breakdown.get("fundamentals_bonus"),
                "rsi_bonus": underlying_breakdown.get("rsi_bonus"),
                "iv_bonus": underlying_breakdown.get("iv_bonus"),
                "mr_bonus": underlying_breakdown.get("mr_bonus"),
                "fundamentals_penalty": underlying_breakdown.get("fundamentals_penalty"),
                "liquidity_bonus": option_selected.get("liquidity_bonus"),
            },
        }
        
        # Store trade card in pick_metrics
        pick_metrics["trade_card"] = trade_card
    
    # 2c) Log trade cards for selected picks
    if selected_indices:
        logger.info("=" * 80)
        logger.info("TRADE CARDS (Selected for Portfolio):")
        logger.info("=" * 80)
        for idx in sorted(selected_indices):
            pick = pick_rows[idx]
            trade_card = pick.get("pick_metrics", {}).get("trade_card", {})
            symbol = trade_card.get("symbol", "UNKNOWN")
            exp_date = trade_card.get("exp_date", "UNKNOWN")
            strike = trade_card.get("strike", 0.0)
            delta = trade_card.get("delta", 0.0)
            bid = trade_card.get("bid", 0.0)
            cash_net = trade_card.get("required_cash_net", 0.0)
            total_score = trade_card.get("score_breakdown", {}).get("total_score", 0.0)
            
            # First line: main trade details
            logger.info(
                f"TRADE CARD: {symbol} SELL CSP {exp_date} ${strike:.2f} "
                f"(delta={delta:.3f}, bid=${bid:.2f}, cash=${cash_net:,.2f}, total_score={total_score:.2f})"
            )
            
            # Second line: fundamentals/rsi/iv/earnings
            fund_score = trade_card.get("fundamentals_score")
            rsi = trade_card.get("rsi")
            iv_current = trade_card.get("iv_current")
            iv_rank = trade_card.get("iv_rank")
            days_to_earn = trade_card.get("days_to_earnings")
            
            fund_str = f"fund={fund_score:.1f}" if fund_score is not None else "fund=N/A"
            rsi_str = f"rsi={rsi:.1f}" if rsi is not None else "rsi=N/A"
            iv_str = f"iv={iv_current:.2f}" if iv_current is not None else "iv=N/A"
            iv_rank_str = f"iv_rank={iv_rank:.1f}" if iv_rank is not None else "iv_rank=N/A"
            earn_str = f"earn_in={days_to_earn}d" if days_to_earn is not None else "earn=N/A"
            
            logger.info(f"  └─ {fund_str} | {rsi_str} | {iv_str} ({iv_rank_str}) | {earn_str}")
        logger.info("=" * 80)

    # 4) Delete existing CSP picks for this run_id, then insert new ones
    # (This ensures idempotent reruns)
    logger.info(f"Deleting existing CSP picks for run_id={run_id}")
    delete_res = (
        sb.table("screening_picks")
        .delete()
        .eq("run_id", run_id)
        .eq("action", "CSP")
        .execute()
    )

    # 5) Insert new picks (batch insert)
    logger.info(f"Inserting {len(pick_rows)} screening_picks rows...")
    insert_res = sb.table("screening_picks").insert(pick_rows).execute()
    
    # Check for errors
    if hasattr(insert_res, "error") and insert_res.error:
        raise RuntimeError(f"Supabase error inserting picks: {insert_res.error}")
    
    logger.info(f"✅ build_csp_picks complete. Created {len(pick_rows)} CSP picks for run_id={run_id} ({selected_count} selected for portfolio)")


if __name__ == "__main__":
    main()
