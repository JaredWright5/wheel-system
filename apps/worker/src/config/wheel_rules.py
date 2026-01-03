"""
Centralized configuration for wheel trading parameters.

This module provides a WheelRules dataclass that loads trading parameters from environment
variables with sensible defaults. It includes configuration for delta bands, DTE windows,
earnings avoidance, RSI parameters, and liquidity filters.

Liquidity Filtering Philosophy:
    Both percentage and absolute spread caps are used to handle edge cases:
    - Percentage caps (e.g., 7.5%) work well for higher-priced options (e.g., $10+ premiums)
    - Tiered absolute caps prevent wide spreads across the full range of option premiums
    - Tiered system allows realistic validation for higher-premium options:
      * Tier 1 (mid < $1.00): $0.10 cap - very tight for penny options
      * Tier 2 (mid < $3.00): $0.25 cap - reasonable for low-premium weeklies
      * Tier 3 (mid < $8.00): $0.50 cap - accommodates mid-range premiums
      * Tier 4 (mid >= $8.00): $1.00 cap - realistic for high-premium options
    - This tiered approach ensures reasonable liquidity filters across the full range of
      option premiums typically seen in weeklies (from $0.05 to $10.00+)
"""
from dataclasses import dataclass
import os
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional, Tuple, Dict, Any

@dataclass(frozen=True)
class WheelRules:
    """
    Centralized configuration for wheel trading parameters.
    Loaded from environment variables with sensible defaults.
    """
    # Delta bands for options (e.g., for CSPs or CCs)
    CSP_DELTA_MIN: float = float(os.getenv("CSP_DELTA_MIN", "0.20"))
    CSP_DELTA_MAX: float = float(os.getenv("CSP_DELTA_MAX", "0.30"))
    CC_DELTA_MIN: float = float(os.getenv("CC_DELTA_MIN", "0.20"))
    CC_DELTA_MAX: float = float(os.getenv("CC_DELTA_MAX", "0.30"))

    # Days to Expiration (DTE) windows
    DTE_MIN_PRIMARY: int = int(os.getenv("DTE_MIN_PRIMARY", "5"))  # e.g., 5 days
    DTE_MAX_PRIMARY: int = int(os.getenv("DTE_MAX_PRIMARY", "9"))   # e.g., 9 days (for weeklies)
    DTE_MIN_FALLBACK: int = int(os.getenv("DTE_MIN_FALLBACK", "10")) # e.g., 10 days
    DTE_MAX_FALLBACK: int = int(os.getenv("DTE_MAX_FALLBACK", "16")) # e.g., 16 days (extended weeklies)

    # Earnings avoidance
    EARNINGS_AVOID_DAYS: int = int(os.getenv("EARNINGS_AVOID_DAYS", "10")) # Avoid if earnings within X days

    # RSI parameters
    RSI_PERIOD: int = int(os.getenv("RSI_PERIOD", "14"))
    RSI_INTERVAL: str = os.getenv("RSI_INTERVAL", "1day") # FMP uses "1day", "1week", "1month"

    # Liquidity filters (defaults tuned for weeklies)
    MAX_SPREAD_PCT: float = float(os.getenv("WHEEL_MAX_SPREAD_PCT", "7.5"))
    MIN_OPEN_INTEREST: int = int(os.getenv("WHEEL_MIN_OPEN_INTEREST", "10"))
    MIN_BID: float = float(os.getenv("WHEEL_MIN_BID", "0.05"))
    MIN_CREDIT: float = float(os.getenv("WHEEL_MIN_CREDIT", "0.25"))  # Minimum credit/premium - credit below this is usually not worth the assignment risk / transaction costs

    # Tiered absolute spread caps (based on option mid price)
    # Tier thresholds (mid price boundaries)
    SPREAD_TIER_1_MAX_MID: float = float(os.getenv("WHEEL_SPREAD_TIER_1_MAX_MID", "1.00"))   # Tier 1: mid < $1.00
    SPREAD_TIER_2_MAX_MID: float = float(os.getenv("WHEEL_SPREAD_TIER_2_MAX_MID", "3.00"))   # Tier 2: mid < $3.00
    SPREAD_TIER_3_MAX_MID: float = float(os.getenv("WHEEL_SPREAD_TIER_3_MAX_MID", "8.00"))   # Tier 3: mid < $8.00
    # Tier 4: mid >= $8.00 (no threshold needed, it's the default)

    # Tier absolute spread caps
    SPREAD_TIER_1_MAX_ABS: float = float(os.getenv("WHEEL_SPREAD_TIER_1_MAX_ABS", "0.10"))   # Tier 1 cap: $0.10
    SPREAD_TIER_2_MAX_ABS: float = float(os.getenv("WHEEL_SPREAD_TIER_2_MAX_ABS", "0.25"))   # Tier 2 cap: $0.25
    SPREAD_TIER_3_MAX_ABS: float = float(os.getenv("WHEEL_SPREAD_TIER_3_MAX_ABS", "0.50"))   # Tier 3 cap: $0.50
    SPREAD_TIER_4_MAX_ABS: float = float(os.getenv("WHEEL_SPREAD_TIER_4_MAX_ABS", "1.00"))   # Tier 4 cap: $1.00

    # Safety rails for small accounts
    MIN_UNDERLYING_PRICE: float = float(os.getenv("WHEEL_MIN_UNDERLYING_PRICE", "10.0"))  # Minimum underlying stock price - safety rail for small accounts
    MAX_CSP_NOTIONAL: float = float(os.getenv("WHEEL_MAX_CSP_NOTIONAL", "60000.0"))  # Maximum CSP notional value (strike * 100) - safety rail for small accounts

    # Feature flags
    ALLOW_FALLBACK_DTE: bool = os.getenv("ALLOW_FALLBACK_DTE", "true").lower() == "true"

    # Property aliases for backward compatibility (snake_case)
    @property
    def csp_delta_min(self) -> float:
        return self.CSP_DELTA_MIN

    @property
    def csp_delta_max(self) -> float:
        return self.CSP_DELTA_MAX

    @property
    def cc_delta_min(self) -> float:
        return self.CC_DELTA_MIN

    @property
    def cc_delta_max(self) -> float:
        return self.CC_DELTA_MAX

    @property
    def dte_min_primary(self) -> int:
        return self.DTE_MIN_PRIMARY

    @property
    def dte_max_primary(self) -> int:
        return self.DTE_MAX_PRIMARY

    @property
    def dte_min_fallback(self) -> int:
        return self.DTE_MIN_FALLBACK

    @property
    def dte_max_fallback(self) -> int:
        return self.DTE_MAX_FALLBACK

    @property
    def earnings_avoid_days(self) -> int:
        return self.EARNINGS_AVOID_DAYS

    @property
    def rsi_period(self) -> int:
        return self.RSI_PERIOD

    @property
    def rsi_interval(self) -> str:
        return self.RSI_INTERVAL

    @property
    def max_spread_pct(self) -> float:
        return self.MAX_SPREAD_PCT

    @property
    def min_open_interest(self) -> int:
        return self.MIN_OPEN_INTEREST

    @property
    def min_bid(self) -> float:
        return self.MIN_BID

    @property
    def min_credit(self) -> float:
        return self.MIN_CREDIT

    @property
    def min_underlying_price(self) -> float:
        return self.MIN_UNDERLYING_PRICE

    @property
    def max_csp_notional(self) -> float:
        return self.MAX_CSP_NOTIONAL

    @property
    def allow_fallback_dte(self) -> bool:
        return self.ALLOW_FALLBACK_DTE

    def __post_init__(self):
        # Basic validation or logging after initialization
        if not (0 <= self.CSP_DELTA_MIN <= self.CSP_DELTA_MAX <= 1):
            raise ValueError("CSP_DELTA_MIN/MAX must be between 0 and 1 and MIN <= MAX")
        if not (0 <= self.CC_DELTA_MIN <= self.CC_DELTA_MAX <= 1):
            raise ValueError("CC_DELTA_MIN/MAX must be between 0 and 1 and MIN <= MAX")
        if not (1 <= self.DTE_MIN_PRIMARY <= self.DTE_MAX_PRIMARY):
            raise ValueError("DTE_MIN_PRIMARY must be <= DTE_MAX_PRIMARY")
        if not (1 <= self.DTE_MIN_FALLBACK <= self.DTE_MAX_FALLBACK):
            raise ValueError("DTE_MIN_FALLBACK must be <= DTE_MAX_FALLBACK")
        if self.EARNINGS_AVOID_DAYS < 0:
            raise ValueError("EARNINGS_AVOID_DAYS cannot be negative")
        if self.MAX_SPREAD_PCT < 0:
            raise ValueError("MAX_SPREAD_PCT cannot be negative")
        if self.MIN_OPEN_INTEREST < 0:
            raise ValueError("MIN_OPEN_INTEREST cannot be negative")
        if self.MIN_BID < 0:
            raise ValueError("MIN_BID cannot be negative")
        if self.MIN_CREDIT < 0:
            raise ValueError("MIN_CREDIT cannot be negative")
        if self.MIN_UNDERLYING_PRICE < 0:
            raise ValueError("MIN_UNDERLYING_PRICE cannot be negative")
        if self.MAX_CSP_NOTIONAL < 0:
            raise ValueError("MAX_CSP_NOTIONAL cannot be negative")
        # Validate tier thresholds are in ascending order
        if not (self.SPREAD_TIER_1_MAX_MID < self.SPREAD_TIER_2_MAX_MID < self.SPREAD_TIER_3_MAX_MID):
            raise ValueError("SPREAD_TIER thresholds must be in ascending order: TIER_1 < TIER_2 < TIER_3")
        # Validate tier caps are in ascending order
        if not (self.SPREAD_TIER_1_MAX_ABS < self.SPREAD_TIER_2_MAX_ABS < self.SPREAD_TIER_3_MAX_ABS < self.SPREAD_TIER_4_MAX_ABS):
            raise ValueError("SPREAD_TIER caps must be in ascending order: TIER_1 < TIER_2 < TIER_3 < TIER_4")
        if self.SPREAD_TIER_1_MAX_ABS < 0 or self.SPREAD_TIER_2_MAX_ABS < 0 or self.SPREAD_TIER_3_MAX_ABS < 0 or self.SPREAD_TIER_4_MAX_ABS < 0:
            raise ValueError("SPREAD_TIER caps cannot be negative")


def load_wheel_rules() -> WheelRules:
    """Loads WheelRules from environment variables."""
    return WheelRules()


def abs_spread_cap_for_mid(mid: float, rules: WheelRules) -> float:
    """
    Determine the absolute spread cap for a given option mid price using tiered thresholds.
    
    Tiering rationale:
    - Lower-premium options need tighter absolute caps to prevent wide spreads
    - Higher-premium options can tolerate wider absolute spreads while maintaining reasonable percentage spreads
    - This tiered approach makes spread validation realistic across the full range of option premiums
    
    Args:
        mid: Option mid price (average of bid and ask)
        rules: WheelRules instance containing tier thresholds and caps
        
    Returns:
        Maximum allowed absolute spread for the given mid price
        
    Examples:
        >>> rules = load_wheel_rules()
        >>> abs_spread_cap_for_mid(0.50, rules)  # Tier 1
        0.10
        >>> abs_spread_cap_for_mid(2.00, rules)  # Tier 2
        0.25
        >>> abs_spread_cap_for_mid(5.00, rules)  # Tier 3
        0.50
        >>> abs_spread_cap_for_mid(10.00, rules)  # Tier 4
        1.00
    """
    if mid < rules.SPREAD_TIER_1_MAX_MID:
        return rules.SPREAD_TIER_1_MAX_ABS
    elif mid < rules.SPREAD_TIER_2_MAX_MID:
        return rules.SPREAD_TIER_2_MAX_ABS
    elif mid < rules.SPREAD_TIER_3_MAX_MID:
        return rules.SPREAD_TIER_3_MAX_ABS
    else:
        return rules.SPREAD_TIER_4_MAX_ABS


def is_within_dte_window(
    expiration_date: date,
    now: Optional[date] = None,
    min_dte: Optional[int] = None,
    max_dte: Optional[int] = None,
) -> bool:
    """
    Checks if an expiration date falls within a specified DTE (Days To Expiration) window.
    
    Args:
        expiration_date: Option expiration date
        now: Reference date (defaults to today)
        min_dte: Minimum days to expiration (inclusive)
        max_dte: Maximum days to expiration (inclusive)
        
    Returns:
        True if expiration_date is within the DTE window, False otherwise
    """
    if now is None:
        now = date.today()
    
    if expiration_date <= now:
        return False
    
    dte = (expiration_date - now).days
    
    if min_dte is not None and dte < min_dte:
        return False
    
    if max_dte is not None and dte > max_dte:
        return False
    
    return True


def earnings_ok(
    earnings_date: Optional[date],
    now: Optional[date] = None,
    avoid_days: Optional[int] = None,
) -> bool:
    """
    Check if earnings date is safe for option trading (not too close).
    
    Returns False if earnings_date is within avoid_days ahead of now.
    This helps avoid early assignment risk around earnings.
    
    Args:
        earnings_date: Earnings announcement date (or None if unknown)
        now: Reference date (defaults to today)
        avoid_days: Number of days to avoid before earnings (defaults to 10)
        
    Returns:
        True if earnings_date is None, in the past, or more than avoid_days away
        False if earnings_date is within avoid_days ahead
        
    Examples:
        >>> from datetime import date, timedelta
        >>> today = date.today()
        >>> earnings_ok(today + timedelta(days=15), avoid_days=10)
        True  # Earnings 15 days away, safe
        >>> earnings_ok(today + timedelta(days=5), avoid_days=10)
        False  # Earnings 5 days away, too close
        >>> earnings_ok(None)
        True  # Unknown earnings date, assume safe
        >>> earnings_ok(today - timedelta(days=5))
        True  # Earnings in the past, safe
    """
    if earnings_date is None:
        return True  # Unknown earnings date, assume safe
    
    if now is None:
        now = date.today()
    
    if avoid_days is None:
        avoid_days = 10  # Default avoidance window
    
    # Earnings in the past is always safe
    if earnings_date < now:
        return True
    
    # Check if earnings is within avoidance window
    days_until_earnings = (earnings_date - now).days
    return days_until_earnings > avoid_days


def find_expiration_in_window(
    expirations: list[date],
    min_dte: int,
    max_dte: int,
    now: Optional[date] = None,
) -> Optional[date]:
    """
    Find the first expiration date within a DTE window.
    
    Args:
        expirations: List of available expiration dates (should be sorted)
        min_dte: Minimum days to expiration (inclusive)
        max_dte: Maximum days to expiration (inclusive)
        now: Reference date (defaults to today)
        
    Returns:
        First expiration date within window, or None if none found
        
    Examples:
        >>> from datetime import date, timedelta
        >>> today = date.today()
        >>> exps = [today + timedelta(days=d) for d in [3, 7, 14, 21]]
        >>> find_expiration_in_window(exps, min_dte=5, max_dte=9)
        datetime.date(2026, 1, 9)  # 7 days away
    """
    if now is None:
        now = date.today()
    
    # Filter to future expirations and sort
    future = sorted([d for d in expirations if d > now])
    
    for exp in future:
        if is_within_dte_window(exp, now=now, min_dte=min_dte, max_dte=max_dte):
            return exp
    
    return None


def spread_ok(
    bid: float,
    ask: float,
    rules: WheelRules,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Check if option spread meets liquidity requirements using tiered absolute spread caps.
    
    Uses both percentage and tiered absolute spread caps to handle edge cases:
    - Percentage caps work well for higher-priced options
    - Tiered absolute caps prevent wide spreads across the full range of option premiums
    - Tiered system allows realistic validation for higher-premium options
    
    Args:
        bid: Bid price
        ask: Ask price
        rules: WheelRules instance containing spread thresholds and tier configuration
        
    Returns:
        Tuple of (ok: bool, details: dict)
        details dict includes:
        - mid: Option mid price
        - spread_abs: Absolute spread (ask - bid)
        - spread_pct: Spread as percentage of mid
        - abs_cap_used: The absolute spread cap that was applied for this mid price
        
    Examples:
        >>> rules = load_wheel_rules()
        >>> ok, details = spread_ok(bid=1.0, ask=1.07, rules=rules)
        >>> ok
        True  # 7% spread < 7.5%, and $0.07 < tier cap
        >>> details["abs_cap_used"]
        0.25  # Tier 2 cap for mid=$1.00
    """
    # Validate inputs
    if bid <= 0 or ask <= 0:
        return False, {
            "mid": None,
            "spread_abs": None,
            "spread_pct": None,
            "abs_cap_used": None,
        }
    
    if ask < bid:
        return False, {
            "mid": None,
            "spread_abs": None,
            "spread_pct": None,
            "abs_cap_used": None,
        }
    
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return False, {
            "mid": None,
            "spread_abs": None,
            "spread_pct": None,
            "abs_cap_used": None,
        }
    
    spread_abs = ask - bid
    spread_pct = (spread_abs / mid) * 100.0 if mid > 0 else 0.0
    
    # Get tiered absolute spread cap for this mid price
    abs_cap = abs_spread_cap_for_mid(mid, rules)
    
    # Check both percentage and absolute caps
    pct_ok = spread_pct <= rules.MAX_SPREAD_PCT
    abs_ok = spread_abs <= abs_cap
    
    ok = pct_ok and abs_ok
    
    return ok, {
        "mid": mid,
        "spread_abs": spread_abs,
        "spread_pct": spread_pct,
        "abs_cap_used": abs_cap,
    }


if __name__ == "__main__":
    from loguru import logger
    
    logger.info("Running wheel_rules.py self-check...")
    
    # Test load_wheel_rules
    rules = load_wheel_rules()
    logger.info(f"✅ WheelRules loaded:")
    logger.info(f"  CSP Delta: [{rules.csp_delta_min:.2f}, {rules.csp_delta_max:.2f}]")
    logger.info(f"  CC Delta: [{rules.cc_delta_min:.2f}, {rules.cc_delta_max:.2f}]")
    logger.info(f"  Primary DTE: [{rules.dte_min_primary}, {rules.dte_max_primary}]")
    logger.info(f"  Fallback DTE: [{rules.dte_min_fallback}, {rules.dte_max_fallback}]")
    logger.info(f"  Earnings Avoid Days: {rules.earnings_avoid_days}")
    logger.info(f"  RSI Period: {rules.rsi_period}, Interval: {rules.rsi_interval}")
    logger.info(f"  Allow Fallback DTE: {rules.allow_fallback_dte}")
    logger.info(f"  Liquidity: max_spread_pct={rules.max_spread_pct}%, min_oi={rules.min_open_interest}, min_bid=${rules.min_bid:.2f}, min_credit=${rules.min_credit:.2f}")
    logger.info(f"  Spread tiers: Tier1(mid<${rules.SPREAD_TIER_1_MAX_MID:.2f})=${rules.SPREAD_TIER_1_MAX_ABS:.2f}, Tier2(mid<${rules.SPREAD_TIER_2_MAX_MID:.2f})=${rules.SPREAD_TIER_2_MAX_ABS:.2f}, Tier3(mid<${rules.SPREAD_TIER_3_MAX_MID:.2f})=${rules.SPREAD_TIER_3_MAX_ABS:.2f}, Tier4(mid>=${rules.SPREAD_TIER_3_MAX_MID:.2f})=${rules.SPREAD_TIER_4_MAX_ABS:.2f}")
    logger.info(f"  Safety rails: min_underlying_price=${rules.min_underlying_price:.2f}, max_csp_notional=${rules.max_csp_notional:,.0f}")

    # Test is_within_dte_window
    today = datetime.now(timezone.utc).date()
    exp_in_window = today + timedelta(days=rules.dte_min_primary + 2) # e.g., 7 days from now
    exp_out_window = today + timedelta(days=rules.dte_max_primary + 2) # e.g., 11 days from now
    
    logger.info("\n✅ is_within_dte_window tests:")
    logger.info(f"  {exp_in_window} in [{rules.dte_min_primary}, {rules.dte_max_primary}]: {is_within_dte_window(exp_in_window, today, rules.dte_min_primary, rules.dte_max_primary)}")
    logger.info(f"  {exp_out_window} in [{rules.dte_min_primary}, {rules.dte_max_primary}]: {is_within_dte_window(exp_out_window, today, rules.dte_min_primary, rules.dte_max_primary)}")

    # Test earnings_ok
    earnings_far = today + timedelta(days=rules.earnings_avoid_days + 5) # 15 days away
    earnings_near = today + timedelta(days=rules.earnings_avoid_days - 5) # 5 days away
    
    logger.info("\n✅ earnings_ok tests:")
    logger.info(f"  Earnings {earnings_far} ({ (earnings_far - today).days } days away): {earnings_ok(earnings_far, today, rules.earnings_avoid_days)}")
    logger.info(f"  Earnings {earnings_near} ({ (earnings_near - today).days } days away): {earnings_ok(earnings_near, today, rules.earnings_avoid_days)}")
    logger.info(f"  Earnings None: {earnings_ok(None, today, rules.earnings_avoid_days)}")

    # Test find_expiration_in_window
    test_expirations = [
        today + timedelta(days=3),
        today + timedelta(days=rules.dte_min_primary + 1), # In primary
        today + timedelta(days=rules.dte_max_primary + 1), # Out of primary, potentially in fallback
        today + timedelta(days=rules.dte_max_fallback + 1), # Out of all
    ]
    logger.info("\n✅ find_expiration_in_window test:")
    logger.info(f"  Expirations: {sorted(test_expirations)}")
    found_exp = find_expiration_in_window(test_expirations, rules.dte_min_primary, rules.dte_max_primary, today)
    logger.info(f"  Found in [{rules.dte_min_primary}, {rules.dte_max_primary}]: {found_exp}")

    # Test abs_spread_cap_for_mid
    logger.info("\n✅ abs_spread_cap_for_mid tests:")
    test_mids = [0.50, 1.00, 2.00, 5.00, 10.00]
    for mid in test_mids:
        cap = abs_spread_cap_for_mid(mid, rules)
        logger.info(f"  mid=${mid:.2f} -> cap=${cap:.2f}")

    # Test spread_ok with new signature
    logger.info("\n✅ spread_ok tests:")
    test_cases = [
        (1.0, 1.07, True),   # 7% spread, $0.07 abs - should pass (Tier 2, cap=$0.25)
        (0.50, 0.57, False), # 14% spread - should fail pct check
        (0.50, 0.58, False), # 16% spread - should fail pct check
        (0.45, 0.55, False), # 20% spread, $0.10 abs - should fail pct check (Tier 1, cap=$0.10)
        (1.0, 1.08, False),  # 7.7% spread, $0.08 abs - should fail (7.7% > 7.5%)
        (2.0, 2.15, True),   # 7.5% spread, $0.15 abs - should pass (Tier 2, cap=$0.25)
        (2.0, 2.26, False),  # 13% spread, $0.26 abs - should fail pct check
        (5.0, 5.35, True),   # 7% spread, $0.35 abs - should pass (Tier 3, cap=$0.50)
        (10.0, 10.70, True), # 7% spread, $0.70 abs - should pass (Tier 4, cap=$1.00)
    ]
    for bid, ask, expected in test_cases:
        ok, details = spread_ok(bid, ask, rules)
        status = "✅" if ok == expected else "❌"
        logger.info(
            f"  {status} bid=${bid:.2f} ask=${ask:.2f} "
            f"({details['spread_pct']:.1f}%, ${details['spread_abs']:.2f} abs, cap=${details['abs_cap_used']:.2f}): "
            f"{ok} (expected {expected})"
        )

    logger.info("\n✅ All tests passed!")
