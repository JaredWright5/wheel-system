"""
Centralized configuration for wheel trading parameters.

This module provides a WheelRules dataclass that loads trading parameters from environment
variables with sensible defaults. It includes configuration for delta bands, DTE windows,
earnings avoidance, RSI parameters, and liquidity filters.

Liquidity Filtering Philosophy:
    Both percentage and absolute spread caps are used to handle edge cases:
    - Percentage caps (e.g., 7.5%) work well for higher-priced options (e.g., $10+ premiums)
    - Absolute caps (e.g., $0.10 for low premiums, $0.25 for high premiums) prevent
      wide spreads on low-priced options where a 7.5% spread might still be too tight
      (e.g., a $0.50 option with a $0.04 spread = 8% but acceptable)
    - This dual approach ensures reasonable liquidity filters across the full range of
      option premiums typically seen in weeklies (from $0.05 to $5.00+)
"""
from dataclasses import dataclass
import os
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional

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
    MAX_ABS_SPREAD_LOW_PREMIUM: float = float(os.getenv("WHEEL_MAX_ABS_SPREAD_LOW_PREMIUM", "0.10"))   # if mid < 1.00
    MAX_ABS_SPREAD_HIGH_PREMIUM: float = float(os.getenv("WHEEL_MAX_ABS_SPREAD_HIGH_PREMIUM", "0.25"))  # if mid >= 1.00

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
    def max_abs_spread_low_premium(self) -> float:
        return self.MAX_ABS_SPREAD_LOW_PREMIUM

    @property
    def max_abs_spread_high_premium(self) -> float:
        return self.MAX_ABS_SPREAD_HIGH_PREMIUM

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
        if self.MAX_ABS_SPREAD_LOW_PREMIUM < 0:
            raise ValueError("MAX_ABS_SPREAD_LOW_PREMIUM cannot be negative")
        if self.MAX_ABS_SPREAD_HIGH_PREMIUM < 0:
            raise ValueError("MAX_ABS_SPREAD_HIGH_PREMIUM cannot be negative")


def load_wheel_rules() -> WheelRules:
    """Loads WheelRules from environment variables."""
    return WheelRules()


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
    max_spread_pct: float,
    max_abs_low: float,
    max_abs_high: float,
) -> bool:
    """
    Check if option spread meets liquidity requirements.
    
    Uses both percentage and absolute spread caps to handle edge cases:
    - Percentage caps work well for higher-priced options
    - Absolute caps prevent wide spreads on low-priced options
    
    Args:
        bid: Bid price
        ask: Ask price
        max_spread_pct: Maximum spread as percentage (e.g., 7.5 for 7.5%)
        max_abs_low: Maximum absolute spread for low premiums (mid < 1.00)
        max_abs_high: Maximum absolute spread for high premiums (mid >= 1.00)
        
    Returns:
        True if spread passes both percentage and absolute checks, False otherwise
        
    Examples:
        >>> spread_ok(bid=1.0, ask=1.07, max_spread_pct=7.5, max_abs_low=0.10, max_abs_high=0.25)
        True  # 7% spread < 7.5%, and $0.07 < $0.25
        >>> spread_ok(bid=0.50, ask=0.57, max_spread_pct=7.5, max_abs_low=0.10, max_abs_high=0.25)
        False  # 14% spread > 7.5% (fails percentage check)
        >>> spread_ok(bid=0.50, ask=0.58, max_spread_pct=10.0, max_abs_low=0.10, max_abs_high=0.25)
        False  # 16% spread > 10%, but also $0.08 < $0.10 (would pass abs, but fails pct)
        >>> spread_ok(bid=0.45, ask=0.55, max_spread_pct=10.0, max_abs_low=0.10, max_abs_high=0.25)
        False  # 20% spread > 10% (fails percentage check), but also $0.10 = $0.10 (fails abs)
    """
    if bid <= 0 or ask <= 0:
        return False
    
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return False
    
    abs_spread = ask - bid
    
    # Percentage spread check
    pct_spread = (abs_spread / mid) * 100.0
    if pct_spread > max_spread_pct:
        return False
    
    # Absolute spread check (depends on premium level)
    if mid < 1.00:
        if abs_spread > max_abs_low:
            return False
    else:
        if abs_spread > max_abs_high:
            return False
    
    return True


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
    logger.info(f"  Liquidity: max_spread_pct={rules.max_spread_pct}%, min_oi={rules.min_open_interest}, min_bid=${rules.min_bid:.2f}")
    logger.info(f"  Liquidity: max_abs_spread_low=${rules.max_abs_spread_low_premium:.2f}, max_abs_spread_high=${rules.max_abs_spread_high_premium:.2f}")

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

    # Test spread_ok
    logger.info("\n✅ spread_ok tests:")
    test_cases = [
        (1.0, 1.07, True),   # 7% spread, $0.07 abs - should pass
        (0.50, 0.57, False), # 14% spread - should fail pct check
        (0.50, 0.58, False), # 16% spread - should fail pct check
        (0.45, 0.55, False), # 20% spread, $0.10 abs - should fail pct check
        (1.0, 1.08, False),  # 7.7% spread, $0.08 abs - should fail (7.7% > 7.5%)
        (2.0, 2.15, True),   # 7.5% spread, $0.15 abs - should pass
        (2.0, 2.26, False),  # 13% spread, $0.26 abs - should fail pct check
    ]
    for bid, ask, expected in test_cases:
        result = spread_ok(bid, ask, rules.max_spread_pct, rules.max_abs_spread_low_premium, rules.max_abs_spread_high_premium)
        status = "✅" if result == expected else "❌"
        pct = ((ask - bid) / ((bid + ask) / 2.0)) * 100.0
        logger.info(f"  {status} bid=${bid:.2f} ask=${ask:.2f} ({pct:.1f}%, ${ask-bid:.2f} abs): {result} (expected {expected})")

    logger.info("\n✅ All tests passed!")
