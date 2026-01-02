"""
Wheel Trading Rules Configuration

Centralizes all wheel strategy parameters used across workers (screener + pick builders).
Uses environment variables with sensible defaults.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional


@dataclass
class WheelRules:
    """
    Centralized wheel trading rules configuration.
    
    All parameters can be overridden via environment variables.
    Defaults are optimized for weekly options trading.
    """
    # CSP (Cash-Secured Put) delta range
    csp_delta_min: float = 0.20
    csp_delta_max: float = 0.30
    
    # CC (Covered Call) delta range
    cc_delta_min: float = 0.20
    cc_delta_max: float = 0.30
    
    # Primary DTE (Days To Expiration) window for weekly options
    dte_min_primary: int = 5
    dte_max_primary: int = 9
    
    # Fallback DTE window (used if primary window has no options)
    dte_min_fallback: int = 10
    dte_max_fallback: int = 16
    
    # Earnings avoidance: skip options if earnings within N days
    earnings_avoid_days: int = 10
    
    # RSI (Relative Strength Index) parameters
    rsi_period: int = 14
    rsi_interval: str = "1day"
    
    # Whether to allow fallback DTE window if primary has no options
    allow_fallback_dte: bool = True
    
    @classmethod
    def from_env(cls) -> WheelRules:
        """
        Load WheelRules from environment variables with defaults.
        
        Environment variables (all optional):
        - CSP_DELTA_MIN, CSP_DELTA_MAX
        - CC_DELTA_MIN, CC_DELTA_MAX
        - DTE_MIN_PRIMARY, DTE_MAX_PRIMARY
        - DTE_MIN_FALLBACK, DTE_MAX_FALLBACK
        - EARNINGS_AVOID_DAYS
        - RSI_PERIOD, RSI_INTERVAL
        - ALLOW_FALLBACK_DTE (true/false)
        
        Returns:
            WheelRules instance with values from env or defaults
        """
        def _float_env(key: str, default: float) -> float:
            val = os.getenv(key)
            return float(val) if val else default
        
        def _int_env(key: str, default: int) -> int:
            val = os.getenv(key)
            return int(val) if val else default
        
        def _str_env(key: str, default: str) -> str:
            return os.getenv(key, default)
        
        def _bool_env(key: str, default: bool) -> bool:
            val = os.getenv(key, "").lower()
            if val in ("true", "1", "yes", "on"):
                return True
            elif val in ("false", "0", "no", "off"):
                return False
            return default
        
        return cls(
            csp_delta_min=_float_env("CSP_DELTA_MIN", 0.20),
            csp_delta_max=_float_env("CSP_DELTA_MAX", 0.30),
            cc_delta_min=_float_env("CC_DELTA_MIN", 0.20),
            cc_delta_max=_float_env("CC_DELTA_MAX", 0.30),
            dte_min_primary=_int_env("DTE_MIN_PRIMARY", 5),
            dte_max_primary=_int_env("DTE_MAX_PRIMARY", 9),
            dte_min_fallback=_int_env("DTE_MIN_FALLBACK", 10),
            dte_max_fallback=_int_env("DTE_MAX_FALLBACK", 16),
            earnings_avoid_days=_int_env("EARNINGS_AVOID_DAYS", 10),
            rsi_period=_int_env("RSI_PERIOD", 14),
            rsi_interval=_str_env("RSI_INTERVAL", "1day"),
            allow_fallback_dte=_bool_env("ALLOW_FALLBACK_DTE", True),
        )


def load_wheel_rules() -> WheelRules:
    """
    Load wheel rules from environment variables.
    
    Returns:
        WheelRules instance configured from environment or defaults
    """
    return WheelRules.from_env()


def is_within_dte_window(
    expiration_date: date,
    now: Optional[date] = None,
    min_dte: Optional[int] = None,
    max_dte: Optional[int] = None,
) -> bool:
    """
    Check if an expiration date falls within a DTE (Days To Expiration) window.
    
    Args:
        expiration_date: Option expiration date
        now: Reference date (defaults to today in UTC)
        min_dte: Minimum days to expiration (inclusive)
        max_dte: Maximum days to expiration (inclusive)
        
    Returns:
        True if expiration_date is within [min_dte, max_dte] days from now
        
    Examples:
        >>> from datetime import date, timedelta
        >>> exp = date.today() + timedelta(days=7)
        >>> is_within_dte_window(exp, min_dte=5, max_dte=9)
        True
        >>> is_within_dte_window(exp, min_dte=10, max_dte=14)
        False
    """
    if now is None:
        now = date.today()
    
    if expiration_date <= now:
        return False  # Expired or expiring today
    
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

