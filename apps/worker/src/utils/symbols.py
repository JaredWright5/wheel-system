"""
Symbol normalization utilities for cross-provider matching.

Provides functions to normalize equity symbols and convert between different
provider formats (FMP API, universe CSV, etc.).

Canonical form uses dot notation for class shares (e.g., "BRK.B", "BF.B").
"""
from typing import Set


# Known class share tickers that require special handling
_CLASS_SHARE_TICKERS: Set[str] = {"BRK", "BF"}


def normalize_equity_symbol(symbol: str) -> str:
    """
    Normalize an equity symbol to canonical form.
    
    Canonical form uses dot notation for class shares (e.g., "BRK.B", "BF.B").
    
    Steps:
    1. Strip whitespace and convert to uppercase
    2. Replace "-" class share separator with "." ONLY for known class share tickers
       (BRK-B -> BRK.B, BF-B -> BF.B)
    3. Replace "/" with "." (handles rare edge cases)
    4. Return result
    
    Args:
        symbol: Stock symbol in any format (e.g., "BRK-B", "BRK.B", "brk-b")
        
    Returns:
        Canonical symbol (e.g., "BRK.B", "AAPL")
        
    Examples:
        >>> normalize_equity_symbol("BRK-B")
        'BRK.B'
        >>> normalize_equity_symbol("BRK.B")
        'BRK.B'
        >>> normalize_equity_symbol("  aapl  ")
        'AAPL'
        >>> normalize_equity_symbol("BF-B")
        'BF.B'
    """
    if not symbol:
        return symbol
    
    # Strip and uppercase
    normalized = symbol.strip().upper()
    
    # Replace "/" with "." (rare edge cases)
    normalized = normalized.replace("/", ".")
    
    # Replace "-" with "." ONLY for known class share tickers
    if "-" in normalized:
        ticker_base = normalized.split("-")[0]
        if ticker_base in _CLASS_SHARE_TICKERS:
            normalized = normalized.replace("-", ".")
    
    return normalized


def to_fmp_symbol(symbol: str) -> str:
    """
    Convert a symbol to FMP API format.
    
    FMP API expects class shares with hyphens (e.g., "BRK-B" instead of "BRK.B").
    
    Steps:
    1. Normalize to canonical form first
    2. For known class shares, convert "." to "-" (BRK.B -> BRK-B, BF.B -> BF-B)
    3. Otherwise return normalized symbol
    
    Args:
        symbol: Symbol in any format (e.g., "BRK.B", "BRK-B", "AAPL")
        
    Returns:
        FMP-formatted symbol (e.g., "BRK-B", "AAPL")
        
    Examples:
        >>> to_fmp_symbol("BRK.B")
        'BRK-B'
        >>> to_fmp_symbol("BRK-B")
        'BRK-B'
        >>> to_fmp_symbol("AAPL")
        'AAPL'
        >>> to_fmp_symbol("BF.B")
        'BF-B'
    """
    if not symbol:
        return symbol
    
    # Normalize first to ensure canonical form
    normalized = normalize_equity_symbol(symbol)
    
    # Convert "." to "-" for known class share tickers
    if "." in normalized:
        ticker_base = normalized.split(".")[0]
        if ticker_base in _CLASS_SHARE_TICKERS:
            normalized = normalized.replace(".", "-")
    
    return normalized


def to_universe_symbol(symbol: str) -> str:
    """
    Convert a symbol to universe format (canonical form with dot notation).
    
    Universe symbols use dot notation for class shares (e.g., "BRK.B").
    This function converts any format to the canonical universe format.
    
    Steps:
    1. Uppercase and strip
    2. For known class shares, convert "-" to "." (BRK-B -> BRK.B, BF-B -> BF.B)
    3. Otherwise return symbol
    
    Args:
        symbol: Symbol in any format (e.g., "BRK-B", "BRK.B", "AAPL")
        
    Returns:
        Universe-formatted symbol (canonical form, e.g., "BRK.B", "AAPL")
        
    Examples:
        >>> to_universe_symbol("BRK-B")
        'BRK.B'
        >>> to_universe_symbol("BRK.B")
        'BRK.B'
        >>> to_universe_symbol("AAPL")
        'AAPL'
        >>> to_universe_symbol("BF-B")
        'BF.B'
    """
    if not symbol:
        return symbol
    
    # Uppercase and strip
    normalized = symbol.strip().upper()
    
    # Convert "-" to "." for known class share tickers
    if "-" in normalized:
        ticker_base = normalized.split("-")[0]
        if ticker_base in _CLASS_SHARE_TICKERS:
            normalized = normalized.replace("-", ".")
    
    return normalized


if __name__ == "__main__":
    # Self-test with assertions
    print("Testing normalize_equity_symbol:")
    assert normalize_equity_symbol("BRK-B") == "BRK.B"
    assert normalize_equity_symbol("BRK.B") == "BRK.B"
    assert normalize_equity_symbol("brk-b") == "BRK.B"
    assert normalize_equity_symbol("brk.b") == "BRK.B"
    assert normalize_equity_symbol("  BRK-B  ") == "BRK.B"
    assert normalize_equity_symbol("BF-B") == "BF.B"
    assert normalize_equity_symbol("BF.B") == "BF.B"
    assert normalize_equity_symbol("AAPL") == "AAPL"
    assert normalize_equity_symbol("  aapl  ") == "AAPL"
    assert normalize_equity_symbol("MSFT") == "MSFT"
    assert normalize_equity_symbol("") == ""
    # Test that unknown tickers with hyphens are NOT converted
    assert normalize_equity_symbol("TEST-TICKER") == "TEST-TICKER"
    print("  ✅ All normalize_equity_symbol tests passed")
    
    print("\nTesting to_fmp_symbol:")
    assert to_fmp_symbol("BRK.B") == "BRK-B"
    assert to_fmp_symbol("BRK-B") == "BRK-B"  # Already normalized
    assert to_fmp_symbol("bf.b") == "BF-B"
    assert to_fmp_symbol("BF-B") == "BF-B"
    assert to_fmp_symbol("AAPL") == "AAPL"
    assert to_fmp_symbol("") == ""
    # Test that unknown tickers with dots are NOT converted
    assert to_fmp_symbol("TEST.TICKER") == "TEST.TICKER"
    print("  ✅ All to_fmp_symbol tests passed")
    
    print("\nTesting to_universe_symbol:")
    assert to_universe_symbol("BRK-B") == "BRK.B"
    assert to_universe_symbol("BRK.B") == "BRK.B"
    assert to_universe_symbol("bf-b") == "BF.B"
    assert to_universe_symbol("BF.B") == "BF.B"
    assert to_universe_symbol("AAPL") == "AAPL"
    assert to_universe_symbol("") == ""
    # Test that unknown tickers with hyphens are NOT converted
    assert to_universe_symbol("TEST-TICKER") == "TEST-TICKER"
    print("  ✅ All to_universe_symbol tests passed")
    
    print("\n✅ All tests passed")
