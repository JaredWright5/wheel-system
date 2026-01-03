"""
Symbol normalization utilities for cross-provider matching.

Provides functions to normalize equity symbols and convert between different
provider formats (FMP API, universe CSV, etc.).
"""
from typing import Dict


# Mapping of known tickers that require special handling for class shares
# Format: {fmp_format: universe_format}
_CLASS_SHARE_MAPPING: Dict[str, str] = {
    "BRK-B": "BRK.B",
    "BF-B": "BF.B",
    # Add more as needed
}

# Reverse mapping for universe -> FMP
_FMP_MAPPING: Dict[str, str] = {v: k for k, v in _CLASS_SHARE_MAPPING.items()}


def normalize_equity_symbol(symbol: str) -> str:
    """
    Normalize an equity symbol to a canonical form.
    
    This function creates a standard representation of a stock symbol that can be
    used for matching across different providers. The canonical form uses dot notation
    for class shares (e.g., "BRK.B").
    
    Steps:
    1. Trim whitespace
    2. Convert to uppercase
    3. Convert class-share separators to canonical form (BRK-B -> BRK.B)
    4. Convert "/" to "." (handles edge cases)
    5. Leave other characters as-is
    
    Args:
        symbol: Stock symbol in any format (e.g., "BRK-B", "BRK.B", "brk.b")
        
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
    
    # Trim whitespace and uppercase
    normalized = symbol.strip().upper()
    
    # Convert "/" to "." (handles edge cases)
    normalized = normalized.replace("/", ".")
    
    # Convert known class-share formats to canonical form (hyphen -> dot)
    # Check if it matches any FMP format in our mapping
    if normalized in _CLASS_SHARE_MAPPING:
        return _CLASS_SHARE_MAPPING[normalized]
    
    # If it's already in universe format, return as-is
    if normalized in _FMP_MAPPING:
        return normalized
    
    # For unknown patterns, check if it has a hyphen that might be a class share
    # We'll be conservative and only convert known patterns
    # This leaves other symbols unchanged
    
    return normalized


def to_fmp_symbol(symbol: str) -> str:
    """
    Convert a canonical symbol to FMP API format.
    
    FMP API expects class shares with hyphens (e.g., "BRK-B" instead of "BRK.B").
    This function converts canonical symbols to the format expected by FMP.
    
    Args:
        symbol: Canonical symbol (e.g., "BRK.B", "AAPL")
        
    Returns:
        FMP-formatted symbol (e.g., "BRK-B", "AAPL")
        
    Examples:
        >>> to_fmp_symbol("BRK.B")
        'BRK-B'
        >>> to_fmp_symbol("AAPL")
        'AAPL'
        >>> to_fmp_symbol("BF.B")
        'BF-B'
    """
    if not symbol:
        return symbol
    
    # Normalize first to ensure we're working with canonical form
    canonical = normalize_equity_symbol(symbol)
    
    # Convert to FMP format if we have a mapping
    return _FMP_MAPPING.get(canonical, canonical)


def to_universe_symbol(symbol: str) -> str:
    """
    Convert a symbol to universe format (canonical form with dot notation).
    
    Universe symbols (e.g., from CSV files) use dot notation for class shares
    (e.g., "BRK.B"). This function converts FMP format or other formats to
    the canonical universe format.
    
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
    """
    if not symbol:
        return symbol
    
    # Normalize to canonical form (which uses dot notation)
    return normalize_equity_symbol(symbol)


if __name__ == "__main__":
    # Self-test
    from loguru import logger
    
    logger.info("Testing normalize_equity_symbol:")
    test_cases_normalize = [
        ("BRK-B", "BRK.B"),
        ("BRK.B", "BRK.B"),
        ("brk-b", "BRK.B"),
        ("brk.b", "BRK.B"),
        ("  BRK-B  ", "BRK.B"),
        ("BF-B", "BF.B"),
        ("BF.B", "BF.B"),
        ("AAPL", "AAPL"),
        ("  aapl  ", "AAPL"),
        ("MSFT", "MSFT"),
        ("", ""),
    ]
    for original, expected in test_cases_normalize:
        result = normalize_equity_symbol(original)
        status = "✅" if result == expected else "❌"
        logger.info(f"  {status} '{original}' -> '{result}' (expected '{expected}')")
        assert result == expected, f"Failed: '{original}' -> '{result}' (expected '{expected}')"
    
    logger.info("\nTesting to_fmp_symbol:")
    test_cases_fmp = [
        ("BRK.B", "BRK-B"),
        ("BRK-B", "BRK-B"),  # Already in FMP format, normalize first
        ("BF.B", "BF-B"),
        ("AAPL", "AAPL"),
        ("", ""),
    ]
    for original, expected in test_cases_fmp:
        result = to_fmp_symbol(original)
        status = "✅" if result == expected else "❌"
        logger.info(f"  {status} '{original}' -> '{result}' (expected '{expected}')")
        assert result == expected, f"Failed: '{original}' -> '{result}' (expected '{expected}')"
    
    logger.info("\nTesting to_universe_symbol:")
    test_cases_universe = [
        ("BRK-B", "BRK.B"),
        ("BRK.B", "BRK.B"),
        ("BF-B", "BF.B"),
        ("BF.B", "BF.B"),
        ("AAPL", "AAPL"),
        ("", ""),
    ]
    for original, expected in test_cases_universe:
        result = to_universe_symbol(original)
        status = "✅" if result == expected else "❌"
        logger.info(f"  {status} '{original}' -> '{result}' (expected '{expected}')")
        assert result == expected, f"Failed: '{original}' -> '{result}' (expected '{expected}')"
    
    logger.info("\n✅ All tests passed")
