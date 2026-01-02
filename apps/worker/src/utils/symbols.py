"""
Symbol normalization utilities for API integrations.
"""
from typing import Any


def normalize_for_fmp(symbol: str) -> str:
    """
    Normalize stock symbol for FMP API requests.
    
    Converts common class share forms:
    - "BRK.B" -> "BRK-B"
    - "BF.B"  -> "BF-B"
    
    Args:
        symbol: Stock symbol (e.g., "BRK.B", "AAPL")
        
    Returns:
        Normalized symbol (e.g., "BRK-B", "AAPL")
    """
    if not symbol:
        return symbol
    
    # Convert dot notation to hyphen for class shares
    # Common patterns: "BRK.B" -> "BRK-B", "BF.B" -> "BF-B"
    if "." in symbol:
        symbol = symbol.replace(".", "-")
    
    return symbol


def normalize_for_display(symbol: str) -> str:
    """
    Normalize symbol for display (returns original input).
    
    This is a placeholder function that maintains the original symbol format
    for display purposes. Currently just returns the input as-is.
    
    Args:
        symbol: Stock symbol
        
    Returns:
        Original symbol (unchanged)
    """
    return symbol

