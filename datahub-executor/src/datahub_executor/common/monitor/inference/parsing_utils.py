"""
Parsing utilities for monitor inference.

This module provides type-safe parsing helpers for extracting typed values
from string maps (like DataHub's map[string, string] schema fields).
"""

import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def parse_int(params: Dict[str, str], key: str) -> Optional[int]:
    """
    Parse an integer from a string map with graceful fallback.

    Args:
        params: Dictionary with string values
        key: Key to look up

    Returns:
        Parsed integer, or None if key is missing or value is invalid
    """
    val = params.get(key)
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        logger.warning(f"Invalid integer for {key}: {val!r}")
        return None


def parse_float(params: Dict[str, str], key: str) -> Optional[float]:
    """
    Parse a float from a string map with graceful fallback.

    Args:
        params: Dictionary with string values
        key: Key to look up

    Returns:
        Parsed float, or None if key is missing or value is invalid
    """
    val = params.get(key)
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        logger.warning(f"Invalid float for {key}: {val!r}")
        return None
