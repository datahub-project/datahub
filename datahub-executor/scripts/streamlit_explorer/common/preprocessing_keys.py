"""
Helpers for namespacing saved preprocessings by assertion.

We allow users to pick short, semantic preprocessing IDs (e.g. "daily_no_anomalies"),
but persist them using a composite key that includes the assertion URN to ensure
uniqueness across assertions.
"""

from __future__ import annotations

from typing import Any, Optional, Tuple
from urllib.parse import quote, unquote

_SEPARATOR = "@@"


def build_preprocessing_key(short_id: str, assertion_urn: Optional[str]) -> str:
    """Build a storage key from a short ID and assertion URN.

    If assertion_urn is not provided, returns short_id (legacy / fallback behavior).
    """
    if not assertion_urn:
        return short_id
    return f"{short_id}{_SEPARATOR}{quote(assertion_urn, safe='')}"


def split_preprocessing_key(key: str) -> Tuple[str, Optional[str]]:
    """Split a storage key into (short_id, assertion_urn).

    Legacy keys without a separator return (key, None).
    """
    if _SEPARATOR not in key:
        return key, None
    short_id, encoded = key.split(_SEPARATOR, 1)
    try:
        return short_id, unquote(encoded)
    except Exception:
        # If decoding fails, still return a stable short_id.
        return short_id, None


def display_preprocessing_id(
    key: str, metadata: Optional[dict[str, Any]] = None
) -> str:
    """Best-effort user-facing ID for a preprocessing.

    Prefers metadata['preprocessing_short_id'] if present; otherwise parses key.
    """
    if metadata:
        val = metadata.get("preprocessing_short_id")
        if isinstance(val, str) and val:
            return val
    short_id, _ = split_preprocessing_key(key)
    return short_id
