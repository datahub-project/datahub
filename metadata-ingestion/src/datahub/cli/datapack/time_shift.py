"""Time-shift utility for rebasing temporal metadata in data pack files.

All DataHub timestamps are epoch milliseconds UTC. When loading a data pack
captured in the past, this module shifts temporal fields so the pack appears
"fresh" relative to the current time (or a user-specified anchor).
"""

import json
import logging
import pathlib
import tempfile
import time
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)

# Fields that are always epoch millis and should be shifted
TIMESTAMP_FIELDS: Set[str] = {
    "lastObserved",
    "timestampMillis",
    "lastUpdatedTimestamp",
}

# Fields that are epoch millis only when they appear alongside "actor" (AuditStamp pattern)
AUDIT_STAMP_TIME_FIELD = "time"
AUDIT_STAMP_MARKER_FIELD = "actor"


def _shift_value(value: int, delta_ms: int) -> int:
    """Shift a timestamp value, clamping to non-negative."""
    shifted = value + delta_ms
    return max(0, shifted)


def _walk_and_shift(
    obj: object,
    delta_ms: int,
    extra_fields: Optional[Set[str]] = None,
) -> object:
    """Recursively walk a JSON-like structure and shift timestamp fields.

    Args:
        obj: JSON-deserialized object (dict, list, or scalar)
        delta_ms: Milliseconds to add to each timestamp
        extra_fields: Additional field names to treat as timestamps
    """
    shift_fields = TIMESTAMP_FIELDS | (extra_fields or set())

    if isinstance(obj, dict):
        result: Dict[str, object] = {}
        # Detect AuditStamp pattern: dict with both "time" and "actor" keys
        is_audit_stamp = (
            AUDIT_STAMP_TIME_FIELD in obj and AUDIT_STAMP_MARKER_FIELD in obj
        )

        for key, value in obj.items():
            if (
                key in shift_fields
                and isinstance(value, (int, float))
                or (
                    is_audit_stamp
                    and key == AUDIT_STAMP_TIME_FIELD
                    and isinstance(value, (int, float))
                )
            ):
                result[key] = _shift_value(int(value), delta_ms)
            else:
                result[key] = _walk_and_shift(value, delta_ms, extra_fields)
        return result
    elif isinstance(obj, list):
        return [_walk_and_shift(item, delta_ms, extra_fields) for item in obj]
    else:
        return obj


def time_shift_file(
    input_path: pathlib.Path,
    reference_timestamp: int,
    target_timestamp: Optional[int] = None,
    extra_fields: Optional[List[str]] = None,
) -> pathlib.Path:
    """Create a time-shifted copy of a data pack file.

    Args:
        input_path: Path to the original MCP/MCE JSON file.
        reference_timestamp: Epoch millis when the pack was captured (the anchor).
        target_timestamp: Epoch millis to shift to. Defaults to current time.
        extra_fields: Additional field names to treat as timestamps.

    Returns:
        Path to a new temporary file with shifted timestamps.
    """
    if target_timestamp is None:
        target_timestamp = int(time.time() * 1000)

    delta_ms = target_timestamp - reference_timestamp

    if delta_ms == 0:
        logger.debug("No time shift needed (delta=0)")
        return input_path

    logger.info(
        "Time-shifting data pack by %+.1f days (delta=%d ms)",
        delta_ms / (1000 * 60 * 60 * 24),
        delta_ms,
    )

    with open(input_path) as f:
        data = json.load(f)

    extra_set = set(extra_fields) if extra_fields else None
    shifted_data = _walk_and_shift(data, delta_ms, extra_set)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, prefix="datapack-shifted-"
    ) as tmp:
        json.dump(shifted_data, tmp)
        return pathlib.Path(tmp.name)
