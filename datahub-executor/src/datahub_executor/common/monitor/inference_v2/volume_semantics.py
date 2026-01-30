"""
Helpers for interpreting volume time series semantics in inference_v2.

The key decision for volume preprocessing is whether the input values represent:
- cumulative totals (running total / absolute row count), or
- per-window changes (deltas).

We keep this logic in inference_v2 so other callers (e.g. Streamlit tooling)
can pass through model fields and avoid duplicating mapping logic.
"""

from __future__ import annotations

from typing import Optional, Tuple, Union

from datahub_executor.common.types import VolumeAssertionType


def resolve_volume_series_semantics(
    volume_assertion_type: Optional[Union[VolumeAssertionType, str]],
) -> Tuple[bool, Optional[bool]]:
    """
    Resolve whether a volume time series is cumulative and whether it is delta data.

    Args:
        volume_assertion_type: The volume assertion subtype. Accepts either the
            `VolumeAssertionType` enum or its string value, e.g. "ROW_COUNT_TOTAL".

    Returns:
        (is_dataframe_cumulative, is_delta)

        - is_dataframe_cumulative: True when the observed metric is cumulative totals
          and must be differenced (convert_cumulative) for forecasting.
        - is_delta: True when the observed metric is already a delta/change series.
          None when unknown / not provided.
    """

    if volume_assertion_type is None:
        return False, None

    # Normalize to enum when possible.
    v_type: Optional[VolumeAssertionType] = None
    if isinstance(volume_assertion_type, VolumeAssertionType):
        v_type = volume_assertion_type
    elif isinstance(volume_assertion_type, str):
        raw = volume_assertion_type.strip()
        if raw:
            try:
                v_type = VolumeAssertionType(raw)
            except Exception:
                v_type = None

    if v_type is None:
        return False, None

    total_types = {
        VolumeAssertionType.ROW_COUNT_TOTAL,
        VolumeAssertionType.INCREMENTING_SEGMENT_ROW_COUNT_TOTAL,
    }
    change_types = {
        VolumeAssertionType.ROW_COUNT_CHANGE,
        VolumeAssertionType.INCREMENTING_SEGMENT_ROW_COUNT_CHANGE,
    }

    if v_type in total_types:
        return True, False
    if v_type in change_types:
        return False, True

    return False, None
