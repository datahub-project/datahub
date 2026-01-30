from __future__ import annotations

import pytest

pytest.importorskip("datahub_observe")


def test_resolve_volume_series_semantics_total_types() -> None:
    from datahub_executor.common.monitor.inference_v2.volume_semantics import (
        resolve_volume_series_semantics,
    )
    from datahub_executor.common.types import VolumeAssertionType

    for v_type in (
        VolumeAssertionType.ROW_COUNT_TOTAL,
        VolumeAssertionType.INCREMENTING_SEGMENT_ROW_COUNT_TOTAL,
        "ROW_COUNT_TOTAL",
        "INCREMENTING_SEGMENT_ROW_COUNT_TOTAL",
    ):
        is_cumulative, is_delta = resolve_volume_series_semantics(v_type)
        assert is_cumulative is True
        assert is_delta is False


def test_resolve_volume_series_semantics_change_types() -> None:
    from datahub_executor.common.monitor.inference_v2.volume_semantics import (
        resolve_volume_series_semantics,
    )
    from datahub_executor.common.types import VolumeAssertionType

    for v_type in (
        VolumeAssertionType.ROW_COUNT_CHANGE,
        VolumeAssertionType.INCREMENTING_SEGMENT_ROW_COUNT_CHANGE,
        "ROW_COUNT_CHANGE",
        "INCREMENTING_SEGMENT_ROW_COUNT_CHANGE",
    ):
        is_cumulative, is_delta = resolve_volume_series_semantics(v_type)
        assert is_cumulative is False
        assert is_delta is True


def test_resolve_volume_series_semantics_unknown_is_safe() -> None:
    from datahub_executor.common.monitor.inference_v2.volume_semantics import (
        resolve_volume_series_semantics,
    )

    assert resolve_volume_series_semantics(None) == (False, None)
    assert resolve_volume_series_semantics("") == (False, None)
    assert resolve_volume_series_semantics("NOT_A_REAL_TYPE") == (False, None)
