from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pytest.importorskip("datahub_observe")


def _make_volume_assertion(volume_type: str):
    from datahub_executor.common.types import (
        Assertion,
        AssertionEntity,
        AssertionType,
        VolumeAssertion,
        VolumeAssertionType,
    )

    return Assertion(
        urn="urn:li:assertion:test",
        type=AssertionType.VOLUME,
        entity=AssertionEntity(
            urn="urn:li:dataset:test",
            platform_urn="urn:li:dataPlatform:test",
        ),
        volume_assertion=VolumeAssertion(type=VolumeAssertionType(volume_type)),
    )


def test_volume_trainer_v2_sets_is_delta_and_is_dataframe_cumulative_for_total() -> (
    None
):
    from datahub_executor.common.monitor.inference_v2.volume_trainer_v2 import (
        VolumeTrainerV2,
    )

    trainer = VolumeTrainerV2(
        graph=MagicMock(),
        metrics_client=MagicMock(),
        monitor_client=MagicMock(),
    )
    assertion = _make_volume_assertion("ROW_COUNT_TOTAL")
    evaluation_spec = MagicMock()
    evaluation_spec.context = None
    ctx = trainer.get_training_context(
        assertion=assertion,
        adjustment_settings=None,
        evaluation_spec=evaluation_spec,
    )
    assert ctx.is_dataframe_cumulative is True
    assert ctx.is_delta is False


def test_volume_trainer_v2_sets_is_delta_for_change() -> None:
    from datahub_executor.common.monitor.inference_v2.volume_trainer_v2 import (
        VolumeTrainerV2,
    )

    trainer = VolumeTrainerV2(
        graph=MagicMock(),
        metrics_client=MagicMock(),
        monitor_client=MagicMock(),
    )
    assertion = _make_volume_assertion("ROW_COUNT_CHANGE")
    evaluation_spec = MagicMock()
    evaluation_spec.context = None
    ctx = trainer.get_training_context(
        assertion=assertion,
        adjustment_settings=None,
        evaluation_spec=evaluation_spec,
    )
    assert ctx.is_dataframe_cumulative is False
    assert ctx.is_delta is True
