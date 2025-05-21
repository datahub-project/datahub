import pathlib
from typing import Any, Dict

import pytest

from acryl_datahub_cloud._sdk_extras.entities.monitor import Monitor
from datahub.errors import SdkUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import (
    DatasetUrn,
    MonitorUrn,
)
from datahub.testing.sdk_v2_helpers import assert_entity_golden

_GOLDEN_DIR = pathlib.Path(__file__).parent / "monitor_golden"

_any_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,1234567890,PROD)"
_any_monitor_id = "my_monitor_id"
_any_monitor_urn = MonitorUrn(
    entity=DatasetUrn.from_string(_any_dataset_urn),
    id=_any_monitor_id,
)
_any_monitor_type = "ASSERTION"
_any_monitor_status_mode = "ACTIVE"
_any_monitor_info = (_any_monitor_type, _any_monitor_status_mode)
_any_complex_monitor_info = models.MonitorInfoClass(
    type=models.MonitorTypeClass.FRESHNESS,
    status=models.MonitorStatusClass(
        mode=models.MonitorModeClass.PASSIVE,
        state=models.MonitorStateClass.TRAINING,
        reviewedAt=1234567890,
    ),
    executorId="executor_id",
)
_any_additional_args_for_complex_monitor: Dict[str, Any] = dict(
    external_url="https://example.com/monitor/1234",
    custom_properties={"key": "value"},
)


def test_monitor_basic() -> None:
    monitor = Monitor(
        id=_any_monitor_urn,
        info=_any_monitor_info,
    )
    assert monitor.urn == _any_monitor_urn
    assert monitor.info == models.MonitorInfoClass(
        type=models.MonitorTypeClass.ASSERTION,
        status=models.MonitorStatusClass(
            mode=models.MonitorModeClass.ACTIVE,
        ),
    )

    assert_entity_golden(monitor, _GOLDEN_DIR / "test_monitor_basic_golden.json")


def test_monitor_basic_other_ids() -> None:
    # Tuple[DatasetUrn, str]
    monitor = Monitor(
        id=(DatasetUrn.from_string(_any_dataset_urn), _any_monitor_id),
        info=_any_monitor_info,
    )
    assert monitor.urn == _any_monitor_urn

    # models.MonitorKeyClass
    monitor = Monitor(
        id=models.MonitorKeyClass(
            entity=str(DatasetUrn.from_string(_any_dataset_urn)),
            id=_any_monitor_id,
        ),
        info=_any_monitor_info,
    )
    assert monitor.urn == _any_monitor_urn

    with pytest.raises(SdkUsageError):
        # models.MonitorKeyClass with no valid dataset urn
        Monitor(
            id=models.MonitorKeyClass(
                entity="urn:li:dataset:not-valid-dataset",
                id=_any_monitor_id,
            ),
            info=_any_monitor_info,
        )

    with pytest.raises(SdkUsageError):
        # models.MonitorKeyClass with valid urn but not dataset
        Monitor(
            id=models.MonitorKeyClass(
                entity="urn:li:corpGroup:any-valid-group",
                id=_any_monitor_id,
            ),
            info=_any_monitor_info,
        )


def test_monitor_basic_other_infos() -> None:
    # Tuple[MonitorTypeClass, MonitorModeClass]
    monitor = Monitor(
        id=_any_monitor_urn,
        info=(
            models.MonitorTypeClass.FRESHNESS,
            models.MonitorModeClass.PASSIVE,
        ),
    )
    assert monitor.info == models.MonitorInfoClass(
        type=models.MonitorTypeClass.FRESHNESS,
        status=models.MonitorStatusClass(
            mode=models.MonitorModeClass.PASSIVE,
        ),
    )

    # models.MonitorInfoClass
    monitor = Monitor(
        id=_any_monitor_urn,
        info=_any_complex_monitor_info,
    )
    assert monitor.info == _any_complex_monitor_info

    # Tuple[str, str] - not valid MonitorTypeClass value
    with pytest.raises(SdkUsageError):
        Monitor(
            id=_any_monitor_urn,
            info=("NOT_VALID_MONITOR_TYPE", "ACTIVE"),
        )

    # Tuple[str, str] - not valid MonitorModeClass value
    with pytest.raises(SdkUsageError):
        Monitor(
            id=_any_monitor_urn,
            info=("ASSERTION", "NOT_VALID_MONITOR_MODE"),
        )


def test_monitor_basic_custom_properties_overwrites_info() -> None:
    monitor = Monitor(
        id=_any_monitor_urn,
        info=models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(
                mode=models.MonitorModeClass.ACTIVE,
            ),
            customProperties={"key": "value"},
        ),
        custom_properties={"key2": "value2"},
    )
    assert monitor.custom_properties == {"key2": "value2"}


def test_monitor_basic_external_url_overwrites_info() -> None:
    monitor = Monitor(
        id=_any_monitor_urn,
        info=models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(
                mode=models.MonitorModeClass.ACTIVE,
            ),
            externalUrl="https://example.com/monitor/1234",
        ),
        external_url="https://example.com/monitor/4321",
    )
    assert monitor.external_url == "https://example.com/monitor/4321"


def test_monitor_complex() -> None:
    monitor = Monitor(
        id=_any_monitor_urn,
        info=_any_complex_monitor_info,
        **_any_additional_args_for_complex_monitor,
    )

    assert_entity_golden(monitor, _GOLDEN_DIR / "test_monitor_complex_golden.json")
