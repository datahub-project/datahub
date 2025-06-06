import pathlib
from typing import Any, Dict

import pytest

from acryl_datahub_cloud._sdk_extras.entities.monitor import (
    Monitor,
    _get_nested_field_for_entity_with_default,
)
from datahub.errors import SdkUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import (
    AssertionUrn,
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
    assertionMonitor=models.AssertionMonitorClass(
        assertions=[
            models.AssertionEvaluationSpecClass(
                assertion="urn:li:assertion:assertion1",
                schedule=models.CronScheduleClass(
                    cron="0 0 * * *",
                    timezone="UTC",
                ),
                parameters=models.AssertionEvaluationParametersClass(
                    type=models.AssertionEvaluationParametersTypeClass.DATASET_FRESHNESS,
                    datasetFreshnessParameters=models.DatasetFreshnessAssertionParametersClass(
                        sourceType=models.DatasetFreshnessSourceTypeClass.INFORMATION_SCHEMA,
                        field=models.FreshnessFieldSpecClass(
                            path="last_updated_schema_field_path",
                            type="long",
                            nativeType="long",
                            kind=models.FreshnessFieldKindClass.LAST_MODIFIED,
                        ),
                        auditLog=models.AuditLogSpecClass(
                            operationTypes=["operation1"],
                            userName="username",
                        ),
                        dataHubOperation=models.DataHubOperationSpecClass(
                            operationTypes=["operation2"],
                            customOperationTypes=["operation3"],
                        ),
                    ),
                ),
                context=models.AssertionEvaluationContextClass(
                    embeddedAssertions=[
                        models.EmbeddedAssertionClass(
                            evaluationTimeWindow=models.TimeWindowClass(
                                startTimeMillis=1234567890,
                                length=models.TimeWindowSizeClass(
                                    unit=models.CalendarIntervalClass.HOUR,
                                    multiple=12,
                                ),
                            ),
                            context={"key": "value"},
                        ),
                    ],
                    inferenceDetails=models.AssertionInferenceDetailsClass(
                        modelId="model_id",
                        modelVersion="model_version",
                        confidence=0.9,
                        parameters={"param1": "value1"},
                        generatedAt=1234567890,
                    ),
                    stdDev=123.456,
                ),
            ),
        ],
        settings=models.AssertionMonitorSettingsClass(
            adjustmentSettings=models.AssertionAdjustmentSettingsClass(
                algorithm=models.AdjustmentAlgorithmClass.CUSTOM,
                algorithmName="custom_algorithm",
                context={"key": "value"},
                exclusionWindows=[
                    models.AssertionExclusionWindowClass(
                        type=models.AssertionExclusionWindowTypeClass.FIXED_RANGE,
                        displayName="fixed_range",
                        fixedRange=models.AbsoluteTimeWindowClass(
                            startTimeMillis=1234567890,
                            endTimeMillis=1234568790,
                        ),
                    )
                ],
                anomalyExclusionWindows=[
                    models.AssertionExclusionWindowClass(
                        type=models.AssertionExclusionWindowTypeClass.FIXED_RANGE,
                        displayName="fixed_range",
                        fixedRange=models.AbsoluteTimeWindowClass(
                            startTimeMillis=1234567890,
                            endTimeMillis=1234568790,
                        ),
                    )
                ],
                trainingDataLookbackWindowDays=30,
                sensitivity=models.AssertionMonitorSensitivityClass(level=5),
            ),
            capabilities=[models.AssertionMonitorCapabilityClass.ASSERTION_EVALUATION],
        ),
        bootstrapStatus=models.AssertionMonitorBootstrapStatusClass(
            metricsCubeBootstrapStatus=models.AssertionMonitorMetricsCubeBootstrapStatusClass(
                state=models.AssertionMonitorMetricsCubeBootstrapStateClass.PENDING,
                message="Bootstrap in progress",
            ),
        ),
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
    assert (
        monitor.urn.urn()
        == "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:bigquery,1234567890,PROD),my_monitor_id)"
    )

    # Tuple[DatasetUrn, AssertionUrn]
    monitor = Monitor(
        id=(
            DatasetUrn.from_string(_any_dataset_urn),
            AssertionUrn.from_string(f"urn:li:assertion:{_any_monitor_id}"),
        ),
        info=_any_monitor_info,
    )
    expected_monitor_urn = MonitorUrn(
        entity=DatasetUrn.from_string(_any_dataset_urn),
        id=f"urn:li:assertion:{_any_monitor_id}",
    )
    assert monitor.urn == expected_monitor_urn
    assert (
        monitor.urn.urn()
        == "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:bigquery,1234567890,PROD),urn:li:assertion:my_monitor_id)"
    )

    # models.MonitorKeyClass
    monitor = Monitor(
        id=models.MonitorKeyClass(
            entity=str(DatasetUrn.from_string(_any_dataset_urn)),
            id=_any_monitor_id,
        ),
        info=_any_monitor_info,
    )
    assert monitor.urn == _any_monitor_urn
    assert (
        monitor.urn.urn()
        == "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:bigquery,1234567890,PROD),my_monitor_id)"
    )

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


def test_monitor_properties_sensitivity_exclusion_windows_training_data_lookback_days() -> (
    None
):
    monitor_info = models.MonitorInfoClass(
        type=models.MonitorTypeClass.ASSERTION,
        status=models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE),
        assertionMonitor=models.AssertionMonitorClass(
            assertions=[],
            settings=models.AssertionMonitorSettingsClass(
                adjustmentSettings=models.AssertionAdjustmentSettingsClass(
                    sensitivity=models.AssertionMonitorSensitivityClass(level=7),
                    exclusionWindows=[
                        models.AssertionExclusionWindowClass(
                            type=models.AssertionExclusionWindowTypeClass.FIXED_RANGE,
                            displayName="window1",
                            fixedRange=models.AbsoluteTimeWindowClass(
                                startTimeMillis=1234567890,
                                endTimeMillis=1234567990,
                            ),
                        )
                    ],
                    trainingDataLookbackWindowDays=42,
                )
            ),
        ),
    )
    monitor = Monitor(
        id=(DatasetUrn.from_string(_any_dataset_urn), _any_monitor_id),
        info=monitor_info,
    )
    # Test sensitivity property
    assert monitor.sensitivity is not None
    assert monitor.sensitivity.level == 7
    # Test exclusion_windows property
    assert monitor.exclusion_windows is not None
    assert len(monitor.exclusion_windows) == 1
    assert monitor.exclusion_windows[0].displayName == "window1"
    # Test training_data_lookback_days property
    assert monitor.training_data_lookback_days is not None
    assert monitor.training_data_lookback_days == 42


def test_monitor_properties_return_none_when_missing() -> None:
    monitor_info = models.MonitorInfoClass(
        type=models.MonitorTypeClass.ASSERTION,
        status=models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE),
        # No assertionMonitor/settings/adjustmentSettings
    )
    monitor = Monitor(
        id=(DatasetUrn.from_string(_any_dataset_urn), _any_monitor_id),
        info=monitor_info,
    )
    assert monitor.sensitivity is None
    assert monitor.exclusion_windows is None
    assert monitor.training_data_lookback_days is None


def test_get_nested_field_for_entity_with_default() -> None:
    monitor_info = models.MonitorInfoClass(
        type=models.MonitorTypeClass.ASSERTION,
        status=models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE),
        assertionMonitor=models.AssertionMonitorClass(
            assertions=[],
            settings=models.AssertionMonitorSettingsClass(
                adjustmentSettings=models.AssertionAdjustmentSettingsClass(
                    sensitivity=models.AssertionMonitorSensitivityClass(level=3),
                )
            ),
        ),
    )
    monitor = Monitor(
        id=(DatasetUrn.from_string(_any_dataset_urn), _any_monitor_id),
        info=monitor_info,
    )
    # Should return the nested sensitivity object
    result = _get_nested_field_for_entity_with_default(
        monitor,
        "info.assertionMonitor.settings.adjustmentSettings.sensitivity",
        default="not_found",
    )
    assert isinstance(result, models.AssertionMonitorSensitivityClass)
    assert result.level == 3
    # Should return the default if path is missing
    result_missing = _get_nested_field_for_entity_with_default(
        monitor,
        "info.assertionMonitor.settings.adjustmentSettings.nonexistent",
        default="not_found",
    )
    assert result_missing == "not_found"
