import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional, Type, TypedDict, Union
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

import datahub.metadata.schema_classes as models
from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    FreshnessAssertion,
    SmartFreshnessAssertion,
    SmartVolumeAssertion,
    SqlAssertion,
)
from acryl_datahub_cloud.sdk.assertion.smart_column_metric_assertion import (
    SmartColumnMetricAssertion,
)
from acryl_datahub_cloud.sdk.assertion_client.smart_freshness import (
    SmartFreshnessAssertionClient,
)
from acryl_datahub_cloud.sdk.assertion_client.smart_volume import (
    SmartVolumeAssertionClient,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    _DETECTION_MECHANISM_CONCRETE_TYPES,
    ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
    DEFAULT_DETECTION_MECHANISM,
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
    DEFAULT_NAME_PREFIX,
    DEFAULT_SCHEDULE,
    DEFAULT_SENSITIVITY,
    AssertionIncidentBehavior,
    AssertionIncidentBehaviorInputTypes,
    DetectionMechanism,
    DetectionMechanismInputTypes,
    FixedRangeExclusionWindow,
    InferenceSensitivity,
    TimeWindowSizeInputTypes,
    _DetectionMechanismTypes,
)
from acryl_datahub_cloud.sdk.assertion_input.column_metric_constants import (
    MetricInputType,
    OperatorInputType,
)
from acryl_datahub_cloud.sdk.assertion_input.smart_freshness_assertion_input import (
    _SmartFreshnessAssertionInput,
)
from acryl_datahub_cloud.sdk.assertion_input.smart_volume_assertion_input import (
    _SmartVolumeAssertionInput,
)
from acryl_datahub_cloud.sdk.assertion_input.sql_assertion_input import (
    SqlAssertionCondition,
    SqlAssertionCriteria,
)
from acryl_datahub_cloud.sdk.assertions_client import (
    DEFAULT_CREATED_BY,
    AssertionsClient,
)
from acryl_datahub_cloud.sdk.entities.assertion import (
    Assertion,
    TagsInputType,
)
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import (
    SDKNotYetSupportedError,
    SDKUsageError,
    SDKUsageErrorWithExamples,
)
from datahub.emitter.mce_builder import make_ts_millis
from datahub.errors import ItemNotFoundError, SdkUsageError
from datahub.metadata.schema_classes import (
    AssertionStdOperatorClass,
    FieldMetricTypeClass,
)
from datahub.metadata.urns import (
    AssertionUrn,
    CorpUserUrn,
    DatasetUrn,
    MonitorUrn,
    TagUrn,
)
from datahub.utilities.urns.error import InvalidUrnError
from tests.sdk.assertions.conftest import StubDataHubClient

FROZEN_TIME = datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc)

_any_user = CorpUserUrn.from_string("urn:li:corpuser:test_user")
_any_dataset_urn = DatasetUrn.from_string(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)"
)
_any_assertion_urn = AssertionUrn.from_string("urn:li:assertion:test_assertion")

GENERATED_DISPLAY_NAME_LENGTH = 22


@dataclass
class SmartFreshnessAssertionInputParams:
    dataset_urn: Union[str, DatasetUrn]
    display_name: Optional[str] = None
    detection_mechanism: Optional[DetectionMechanismInputTypes] = None
    sensitivity: Optional[InferenceSensitivity] = None
    exclusion_windows: Optional[list[FixedRangeExclusionWindow]] = None
    training_data_lookback_days: Optional[int] = None
    incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None
    tags: Optional[TagsInputType] = None
    updated_by: Optional[CorpUserUrn] = None


@dataclass
class SmartFreshnessAssertionOutputParams:
    dataset_urn: Union[str, DatasetUrn]
    display_name: str
    detection_mechanism: _DetectionMechanismTypes
    sensitivity: InferenceSensitivity
    exclusion_windows: list[FixedRangeExclusionWindow]
    training_data_lookback_days: int
    incident_behavior: list[AssertionIncidentBehavior]
    tags: TagsInputType
    created_by: CorpUserUrn
    created_at: datetime
    updated_by: CorpUserUrn
    updated_at: datetime


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "input_params, expected_output_params",
    [
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
            ),
            SmartFreshnessAssertionOutputParams(
                dataset_urn=_any_dataset_urn,
                display_name="New Assertion",
                detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
                sensitivity=DEFAULT_SENSITIVITY,
                exclusion_windows=[],
                training_data_lookback_days=ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
                incident_behavior=[],
                tags=[],
                created_by=DEFAULT_CREATED_BY,
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                updated_by=DEFAULT_CREATED_BY,
                updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
            ),
            id="minimal_valid_input",
        ),
        # Minimal valid input with all fields
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                display_name="Test Assertion",
                detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
                sensitivity=InferenceSensitivity.LOW,
                exclusion_windows=[
                    FixedRangeExclusionWindow(
                        start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                        end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                    )
                ],
                training_data_lookback_days=30,
                incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
                tags=["urn:li:tag:my_tag_1"],
                updated_by=_any_user,
            ),
            SmartFreshnessAssertionOutputParams(
                dataset_urn=_any_dataset_urn,
                display_name="Test Assertion",
                detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
                sensitivity=InferenceSensitivity.LOW,
                exclusion_windows=[
                    FixedRangeExclusionWindow(
                        start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                        end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                    )
                ],
                training_data_lookback_days=30,
                incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
                tags=[TagUrn.from_string("urn:li:tag:my_tag_1")],
                created_by=_any_user,
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                updated_by=_any_user,
                updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
            ),
            id="minimal_valid_full_input",
        ),
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                display_name="Test Assertion",
                detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
                sensitivity=InferenceSensitivity.LOW,
                exclusion_windows=[
                    FixedRangeExclusionWindow(
                        start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                        end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                    ),
                    FixedRangeExclusionWindow(
                        start=datetime(2025, 1, 3, tzinfo=timezone.utc),
                        end=datetime(2025, 1, 4, tzinfo=timezone.utc),
                    ),
                ],
                training_data_lookback_days=30,
                incident_behavior=[
                    AssertionIncidentBehavior.RAISE_ON_FAIL,
                    AssertionIncidentBehavior.RESOLVE_ON_PASS,
                ],
                tags=["urn:li:tag:my_tag_1", "urn:li:tag:my_tag_2"],
                updated_by=_any_user,
            ),
            SmartFreshnessAssertionOutputParams(
                dataset_urn=_any_dataset_urn,
                display_name="Test Assertion",
                detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
                sensitivity=InferenceSensitivity.LOW,
                exclusion_windows=[
                    FixedRangeExclusionWindow(
                        start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                        end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                    ),
                    FixedRangeExclusionWindow(
                        start=datetime(2025, 1, 3, tzinfo=timezone.utc),
                        end=datetime(2025, 1, 4, tzinfo=timezone.utc),
                    ),
                ],
                training_data_lookback_days=30,
                incident_behavior=[
                    AssertionIncidentBehavior.RAISE_ON_FAIL,
                    AssertionIncidentBehavior.RESOLVE_ON_PASS,
                ],
                tags=[
                    TagUrn.from_string("urn:li:tag:my_tag_1"),
                    TagUrn.from_string("urn:li:tag:my_tag_2"),
                ],
                created_by=_any_user,
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                updated_by=_any_user,
                updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
            ),
            id="multiple_incident_behaviors_and_tags_and_exclusion_windows",
        ),
        # Test string incident_behavior input
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                display_name="Test Assertion String Incident Behavior",
                detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
                sensitivity=InferenceSensitivity.LOW,
                training_data_lookback_days=30,
                incident_behavior="raise_on_fail",  # String input
                tags=["urn:li:tag:my_tag_1"],
                updated_by=_any_user,
            ),
            SmartFreshnessAssertionOutputParams(
                dataset_urn=_any_dataset_urn,
                display_name="Test Assertion String Incident Behavior",
                detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
                sensitivity=InferenceSensitivity.LOW,
                exclusion_windows=[],
                training_data_lookback_days=30,
                incident_behavior=[
                    AssertionIncidentBehavior.RAISE_ON_FAIL
                ],  # Expected enum output
                tags=[TagUrn.from_string("urn:li:tag:my_tag_1")],
                created_by=_any_user,
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                updated_by=_any_user,
                updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
            ),
            id="string_incident_behavior_input",
        ),
    ],
)
def test_create_smart_freshness_assertion_valid_simple_input(
    freshness_stub_datahub_client: StubDataHubClient,
    input_params: SmartFreshnessAssertionInputParams,
    expected_output_params: SmartFreshnessAssertionOutputParams,
) -> None:
    # Arrange
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing

    # Act - Use sync_smart_freshness_assertion with urn=None for create scenario
    assertion = client.sync_smart_freshness_assertion(**asdict(input_params))

    # Assert
    _validate_assertion_vs_input(assertion, input_params, expected_output_params)


class OtherOutputParams(TypedDict, total=False):
    dataset_urn: Union[str, DatasetUrn]
    sensitivity: InferenceSensitivity
    exclusion_windows: list[FixedRangeExclusionWindow]
    training_data_lookback_days: int
    incident_behavior: list[AssertionIncidentBehavior]
    tags: TagsInputType
    created_by: CorpUserUrn
    created_at: datetime
    updated_by: CorpUserUrn
    updated_at: datetime


_OTHER_OUTPUT_PARAMS: OtherOutputParams = {
    "dataset_urn": _any_dataset_urn,
    "sensitivity": DEFAULT_SENSITIVITY,
    "exclusion_windows": [],
    "training_data_lookback_days": ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
    "incident_behavior": [],
    "tags": [],
    "created_by": DEFAULT_CREATED_BY,
    "created_at": datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
    "updated_by": DEFAULT_CREATED_BY,
    "updated_at": datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
}


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "input_params, field_spec_type, field_spec_kind, expected_output_params",
    [
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                display_name="Last Modified Column Detection Mechanism Assertion",
                detection_mechanism=DetectionMechanism.LAST_MODIFIED_COLUMN(
                    column_name="last_modified", additional_filter="amount > 1000"
                ),
            ),
            "DateTypeClass",
            models.FreshnessFieldKindClass.LAST_MODIFIED,
            SmartFreshnessAssertionOutputParams(
                **_OTHER_OUTPUT_PARAMS,
                display_name="Last Modified Column Detection Mechanism Assertion",
                detection_mechanism=DetectionMechanism.LAST_MODIFIED_COLUMN(
                    column_name="last_modified", additional_filter="amount > 1000"
                ),
            ),
            id="last_modified_column_detection_mechanism",
        ),
        # Last modified without additional filter
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                display_name="Last Modified Column Detection Mechanism Assertion",
                detection_mechanism=DetectionMechanism.LAST_MODIFIED_COLUMN(
                    column_name="last_modified", additional_filter=None
                ),
            ),
            "DateTypeClass",
            models.FreshnessFieldKindClass.LAST_MODIFIED,
            SmartFreshnessAssertionOutputParams(
                **_OTHER_OUTPUT_PARAMS,
                display_name="Last Modified Column Detection Mechanism Assertion",
                detection_mechanism=DetectionMechanism.LAST_MODIFIED_COLUMN(
                    column_name="last_modified", additional_filter=None
                ),
            ),
            id="last_modified_column_detection_mechanism_without_additional_filter",
        ),
    ],
)
def test_create_smart_freshness_assertion_valid_complex_detection_mechanism_input(
    freshness_stub_datahub_client: StubDataHubClient,
    input_params: SmartFreshnessAssertionInputParams,
    field_spec_type: str,
    field_spec_kind: models.FreshnessFieldKindClass,
    expected_output_params: SmartFreshnessAssertionOutputParams,
) -> None:
    # Arrange
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing
    with patch.object(
        _SmartFreshnessAssertionInput, "_create_field_spec", new_callable=MagicMock
    ) as mock_create_field_spec:
        mock_create_field_spec.return_value = models.FreshnessFieldSpecClass(
            path="path",
            type=field_spec_type,
            nativeType="nativeType",
            kind=field_spec_kind,
        )

        # Act - Use sync_smart_freshness_assertion with urn=None for create scenario
        assertion = client.sync_smart_freshness_assertion(**asdict(input_params))

    # Assert
    _validate_assertion_vs_input(assertion, input_params, expected_output_params)


def test_create_smart_freshness_assertion_entities_client_called(
    freshness_stub_datahub_client: StubDataHubClient,
) -> None:
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    client.client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    # Use sync_smart_freshness_assertion with urn=None for create scenario
    assertion = client.sync_smart_freshness_assertion(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
    )
    assert mock_upsert.call_count == 2
    assert assertion


@pytest.mark.parametrize(
    "input_params, error_type, expected_error_message",
    [
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                detection_mechanism="invalid_detection_mechanism",
            ),
            SDKUsageErrorWithExamples,
            "Invalid detection mechanism type: invalid_detection_mechanism",
            id="invalid_detection_mechanism",
        ),
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                detection_mechanism=DetectionMechanism.HIGH_WATERMARK_COLUMN(
                    column_name="high_watermark", additional_filter="amount > 1000"
                ),
            ),
            SDKNotYetSupportedError,
            "This feature is not yet supported in the Python SDK",
            id="unsupported_detection_mechanism_high_watermark_column",
        ),
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                sensitivity="invalid_sensitivity",  # type: ignore[arg-type] # Test invalid input
            ),
            SDKUsageErrorWithExamples,
            "Invalid inference sensitivity: invalid_sensitivity",
            id="invalid_sensitivity",
        ),
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                exclusion_windows="invalid_exclusion_windows",  # type: ignore[arg-type] # Test invalid input
            ),
            SDKUsageErrorWithExamples,
            "Invalid exclusion window: invalid_exclusion_windows",
            id="invalid_exclusion_windows",
        ),
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                training_data_lookback_days=-1,
            ),
            SDKUsageError,
            "Training data lookback days must be non-negative",
            id="negative_training_data_lookback_days",
        ),
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                incident_behavior="invalid_incident_behavior",  # type: ignore[arg-type] # Test invalid input
            ),
            SDKUsageErrorWithExamples,
            "Invalid incident behavior: invalid_incident_behavior",
            id="invalid_incident_behavior",
        ),
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                tags="urn:li:tag:",  # type: ignore[arg-type] # Test invalid input - empty tag name
            ),
            InvalidUrnError,
            "Expecting a TagUrn but got urn:li:tag:",
            id="invalid_tag_urn",
        ),
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                updated_by="invalid_updated_by",  # type: ignore[arg-type] # Test invalid input
            ),
            SdkUsageError,
            re.escape(
                "Invalid actor for last updated tuple, expected 'urn:li:corpuser:*' or 'urn:li:corpGroup:*'"
            ),
            id="invalid_updated_by_urn",
        ),
    ],
)
def test_create_smart_freshness_assertion_invalid_input(
    freshness_stub_datahub_client: StubDataHubClient,
    input_params: SmartFreshnessAssertionInputParams,
    error_type: Type[Exception],
    expected_error_message: str,
) -> None:
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing
    with pytest.raises(error_type, match=expected_error_message):
        # Use sync_smart_freshness_assertion with urn=None for create scenario
        client.sync_smart_freshness_assertion(**asdict(input_params))


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "enabled, expected_monitor_mode",
    [
        pytest.param(True, models.MonitorModeClass.ACTIVE, id="enabled_true"),
        pytest.param(False, models.MonitorModeClass.INACTIVE, id="enabled_false"),
    ],
)
def test_create_smart_freshness_assertion_enabled_parameter(
    freshness_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    enabled: bool,
    expected_monitor_mode: models.MonitorModeClass,
) -> None:
    """Test that the enabled parameter controls the monitor mode correctly."""
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    client.client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing

    # Use sync_smart_freshness_assertion with urn=None for create scenario
    client.sync_smart_freshness_assertion(
        dataset_urn=any_dataset_urn,
        enabled=enabled,
    )

    # Verify that upsert was called with the correct monitor mode
    assert mock_upsert.call_count == 2  # assertion + monitor

    # Check the monitor entity (second call)
    monitor_entity = mock_upsert.call_args_list[1][0][0]
    assert monitor_entity.info.status.mode == expected_monitor_mode


@freeze_time(FROZEN_TIME)
def test_create_smart_freshness_assertion_enabled_defaults_to_true(
    freshness_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that the enabled parameter defaults to True when not specified."""
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    client.client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing

    # Don't specify enabled parameter - use sync_smart_freshness_assertion with urn=None for create scenario
    client.sync_smart_freshness_assertion(
        dataset_urn=any_dataset_urn,
    )

    # Verify that monitor is created as ACTIVE (default enabled=True)
    assert mock_upsert.call_count == 2  # assertion + monitor

    monitor_entity = mock_upsert.call_args_list[1][0][0]
    assert monitor_entity.info.status.mode == models.MonitorModeClass.ACTIVE


@dataclass
class SmartFreshnessAssertionUpsertInputParams:
    dataset_urn: Union[str, DatasetUrn]
    urn: Optional[Union[str, AssertionUrn]] = None
    display_name: Optional[str] = None
    detection_mechanism: Optional[DetectionMechanismInputTypes] = None
    sensitivity: Optional[InferenceSensitivity] = None
    exclusion_windows: Optional[list[FixedRangeExclusionWindow]] = None
    training_data_lookback_days: Optional[int] = None
    incident_behavior: Optional[list[AssertionIncidentBehavior]] = None
    tags: Optional[TagsInputType] = None
    updated_by: Optional[CorpUserUrn] = None


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "detection_mechanism, expected_detection_mechanism",
    [
        pytest.param(
            DetectionMechanism.INFORMATION_SCHEMA,
            DetectionMechanism.INFORMATION_SCHEMA,
            id="simple-information_schema",
        ),
        pytest.param(
            {
                "type": "last_modified_column",
                "column_name": "last_modified",
                "additional_filter": "last_modified > '2021-01-01'",
            },
            DetectionMechanism.LAST_MODIFIED_COLUMN(
                column_name="last_modified",
                additional_filter="last_modified > '2021-01-01'",
            ),
            id="parametrized-last_modified_column_with_additional_filter",
        ),
    ],
)
def _validate_assertion_vs_input(
    assertion: SmartFreshnessAssertion,
    input_params: Union[
        SmartFreshnessAssertionInputParams, SmartFreshnessAssertionUpsertInputParams
    ],
    expected_output_params: SmartFreshnessAssertionOutputParams,
) -> None:
    if input_params.display_name is not None:
        assert assertion.display_name == expected_output_params.display_name
    else:
        assert assertion.display_name.startswith(
            DEFAULT_NAME_PREFIX
        )  # Generated display name
        assert len(assertion.display_name) == GENERATED_DISPLAY_NAME_LENGTH
    assert isinstance(
        assertion.detection_mechanism, _DETECTION_MECHANISM_CONCRETE_TYPES
    )
    assert (
        assertion.detection_mechanism.type
        == expected_output_params.detection_mechanism.type
    )
    assert assertion.sensitivity.value == expected_output_params.sensitivity.value
    assert assertion.exclusion_windows == expected_output_params.exclusion_windows
    assert (
        assertion.training_data_lookback_days
        == expected_output_params.training_data_lookback_days
    )
    assert assertion.incident_behavior == expected_output_params.incident_behavior
    assert assertion.tags == expected_output_params.tags
    assert assertion.created_by == expected_output_params.created_by
    assert assertion.created_at == expected_output_params.created_at
    assert assertion.updated_by == expected_output_params.updated_by
    assert assertion.updated_at == expected_output_params.updated_at


@dataclass
class SmartVolumeAssertionInputParams:
    dataset_urn: Union[str, DatasetUrn]
    display_name: Optional[str] = None
    detection_mechanism: Optional[DetectionMechanismInputTypes] = None
    sensitivity: Optional[InferenceSensitivity] = None
    exclusion_windows: Optional[list[FixedRangeExclusionWindow]] = None
    training_data_lookback_days: Optional[int] = None
    incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None
    tags: Optional[TagsInputType] = None
    created_by: Optional[CorpUserUrn] = None
    schedule: Optional[Union[str, models.CronScheduleClass]] = None


@dataclass
class SmartVolumeAssertionOutputParams:
    dataset_urn: Union[str, DatasetUrn]
    display_name: str
    detection_mechanism: _DetectionMechanismTypes
    sensitivity: InferenceSensitivity
    exclusion_windows: list[FixedRangeExclusionWindow]
    training_data_lookback_days: int
    incident_behavior: list[AssertionIncidentBehavior]
    tags: TagsInputType
    created_by: CorpUserUrn
    created_at: datetime
    updated_by: CorpUserUrn
    updated_at: datetime
    schedule: models.CronScheduleClass


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "input_params, expected_output_params",
    [
        pytest.param(
            SmartVolumeAssertionInputParams(
                dataset_urn=_any_dataset_urn,
            ),
            SmartVolumeAssertionOutputParams(
                dataset_urn=_any_dataset_urn,
                display_name=DEFAULT_NAME_PREFIX,
                detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
                sensitivity=DEFAULT_SENSITIVITY,
                exclusion_windows=[],
                training_data_lookback_days=ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
                incident_behavior=[],
                tags=[],
                created_by=DEFAULT_CREATED_BY,
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                updated_by=DEFAULT_CREATED_BY,
                updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                schedule=DEFAULT_SCHEDULE,
            ),
            id="minimal_valid_input",
        ),
        # Minimal valid input with all fields
        pytest.param(
            SmartVolumeAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                display_name="Test Assertion",
                detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
                sensitivity=InferenceSensitivity.LOW,
                exclusion_windows=[
                    FixedRangeExclusionWindow(
                        start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                        end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                    )
                ],
                training_data_lookback_days=30,
                incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
                tags=["urn:li:tag:my_tag_1"],
                created_by=_any_user,
                schedule=models.CronScheduleClass(cron="0 * * * *", timezone="UTC"),
            ),
            SmartVolumeAssertionOutputParams(
                dataset_urn=_any_dataset_urn,
                display_name="Test Assertion",
                detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
                sensitivity=InferenceSensitivity.LOW,
                exclusion_windows=[
                    FixedRangeExclusionWindow(
                        start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                        end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                    )
                ],
                training_data_lookback_days=30,
                incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
                tags=[TagUrn.from_string("urn:li:tag:my_tag_1")],
                created_by=_any_user,
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                updated_by=_any_user,
                updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                schedule=models.CronScheduleClass(cron="0 * * * *", timezone="UTC"),
            ),
            id="minimal_valid_full_input",
        ),
        pytest.param(
            SmartVolumeAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                display_name="Test Assertion",
                detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
                sensitivity=InferenceSensitivity.LOW,
                exclusion_windows=[
                    FixedRangeExclusionWindow(
                        start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                        end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                    ),
                    FixedRangeExclusionWindow(
                        start=datetime(2025, 1, 3, tzinfo=timezone.utc),
                        end=datetime(2025, 1, 4, tzinfo=timezone.utc),
                    ),
                ],
                training_data_lookback_days=30,
                incident_behavior=[
                    AssertionIncidentBehavior.RAISE_ON_FAIL,
                    AssertionIncidentBehavior.RESOLVE_ON_PASS,
                ],
                tags=["urn:li:tag:my_tag_1", "urn:li:tag:my_tag_2"],
                created_by=_any_user,
                schedule=models.CronScheduleClass(cron="0 * * * *", timezone="UTC"),
            ),
            SmartVolumeAssertionOutputParams(
                dataset_urn=_any_dataset_urn,
                display_name="Test Assertion",
                detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
                sensitivity=InferenceSensitivity.LOW,
                exclusion_windows=[
                    FixedRangeExclusionWindow(
                        start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                        end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                    ),
                    FixedRangeExclusionWindow(
                        start=datetime(2025, 1, 3, tzinfo=timezone.utc),
                        end=datetime(2025, 1, 4, tzinfo=timezone.utc),
                    ),
                ],
                training_data_lookback_days=30,
                incident_behavior=[
                    AssertionIncidentBehavior.RAISE_ON_FAIL,
                    AssertionIncidentBehavior.RESOLVE_ON_PASS,
                ],
                tags=[
                    TagUrn.from_string("urn:li:tag:my_tag_1"),
                    TagUrn.from_string("urn:li:tag:my_tag_2"),
                ],
                created_by=_any_user,
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                updated_by=_any_user,
                updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                schedule=models.CronScheduleClass(cron="0 * * * *", timezone="UTC"),
            ),
            id="multiple_incident_behaviors_and_tags_and_exclusion_windows",
        ),
        # Test string incident_behavior input for smart volume assertion
        pytest.param(
            SmartVolumeAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                display_name="Test Smart Volume Assertion String Incident Behavior",
                detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
                sensitivity=InferenceSensitivity.LOW,
                training_data_lookback_days=30,
                incident_behavior="resolve_on_pass",  # String input
                tags=["urn:li:tag:my_tag_1"],
                created_by=_any_user,
                schedule=models.CronScheduleClass(cron="0 * * * *", timezone="UTC"),
            ),
            SmartVolumeAssertionOutputParams(
                dataset_urn=_any_dataset_urn,
                display_name="Test Smart Volume Assertion String Incident Behavior",
                detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
                sensitivity=InferenceSensitivity.LOW,
                exclusion_windows=[],
                training_data_lookback_days=30,
                incident_behavior=[
                    AssertionIncidentBehavior.RESOLVE_ON_PASS
                ],  # Expected enum output
                tags=[TagUrn.from_string("urn:li:tag:my_tag_1")],
                created_by=_any_user,
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                updated_by=_any_user,
                updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                schedule=models.CronScheduleClass(cron="0 * * * *", timezone="UTC"),
            ),
            id="string_incident_behavior_input_smart_volume",
        ),
    ],
)
def test_create_smart_volume_assertion_valid_simple_input(
    freshness_stub_datahub_client: StubDataHubClient,
    input_params: SmartVolumeAssertionInputParams,
    expected_output_params: SmartVolumeAssertionOutputParams,
) -> None:
    # Arrange
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    # Act
    assertion = client._smart_volume_client._create_smart_volume_assertion(
        **asdict(input_params)
    )

    # Assert
    _validate_volume_assertion_vs_input(assertion, input_params, expected_output_params)


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "detection_mechanism, expected_detection_mechanism",
    [
        pytest.param(
            DetectionMechanism.INFORMATION_SCHEMA,
            DetectionMechanism.INFORMATION_SCHEMA,
            id="simple-information_schema",
        ),
        pytest.param(
            {
                "type": "query",
                "additional_filter": "last_modified > '2021-01-01'",
            },
            DetectionMechanism.QUERY(
                additional_filter="last_modified > '2021-01-01'",
            ),
            id="parametrized-query-with-additional-filter",
        ),
    ],
)
def test_create_smart_volume_assertion_valid_complex_detection_mechanism_input(
    freshness_stub_datahub_client: StubDataHubClient,
    detection_mechanism: DetectionMechanismInputTypes,
    expected_detection_mechanism: _DetectionMechanismTypes,
) -> None:
    # Arrange
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    # Act
    assertion = client._smart_volume_client._create_smart_volume_assertion(
        dataset_urn=_any_dataset_urn,
        display_name="Test Assertion",
        detection_mechanism=detection_mechanism,
    )

    # Assert
    assert assertion.detection_mechanism == expected_detection_mechanism


def test_create_smart_volume_assertion_entities_client_called(
    freshness_stub_datahub_client: StubDataHubClient,
) -> None:
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create = MagicMock()
    client.client.entities.create = mock_create  # type: ignore[method-assign] # Override for testing
    assertion = client._smart_volume_client._create_smart_volume_assertion(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
    )
    assert mock_create.call_count == 2
    assert assertion


@pytest.mark.parametrize(
    "input_params, error_type, expected_error_message",
    [
        pytest.param(
            SmartVolumeAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                detection_mechanism="invalid_detection_mechanism",
            ),
            SDKUsageErrorWithExamples,
            "Invalid detection mechanism type: invalid_detection_mechanism",
            id="invalid_detection_mechanism",
        ),
        pytest.param(
            SmartVolumeAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                sensitivity="invalid_sensitivity",  # type: ignore[arg-type] # Test invalid input
            ),
            SDKUsageErrorWithExamples,
            "Invalid inference sensitivity: invalid_sensitivity",
            id="invalid_sensitivity",
        ),
        pytest.param(
            SmartVolumeAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                exclusion_windows="invalid_exclusion_windows",  # type: ignore[arg-type] # Test invalid input
            ),
            SDKUsageErrorWithExamples,
            "Invalid exclusion window: invalid_exclusion_windows",
            id="invalid_exclusion_windows",
        ),
        pytest.param(
            SmartVolumeAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                training_data_lookback_days=-1,
            ),
            SDKUsageError,
            "Training data lookback days must be non-negative",
            id="negative_training_data_lookback_days",
        ),
        pytest.param(
            SmartVolumeAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                incident_behavior="invalid_incident_behavior",  # type: ignore[arg-type] # Test invalid input
            ),
            SDKUsageErrorWithExamples,
            "Invalid incident behavior: invalid_incident_behavior",
            id="invalid_incident_behavior",
        ),
        pytest.param(
            SmartVolumeAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                tags="urn:li:tag:",  # type: ignore[arg-type] # Test invalid input - empty tag name
            ),
            InvalidUrnError,
            "Expecting a TagUrn but got urn:li:tag:",
            id="invalid_tag_urn",
        ),
        pytest.param(
            SmartVolumeAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                created_by="invalid_created_by",  # type: ignore[arg-type] # Test invalid input
            ),
            SdkUsageError,
            re.escape(
                "Invalid actor for last updated tuple, expected 'urn:li:corpuser:*' or 'urn:li:corpGroup:*'"
            ),
            id="invalid_created_by_urn",
        ),
        pytest.param(
            SmartVolumeAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                schedule="invalid_cron",  # type: ignore[arg-type] # Test invalid input
            ),
            SDKUsageError,
            "Invalid cron expression or timezone: invalid_cron",
            id="invalid_schedule",
        ),
    ],
)
def test_create_smart_volume_assertion_invalid_input(
    freshness_stub_datahub_client: StubDataHubClient,
    input_params: SmartVolumeAssertionInputParams,
    error_type: Type[Exception],
    expected_error_message: str,
) -> None:
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing
    with pytest.raises(error_type, match=expected_error_message):
        client._smart_volume_client._create_smart_volume_assertion(
            **asdict(input_params)
        )


@dataclass
class SmartVolumeAssertionUpsertInputParams:
    dataset_urn: Union[str, DatasetUrn]
    urn: Optional[Union[str, AssertionUrn]] = None
    display_name: Optional[str] = None
    detection_mechanism: Optional[DetectionMechanismInputTypes] = None
    sensitivity: Optional[InferenceSensitivity] = None
    exclusion_windows: Optional[list[FixedRangeExclusionWindow]] = None
    training_data_lookback_days: Optional[int] = None
    incident_behavior: Optional[list[AssertionIncidentBehavior]] = None
    tags: Optional[TagsInputType] = None
    updated_by: Optional[CorpUserUrn] = None
    schedule: Optional[Union[str, models.CronScheduleClass]] = None


@freeze_time(FROZEN_TIME)
def test_sync_smart_volume_assertion_valid_simple_input(
    volume_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_monitor_urn: MonitorUrn,
    any_assertion_urn: AssertionUrn,
    volume_assertion_entity_with_all_fields: Assertion,
    volume_monitor_with_all_fields: Monitor,
) -> None:
    """Test with all fields set to default values."""

    # MyPy assertions - we know these are not None from the fixtures
    assert volume_assertion_entity_with_all_fields.description is not None
    assert volume_assertion_entity_with_all_fields.source is not None
    assert volume_assertion_entity_with_all_fields.source.created is not None
    assert volume_assertion_entity_with_all_fields.source.created.actor is not None
    assert volume_assertion_entity_with_all_fields.source.created.time is not None
    assert volume_monitor_with_all_fields.info is not None
    assert volume_monitor_with_all_fields.info.assertionMonitor is not None
    assert volume_monitor_with_all_fields.info.assertionMonitor.settings is not None
    assert (
        volume_monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings
        is not None
    )
    assert (
        volume_monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings.trainingDataLookbackWindowDays
        is not None
    )
    assert (
        volume_monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings.sensitivity
        is not None
    )
    assert (
        volume_monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings.sensitivity.level
        is not None
    )
    assert (
        volume_monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings.exclusionWindows
        is not None
    )

    # Arrange
    input_params = SmartVolumeAssertionUpsertInputParams(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
    )
    mock_upsert = MagicMock()
    volume_stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    client = AssertionsClient(volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Act
    assertion = client.sync_smart_volume_assertion(**asdict(input_params))

    # Assert
    _validate_volume_assertion_vs_input(
        assertion,
        input_params,
        SmartVolumeAssertionOutputParams(
            dataset_urn=input_params.dataset_urn,
            display_name=volume_assertion_entity_with_all_fields.description,  # From fixture, preserved by merge
            detection_mechanism=DEFAULT_DETECTION_MECHANISM,  # Default
            sensitivity=InferenceSensitivity.LOW,  # From fixture, preserved by merge (level=1 maps to LOW)
            exclusion_windows=[  # From fixture, preserved by merge
                FixedRangeExclusionWindow(
                    start=datetime(2021, 1, 1, tzinfo=timezone.utc),
                    end=datetime(2021, 1, 2, tzinfo=timezone.utc),
                )
            ],
            training_data_lookback_days=volume_monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings.trainingDataLookbackWindowDays,  # From fixture, preserved by merge
            incident_behavior=[  # From fixture, preserved by merge
                AssertionIncidentBehavior.RAISE_ON_FAIL,
                AssertionIncidentBehavior.RESOLVE_ON_PASS,
            ],
            tags=[
                TagUrn.from_string("urn:li:tag:smart_volume_assertion_tag")
            ],  # From fixture, preserved by merge
            created_by=CorpUserUrn.from_string(
                volume_assertion_entity_with_all_fields.source.created.actor
            ),
            created_at=datetime(
                2021, 1, 1, tzinfo=timezone.utc
            ),  # From fixture, preserved by merge
            updated_by=DEFAULT_CREATED_BY,
            updated_at=FROZEN_TIME,
            schedule=DEFAULT_SCHEDULE,
        ),
    )

    assert mock_upsert.call_count == 2

    called_with_assertion = mock_upsert.call_args_list[0][0][0]
    assert called_with_assertion.urn == any_assertion_urn
    assert isinstance(called_with_assertion.info, models.VolumeAssertionInfoClass)
    assert (
        called_with_assertion.info.type
        == models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL
    )
    assert called_with_assertion.info.entity == str(any_dataset_urn)
    assert (
        called_with_assertion.on_success
        == volume_assertion_entity_with_all_fields.on_success
    )  # From fixture, preserved by merge
    assert (
        called_with_assertion.on_failure
        == volume_assertion_entity_with_all_fields.on_failure
    )  # From fixture, preserved by merge
    assert (
        called_with_assertion.tags == volume_assertion_entity_with_all_fields.tags
    )  # From fixture, preserved by merge
    assert (
        called_with_assertion.source.created.time
        == volume_assertion_entity_with_all_fields.source.created.time
    )  # From fixture, preserved by merge
    assert (
        called_with_assertion.source.created.actor
        == volume_assertion_entity_with_all_fields.source.created.actor
    )  # From fixture, preserved by merge
    assert called_with_assertion.last_updated.time == make_ts_millis(
        FROZEN_TIME
    )  # New update time
    assert called_with_assertion.last_updated.actor == str(
        DEFAULT_CREATED_BY
    )  # New update actor

    called_with_monitor = mock_upsert.call_args_list[1][0][0]
    assert called_with_monitor.urn == any_monitor_urn
    assert called_with_monitor.info.type == models.MonitorTypeClass.ASSERTION
    assert called_with_monitor.info.status.mode == models.MonitorModeClass.ACTIVE
    assert called_with_monitor.info.assertionMonitor.assertions[0].assertion == str(
        any_assertion_urn
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[0].schedule
        == DEFAULT_SCHEDULE
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[0].parameters.type
        == models.AssertionEvaluationParametersTypeClass.DATASET_VOLUME
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[
            0
        ].parameters.datasetVolumeParameters.sourceType
        == models.DatasetVolumeSourceTypeClass.INFORMATION_SCHEMA
    )
    assert (
        called_with_monitor.info.assertionMonitor.settings.adjustmentSettings.sensitivity.level
        == volume_monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings.sensitivity.level
    )
    assert len(
        called_with_monitor.info.assertionMonitor.settings.adjustmentSettings.exclusionWindows
    ) == len(
        volume_monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings.exclusionWindows
    )
    assert (
        called_with_monitor.info.assertionMonitor.settings.adjustmentSettings.trainingDataLookbackWindowDays
        == volume_monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings.trainingDataLookbackWindowDays
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "detection_mechanism, expected_detection_mechanism",
    [
        pytest.param(
            DetectionMechanism.QUERY(additional_filter="id > 100"),
            DetectionMechanism.QUERY(additional_filter="id > 100"),
            id="query with additional filter",
        ),
        pytest.param(
            DetectionMechanism.QUERY(additional_filter=None),
            DetectionMechanism.QUERY(additional_filter=None),
            id="query without additional filter",
        ),
        pytest.param(
            DetectionMechanism.QUERY(),
            DetectionMechanism.QUERY(),
            id="query with default additional filter, defaulted to None",
        ),
        pytest.param(
            DetectionMechanism.INFORMATION_SCHEMA,
            DetectionMechanism.INFORMATION_SCHEMA,
            id="information_schema",
        ),
        pytest.param(
            DetectionMechanism.DATASET_PROFILE,
            DetectionMechanism.DATASET_PROFILE,
            id="dataset_profile",
        ),
    ],
)
def test_sync_smart_volume_assertion_valid_full_input(
    volume_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_monitor_urn: MonitorUrn,
    any_assertion_urn: AssertionUrn,
    volume_assertion_entity_with_all_fields: Assertion,
    detection_mechanism: DetectionMechanismInputTypes,
    expected_detection_mechanism: _DetectionMechanismTypes,
) -> None:
    """Test with all fields set to default values."""

    # MyPy assertions - we know these are not None from the fixtures
    assert volume_assertion_entity_with_all_fields.source is not None
    assert volume_assertion_entity_with_all_fields.source.created is not None
    assert volume_assertion_entity_with_all_fields.source.created.actor is not None

    # Arrange
    input_params = SmartVolumeAssertionUpsertInputParams(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        display_name="test_display_name",
        detection_mechanism=detection_mechanism,
        sensitivity=InferenceSensitivity.HIGH,  # Not default
        exclusion_windows=[
            FixedRangeExclusionWindow(
                start=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                end=datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
            )
        ],
        training_data_lookback_days=99,  # Not default
        incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],  # Not default
        tags=[TagUrn.from_string("urn:li:tag:test_tag")],  # Not default
        updated_by=CorpUserUrn.from_string("urn:li:corpuser:test_user"),  # Not default
        schedule=models.CronScheduleClass(
            cron="0 * * * *", timezone="UTC"
        ),  # Not default
    )
    mock_upsert = MagicMock()
    volume_stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    client = AssertionsClient(volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Act
    assertion = client.sync_smart_volume_assertion(**asdict(input_params))

    # Assert
    assert isinstance(
        input_params.schedule, models.CronScheduleClass
    )  # We know it's CronScheduleClass in this test
    _validate_volume_assertion_vs_input(
        assertion,
        input_params,
        SmartVolumeAssertionOutputParams(
            dataset_urn=input_params.dataset_urn,
            display_name=input_params.display_name or "",
            detection_mechanism=expected_detection_mechanism,
            sensitivity=InferenceSensitivity.HIGH,
            exclusion_windows=[
                FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    end=datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
                )
            ],
            training_data_lookback_days=99,
            incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
            tags=[TagUrn.from_string("urn:li:tag:test_tag")],
            created_by=CorpUserUrn.from_string(
                volume_assertion_entity_with_all_fields.source.created.actor
            ),
            created_at=datetime(
                2021, 1, 1, tzinfo=timezone.utc
            ),  # From fixture, preserved by merge
            updated_by=CorpUserUrn.from_string("urn:li:corpuser:test_user"),
            updated_at=FROZEN_TIME,
            schedule=input_params.schedule,
        ),
    )

    assert mock_upsert.call_count == 2

    called_with_assertion = mock_upsert.call_args_list[0][0][0]
    assert called_with_assertion.urn == any_assertion_urn
    assert isinstance(called_with_assertion.info, models.VolumeAssertionInfoClass)
    assert isinstance(called_with_assertion.info, models.VolumeAssertionInfoClass)
    assert (
        called_with_assertion.info.type
        == models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL
    )
    assert called_with_assertion.info.entity == str(any_dataset_urn)

    called_with_monitor = mock_upsert.call_args_list[1][0][0]
    assert called_with_monitor.urn == any_monitor_urn
    assert called_with_monitor.info.type == models.MonitorTypeClass.ASSERTION
    assert called_with_monitor.info.status.mode == models.MonitorModeClass.ACTIVE
    assert called_with_monitor.info.assertionMonitor.assertions[0].assertion == str(
        any_assertion_urn
    )
    # Schedule is updated from input parameters for volume assertions
    assert input_params.schedule is not None  # We know it's set in this test
    assert isinstance(
        input_params.schedule, models.CronScheduleClass
    )  # We know it's CronScheduleClass in this test
    assert (
        called_with_monitor.info.assertionMonitor.assertions[0].schedule.cron
        == input_params.schedule.cron
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[0].schedule.timezone
        == input_params.schedule.timezone
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[0].parameters.type
        == models.AssertionEvaluationParametersTypeClass.DATASET_VOLUME
    )


@pytest.mark.parametrize(
    "urn, expected_create_assertion_call_count, expected_upsert_entity_call_count",
    [
        pytest.param(None, 1, 0, id="urn_is_none"),
        pytest.param(_any_assertion_urn, 0, 2, id="urn_is_not_none"),
    ],
)
def test_sync_smart_volume_assertion_calls_create_assertion_if_urn_is_not_set(
    volume_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    urn: Optional[Union[str, AssertionUrn]],
    expected_create_assertion_call_count: int,
    expected_upsert_entity_call_count: int,
) -> None:
    client = AssertionsClient(volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert_entity = MagicMock()
    client.client.entities.upsert = mock_upsert_entity  # type: ignore[method-assign] # Override for testing
    mock_create_assertion = MagicMock()
    client._smart_volume_client._create_smart_volume_assertion = mock_create_assertion  # type: ignore[method-assign] # Override for testing
    client.sync_smart_volume_assertion(
        dataset_urn=any_dataset_urn,
        urn=urn,
    )
    assert mock_create_assertion.call_count == expected_create_assertion_call_count
    assert mock_upsert_entity.call_count == expected_upsert_entity_call_count
    if urn is None:
        assert mock_create_assertion.call_args[1]["dataset_urn"] == any_dataset_urn


@pytest.mark.parametrize(
    "updated_by, expected_updated_by",
    [
        pytest.param(None, DEFAULT_CREATED_BY, id="no_updated_by_set"),
        pytest.param(
            CorpUserUrn.from_string("urn:li:corpuser:test_user"),
            CorpUserUrn.from_string("urn:li:corpuser:test_user"),
            id="updated_by_set",
        ),
    ],
)
def test_sync_smart_volume_assertion_uses_default_if_updated_by_is_not_set(
    volume_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
    updated_by: Optional[CorpUserUrn],
    expected_updated_by: CorpUserUrn,
) -> None:
    client = AssertionsClient(volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create_assertion = MagicMock()
    client.client.entities.create = mock_create_assertion  # type: ignore[method-assign] # Override for testing
    mock_upsert = MagicMock()
    volume_stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing

    client.sync_smart_volume_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        updated_by=updated_by,
    )
    assertion_entity_upserted = mock_upsert.call_args_list[0][0][0]
    assert assertion_entity_upserted.last_updated.actor == str(expected_updated_by)
    assert mock_create_assertion.call_count == 0


def test_sync_smart_volume_assertion_uses_string_incident_behavior(
    volume_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
    volume_assertion_entity_with_all_fields: Assertion,
    volume_monitor_with_all_fields: Monitor,
) -> None:
    """Test that sync_smart_volume_assertion accepts string incident_behavior."""
    client = AssertionsClient(volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    client.client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing

    # Act - call with string incident_behavior
    assertion = client.sync_smart_volume_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        incident_behavior="raise_on_fail",  # String input
        updated_by="urn:li:corpuser:test_user",
    )

    # Assert - should convert string to enum list
    assert assertion.incident_behavior == [AssertionIncidentBehavior.RAISE_ON_FAIL]


def _validate_volume_assertion_vs_input(
    assertion: SmartVolumeAssertion,
    input_params: Union[
        SmartVolumeAssertionInputParams, SmartVolumeAssertionUpsertInputParams
    ],
    expected_output_params: SmartVolumeAssertionOutputParams,
) -> None:
    if input_params.display_name is not None:
        assert assertion.display_name == expected_output_params.display_name
    else:
        # For sync/upsert operations, we might preserve existing display name
        # For create operations, we generate new display names
        if (
            isinstance(input_params, SmartVolumeAssertionUpsertInputParams)
            and input_params.urn is not None
        ):
            # Sync/merge case - check against expected (might be preserved from existing)
            assert assertion.display_name == expected_output_params.display_name
        else:
            # Create case - check for generated display name
            assert assertion.display_name.startswith(
                DEFAULT_NAME_PREFIX
            )  # Generated display name
            assert len(assertion.display_name) == GENERATED_DISPLAY_NAME_LENGTH
    assert isinstance(
        assertion.detection_mechanism, _DETECTION_MECHANISM_CONCRETE_TYPES
    )
    assert (
        assertion.detection_mechanism.type
        == expected_output_params.detection_mechanism.type
    )
    assert assertion.sensitivity.value == expected_output_params.sensitivity.value
    assert assertion.exclusion_windows == expected_output_params.exclusion_windows
    assert (
        assertion.training_data_lookback_days
        == expected_output_params.training_data_lookback_days
    )
    assert assertion.incident_behavior == expected_output_params.incident_behavior
    assert assertion.tags == expected_output_params.tags
    assert assertion.created_by == expected_output_params.created_by
    assert assertion.created_at == expected_output_params.created_at
    assert assertion.updated_by == expected_output_params.updated_by
    assert assertion.updated_at == expected_output_params.updated_at
    assert assertion.schedule.cron == expected_output_params.schedule.cron
    assert assertion.schedule.timezone == expected_output_params.schedule.timezone


@dataclass
class RetrieveAssertionAndMonitorTestParams:
    assertion_exists: bool
    monitor_exists: bool


# Test retrieve_assertion_and_monitor_by_urn helper function directly
@pytest.mark.parametrize(
    "test_params",
    [
        pytest.param(
            RetrieveAssertionAndMonitorTestParams(
                assertion_exists=True,
                monitor_exists=True,
            ),
            id="both_exist",
        ),
        pytest.param(
            RetrieveAssertionAndMonitorTestParams(
                assertion_exists=True,
                monitor_exists=False,
            ),
            id="assertion_only",
        ),
        pytest.param(
            RetrieveAssertionAndMonitorTestParams(
                assertion_exists=False,
                monitor_exists=True,
            ),
            id="monitor_only",
        ),
        pytest.param(
            RetrieveAssertionAndMonitorTestParams(
                assertion_exists=False,
                monitor_exists=False,
            ),
            id="neither_exist",
        ),
    ],
)
def test_retrieve_assertion_and_monitor(
    freshness_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
    any_monitor_urn: MonitorUrn,
    freshness_assertion_entity_with_all_fields: Assertion,
    freshness_monitor_with_all_fields: Monitor,
    test_params: RetrieveAssertionAndMonitorTestParams,
) -> None:
    """Test retrieve_assertion_and_monitor_by_urn with different existence scenarios."""
    from acryl_datahub_cloud.sdk.assertion_client.helpers import (
        retrieve_assertion_and_monitor_by_urn,
    )

    # Mock entity retrieval based on existence flags
    def mock_get_entity(urn) -> Union[Assertion, Monitor, None]:  # type: ignore[no-untyped-def]
        if str(urn) == str(any_assertion_urn):
            if test_params.assertion_exists:
                return freshness_assertion_entity_with_all_fields
            else:
                raise ItemNotFoundError("Assertion not found")
        elif str(urn) == str(any_monitor_urn):
            if test_params.monitor_exists:
                return freshness_monitor_with_all_fields
            else:
                raise ItemNotFoundError("Monitor not found")
        else:
            raise ItemNotFoundError("Entity not found")

    freshness_stub_datahub_client.entities.get = mock_get_entity  # type: ignore[method-assign]

    # Execute
    maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
        retrieve_assertion_and_monitor_by_urn(
            freshness_stub_datahub_client,  # type: ignore[arg-type]
            any_assertion_urn,
            any_dataset_urn,
        )
    )

    # Assert results
    if test_params.assertion_exists:
        assert maybe_assertion_entity is not None
        assert maybe_assertion_entity.urn == any_assertion_urn
        assert isinstance(maybe_assertion_entity, Assertion)
    else:
        assert maybe_assertion_entity is None

    if test_params.monitor_exists:
        assert maybe_monitor_entity is not None
        assert maybe_monitor_entity.urn == any_monitor_urn
        assert isinstance(maybe_monitor_entity, Monitor)
    else:
        assert maybe_monitor_entity is None

    # Monitor URN should always be generated correctly
    assert monitor_urn == any_monitor_urn
    assert str(monitor_urn).endswith(f"({any_dataset_urn},{any_assertion_urn})")


@dataclass
class RetrieveAssertionAndMonitorClientTestParams:
    assertion_exists: bool
    monitor_exists: bool
    client_class: Type
    assertion_input_spec: Type
    urn_is_none: bool = False
    expected_error: Optional[Type[Exception]] = None
    expected_error_message: Optional[str] = None


# Test individual client _retrieve_assertion_and_monitor methods with input types
@pytest.mark.parametrize(
    "test_params",
    [
        pytest.param(
            RetrieveAssertionAndMonitorClientTestParams(
                assertion_exists=True,
                monitor_exists=True,
                client_class=SmartFreshnessAssertionClient,
                assertion_input_spec=_SmartFreshnessAssertionInput,
            ),
            id="both_exist_smart_freshness",
        ),
        pytest.param(
            RetrieveAssertionAndMonitorClientTestParams(
                assertion_exists=True,
                monitor_exists=True,
                client_class=SmartVolumeAssertionClient,
                assertion_input_spec=_SmartVolumeAssertionInput,
            ),
            id="both_exist_smart_volume",
        ),
        pytest.param(
            RetrieveAssertionAndMonitorClientTestParams(
                assertion_exists=True,
                monitor_exists=False,
                client_class=SmartFreshnessAssertionClient,
                assertion_input_spec=_SmartFreshnessAssertionInput,
            ),
            id="assertion_only_smart_freshness",
        ),
        pytest.param(
            RetrieveAssertionAndMonitorClientTestParams(
                assertion_exists=True,
                monitor_exists=False,
                client_class=SmartVolumeAssertionClient,
                assertion_input_spec=_SmartVolumeAssertionInput,
            ),
            id="assertion_only_smart_volume",
        ),
        pytest.param(
            RetrieveAssertionAndMonitorClientTestParams(
                assertion_exists=False,
                monitor_exists=True,
                client_class=SmartFreshnessAssertionClient,
                assertion_input_spec=_SmartFreshnessAssertionInput,
            ),
            id="monitor_only_smart_freshness",
        ),
        pytest.param(
            RetrieveAssertionAndMonitorClientTestParams(
                assertion_exists=False,
                monitor_exists=True,
                client_class=SmartVolumeAssertionClient,
                assertion_input_spec=_SmartVolumeAssertionInput,
            ),
            id="monitor_only_smart_volume",
        ),
        pytest.param(
            RetrieveAssertionAndMonitorClientTestParams(
                assertion_exists=False,
                monitor_exists=False,
                client_class=SmartFreshnessAssertionClient,
                assertion_input_spec=_SmartFreshnessAssertionInput,
            ),
            id="neither_exist_smart_freshness",
        ),
        pytest.param(
            RetrieveAssertionAndMonitorClientTestParams(
                assertion_exists=False,
                monitor_exists=False,
                client_class=SmartVolumeAssertionClient,
                assertion_input_spec=_SmartVolumeAssertionInput,
            ),
            id="neither_exist_smart_volume",
        ),
        pytest.param(
            RetrieveAssertionAndMonitorClientTestParams(
                assertion_exists=False,
                monitor_exists=False,
                client_class=SmartFreshnessAssertionClient,
                assertion_input_spec=_SmartFreshnessAssertionInput,
                urn_is_none=True,
                expected_error=AssertionError,
                expected_error_message="URN is required",
            ),
            id="urn_is_none_smart_freshness",
        ),
        pytest.param(
            RetrieveAssertionAndMonitorClientTestParams(
                assertion_exists=False,
                monitor_exists=False,
                client_class=SmartVolumeAssertionClient,
                assertion_input_spec=_SmartVolumeAssertionInput,
                urn_is_none=True,
                expected_error=AssertionError,
                expected_error_message="URN is required",
            ),
            id="urn_is_none_smart_volume",
        ),
    ],
)
def test_retrieve_assertion_and_monitor_with_client(
    freshness_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
    any_monitor_urn: MonitorUrn,
    freshness_assertion_entity_with_all_fields: Assertion,
    freshness_monitor_with_all_fields: Monitor,
    test_params: RetrieveAssertionAndMonitorClientTestParams,
) -> None:
    """Test individual client _retrieve_assertion_and_monitor with different input types."""
    # Create the specific client
    client = test_params.client_class(freshness_stub_datahub_client)  # type: ignore[arg-type]

    # Create mock assertion input using fixture values
    assertion_urn = None if test_params.urn_is_none else any_assertion_urn
    assertion_input = MagicMock(
        spec=test_params.assertion_input_spec,
        urn=assertion_urn,
        dataset_urn=any_dataset_urn,
    )

    # Mock entity retrieval based on existence flags
    def mock_get_entity(urn) -> Union[Assertion, Monitor, None]:  # type: ignore[no-untyped-def]
        if assertion_urn and str(urn) == str(assertion_urn):
            if test_params.assertion_exists:
                return freshness_assertion_entity_with_all_fields
            else:
                raise ItemNotFoundError("Assertion not found")
        elif str(urn) == str(any_monitor_urn):
            if test_params.monitor_exists:
                return freshness_monitor_with_all_fields
            else:
                raise ItemNotFoundError("Monitor not found")
        else:
            raise ItemNotFoundError("Entity not found")

    freshness_stub_datahub_client.entities.get = mock_get_entity  # type: ignore[method-assign]

    # Act & Assert
    if test_params.expected_error:
        with pytest.raises(
            test_params.expected_error, match=test_params.expected_error_message
        ):
            client._retrieve_assertion_and_monitor(assertion_input)
        return

    # Normal execution path
    maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
        client._retrieve_assertion_and_monitor(assertion_input)
    )

    # Assert results
    if test_params.assertion_exists:
        assert maybe_assertion_entity is not None
        assert maybe_assertion_entity.urn == assertion_urn
        assert isinstance(maybe_assertion_entity, Assertion)
    else:
        assert maybe_assertion_entity is None

    if test_params.monitor_exists:
        assert maybe_monitor_entity is not None
        assert maybe_monitor_entity.urn == any_monitor_urn
        assert isinstance(maybe_monitor_entity, Monitor)
    else:
        assert maybe_monitor_entity is None

    # Monitor URN should always be generated correctly
    assert monitor_urn == any_monitor_urn
    if assertion_urn:  # Only check format if urn is not None
        assert str(monitor_urn).endswith(f"({any_dataset_urn},{assertion_urn})")


# Freshness assertion sync tests


@dataclass
class FreshnessAssertionUpsertInputParams:
    dataset_urn: Union[str, DatasetUrn]
    urn: Optional[Union[str, AssertionUrn]] = None
    display_name: Optional[str] = None
    enabled: Optional[bool] = None
    detection_mechanism: Optional[DetectionMechanismInputTypes] = None
    incident_behavior: Optional[
        Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
    ] = None
    tags: Optional[TagsInputType] = None
    updated_by: Optional[CorpUserUrn] = None
    schedule: Optional[Union[str, models.CronScheduleClass]] = None
    freshness_schedule_check_type: Optional[
        Union[str, models.FreshnessAssertionScheduleTypeClass]
    ] = None
    lookback_window: Optional[TimeWindowSizeInputTypes] = None


@dataclass
class FreshnessAssertionOutputParams:
    dataset_urn: Union[str, DatasetUrn]
    display_name: str
    detection_mechanism: _DetectionMechanismTypes
    incident_behavior: list[AssertionIncidentBehavior]
    tags: TagsInputType
    created_by: CorpUserUrn
    created_at: datetime
    updated_by: CorpUserUrn
    updated_at: datetime
    schedule: models.CronScheduleClass
    freshness_schedule_check_type: Union[
        str, models.FreshnessAssertionScheduleTypeClass
    ]
    lookback_window: Optional[TimeWindowSizeInputTypes]


@freeze_time(FROZEN_TIME)
def test_sync_freshness_assertion_valid_simple_input(
    freshness_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_monitor_urn: MonitorUrn,
    any_assertion_urn: AssertionUrn,
    freshness_assertion_entity_with_all_fields: Assertion,
) -> None:
    """Test sync_freshness_assertion with minimal input parameters."""

    # MyPy assertions - we know these are not None from the fixtures
    assert freshness_assertion_entity_with_all_fields.description is not None
    assert freshness_assertion_entity_with_all_fields.source is not None
    assert freshness_assertion_entity_with_all_fields.source.created is not None
    assert freshness_assertion_entity_with_all_fields.source.created.actor is not None
    assert freshness_assertion_entity_with_all_fields.source.created.time is not None

    # Arrange
    input_params = FreshnessAssertionUpsertInputParams(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
    )
    mock_upsert = MagicMock()
    freshness_stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Act
    assertion = client.sync_freshness_assertion(**asdict(input_params))

    # Assert
    _validate_freshness_assertion_vs_input(
        assertion,
        input_params,
        FreshnessAssertionOutputParams(
            dataset_urn=input_params.dataset_urn,
            display_name=freshness_assertion_entity_with_all_fields.description,  # From fixture, preserved by merge
            detection_mechanism=DetectionMechanism.LAST_MODIFIED_COLUMN(
                column_name="field"
            ),  # From fixture
            incident_behavior=[  # From fixture, preserved by merge
                AssertionIncidentBehavior.RAISE_ON_FAIL,
                AssertionIncidentBehavior.RESOLVE_ON_PASS,
            ],
            tags=[
                TagUrn.from_string("urn:li:tag:smart_freshness_assertion_tag")
            ],  # From fixture, preserved by merge
            created_by=CorpUserUrn.from_string(
                freshness_assertion_entity_with_all_fields.source.created.actor
            ),
            created_at=datetime(
                2021, 1, 1, tzinfo=timezone.utc
            ),  # From fixture, preserved by merge
            updated_by=DEFAULT_CREATED_BY,
            updated_at=FROZEN_TIME,
            schedule=DEFAULT_SCHEDULE,
            freshness_schedule_check_type=models.FreshnessAssertionScheduleTypeClass.SINCE_THE_LAST_CHECK,
            lookback_window=None,
        ),
    )

    assert mock_upsert.call_count == 2

    called_with_assertion = mock_upsert.call_args_list[0][0][0]
    assert called_with_assertion.urn == any_assertion_urn
    assert isinstance(called_with_assertion.info, models.FreshnessAssertionInfoClass)
    assert (
        called_with_assertion.info.type
        == models.FreshnessAssertionTypeClass.DATASET_CHANGE
    )
    assert called_with_assertion.info.entity == str(any_dataset_urn)

    called_with_monitor = mock_upsert.call_args_list[1][0][0]
    assert called_with_monitor.urn == any_monitor_urn
    assert called_with_monitor.info.type == models.MonitorTypeClass.ASSERTION
    assert called_with_monitor.info.status.mode == models.MonitorModeClass.ACTIVE


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "detection_mechanism, expected_detection_mechanism",
    [
        pytest.param(
            DetectionMechanism.INFORMATION_SCHEMA,
            DetectionMechanism.INFORMATION_SCHEMA,
            id="information_schema",
        ),
        pytest.param(
            DetectionMechanism.AUDIT_LOG,
            DetectionMechanism.AUDIT_LOG,
            id="audit_log",
        ),
        pytest.param(
            DetectionMechanism.DATAHUB_OPERATION,
            DetectionMechanism.DATAHUB_OPERATION,
            id="datahub_operation",
        ),
        pytest.param(
            DetectionMechanism.LAST_MODIFIED_COLUMN(column_name="field"),
            DetectionMechanism.LAST_MODIFIED_COLUMN(column_name="field"),
            id="last_modified_column",
        ),
    ],
)
def test_sync_freshness_assertion_valid_full_input(
    freshness_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_monitor_urn: MonitorUrn,
    any_assertion_urn: AssertionUrn,
    freshness_assertion_entity_with_all_fields: Assertion,
    detection_mechanism: DetectionMechanismInputTypes,
    expected_detection_mechanism: _DetectionMechanismTypes,
) -> None:
    """Test sync_freshness_assertion with full input parameters."""

    # MyPy assertions - we know these are not None from the fixtures
    assert freshness_assertion_entity_with_all_fields.source is not None
    assert freshness_assertion_entity_with_all_fields.source.created is not None
    assert freshness_assertion_entity_with_all_fields.source.created.actor is not None

    # Arrange
    input_params = FreshnessAssertionUpsertInputParams(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        display_name="test_display_name",
        enabled=True,
        detection_mechanism=detection_mechanism,
        incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],  # Not default
        tags=[TagUrn.from_string("urn:li:tag:test_tag")],  # Not default
        updated_by=CorpUserUrn.from_string("urn:li:corpuser:test_user"),  # Not default
        schedule=models.CronScheduleClass(
            cron="0 0 * * *", timezone="UTC"
        ),  # Daily schedule
        freshness_schedule_check_type=models.FreshnessAssertionScheduleTypeClass.SINCE_THE_LAST_CHECK,
    )
    mock_upsert = MagicMock()
    freshness_stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Act
    assertion = client.sync_freshness_assertion(**asdict(input_params))

    # Assert
    assert isinstance(
        input_params.schedule, models.CronScheduleClass
    )  # We know it's CronScheduleClass in this test
    _validate_freshness_assertion_vs_input(
        assertion,
        input_params,
        FreshnessAssertionOutputParams(
            dataset_urn=input_params.dataset_urn,
            display_name=input_params.display_name or "",
            detection_mechanism=expected_detection_mechanism,
            incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
            tags=[TagUrn.from_string("urn:li:tag:test_tag")],
            created_by=CorpUserUrn.from_string(
                freshness_assertion_entity_with_all_fields.source.created.actor
            ),
            created_at=datetime(
                2021, 1, 1, tzinfo=timezone.utc
            ),  # From fixture, preserved by merge
            updated_by=CorpUserUrn.from_string("urn:li:corpuser:test_user"),
            updated_at=FROZEN_TIME,
            schedule=input_params.schedule,
            freshness_schedule_check_type=models.FreshnessAssertionScheduleTypeClass.SINCE_THE_LAST_CHECK,
            lookback_window=None,
        ),
    )

    assert mock_upsert.call_count == 2

    called_with_assertion = mock_upsert.call_args_list[0][0][0]
    assert called_with_assertion.urn == any_assertion_urn
    assert isinstance(called_with_assertion.info, models.FreshnessAssertionInfoClass)
    assert (
        called_with_assertion.info.type
        == models.FreshnessAssertionTypeClass.DATASET_CHANGE
    )
    assert called_with_assertion.info.entity == str(any_dataset_urn)

    called_with_monitor = mock_upsert.call_args_list[1][0][0]
    assert called_with_monitor.urn == any_monitor_urn
    assert called_with_monitor.info.type == models.MonitorTypeClass.ASSERTION
    assert called_with_monitor.info.status.mode == models.MonitorModeClass.ACTIVE


@pytest.mark.parametrize(
    "urn, expected_create_assertion_call_count, expected_upsert_entity_call_count",
    [
        pytest.param(None, 1, 0, id="urn_is_none"),
        pytest.param(_any_assertion_urn, 0, 2, id="urn_is_not_none"),
    ],
)
def test_sync_freshness_assertion_calls_create_assertion_if_urn_is_not_set(
    freshness_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    urn: Optional[Union[str, AssertionUrn]],
    expected_create_assertion_call_count: int,
    expected_upsert_entity_call_count: int,
) -> None:
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert_entity = MagicMock()
    freshness_stub_datahub_client.entities.upsert = mock_upsert_entity  # type: ignore[method-assign] # Override for testing
    mock_create_assertion = MagicMock()
    client._freshness_client._create_freshness_assertion = mock_create_assertion  # type: ignore[method-assign] # Override for testing
    client.sync_freshness_assertion(
        dataset_urn=any_dataset_urn,
        urn=urn,
    )
    assert mock_create_assertion.call_count == expected_create_assertion_call_count
    assert mock_upsert_entity.call_count == expected_upsert_entity_call_count
    if urn is None:
        assert mock_create_assertion.call_args[1]["dataset_urn"] == any_dataset_urn


@pytest.mark.parametrize(
    "updated_by, expected_updated_by",
    [
        pytest.param(None, DEFAULT_CREATED_BY, id="no_updated_by_set"),
        pytest.param(
            CorpUserUrn.from_string("urn:li:corpuser:test_user"),
            CorpUserUrn.from_string("urn:li:corpuser:test_user"),
            id="updated_by_set",
        ),
    ],
)
def test_sync_freshness_assertion_uses_default_if_updated_by_is_not_set(
    freshness_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
    updated_by: Optional[CorpUserUrn],
    expected_updated_by: CorpUserUrn,
) -> None:
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create_assertion = MagicMock()
    client.client.entities.create = mock_create_assertion  # type: ignore[method-assign] # Override for testing
    mock_upsert = MagicMock()
    freshness_stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing

    client.sync_freshness_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        updated_by=updated_by,
    )
    assertion_entity_upserted = mock_upsert.call_args_list[0][0][0]
    assert assertion_entity_upserted.last_updated.actor == str(expected_updated_by)
    assert mock_create_assertion.call_count == 0


def _validate_freshness_assertion_vs_input(
    assertion: FreshnessAssertion,
    input_params: FreshnessAssertionUpsertInputParams,
    expected_output_params: FreshnessAssertionOutputParams,
) -> None:
    if input_params.display_name is not None:
        assert assertion.display_name == expected_output_params.display_name
    else:
        # For sync/upsert operations, we might preserve existing display name
        if input_params.urn is not None:
            # Sync/merge case - check against expected (might be preserved from existing)
            assert assertion.display_name == expected_output_params.display_name
        else:
            # Create case - check for generated display name
            assert assertion.display_name.startswith(
                "New Assertion"
            )  # Generated display name
            assert len(assertion.display_name) == GENERATED_DISPLAY_NAME_LENGTH
    assert isinstance(
        assertion.detection_mechanism, _DETECTION_MECHANISM_CONCRETE_TYPES
    )
    assert (
        assertion.detection_mechanism.type
        == expected_output_params.detection_mechanism.type
    )
    assert assertion.incident_behavior == expected_output_params.incident_behavior
    assert assertion.tags == expected_output_params.tags
    assert assertion.created_by == expected_output_params.created_by
    assert assertion.created_at == expected_output_params.created_at
    assert assertion.updated_by == expected_output_params.updated_by
    assert assertion.updated_at == expected_output_params.updated_at
    assert assertion.schedule.cron == expected_output_params.schedule.cron
    assert assertion.schedule.timezone == expected_output_params.schedule.timezone
    assert (
        assertion._freshness_schedule_check_type
        == expected_output_params.freshness_schedule_check_type
    )
    if expected_output_params.lookback_window is not None:
        assert assertion.lookback_window == expected_output_params.lookback_window
    else:
        assert assertion.lookback_window is None


def test_sync_freshness_assertion_uses_string_incident_behavior(
    freshness_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test that sync_freshness_assertion accepts string incident_behavior."""
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create = MagicMock()
    client.client.entities.create = mock_create  # type: ignore[method-assign] # Override for testing

    # Act - call with string incident_behavior (without URN to trigger create path)
    assertion = client.sync_freshness_assertion(
        dataset_urn=_any_dataset_urn,
        incident_behavior="raise_on_fail",  # String input
    )

    # Assert that the method completed successfully
    assert assertion is not None
    assert mock_create.call_count == 2  # Creates both assertion and monitor


def test_create_freshness_assertion_uses_string_incident_behavior(
    freshness_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test that _create_freshness_assertion accepts string incident_behavior."""
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create = MagicMock()
    client.client.entities.create = mock_create  # type: ignore[method-assign] # Override for testing

    # Act - call with string incident_behavior
    assertion = client._freshness_client._create_freshness_assertion(
        dataset_urn=_any_dataset_urn,
        incident_behavior="resolve_on_pass",  # String input
        created_by="urn:li:corpuser:test_user",
    )

    # Assert that the method completed successfully
    assert assertion is not None
    assert mock_create.call_count == 2  # Creates both assertion and monitor


def test_sync_volume_assertion_uses_string_incident_behavior(
    freshness_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test that sync_volume_assertion accepts string incident_behavior."""
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create = MagicMock()
    client.client.entities.create = mock_create  # type: ignore[method-assign] # Override for testing

    # Act - call with string incident_behavior (without URN to trigger create path)
    assertion = client.sync_volume_assertion(
        dataset_urn=_any_dataset_urn,
        criteria_condition="ROW_COUNT_IS_WITHIN_A_RANGE",
        criteria_parameters=(100, 1000),
        incident_behavior="raise_on_fail",  # String input
    )

    # Assert that the method completed successfully
    assert assertion is not None
    assert mock_create.call_count == 2  # Creates both assertion and monitor


def test_create_volume_assertion_uses_string_incident_behavior(
    freshness_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test that _create_volume_assertion accepts string incident_behavior."""
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create = MagicMock()
    client.client.entities.create = mock_create  # type: ignore[method-assign] # Override for testing

    # Act - call with string incident_behavior
    assertion = client._volume_client._create_volume_assertion(
        dataset_urn=_any_dataset_urn,
        criteria_condition="ROW_COUNT_IS_WITHIN_A_RANGE",
        criteria_parameters=(100, 1000),
        incident_behavior="resolve_on_pass",  # String input
        created_by="urn:li:corpuser:test_user",
    )

    # Assert that the method completed successfully
    assert assertion is not None
    assert mock_create.call_count == 2  # Creates both assertion and monitor


# SQL Assertion tests


@dataclass
class SqlAssertionInputParams:
    dataset_urn: Union[str, DatasetUrn]
    statement: str
    criteria_condition: Union[SqlAssertionCondition, str]
    criteria_parameters: Union[
        Union[float, int], tuple[Union[float, int], Union[float, int]]
    ]
    display_name: Optional[str] = None
    enabled: Optional[bool] = None
    incident_behavior: Optional[
        Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
    ] = None
    tags: Optional[TagsInputType] = None
    created_by: Optional[CorpUserUrn] = None
    schedule: Optional[Union[str, models.CronScheduleClass]] = None


@dataclass
class SqlAssertionUpsertInputParams:
    dataset_urn: Union[str, DatasetUrn]
    statement: str
    criteria: SqlAssertionCriteria
    urn: Optional[Union[str, AssertionUrn]] = None
    display_name: Optional[str] = None
    enabled: Optional[bool] = None
    incident_behavior: Optional[
        Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
    ] = None
    tags: Optional[TagsInputType] = None
    updated_by: Optional[CorpUserUrn] = None
    schedule: Optional[Union[str, models.CronScheduleClass]] = None


@dataclass
class SqlAssertionOutputParams:
    dataset_urn: Union[str, DatasetUrn]
    statement: str
    criteria_condition: Union[SqlAssertionCondition, str]
    criteria_parameters: Union[
        Union[float, int], tuple[Union[float, int], Union[float, int]]
    ]
    display_name: str
    incident_behavior: list[AssertionIncidentBehavior]
    tags: TagsInputType
    created_by: CorpUserUrn
    created_at: datetime
    updated_by: CorpUserUrn
    updated_at: datetime
    schedule: models.CronScheduleClass


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "input_params, expected_output_params",
    [
        pytest.param(
            SqlAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                statement="SELECT COUNT(*) FROM table",
                criteria_condition=SqlAssertionCondition.IS_GREATER_THAN,
                criteria_parameters=100,
            ),
            SqlAssertionOutputParams(
                dataset_urn=_any_dataset_urn,
                statement="SELECT COUNT(*) FROM table",
                criteria_condition=SqlAssertionCondition.IS_GREATER_THAN,
                criteria_parameters=100,
                display_name="New Assertion",
                incident_behavior=[],
                tags=[],
                created_by=DEFAULT_CREATED_BY,
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                updated_by=DEFAULT_CREATED_BY,
                updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                schedule=DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
            ),
            id="minimal_valid_input_metric",
        ),
        pytest.param(
            SqlAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                statement="SELECT COUNT(*) FROM table",
                criteria_condition=SqlAssertionCondition.GROWS_WITHIN_A_RANGE_PERCENTAGE,
                criteria_parameters=(-10.0, 10.0),
                display_name="Test SQL Assertion",
                enabled=True,
                incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
                tags=["urn:li:tag:my_tag_1"],
                created_by=_any_user,
                schedule=models.CronScheduleClass(cron="0 * * * *", timezone="UTC"),
            ),
            SqlAssertionOutputParams(
                dataset_urn=_any_dataset_urn,
                statement="SELECT COUNT(*) FROM table",
                criteria_condition=SqlAssertionCondition.GROWS_WITHIN_A_RANGE_PERCENTAGE,
                criteria_parameters=(-10.0, 10.0),
                display_name="Test SQL Assertion",
                incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
                tags=[TagUrn.from_string("urn:li:tag:my_tag_1")],
                created_by=_any_user,
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                updated_by=_any_user,
                updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                schedule=models.CronScheduleClass(cron="0 * * * *", timezone="UTC"),
            ),
            id="full_input_metric_change",
        ),
    ],
)
def test_create_sql_assertion_valid_input(
    freshness_stub_datahub_client: StubDataHubClient,
    input_params: SqlAssertionInputParams,
    expected_output_params: SqlAssertionOutputParams,
) -> None:
    # Arrange
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    # Act
    assertion = client._sql_client._create_sql_assertion(**asdict(input_params))

    # Assert
    _validate_sql_assertion_vs_input(assertion, input_params, expected_output_params)


def test_create_sql_assertion_entities_client_called(
    freshness_stub_datahub_client: StubDataHubClient,
) -> None:
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create = MagicMock()
    client.client.entities.create = mock_create  # type: ignore[method-assign] # Override for testing

    assertion = client._sql_client._create_sql_assertion(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        statement="SELECT COUNT(*) FROM table",
        criteria_condition=SqlAssertionCondition.IS_GREATER_THAN,
        criteria_parameters=100,
        incident_behavior=None,
        tags=None,
    )
    assert mock_create.call_count == 2
    assert assertion


@pytest.mark.parametrize(
    "input_params, error_type, expected_error_message",
    [
        pytest.param(
            SqlAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                statement="SELECT COUNT(*) FROM table",
                criteria_condition=SqlAssertionCondition.IS_WITHIN_A_RANGE,
                criteria_parameters=100,  # Single value for BETWEEN operator
            ),
            SDKUsageError,
            "The parameter value of SqlAssertionCriteria must be a tuple range for condition",
            id="between_operator_single_value",
        ),
    ],
)
def test_create_sql_assertion_invalid_input(
    freshness_stub_datahub_client: StubDataHubClient,
    input_params: SqlAssertionInputParams,
    error_type: Type[Exception],
    expected_error_message: str,
) -> None:
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing
    with pytest.raises(error_type, match=expected_error_message):
        client._sql_client._create_sql_assertion(**asdict(input_params))


@freeze_time(FROZEN_TIME)
def test_sync_sql_assertion_creates_new_when_urn_not_provided(
    freshness_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test sync_sql_assertion creates new assertion when no URN is provided."""
    # Mock the _create_sql_assertion method to return a dummy SqlAssertion
    mock_sql_assertion = MagicMock()
    mock_sql_assertion.urn = "urn:li:assertion:new_sql_assertion"
    mock_sql_assertion.statement = "SELECT COUNT(*) FROM table"
    mock_sql_assertion.criteria = SqlAssertionCriteria(
        condition=SqlAssertionCondition.IS_GREATER_THAN,
        parameters=100,
    )

    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Mock the _create_sql_assertion method to return our mock
    mock_create_sql_assertion = MagicMock(return_value=mock_sql_assertion)
    client._sql_client._create_sql_assertion = mock_create_sql_assertion  # type: ignore[method-assign]

    # Act - Call without URN, which should trigger creation path
    assertion = client.sync_sql_assertion(
        dataset_urn=any_dataset_urn,
        statement="SELECT COUNT(*) FROM table",
        criteria_condition=SqlAssertionCondition.IS_GREATER_THAN,
        criteria_parameters=100,
    )

    # Assert - should call _create_sql_assertion
    mock_create_sql_assertion.assert_called_once()
    call_args = mock_create_sql_assertion.call_args[1]
    assert call_args["dataset_urn"] == any_dataset_urn
    assert call_args["statement"] == "SELECT COUNT(*) FROM table"
    assert call_args["criteria_condition"] == SqlAssertionCondition.IS_GREATER_THAN
    assert call_args["criteria_parameters"] == 100

    # Should return the mocked assertion
    assert assertion == mock_sql_assertion


def test_sync_sql_assertion_calls_create_assertion_when_urn_is_none(
    freshness_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that sync_sql_assertion calls _create_sql_assertion when urn is None."""
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert_entity = MagicMock()
    freshness_stub_datahub_client.entities.upsert = mock_upsert_entity  # type: ignore[method-assign] # Override for testing
    mock_create_assertion = MagicMock()
    client._sql_client._create_sql_assertion = mock_create_assertion  # type: ignore[method-assign] # Override for testing

    client.sync_sql_assertion(
        dataset_urn=any_dataset_urn,
        urn=None,  # This should trigger creation path
        statement="SELECT COUNT(*) FROM table",
        criteria_condition=SqlAssertionCondition.IS_GREATER_THAN,
        criteria_parameters=100,
    )
    assert mock_create_assertion.call_count == 1
    assert mock_upsert_entity.call_count == 0  # Should not call upsert when creating
    assert mock_create_assertion.call_args[1]["dataset_urn"] == any_dataset_urn


def test_sync_sql_assertion_uses_default_updated_by_when_none_provided(
    freshness_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that sync_sql_assertion uses default updated_by when none is provided."""
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Mock the _create_sql_assertion method to capture the call
    mock_sql_assertion = MagicMock()
    mock_create_assertion = MagicMock(return_value=mock_sql_assertion)
    client._sql_client._create_sql_assertion = mock_create_assertion  # type: ignore[method-assign] # Override for testing

    client.sync_sql_assertion(
        dataset_urn=any_dataset_urn,
        urn=None,  # Use None to trigger creation path
        statement="SELECT COUNT(*) FROM table",
        criteria_condition=SqlAssertionCondition.IS_GREATER_THAN,
        criteria_parameters=100,
        updated_by=None,  # This should default to DEFAULT_CREATED_BY
    )

    # Check that the created_by parameter was set to the default
    call_args = mock_create_assertion.call_args[1]
    assert call_args["created_by"] == DEFAULT_CREATED_BY


def _validate_sql_assertion_vs_input(
    assertion: SqlAssertion,
    input_params: Union[SqlAssertionInputParams, SqlAssertionUpsertInputParams],
    expected_output_params: SqlAssertionOutputParams,
) -> None:
    if input_params.display_name is not None:
        assert assertion.display_name == expected_output_params.display_name
    else:
        # For sync/upsert operations, we might preserve existing display name
        if hasattr(input_params, "urn") and input_params.urn is not None:
            # Sync/merge case - check against expected (might be preserved from existing)
            assert assertion.display_name == expected_output_params.display_name
        else:
            # Create case - check for generated display name
            assert assertion.display_name.startswith(
                "New Assertion"
            )  # Generated display name
            assert len(assertion.display_name) == GENERATED_DISPLAY_NAME_LENGTH

    assert assertion.statement == expected_output_params.statement
    assert assertion.criteria_condition == expected_output_params.criteria_condition
    assert assertion.criteria_parameters == expected_output_params.criteria_parameters
    assert assertion.incident_behavior == expected_output_params.incident_behavior
    assert assertion.tags == expected_output_params.tags
    assert assertion.created_by == expected_output_params.created_by
    assert assertion.created_at == expected_output_params.created_at
    assert assertion.updated_by == expected_output_params.updated_by
    assert assertion.updated_at == expected_output_params.updated_at
    assert assertion.schedule.cron == expected_output_params.schedule.cron
    assert assertion.schedule.timezone == expected_output_params.schedule.timezone


def test_sync_sql_assertion_uses_string_incident_behavior(
    freshness_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test that sync_sql_assertion accepts string incident_behavior."""
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create = MagicMock()
    client.client.entities.create = mock_create  # type: ignore[method-assign] # Override for testing

    # Act - call with string incident_behavior (without URN to trigger create path)
    assertion = client.sync_sql_assertion(
        dataset_urn=_any_dataset_urn,
        statement="SELECT COUNT(*) FROM table",
        criteria_condition="IS_EQUAL_TO",
        criteria_parameters=0,
        incident_behavior="raise_on_fail",  # String input
    )

    # Assert that the method completed successfully
    assert assertion is not None
    assert mock_create.call_count == 2  # Creates both assertion and monitor


def test_sync_sql_assertion_missing_required_params_for_creation(
    freshness_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that sync_sql_assertion raises SDKUsageError when required params are missing for creation (urn=None)."""
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Test missing statement
    with pytest.raises(
        SDKUsageError, match="statement is required when creating a new assertion"
    ):
        client.sync_sql_assertion(
            dataset_urn=any_dataset_urn,
            # statement missing
            criteria_condition=SqlAssertionCondition.IS_GREATER_THAN,
            criteria_parameters=100,
        )

    # Test missing criteria_condition
    with pytest.raises(
        SDKUsageError,
        match="criteria_condition is required when creating a new assertion",
    ):
        client.sync_sql_assertion(
            dataset_urn=any_dataset_urn,
            statement="SELECT COUNT(*) FROM test_table",
            # criteria_condition missing
            criteria_parameters=100,
        )

    # Test missing criteria_parameters
    with pytest.raises(
        SDKUsageError,
        match="criteria_parameters is required when creating a new assertion",
    ):
        client.sync_sql_assertion(
            dataset_urn=any_dataset_urn,
            statement="SELECT COUNT(*) FROM test_table",
            criteria_condition=SqlAssertionCondition.IS_GREATER_THAN,
            # criteria_parameters missing
        )


def test_sync_sql_assertion_optional_params_for_merge(
    sql_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that sync_sql_assertion allows optional params when urn is provided (merge scenario)."""

    client = AssertionsClient(sql_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing

    # This should not raise an error since urn is provided and we can fetch missing params from existing assertion
    result = client.sync_sql_assertion(
        dataset_urn=any_dataset_urn,
        urn="urn:li:assertion:sql_assertion",  # Use the URN from conftest fixtures
        # statement, criteria_condition, criteria_parameters are optional in merge scenario
    )

    # Verify that the values were correctly fetched from the existing assertion
    assert result.statement == "SELECT COUNT(*) FROM test_table"
    assert result._criteria.condition == SqlAssertionCondition.IS_GREATER_THAN
    assert result._criteria.parameters == 100


def test_create_sql_assertion_uses_string_incident_behavior(
    freshness_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test that _create_sql_assertion accepts string incident_behavior."""
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create = MagicMock()
    client.client.entities.create = mock_create  # type: ignore[method-assign] # Override for testing

    # Act - call with string incident_behavior
    assertion = client._sql_client._create_sql_assertion(
        dataset_urn=_any_dataset_urn,
        statement="SELECT COUNT(*) FROM table",
        criteria_condition="IS_EQUAL_TO",
        criteria_parameters=0,
        incident_behavior="resolve_on_pass",  # String input
        tags=None,
        created_by="urn:li:corpuser:test_user",
    )

    # Assert that the method completed successfully
    assert assertion is not None
    assert mock_create.call_count == 2  # Creates both assertion and monitor


# Smart column metric tests
@dataclass
class SmartColumnMetricAssertionInputParams:
    dataset_urn: Union[str, DatasetUrn]
    column_name: str
    metric_type: MetricInputType
    display_name: Optional[str] = None
    detection_mechanism: Optional[DetectionMechanismInputTypes] = None
    sensitivity: Optional[InferenceSensitivity] = None
    exclusion_windows: Optional[list[FixedRangeExclusionWindow]] = None
    training_data_lookback_days: Optional[int] = None
    incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None
    tags: Optional[TagsInputType] = None
    updated_by: Optional[CorpUserUrn] = None
    schedule: Optional[Union[str, models.CronScheduleClass]] = None


@dataclass
class SmartColumnMetricAssertionOutputParams:
    dataset_urn: Union[str, DatasetUrn]
    column_name: str
    metric_type: MetricInputType
    display_name: str
    detection_mechanism: _DetectionMechanismTypes
    sensitivity: InferenceSensitivity
    exclusion_windows: list[FixedRangeExclusionWindow]
    training_data_lookback_days: int
    incident_behavior: list[AssertionIncidentBehavior]
    tags: TagsInputType
    created_by: CorpUserUrn
    created_at: datetime
    updated_by: CorpUserUrn
    updated_at: datetime
    schedule: models.CronScheduleClass
    # For smart assertions, operator is always "between" and criteria_parameters is always (0, 0)
    operator: OperatorInputType = "between"
    criteria_parameters: tuple[int, int] = (0, 0)


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "input_params, expected_output_params",
    [
        pytest.param(
            SmartColumnMetricAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
            ),
            SmartColumnMetricAssertionOutputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
                operator="less_than",
                display_name=DEFAULT_NAME_PREFIX,
                detection_mechanism=DetectionMechanism.ALL_ROWS_QUERY(),
                sensitivity=DEFAULT_SENSITIVITY,
                exclusion_windows=[],
                training_data_lookback_days=ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
                incident_behavior=[],
                tags=[],
                created_by=DEFAULT_CREATED_BY,
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                updated_by=DEFAULT_CREATED_BY,
                updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                schedule=DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
            ),
            id="minimal_valid_input_single_value",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
            ),
            SmartColumnMetricAssertionOutputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
                operator="between",
                display_name=DEFAULT_NAME_PREFIX,
                detection_mechanism=DetectionMechanism.ALL_ROWS_QUERY(),
                sensitivity=DEFAULT_SENSITIVITY,
                exclusion_windows=[],
                training_data_lookback_days=ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
                incident_behavior=[],
                tags=[],
                created_by=DEFAULT_CREATED_BY,
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                updated_by=DEFAULT_CREATED_BY,
                updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                schedule=DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
            ),
            id="minimal_valid_input_range",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
                display_name="Test Assertion",
                detection_mechanism=DetectionMechanism.ALL_ROWS_QUERY(),
                sensitivity=InferenceSensitivity.LOW,
                exclusion_windows=[
                    FixedRangeExclusionWindow(
                        start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                        end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                    )
                ],
                training_data_lookback_days=30,
                incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
                tags=["urn:li:tag:my_tag_1"],
                updated_by=_any_user,
                schedule=models.CronScheduleClass(cron="0 * * * *", timezone="UTC"),
            ),
            SmartColumnMetricAssertionOutputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
                operator="less_than",
                display_name="Test Assertion",
                detection_mechanism=DetectionMechanism.ALL_ROWS_QUERY(),
                sensitivity=InferenceSensitivity.LOW,
                exclusion_windows=[
                    FixedRangeExclusionWindow(
                        start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                        end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                    )
                ],
                training_data_lookback_days=30,
                incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
                tags=[TagUrn.from_string("urn:li:tag:my_tag_1")],
                created_by=_any_user,
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                updated_by=_any_user,
                updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                schedule=models.CronScheduleClass(cron="0 * * * *", timezone="UTC"),
            ),
            id="full_valid_input_single_value",
        ),
        # Test string incident_behavior input for smart column metric assertion
        pytest.param(
            SmartColumnMetricAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
                display_name="Test Smart Column Metric Assertion String Incident Behavior",
                detection_mechanism=DetectionMechanism.ALL_ROWS_QUERY(),
                sensitivity=InferenceSensitivity.LOW,
                training_data_lookback_days=30,
                incident_behavior="raise_on_fail",  # String input
                tags=["urn:li:tag:my_tag_1"],
                updated_by=_any_user,
                schedule=models.CronScheduleClass(cron="0 * * * *", timezone="UTC"),
            ),
            SmartColumnMetricAssertionOutputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
                operator="less_than",
                display_name="Test Smart Column Metric Assertion String Incident Behavior",
                detection_mechanism=DetectionMechanism.ALL_ROWS_QUERY(),
                sensitivity=InferenceSensitivity.LOW,
                exclusion_windows=[],
                training_data_lookback_days=30,
                incident_behavior=[
                    AssertionIncidentBehavior.RAISE_ON_FAIL
                ],  # Expected enum output
                tags=[TagUrn.from_string("urn:li:tag:my_tag_1")],
                created_by=_any_user,
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                updated_by=_any_user,
                updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
                schedule=models.CronScheduleClass(cron="0 * * * *", timezone="UTC"),
            ),
            id="string_incident_behavior_input_smart_column_metric",
        ),
    ],
)
def test_sync_smart_column_metric_assertion_valid_simple_input(
    freshness_stub_datahub_client: StubDataHubClient,
    input_params: SmartColumnMetricAssertionInputParams,
    expected_output_params: SmartColumnMetricAssertionOutputParams,
) -> None:
    # Arrange
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    # Act
    # Filter out operator and criteria_parameters as they are no longer supported for smart assertions
    params_dict = asdict(input_params)
    params_dict.pop("operator", None)
    params_dict.pop("criteria_parameters", None)
    assertion = client.sync_smart_column_metric_assertion(**params_dict)

    # Assert
    _validate_column_metric_assertion_vs_input(
        assertion, input_params, expected_output_params
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "detection_mechanism, expected_detection_mechanism",
    [
        pytest.param(
            DetectionMechanism.ALL_ROWS_QUERY(),
            DetectionMechanism.ALL_ROWS_QUERY(),
            id="all_rows_query",
        ),
        pytest.param(
            DetectionMechanism.ALL_ROWS_QUERY(additional_filter="amount > 1000"),
            DetectionMechanism.ALL_ROWS_QUERY(additional_filter="amount > 1000"),
            id="all_rows_query_with_filter",
        ),
        pytest.param(
            DetectionMechanism.CHANGED_ROWS_QUERY(column_name="updated_at"),
            DetectionMechanism.CHANGED_ROWS_QUERY(column_name="updated_at"),
            id="changed_rows_query",
        ),
        pytest.param(
            DetectionMechanism.CHANGED_ROWS_QUERY(
                column_name="updated_at", additional_filter="amount > 1000"
            ),
            DetectionMechanism.CHANGED_ROWS_QUERY(
                column_name="updated_at", additional_filter="amount > 1000"
            ),
            id="changed_rows_query_with_filter",
        ),
        pytest.param(
            DetectionMechanism.ALL_ROWS_QUERY_DATAHUB_DATASET_PROFILE,
            DetectionMechanism.ALL_ROWS_QUERY_DATAHUB_DATASET_PROFILE,
            id="all_rows_query_datahub_dataset_profile",
        ),
    ],
)
def test_sync_smart_column_metric_assertion_valid_complex_detection_mechanism_input(
    freshness_stub_datahub_client: StubDataHubClient,
    detection_mechanism: DetectionMechanismInputTypes,
    expected_detection_mechanism: _DetectionMechanismTypes,
) -> None:
    # Arrange
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    # Act
    assertion = client.sync_smart_column_metric_assertion(
        dataset_urn=_any_dataset_urn,
        column_name="amount",
        metric_type="null_count",
        detection_mechanism=detection_mechanism,
    )

    # Assert
    assert assertion.detection_mechanism == expected_detection_mechanism


@pytest.mark.parametrize(
    "input_params, error_type, expected_error_message",
    [
        pytest.param(
            SmartColumnMetricAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="invalid_metric_type",  # type: ignore[arg-type] # Test invalid input
            ),
            SDKUsageError,
            "Invalid value for FieldMetricTypeClass: invalid_metric_type, valid options are",
            id="invalid_metric_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
                detection_mechanism="invalid_detection_mechanism",  # type: ignore[arg-type] # Test invalid input
            ),
            SDKUsageErrorWithExamples,
            "Invalid detection mechanism type: invalid_detection_mechanism",
            id="invalid_detection_mechanism",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
                sensitivity="invalid_sensitivity",  # type: ignore[arg-type] # Test invalid input
            ),
            SDKUsageErrorWithExamples,
            "Invalid inference sensitivity: invalid_sensitivity",
            id="invalid_sensitivity",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
                exclusion_windows="invalid_exclusion_windows",  # type: ignore[arg-type] # Test invalid input
            ),
            SDKUsageErrorWithExamples,
            "Invalid exclusion window: invalid_exclusion_windows",
            id="invalid_exclusion_windows",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
                training_data_lookback_days=-1,
            ),
            SDKUsageError,
            "Training data lookback days must be non-negative",
            id="negative_training_data_lookback_days",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
                incident_behavior="invalid_incident_behavior",  # type: ignore[arg-type] # Test invalid input
            ),
            SDKUsageErrorWithExamples,
            "Invalid incident behavior: invalid_incident_behavior",
            id="invalid_incident_behavior",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
                tags="urn:li:tag:",  # type: ignore[arg-type] # Test invalid input - empty tag name
            ),
            InvalidUrnError,
            "Expecting a TagUrn but got urn:li:tag:",
            id="invalid_tag_urn",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
                updated_by="invalid_updated_by",  # type: ignore[arg-type] # Test invalid input
            ),
            SdkUsageError,
            re.escape(
                "Invalid actor for last updated tuple, expected 'urn:li:corpuser:*' or 'urn:li:corpGroup:*'"
            ),
            id="invalid_created_by_urn",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                column_name="amount",
                metric_type="null_count",
                schedule="invalid_cron",  # type: ignore[arg-type] # Test invalid input
            ),
            SDKUsageError,
            "Invalid cron expression or timezone: invalid_cron",
            id="invalid_schedule",
        ),
    ],
)
def test_sync_smart_column_metric_assertion_invalid_input(
    freshness_stub_datahub_client: StubDataHubClient,
    input_params: SmartColumnMetricAssertionInputParams,
    error_type: Type[Exception],
    expected_error_message: str,
) -> None:
    # Arrange
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    # Act & Assert
    with pytest.raises(error_type, match=expected_error_message):
        client.sync_smart_column_metric_assertion(**asdict(input_params))


def test_create_smart_column_metric_assertion_uses_string_incident_behavior(
    freshness_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test that _create_smart_column_metric_assertion accepts string incident_behavior."""
    client = AssertionsClient(freshness_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create = MagicMock()
    client.client.entities.create = mock_create  # type: ignore[method-assign] # Override for testing

    # Act - call with string incident_behavior
    assertion = (
        client._smart_column_metric_client._create_smart_column_metric_assertion(
            dataset_urn=_any_dataset_urn,
            column_name="amount",
            metric_type="null_count",
            incident_behavior="resolve_on_pass",  # String input
            created_by="urn:li:corpuser:test_user",
        )
    )

    # Assert - should convert string to enum list
    assert assertion.incident_behavior == [AssertionIncidentBehavior.RESOLVE_ON_PASS]


def test_sync_smart_column_metric_assertion_missing_required_params_for_creation(
    column_metric_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test that sync_smart_column_metric_assertion raises SDKUsageError when required params are missing for creation (urn=None)."""
    client = AssertionsClient(column_metric_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Test missing column_name
    with pytest.raises(
        SDKUsageError, match="column_name is required when creating a new assertion"
    ):
        client.sync_smart_column_metric_assertion(
            dataset_urn=_any_dataset_urn,
            # column_name missing
            metric_type="null_count",
        )

    # Test missing metric_type
    with pytest.raises(
        SDKUsageError, match="metric_type is required when creating a new assertion"
    ):
        client.sync_smart_column_metric_assertion(
            dataset_urn=_any_dataset_urn,
            column_name="amount",
            # metric_type missing
        )


def test_sync_smart_column_metric_assertion_optional_params_for_merge(
    column_metric_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test that sync_smart_column_metric_assertion allows optional params when urn is provided (merge scenario)."""

    client = AssertionsClient(column_metric_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing

    # This should not raise an error since urn is provided and we can fetch missing params from existing assertion
    result = client.sync_smart_column_metric_assertion(
        dataset_urn=_any_dataset_urn,
        urn="urn:li:assertion:smart_freshness_assertion",  # Use the URN from conftest fixtures
        # column_name, metric_type, operator are optional in merge scenario
    )

    # Verify that the values were correctly fetched from the existing assertion
    assert result.column_name == "amount"
    assert result.metric_type == FieldMetricTypeClass.NULL_COUNT
    assert (
        result.operator == AssertionStdOperatorClass.BETWEEN
    )  # For smart assertions, operator is always "BETWEEN"
    assert result.criteria_parameters == (
        0,
        0,
    )  # For smart assertions, criteria_parameters is always (0, 0)


def _validate_column_metric_assertion_vs_input(
    assertion: SmartColumnMetricAssertion,
    input_params: SmartColumnMetricAssertionInputParams,
    expected_output_params: SmartColumnMetricAssertionOutputParams,
) -> None:
    """Validate that the assertion matches the input parameters and expected output parameters."""
    assert assertion.dataset_urn == expected_output_params.dataset_urn
    assert assertion.column_name == expected_output_params.column_name
    assert (
        str(assertion.metric_type).upper()
        if assertion.metric_type is not None
        else str(expected_output_params.metric_type).upper() is None
        if expected_output_params.metric_type is not None
        else None
    )
    # For smart assertions, operator is always "BETWEEN"
    assert str(assertion.operator).upper() in ["BETWEEN", "OPERATORTYPE.BETWEEN"]
    # For smart assertions, criteria_parameters is always (0, 0)
    assert assertion.criteria_parameters == (0, 0)
    assert assertion.display_name.startswith(expected_output_params.display_name)
    assert assertion.detection_mechanism == expected_output_params.detection_mechanism
    assert assertion.sensitivity == expected_output_params.sensitivity
    assert assertion.exclusion_windows == expected_output_params.exclusion_windows
    assert (
        assertion.training_data_lookback_days
        == expected_output_params.training_data_lookback_days
    )
    assert assertion.incident_behavior == expected_output_params.incident_behavior
    assert assertion.tags == expected_output_params.tags
    assert assertion.created_by == expected_output_params.created_by
    assert assertion.created_at == expected_output_params.created_at
    assert assertion.updated_by == expected_output_params.updated_by
    assert assertion.updated_at == expected_output_params.updated_at
    assert assertion.schedule == expected_output_params.schedule
