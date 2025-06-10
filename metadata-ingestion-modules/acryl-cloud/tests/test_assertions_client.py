import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional, Type, TypedDict, Union
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

import datahub.metadata.schema_classes as models
from acryl_datahub_cloud._sdk_extras.assertion import (
    SmartFreshnessAssertion,
    SmartVolumeAssertion,
)
from acryl_datahub_cloud._sdk_extras.assertion_input import (
    _DETECTION_MECHANISM_CONCRETE_TYPES,
    ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
    DEFAULT_DETECTION_MECHANISM,
    DEFAULT_SCHEDULE,
    DEFAULT_SENSITIVITY,
    AssertionIncidentBehavior,
    DetectionMechanism,
    DetectionMechanismInputTypes,
    FixedRangeExclusionWindow,
    InferenceSensitivity,
    _DetectionMechanismTypes,
    _SmartFreshnessAssertionInput,
)
from acryl_datahub_cloud._sdk_extras.assertions_client import (
    DEFAULT_CREATED_BY,
    AssertionsClient,
)
from acryl_datahub_cloud._sdk_extras.entities.assertion import (
    Assertion,
    TagsInputType,
)
from acryl_datahub_cloud._sdk_extras.entities.monitor import Monitor
from acryl_datahub_cloud._sdk_extras.errors import (
    SDKNotYetSupportedError,
    SDKUsageError,
    SDKUsageErrorWithExamples,
)
from datahub.emitter.mce_builder import make_ts_millis
from datahub.errors import SdkUsageError
from datahub.metadata.urns import (
    AssertionUrn,
    CorpUserUrn,
    DatasetUrn,
    MonitorUrn,
    TagUrn,
)
from datahub.utilities.urns.error import InvalidUrnError
from tests.conftest import StubDataHubClient

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
    incident_behavior: Optional[list[AssertionIncidentBehavior]] = None
    tags: Optional[TagsInputType] = None
    created_by: Optional[CorpUserUrn] = None


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
                created_by=_any_user,
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
                created_by=_any_user,
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
    ],
)
def test_create_smart_freshness_assertion_valid_simple_input(
    stub_datahub_client: StubDataHubClient,
    input_params: SmartFreshnessAssertionInputParams,
    expected_output_params: SmartFreshnessAssertionOutputParams,
) -> None:
    # Arrange
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    # Act
    assertion = client.create_smart_freshness_assertion(**asdict(input_params))

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
    stub_datahub_client: StubDataHubClient,
    input_params: SmartFreshnessAssertionInputParams,
    field_spec_type: str,
    field_spec_kind: models.FreshnessFieldKindClass,
    expected_output_params: SmartFreshnessAssertionOutputParams,
) -> None:
    # Arrange
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing
    with patch.object(
        _SmartFreshnessAssertionInput, "_create_field_spec", new_callable=MagicMock
    ) as mock_create_field_spec:
        mock_create_field_spec.return_value = models.FreshnessFieldSpecClass(
            path="path",
            type=field_spec_type,
            nativeType="nativeType",
            kind=field_spec_kind,
        )

        # Act
        assertion = client.create_smart_freshness_assertion(**asdict(input_params))

    # Assert
    _validate_assertion_vs_input(assertion, input_params, expected_output_params)


def test_create_smart_freshness_assertion_entities_client_called(
    stub_datahub_client: StubDataHubClient,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create = MagicMock()
    client.client.entities.create = mock_create  # type: ignore[method-assign] # Override for testing
    assertion = client.create_smart_freshness_assertion(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
    )
    assert mock_create.call_count == 2
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
                tags="invalid_tags",  # type: ignore[arg-type] # Test invalid input
            ),
            InvalidUrnError,
            "Invalid urn string: invalid_tags. Urns should start with 'urn:li:'",
            id="invalid_tag_urn",
        ),
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                created_by="invalid_created_by",  # type: ignore[arg-type] # Test invalid input
            ),
            SdkUsageError,
            re.escape(
                "Invalid actor for last updated tuple, expected 'urn:li:corpuser:*' or 'urn:li:corpGroup:*'"
            ),
            id="invalid_created_by_urn",
        ),
    ],
)
def test_create_smart_freshness_assertion_invalid_input(
    stub_datahub_client: StubDataHubClient,
    input_params: SmartFreshnessAssertionInputParams,
    error_type: Type[Exception],
    expected_error_message: str,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing
    with pytest.raises(error_type, match=expected_error_message):
        client.create_smart_freshness_assertion(**asdict(input_params))


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
def test_upsert_smart_freshness_assertion_valid_simple_input(
    stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_monitor_urn: MonitorUrn,
    any_assertion_urn: AssertionUrn,
    monitor_with_all_fields: Monitor,
    assertion_entity_with_all_fields: Assertion,
) -> None:
    """Test with all fields set to default values."""

    # Arrange
    input_params = SmartFreshnessAssertionUpsertInputParams(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
    )
    mock_upsert = MagicMock()
    stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Act
    assertion = client.upsert_smart_freshness_assertion(**asdict(input_params))

    # Assert
    _validate_assertion_vs_input(
        assertion,
        input_params,
        SmartFreshnessAssertionOutputParams(
            dataset_urn=input_params.dataset_urn,
            display_name=input_params.display_name or "",
            detection_mechanism=DEFAULT_DETECTION_MECHANISM,  # Default
            sensitivity=DEFAULT_SENSITIVITY,  # Default
            exclusion_windows=[],  # Default
            training_data_lookback_days=ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,  # Default
            incident_behavior=[],  # Default
            tags=[],  # Default
            created_by=DEFAULT_CREATED_BY,
            created_at=FROZEN_TIME,
            updated_by=DEFAULT_CREATED_BY,
            updated_at=FROZEN_TIME,
        ),
    )

    assert mock_upsert.call_count == 2

    called_with_assertion = mock_upsert.call_args_list[0][0][0]
    assert called_with_assertion.urn == any_assertion_urn
    assert isinstance(called_with_assertion.info, models.FreshnessAssertionInfoClass)
    assert isinstance(
        assertion_entity_with_all_fields.info, models.FreshnessAssertionInfoClass
    )
    assert called_with_assertion.info.type == assertion_entity_with_all_fields.info.type
    assert (
        called_with_assertion.info.entity
        == assertion_entity_with_all_fields.info.entity
    )

    called_with_monitor = mock_upsert.call_args_list[1][0][0]
    assert called_with_monitor.urn == any_monitor_urn
    assert called_with_monitor.info.type == monitor_with_all_fields.info.type
    assert (
        called_with_monitor.info.status.mode == monitor_with_all_fields.info.status.mode
    )
    assert called_with_monitor.info.assertionMonitor.assertions[0].assertion == str(
        assertion_entity_with_all_fields.urn
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[0].schedule.cron
        == DEFAULT_SCHEDULE.cron
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[0].schedule.timezone
        == DEFAULT_SCHEDULE.timezone
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[0].parameters.type
        == models.AssertionEvaluationParametersTypeClass.DATASET_FRESHNESS
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[
            0
        ].parameters.datasetFreshnessParameters.sourceType
        == models.DatasetFreshnessSourceTypeClass.INFORMATION_SCHEMA
    )


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
def test_upsert_smart_freshness_assertion_valid_full_input(
    stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_monitor_urn: MonitorUrn,
    any_assertion_urn: AssertionUrn,
    monitor_with_all_fields: Monitor,
    assertion_entity_with_all_fields: Assertion,
    detection_mechanism: DetectionMechanismInputTypes,
    expected_detection_mechanism: _DetectionMechanismTypes,
) -> None:
    """Test with all fields set to default values."""

    # Arrange
    input_params = SmartFreshnessAssertionUpsertInputParams(
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
    )
    mock_upsert = MagicMock()
    stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Act
    field_name = "field"
    field_type = "DateTypeClass"
    with patch.object(
        _SmartFreshnessAssertionInput, "_create_field_spec", new_callable=MagicMock
    ) as mock_create_field_spec:
        mock_create_field_spec.return_value = models.FreshnessFieldSpecClass(
            path=field_name,
            type=field_type,
            nativeType="nativeType",
            kind=models.FreshnessFieldKindClass.LAST_MODIFIED,
        )
        assertion = client.upsert_smart_freshness_assertion(**asdict(input_params))

    # Assert
    _validate_assertion_vs_input(
        assertion,
        input_params,
        SmartFreshnessAssertionOutputParams(
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
            created_by=CorpUserUrn.from_string("urn:li:corpuser:test_user"),
            created_at=FROZEN_TIME,
            updated_by=CorpUserUrn.from_string("urn:li:corpuser:test_user"),
            updated_at=FROZEN_TIME,
        ),
    )

    assert mock_upsert.call_count == 2

    called_with_assertion = mock_upsert.call_args_list[0][0][0]
    assert called_with_assertion.urn == any_assertion_urn
    assert isinstance(called_with_assertion.info, models.FreshnessAssertionInfoClass)
    assert isinstance(
        assertion_entity_with_all_fields.info, models.FreshnessAssertionInfoClass
    )
    assert called_with_assertion.info.type == assertion_entity_with_all_fields.info.type
    assert (
        called_with_assertion.info.entity
        == assertion_entity_with_all_fields.info.entity
    )

    called_with_monitor = mock_upsert.call_args_list[1][0][0]
    assert called_with_monitor.urn == any_monitor_urn
    assert called_with_monitor.info.type == monitor_with_all_fields.info.type
    assert (
        called_with_monitor.info.status.mode == monitor_with_all_fields.info.status.mode
    )
    assert called_with_monitor.info.assertionMonitor.assertions[0].assertion == str(
        assertion_entity_with_all_fields.urn
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[0].schedule.cron
        == DEFAULT_SCHEDULE.cron
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[0].schedule.timezone
        == DEFAULT_SCHEDULE.timezone
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[0].parameters.type
        == models.AssertionEvaluationParametersTypeClass.DATASET_FRESHNESS
    )
    if expected_detection_mechanism == DetectionMechanism.INFORMATION_SCHEMA:
        assert (
            called_with_monitor.info.assertionMonitor.assertions[
                0
            ].parameters.datasetFreshnessParameters.sourceType
            == models.DatasetFreshnessSourceTypeClass.INFORMATION_SCHEMA
        )
    if expected_detection_mechanism == DetectionMechanism.LAST_MODIFIED_COLUMN(
        column_name="last_modified", additional_filter="last_modified > '2021-01-01'"
    ):
        assert (
            called_with_monitor.info.assertionMonitor.assertions[
                0
            ].parameters.datasetFreshnessParameters.sourceType
            == models.DatasetFreshnessSourceTypeClass.FIELD_VALUE
        )


@pytest.mark.parametrize(
    "urn, expected_create_assertion_call_count, expected_upsert_entity_call_count",
    [
        pytest.param(None, 1, 0, id="urn_is_none"),
        pytest.param(_any_assertion_urn, 0, 2, id="urn_is_not_none"),
    ],
)
def test_upsert_smart_freshness_assertion_calls_create_assertion_if_urn_is_not_set(
    stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    urn: Optional[Union[str, AssertionUrn]],
    expected_create_assertion_call_count: int,
    expected_upsert_entity_call_count: int,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert_entity = MagicMock()
    client.client.entities.upsert = mock_upsert_entity  # type: ignore[method-assign] # Override for testing
    mock_create_assertion = MagicMock()
    client.create_smart_freshness_assertion = mock_create_assertion  # type: ignore[method-assign] # Override for testing
    client.upsert_smart_freshness_assertion(
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
def test_upsert_smart_freshness_assertion_uses_default_if_updated_by_is_not_set(
    stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
    updated_by: Optional[CorpUserUrn],
    expected_updated_by: CorpUserUrn,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create_assertion = MagicMock()
    client.client.entities.create = mock_create_assertion  # type: ignore[method-assign] # Override for testing
    mock_upsert = MagicMock()
    stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing

    client.upsert_smart_freshness_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        updated_by=updated_by,
    )
    assertion_entity_upserted = mock_upsert.call_args_list[0][0][0]
    assert assertion_entity_upserted.last_updated.actor == str(expected_updated_by)
    assert mock_create_assertion.call_count == 0


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
    incident_behavior: Optional[list[AssertionIncidentBehavior]] = None
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
    ],
)
def test_create_smart_volume_assertion_valid_simple_input(
    stub_datahub_client: StubDataHubClient,
    input_params: SmartVolumeAssertionInputParams,
    expected_output_params: SmartVolumeAssertionOutputParams,
) -> None:
    # Arrange
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    # Act
    assertion = client.create_smart_volume_assertion(**asdict(input_params))

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
    stub_datahub_client: StubDataHubClient,
    detection_mechanism: DetectionMechanismInputTypes,
    expected_detection_mechanism: _DetectionMechanismTypes,
) -> None:
    # Arrange
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    # Act
    assertion = client.create_smart_volume_assertion(
        dataset_urn=_any_dataset_urn,
        display_name="Test Assertion",
        detection_mechanism=detection_mechanism,
    )

    # Assert
    assert assertion.detection_mechanism == expected_detection_mechanism


def test_create_smart_volume_assertion_entities_client_called(
    stub_datahub_client: StubDataHubClient,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create = MagicMock()
    client.client.entities.create = mock_create  # type: ignore[method-assign] # Override for testing
    assertion = client.create_smart_volume_assertion(
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
                tags="invalid_tags",  # type: ignore[arg-type] # Test invalid input
            ),
            InvalidUrnError,
            "Invalid urn string: invalid_tags. Urns should start with 'urn:li:'",
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
    stub_datahub_client: StubDataHubClient,
    input_params: SmartVolumeAssertionInputParams,
    error_type: Type[Exception],
    expected_error_message: str,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing
    with pytest.raises(error_type, match=expected_error_message):
        client.create_smart_volume_assertion(**asdict(input_params))


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
def test_upsert_smart_volume_assertion_valid_simple_input(
    stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_monitor_urn: MonitorUrn,
    any_assertion_urn: AssertionUrn,
) -> None:
    """Test with all fields set to default values."""

    # Arrange
    input_params = SmartVolumeAssertionUpsertInputParams(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
    )
    mock_upsert = MagicMock()
    stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Act
    assertion = client.upsert_smart_volume_assertion(**asdict(input_params))

    # Assert
    _validate_volume_assertion_vs_input(
        assertion,
        input_params,
        SmartVolumeAssertionOutputParams(
            dataset_urn=input_params.dataset_urn,
            display_name=input_params.display_name or "",
            detection_mechanism=DEFAULT_DETECTION_MECHANISM,  # Default
            sensitivity=DEFAULT_SENSITIVITY,  # Default
            exclusion_windows=[],  # Default
            training_data_lookback_days=ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,  # Default
            incident_behavior=[],  # Default
            tags=[],  # Default
            created_by=DEFAULT_CREATED_BY,
            created_at=FROZEN_TIME,
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
    assert called_with_assertion.on_success == []
    assert called_with_assertion.on_failure == []
    assert called_with_assertion.tags is None
    assert called_with_assertion.source.created.time == make_ts_millis(FROZEN_TIME)
    assert called_with_assertion.source.created.actor == str(DEFAULT_CREATED_BY)
    assert called_with_assertion.last_updated.time == make_ts_millis(FROZEN_TIME)
    assert called_with_assertion.last_updated.actor == str(DEFAULT_CREATED_BY)

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
        == InferenceSensitivity.to_int(DEFAULT_SENSITIVITY)
    )
    assert (
        called_with_monitor.info.assertionMonitor.settings.adjustmentSettings.exclusionWindows
        == []
    )
    assert (
        called_with_monitor.info.assertionMonitor.settings.adjustmentSettings.trainingDataLookbackWindowDays
        == ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS
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
def test_upsert_smart_volume_assertion_valid_full_input(
    stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_monitor_urn: MonitorUrn,
    any_assertion_urn: AssertionUrn,
    monitor_with_all_fields: Monitor,
    assertion_entity_with_all_fields: Assertion,
    detection_mechanism: DetectionMechanismInputTypes,
    expected_detection_mechanism: _DetectionMechanismTypes,
) -> None:
    """Test with all fields set to default values."""

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
    stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Act
    assertion = client.upsert_smart_volume_assertion(**asdict(input_params))

    # Assert
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
            created_by=CorpUserUrn.from_string("urn:li:corpuser:test_user"),
            created_at=FROZEN_TIME,
            updated_by=CorpUserUrn.from_string("urn:li:corpuser:test_user"),
            updated_at=FROZEN_TIME,
            schedule=models.CronScheduleClass(cron="0 * * * *", timezone="UTC"),
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
    assert isinstance(input_params.schedule, models.CronScheduleClass)
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
def test_upsert_smart_volume_assertion_calls_create_assertion_if_urn_is_not_set(
    stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    urn: Optional[Union[str, AssertionUrn]],
    expected_create_assertion_call_count: int,
    expected_upsert_entity_call_count: int,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert_entity = MagicMock()
    client.client.entities.upsert = mock_upsert_entity  # type: ignore[method-assign] # Override for testing
    mock_create_assertion = MagicMock()
    client.create_smart_volume_assertion = mock_create_assertion  # type: ignore[method-assign] # Override for testing
    client.upsert_smart_volume_assertion(
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
def test_upsert_smart_volume_assertion_uses_default_if_updated_by_is_not_set(
    stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
    updated_by: Optional[CorpUserUrn],
    expected_updated_by: CorpUserUrn,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create_assertion = MagicMock()
    client.client.entities.create = mock_create_assertion  # type: ignore[method-assign] # Override for testing
    mock_upsert = MagicMock()
    stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing

    client.upsert_smart_volume_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        updated_by=updated_by,
    )
    assertion_entity_upserted = mock_upsert.call_args_list[0][0][0]
    assert assertion_entity_upserted.last_updated.actor == str(expected_updated_by)
    assert mock_create_assertion.call_count == 0


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
