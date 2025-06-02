import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional, Type, TypedDict, Union
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

import datahub.metadata.schema_classes as models
from acryl_datahub_cloud._sdk_extras.assertion import SmartFreshnessAssertion
from acryl_datahub_cloud._sdk_extras.assertion_input import (
    _DETECTION_MECHANISM_CONCRETE_TYPES,
    _DETECTION_MECHANISM_TYPES,
    ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
    DEFAULT_SENSITIVITY,
    AssertionIncidentBehavior,
    DetectionMechanism,
    DetectionMechanismInputTypes,
    FixedRangeExclusionWindow,
    InferenceSensitivity,
    _SmartFreshnessAssertionInput,
)
from acryl_datahub_cloud._sdk_extras.assertions_client import (
    DEFAULT_CREATED_BY,
    AssertionsClient,
)
from acryl_datahub_cloud._sdk_extras.entities.assertion import (
    TagsInputType,
)
from acryl_datahub_cloud._sdk_extras.errors import (
    SDKUsageError,
    SDKUsageErrorWithExamples,
)
from datahub.errors import SdkUsageError
from datahub.metadata.urns import CorpUserUrn, DatasetUrn, TagUrn
from datahub.utilities.urns.error import InvalidUrnError
from tests.conftest import StubDataHubClient

FROZEN_TIME = "2025-01-01 10:30:00"

_any_user = CorpUserUrn.from_string("urn:li:corpuser:test_user")
_any_dataset_urn = DatasetUrn.from_string(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)"
)

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
    detection_mechanism: _DETECTION_MECHANISM_TYPES
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
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                display_name="High Water Mark Column Detection Mechanism Assertion",
                detection_mechanism=DetectionMechanism.HIGH_WATERMARK_COLUMN(
                    column_name="high_watermark", additional_filter="amount > 1000"
                ),
            ),
            "DateTypeClass",
            models.FreshnessFieldKindClass.HIGH_WATERMARK,
            SmartFreshnessAssertionOutputParams(
                **_OTHER_OUTPUT_PARAMS,
                display_name="High Water Mark Column Detection Mechanism Assertion",
                detection_mechanism=DetectionMechanism.HIGH_WATERMARK_COLUMN(
                    column_name="high_watermark", additional_filter="amount > 1000"
                ),
            ),
            id="high_watermark_column_detection_mechanism",
        ),
        pytest.param(
            SmartFreshnessAssertionInputParams(
                dataset_urn=_any_dataset_urn,
                display_name="High Water Mark Column Detection Mechanism Assertion",
                detection_mechanism=DetectionMechanism.HIGH_WATERMARK_COLUMN(
                    column_name="high_watermark", additional_filter=None
                ),
            ),
            "DateTypeClass",
            models.FreshnessFieldKindClass.HIGH_WATERMARK,
            SmartFreshnessAssertionOutputParams(
                **_OTHER_OUTPUT_PARAMS,
                display_name="High Water Mark Column Detection Mechanism Assertion",
                detection_mechanism=DetectionMechanism.HIGH_WATERMARK_COLUMN(
                    column_name="high_watermark", additional_filter=None
                ),
            ),
            id="high_watermark_column_detection_mechanism_without_additional_filter",
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


def _validate_assertion_vs_input(
    assertion: SmartFreshnessAssertion,
    input_params: SmartFreshnessAssertionInputParams,
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
