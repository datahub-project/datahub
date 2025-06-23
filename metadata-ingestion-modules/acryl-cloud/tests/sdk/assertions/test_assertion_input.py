import dataclasses
import re
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, ContextManager, Optional, Type, TypedDict, Union, cast

import pytest
import tzlocal
from pydantic import ValidationError

import datahub.metadata.schema_classes as models
from acryl_datahub_cloud.sdk.assertion.assertion_base import SqlAssertion
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
    DEFAULT_NAME_PREFIX,
    DEFAULT_NAME_SUFFIX_LENGTH,
    AssertionIncidentBehavior,
    AssertionIncidentBehaviorInputTypes,
    CalendarInterval,
    DatasetSourceType,
    DetectionMechanism,
    FieldSpecType,
    FixedRangeExclusionWindow,
    FixedRangeExclusionWindowInputTypes,
    InferenceSensitivity,
    SDKUsageErrorWithExamples,
    TimeWindowSize,
    TimeWindowSizeInputTypes,
    _AssertionInput,
    _AuditLog,
    _DetectionMechanismTypes,
    _InformationSchema,
    _is_source_type_valid,
    _SmartFreshnessAssertionInput,
    _try_parse_incident_behavior,
    _validate_cron_schedule,
)
from acryl_datahub_cloud.sdk.assertion_input.freshness_assertion_input import (
    _FreshnessAssertionInput,
)
from acryl_datahub_cloud.sdk.assertion_input.sql_assertion_input import (
    SqlAssertionCondition,
    SqlAssertionCriteria,
    _SqlAssertionInput,
)
from acryl_datahub_cloud.sdk.entities.assertion import (
    TagsInputType,
)
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.emitter.mce_builder import make_ts_millis
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, InvalidUrnError
from datahub.sdk.entity_client import EntityClient
from tests.sdk.assertions.conftest import StubEntityClient


class AssertionInputParams(TypedDict, total=False):
    urn: Union[str, AssertionUrn]
    dataset_urn: Union[str, DatasetUrn]
    entity_client: EntityClient
    display_name: str
    detection_mechanism: Union[str, dict[str, str], _DetectionMechanismTypes]
    sensitivity: Union[str, InferenceSensitivity]
    exclusion_windows: Union[
        dict[str, datetime],
        list[dict[str, datetime]],
        FixedRangeExclusionWindow,
        list[FixedRangeExclusionWindow],
    ]
    training_data_lookback_days: int
    incident_behavior: Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
    tags: Optional[TagsInputType]
    enabled: bool
    created_by: Union[str, CorpUserUrn]
    created_at: datetime
    updated_by: Union[str, CorpUserUrn]
    updated_at: datetime


class CreatedUpdatedParams(TypedDict, total=False):
    created_by: Union[str, CorpUserUrn]
    created_at: datetime
    updated_by: Union[str, CorpUserUrn]
    updated_at: datetime


@pytest.fixture
def default_created_updated_params() -> CreatedUpdatedParams:
    return {
        "created_by": CorpUserUrn.from_string("urn:li:corpuser:test_user"),
        "created_at": datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        "updated_by": CorpUserUrn.from_string("urn:li:corpuser:test_user"),
        "updated_at": datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    }


@pytest.mark.parametrize(
    "detection_mechanism, expected_type, expected_additional_kwargs",
    [
        pytest.param(
            DetectionMechanism.INFORMATION_SCHEMA,
            "information_schema",
            {},
            id="information_schema (no kwargs)",
        ),
        pytest.param(
            DetectionMechanism.AUDIT_LOG,
            "audit_log",
            {},
            id="audit_log (no kwargs)",
        ),
        pytest.param(
            DetectionMechanism.DATAHUB_OPERATION,
            "datahub_operation",
            {},
            id="datahub_operation (no kwargs)",
        ),
        pytest.param(
            DetectionMechanism.LAST_MODIFIED_COLUMN(column_name="last_modified"),
            "last_modified_column",
            {"column_name": "last_modified"},
            id="last_modified_column (column_name only)",
        ),
        pytest.param(
            DetectionMechanism.LAST_MODIFIED_COLUMN(
                column_name="last_modified",
                additional_filter="last_modified > '2025-01-01'",
            ),
            "last_modified_column",
            {
                "column_name": "last_modified",
                "additional_filter": "last_modified > '2025-01-01'",
            },
            id="last_modified_column (column_name + additional_filter)",
        ),
        pytest.param(
            DetectionMechanism.HIGH_WATERMARK_COLUMN(column_name="id"),
            "high_watermark_column",
            {"column_name": "id"},
            id="high_watermark_column (column_name only)",
        ),
        pytest.param(
            DetectionMechanism.HIGH_WATERMARK_COLUMN(
                column_name="id", additional_filter="id > 1000"
            ),
            "high_watermark_column",
            {"column_name": "id", "additional_filter": "id > 1000"},
            id="high_watermark_column (column_name + additional_filter)",
        ),
        pytest.param(
            DetectionMechanism.QUERY(additional_filter="id > 1000"),
            "query",
            {"additional_filter": "id > 1000"},
            id="query (additional_filter only)",
        ),
        pytest.param(
            DetectionMechanism.QUERY(),
            "query",
            {"additional_filter": None},
            id="query (default additional_filter)",
        ),
        pytest.param(
            None,
            "information_schema",
            {},
            id="Default detection mechanism (information_schema)",
        ),
    ],
)
def test_assertion_creation_with_detection_mechanism_instance(
    detection_mechanism: _DetectionMechanismTypes,
    expected_type: str,
    expected_additional_kwargs: dict[str, str],
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": freshness_stub_entity_client,
        **default_created_updated_params,
    }
    if detection_mechanism:
        params["detection_mechanism"] = detection_mechanism
    assertion = _SmartFreshnessAssertionInput(**params)
    assert assertion.detection_mechanism.type == expected_type
    for key, value in expected_additional_kwargs.items():
        assert getattr(assertion.detection_mechanism, key) == value


@pytest.mark.parametrize(
    "detection_mechanism_str, expected_type",
    [
        pytest.param(
            "information_schema", "information_schema", id="str: information_schema"
        ),
        pytest.param("audit_log", "audit_log", id="str: audit_log"),
        pytest.param(
            "datahub_operation", "datahub_operation", id="str: datahub_operation"
        ),
    ],
)
def test_assertion_creation_with_detection_mechanism_str(
    detection_mechanism_str: str,
    expected_type: str,
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    assertion = _SmartFreshnessAssertionInput(
        urn=AssertionUrn("urn:li:assertion:123"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        display_name="test_assertion",
        detection_mechanism=detection_mechanism_str,
        entity_client=freshness_stub_entity_client,
        **default_created_updated_params,
    )
    assert assertion.detection_mechanism.type == expected_type


@pytest.mark.parametrize(
    "detection_mechanism_dict, expected_type, expected_additional_kwargs",
    [
        pytest.param(
            {"type": "information_schema"},
            "information_schema",
            {},
            id="dict: information_schema",
        ),
        pytest.param({"type": "audit_log"}, "audit_log", {}, id="dict: audit_log"),
        pytest.param(
            {"type": "datahub_operation"},
            "datahub_operation",
            {},
            id="dict: datahub_operation",
        ),
        pytest.param(
            {"type": "last_modified_column", "column_name": "last_modified"},
            "last_modified_column",
            {"column_name": "last_modified"},
            id="dict: last_modified_column (column_name only)",
        ),
        pytest.param(
            {
                "type": "last_modified_column",
                "column_name": "last_modified",
                "additional_filter": "last_modified > '2025-01-01'",
            },
            "last_modified_column",
            {
                "column_name": "last_modified",
                "additional_filter": "last_modified > '2025-01-01'",
            },
            id="dict: last_modified_column (column_name + additional_filter)",
        ),
        pytest.param(
            {"type": "high_watermark_column", "column_name": "id"},
            "high_watermark_column",
            {"column_name": "id"},
            id="dict: high_watermark_column (column_name only)",
        ),
        pytest.param(
            {
                "type": "high_watermark_column",
                "column_name": "id",
                "additional_filter": "id > 1000",
            },
            "high_watermark_column",
            {"column_name": "id", "additional_filter": "id > 1000"},
            id="dict: high_watermark_column (column_name + additional_filter)",
        ),
        pytest.param(
            None,
            "information_schema",
            {},
            id="Default detection mechanism (information_schema)",
        ),
    ],
)
def test_assertion_creation_with_detection_mechanism_dict(
    detection_mechanism_dict: dict[str, str],
    expected_type: str,
    expected_additional_kwargs: dict[str, str],
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": freshness_stub_entity_client,
        **default_created_updated_params,
    }
    if detection_mechanism_dict:
        params["detection_mechanism"] = detection_mechanism_dict
    assertion = _SmartFreshnessAssertionInput(**params)
    assert assertion.detection_mechanism.type == expected_type
    for key, value in expected_additional_kwargs.items():
        assert getattr(assertion.detection_mechanism, key) == value


@pytest.mark.parametrize(
    "sensitivity, expected_sensitivity, expected_raises",
    [
        pytest.param("high", InferenceSensitivity.HIGH, nullcontext(), id="str: high"),
        pytest.param(
            "medium", InferenceSensitivity.MEDIUM, nullcontext(), id="str: medium"
        ),
        pytest.param("low", InferenceSensitivity.LOW, nullcontext(), id="str: low"),
        pytest.param(
            InferenceSensitivity.HIGH,
            InferenceSensitivity.HIGH,
            nullcontext(),
            id="enum: high",
        ),
        pytest.param(
            InferenceSensitivity.MEDIUM,
            InferenceSensitivity.MEDIUM,
            nullcontext(),
            id="enum: medium",
        ),
        pytest.param(
            InferenceSensitivity.LOW,
            InferenceSensitivity.LOW,
            nullcontext(),
            id="enum: low",
        ),
        pytest.param(
            None,
            InferenceSensitivity.MEDIUM,
            nullcontext(),
            id="None (default to medium)",
        ),
        pytest.param(
            "invalid", None, pytest.raises(SDKUsageErrorWithExamples), id="invalid"
        ),
    ],
)
def test_assertion_creation_with_sensitivity(
    sensitivity: Union[str, InferenceSensitivity],
    expected_sensitivity: InferenceSensitivity,
    expected_raises: ContextManager[Any],
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": freshness_stub_entity_client,
        **default_created_updated_params,
    }
    if sensitivity:
        params["sensitivity"] = sensitivity
    with expected_raises:
        assertion = _SmartFreshnessAssertionInput(**params)
        assert assertion.sensitivity == expected_sensitivity


@pytest.mark.parametrize(
    "exclusion_windows, expected_exclusion_windows, expected_raises",
    [
        pytest.param(
            {
                "start": datetime(2025, 1, 1, 0, 0, 0),
                "end": datetime(2025, 1, 2, 0, 0, 0),
            },
            [
                FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 1, 0, 0, 0),
                    end=datetime(2025, 1, 2, 0, 0, 0),
                )
            ],
            nullcontext(),
            id="dict[str, datetime]: exclusion_window",
        ),
        pytest.param(
            [
                {
                    "start": datetime(2025, 1, 1, 0, 0, 0),
                    "end": datetime(2025, 1, 2, 0, 0, 0),
                }
            ],
            [
                FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 1, 0, 0, 0),
                    end=datetime(2025, 1, 2, 0, 0, 0),
                )
            ],
            nullcontext(),
            id="list[dict[str, datetime]]: exclusion_window",
        ),
        pytest.param(
            [
                {
                    "start": datetime(2025, 1, 1, 0, 0, 0),
                    "end": datetime(2025, 1, 2, 0, 0, 0),
                },
                {
                    "start": datetime(2025, 1, 3, 0, 0, 0),
                    "end": datetime(2025, 1, 4, 0, 0, 0),
                },
            ],
            [
                FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 1, 0, 0, 0),
                    end=datetime(2025, 1, 2, 0, 0, 0),
                ),
                FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 3, 0, 0, 0),
                    end=datetime(2025, 1, 4, 0, 0, 0),
                ),
            ],
            nullcontext(),
            id="list[dict[str, datetime]] with multiple exclusion windows: exclusion_window",
        ),
        pytest.param(
            {"start": "2025-01-01T00:00:00", "end": "2025-01-02T00:00:00"},
            [
                FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 1, 0, 0, 0),
                    end=datetime(2025, 1, 2, 0, 0, 0),
                )
            ],
            nullcontext(),
            id="dict[str, str]: exclusion_window",
        ),
        pytest.param(
            [{"start": "2025-01-01T00:00:00", "end": "2025-01-02T00:00:00"}],
            [
                FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 1, 0, 0, 0),
                    end=datetime(2025, 1, 2, 0, 0, 0),
                )
            ],
            nullcontext(),
            id="list[dict[str, str]]: exclusion_window",
        ),
        pytest.param(
            [
                {"start": "2025-01-01T00:00:00", "end": "2025-01-02T00:00:00"},
                {"start": "2025-01-03T00:00:00", "end": "2025-01-04T00:00:00"},
            ],
            [
                FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 1, 0, 0, 0),
                    end=datetime(2025, 1, 2, 0, 0, 0),
                ),
                FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 3, 0, 0, 0),
                    end=datetime(2025, 1, 4, 0, 0, 0),
                ),
            ],
            nullcontext(),
            id="list[dict[str, str]] with multiple exclusion windows: exclusion_window",
        ),
        pytest.param(
            FixedRangeExclusionWindow(
                start=datetime(2025, 1, 1, 0, 0, 0), end=datetime(2025, 1, 2, 0, 0, 0)
            ),
            [
                FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 1, 0, 0, 0),
                    end=datetime(2025, 1, 2, 0, 0, 0),
                )
            ],
            nullcontext(),
            id="exclusion_window: exclusion_window",
        ),
        pytest.param(
            [
                FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 1, 0, 0, 0),
                    end=datetime(2025, 1, 2, 0, 0, 0),
                )
            ],
            [
                FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 1, 0, 0, 0),
                    end=datetime(2025, 1, 2, 0, 0, 0),
                )
            ],
            nullcontext(),
            id="list[exclusion_window]: exclusion_window",
        ),
        pytest.param(
            [],
            [],
            nullcontext(),
            id="list[]: exclusion_window - default, no exclusion windows provided",
        ),
        pytest.param(
            None,
            [],
            nullcontext(),
            id="None: exclusion_window - default, no exclusion windows provided",
        ),
        pytest.param(
            [None],
            [],
            nullcontext(),
            id="list[None]: exclusion_window - default, no exclusion windows provided",
        ),
        # Error cases
        pytest.param(
            "invalid",
            None,
            pytest.raises(SDKUsageErrorWithExamples),
            id="invalid: exclusion_window - invalid type",
        ),
        pytest.param(
            {"start": "invalid", "end": "2025-01-02T00:00:00"},
            None,
            pytest.raises(ValidationError),
            id="invalid: exclusion_window - invalid datetime and valid string datetime",
        ),
        pytest.param(
            {"start": "invalid", "end": datetime(2025, 1, 2, 0, 0, 0)},
            None,
            pytest.raises(ValidationError),
            id="invalid: exclusion_window - invalid datetime and valid datetime object",
        ),
        pytest.param(
            {"invalid": "2025-01-01T00:00:00", "end": "2025-01-02T00:00:00"},
            None,
            pytest.raises(ValidationError),
            id="invalid: exclusion_window - invalid key",
        ),
    ],
)
def test_assertion_input_creation_with_exclusion_windows(
    exclusion_windows: FixedRangeExclusionWindowInputTypes,
    expected_exclusion_windows: list[FixedRangeExclusionWindow],
    expected_raises: ContextManager[Any],
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": freshness_stub_entity_client,
        **default_created_updated_params,
    }
    if exclusion_windows is not None:
        params["exclusion_windows"] = cast(
            Union[
                dict[str, datetime],
                list[dict[str, datetime]],
                FixedRangeExclusionWindow,
                list[FixedRangeExclusionWindow],
            ],
            exclusion_windows,
        )
    with expected_raises:
        assertion = _SmartFreshnessAssertionInput(**params)
        assert assertion.exclusion_windows == expected_exclusion_windows


@pytest.mark.parametrize(
    "incident_behavior, expected_incident_behavior, expected_raises",
    [
        # String inputs
        pytest.param(
            "raise_on_fail",
            [AssertionIncidentBehavior.RAISE_ON_FAIL],
            nullcontext(),
            id="str: raise_on_fail",
        ),
        pytest.param(
            "resolve_on_pass",
            [AssertionIncidentBehavior.RESOLVE_ON_PASS],
            nullcontext(),
            id="str: resolve_on_pass",
        ),
        # Enum inputs
        pytest.param(
            AssertionIncidentBehavior.RAISE_ON_FAIL,
            [AssertionIncidentBehavior.RAISE_ON_FAIL],
            nullcontext(),
            id="enum: raise_on_fail",
        ),
        pytest.param(
            AssertionIncidentBehavior.RESOLVE_ON_PASS,
            [AssertionIncidentBehavior.RESOLVE_ON_PASS],
            nullcontext(),
            id="enum: resolve_on_pass",
        ),
        # List of strings
        pytest.param(
            ["raise_on_fail", "resolve_on_pass"],
            [
                AssertionIncidentBehavior.RAISE_ON_FAIL,
                AssertionIncidentBehavior.RESOLVE_ON_PASS,
            ],
            nullcontext(),
            id="list[str]: both behaviors",
        ),
        # List of enums
        pytest.param(
            [
                AssertionIncidentBehavior.RAISE_ON_FAIL,
                AssertionIncidentBehavior.RESOLVE_ON_PASS,
            ],
            [
                AssertionIncidentBehavior.RAISE_ON_FAIL,
                AssertionIncidentBehavior.RESOLVE_ON_PASS,
            ],
            nullcontext(),
            id="list[enum]: both behaviors",
        ),
        # Mixed list
        pytest.param(
            ["raise_on_fail", AssertionIncidentBehavior.RESOLVE_ON_PASS],
            [
                AssertionIncidentBehavior.RAISE_ON_FAIL,
                AssertionIncidentBehavior.RESOLVE_ON_PASS,
            ],
            nullcontext(),
            id="mixed list: string and enum",
        ),
        # None input
        pytest.param(
            None,
            [],
            nullcontext(),
            id="None (default to empty list)",
        ),
        # Invalid inputs
        pytest.param(
            "invalid",
            None,
            pytest.raises(SDKUsageErrorWithExamples),
            id="invalid string",
        ),
        pytest.param(
            ["invalid"],
            None,
            pytest.raises(SDKUsageErrorWithExamples),
            id="list with invalid string",
        ),
        pytest.param(
            [None],
            None,
            pytest.raises(SDKUsageErrorWithExamples),
            id="list with None",
        ),
        pytest.param(
            123,
            None,
            pytest.raises(SDKUsageErrorWithExamples),
            id="invalid type (int)",
        ),
    ],
)
def test_assertion_input_creation_with_incident_behavior(
    incident_behavior: Union[
        str, list[str], AssertionIncidentBehavior, list[AssertionIncidentBehavior], None
    ],
    expected_incident_behavior: list[AssertionIncidentBehavior],
    expected_raises: ContextManager[Any],
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": freshness_stub_entity_client,
        **default_created_updated_params,
    }
    if incident_behavior is not None:
        params["incident_behavior"] = cast(
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]],
            incident_behavior,
        )
    with expected_raises:
        assertion = _SmartFreshnessAssertionInput(**params)
        assert assertion.incident_behavior == expected_incident_behavior


@dataclass
class TryParseIncidentBehaviorTestParams:
    """Test parameters for _try_parse_incident_behavior function."""

    input_config: AssertionIncidentBehaviorInputTypes
    expected_output: Union[
        AssertionIncidentBehavior, list[AssertionIncidentBehavior], None
    ]
    should_raise: bool = False
    expected_error_type: Optional[Type[Exception]] = None


@pytest.mark.parametrize(
    "params",
    [
        pytest.param(
            TryParseIncidentBehaviorTestParams(
                input_config=None,
                expected_output=[],
            ),
            id="none_input_returns_empty_list",
        ),
        pytest.param(
            TryParseIncidentBehaviorTestParams(
                input_config="raise_on_fail",
                expected_output=[AssertionIncidentBehavior.RAISE_ON_FAIL],
            ),
            id="valid_string_input_single_behavior",
        ),
        pytest.param(
            TryParseIncidentBehaviorTestParams(
                input_config="resolve_on_pass",
                expected_output=[AssertionIncidentBehavior.RESOLVE_ON_PASS],
            ),
            id="valid_string_input_different_behavior",
        ),
        pytest.param(
            TryParseIncidentBehaviorTestParams(
                input_config="invalid_string",
                expected_output=None,
                should_raise=True,
                expected_error_type=SDKUsageErrorWithExamples,
            ),
            id="invalid_string_input_raises_error",
        ),
        pytest.param(
            TryParseIncidentBehaviorTestParams(
                input_config=AssertionIncidentBehavior.RAISE_ON_FAIL,
                expected_output=[AssertionIncidentBehavior.RAISE_ON_FAIL],
            ),
            id="enum_input_single_behavior",
        ),
        pytest.param(
            TryParseIncidentBehaviorTestParams(
                input_config=[AssertionIncidentBehavior.RAISE_ON_FAIL],
                expected_output=[AssertionIncidentBehavior.RAISE_ON_FAIL],
            ),
            id="enum_list_input_single_behavior",
        ),
        pytest.param(
            TryParseIncidentBehaviorTestParams(
                input_config=[
                    AssertionIncidentBehavior.RAISE_ON_FAIL,
                    AssertionIncidentBehavior.RESOLVE_ON_PASS,
                ],
                expected_output=[
                    AssertionIncidentBehavior.RAISE_ON_FAIL,
                    AssertionIncidentBehavior.RESOLVE_ON_PASS,
                ],
            ),
            id="enum_list_input_multiple_behaviors",
        ),
        pytest.param(
            TryParseIncidentBehaviorTestParams(
                input_config=["raise_on_fail"],
                expected_output=[AssertionIncidentBehavior.RAISE_ON_FAIL],
            ),
            id="string_list_input_single_behavior",
        ),
        pytest.param(
            TryParseIncidentBehaviorTestParams(
                input_config=["raise_on_fail", "resolve_on_pass"],
                expected_output=[
                    AssertionIncidentBehavior.RAISE_ON_FAIL,
                    AssertionIncidentBehavior.RESOLVE_ON_PASS,
                ],
            ),
            id="string_list_input_multiple_behaviors",
        ),
        pytest.param(
            TryParseIncidentBehaviorTestParams(
                input_config=[  # type: ignore # Mixed list of string and enum
                    "raise_on_fail",
                    AssertionIncidentBehavior.RESOLVE_ON_PASS,
                ],
                expected_output=[
                    AssertionIncidentBehavior.RAISE_ON_FAIL,
                    AssertionIncidentBehavior.RESOLVE_ON_PASS,
                ],
            ),
            id="mixed_list_input_string_and_enum",
        ),
        pytest.param(
            TryParseIncidentBehaviorTestParams(
                input_config=["invalid_string"],
                expected_output=None,
                should_raise=True,
                expected_error_type=SDKUsageErrorWithExamples,
            ),
            id="invalid_string_in_list_raises_error",
        ),
        pytest.param(
            TryParseIncidentBehaviorTestParams(
                input_config=[123],  # type: ignore # Invalid type in list
                expected_output=None,
                should_raise=True,
                expected_error_type=SDKUsageErrorWithExamples,
            ),
            id="invalid_type_in_list_raises_error",
        ),
        pytest.param(
            TryParseIncidentBehaviorTestParams(
                input_config=123,  # type: ignore # Invalid type
                expected_output=None,
                should_raise=True,
                expected_error_type=SDKUsageErrorWithExamples,
            ),
            id="invalid_type_input_raises_error",
        ),
    ],
)
def test_try_parse_incident_behavior(
    params: TryParseIncidentBehaviorTestParams,
) -> None:
    """Test _try_parse_incident_behavior function with various input scenarios."""
    if params.should_raise:
        assert params.expected_error_type is not None
        with pytest.raises(params.expected_error_type):
            _try_parse_incident_behavior(params.input_config)
    else:
        result = _try_parse_incident_behavior(params.input_config)
        assert result == params.expected_output


@pytest.mark.parametrize(
    "detection_mechanism, additional_filter",
    [
        pytest.param(
            DetectionMechanism.INFORMATION_SCHEMA, None, id="information_schema"
        ),
        pytest.param(
            DetectionMechanism.LAST_MODIFIED_COLUMN,
            "id > 1000",
            id="last_modified_column with additional filter",
        ),
    ],
)
def test_to_assertion_entity(
    detection_mechanism: _DetectionMechanismTypes,
    additional_filter: Optional[str],
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    if additional_filter:
        detection_mechanism = detection_mechanism(  # type: ignore[operator]  # If additional_filter is not None, then detection_mechanism is a class that is callable
            column_name="column", additional_filter=additional_filter
        )
    else:
        detection_mechanism = detection_mechanism
    assertion_input = _SmartFreshnessAssertionInput(
        urn=AssertionUrn("urn:li:assertion:123"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        display_name="test_assertion",
        detection_mechanism=detection_mechanism,
        sensitivity=InferenceSensitivity.MEDIUM,
        incident_behavior=[
            AssertionIncidentBehavior.RAISE_ON_FAIL,
            AssertionIncidentBehavior.RESOLVE_ON_PASS,
        ],
        tags=["urn:li:tag:my_tag_1", "urn:li:tag:my_tag_2"],
        entity_client=freshness_stub_entity_client,
        **default_created_updated_params,
    )
    assertion = assertion_input.to_assertion_entity()
    assert assertion.urn == AssertionUrn("urn:li:assertion:123")
    assert isinstance(
        assertion.info, models.FreshnessAssertionInfoClass
    )  # Type narrowing
    assert assertion.info.type == models.FreshnessAssertionTypeClass.DATASET_CHANGE
    assert (
        assertion.info.entity
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)"
    )

    if additional_filter:
        assert assertion.info.filter is not None  # Type narrowing
        assert assertion.info.filter.type == models.DatasetFilterTypeClass.SQL
        assert assertion.info.filter.sql == additional_filter
    else:
        assert assertion.info.filter is None
    assert assertion.on_success == [
        models.AssertionActionClass(
            type=models.AssertionActionTypeClass.RESOLVE_INCIDENT,
        )
    ]
    assert assertion.on_failure == [
        models.AssertionActionClass(
            type=models.AssertionActionTypeClass.RAISE_INCIDENT,
        )
    ]
    assert assertion.tags == [
        models.TagAssociationClass(tag="urn:li:tag:my_tag_1"),
        models.TagAssociationClass(tag="urn:li:tag:my_tag_2"),
    ]
    assert assertion.source is not None  # Type narrowing
    assert assertion.source.created is not None  # Type narrowing
    assert assertion.source.created.actor == str(
        default_created_updated_params["created_by"]
    )
    assert assertion.source.created.time == make_ts_millis(
        default_created_updated_params["created_at"]
    )
    assert assertion.last_updated is not None  # Type narrowing
    assert assertion.last_updated.actor == str(
        default_created_updated_params["updated_by"]
    )
    assert assertion.last_updated.time == make_ts_millis(
        default_created_updated_params["updated_at"]
    )


@pytest.mark.parametrize(
    "additional_filter",
    [
        pytest.param("id > 1000", id="additional filter"),
        pytest.param(None, id="no additional filter"),
    ],
)
def test_to_assertion_entity_for_smart_freshness_assertion_with_additional_filter_for_last_modified_column(
    additional_filter: Optional[str],
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    detection_mechanism = DetectionMechanism.LAST_MODIFIED_COLUMN(
        column_name="column", additional_filter=additional_filter
    )
    assertion_input = _SmartFreshnessAssertionInput(
        urn=AssertionUrn("urn:li:assertion:123"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        detection_mechanism=detection_mechanism,
        entity_client=freshness_stub_entity_client,
        **default_created_updated_params,
    )
    assertion = assertion_input.to_assertion_entity()
    assert isinstance(
        assertion.info, models.FreshnessAssertionInfoClass
    )  # Type narrowing
    if additional_filter:
        assert assertion.info.filter is not None  # Type narrowing
        assert assertion.info.filter.type == models.DatasetFilterTypeClass.SQL
        assert assertion.info.filter.sql == additional_filter
    else:
        assert assertion.info.filter is None


def test_to_monitor_entity_with_smart_freshness_assertion_defaults(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    assertion_urn = AssertionUrn(
        "urn:li:assertion:123"
    )  # This is created in the assertion, we are passing it in to the monitor since we are not creating the assertion in this test
    assertion_input = _SmartFreshnessAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=freshness_stub_entity_client,
        **default_created_updated_params,
    )
    monitor = assertion_input.to_monitor_entity(assertion_urn)
    assert (
        monitor.urn.entity
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)"
    )
    assert monitor.urn.id  # We don't care about the value, just that it's not empty
    assert monitor.info.status.mode == models.MonitorModeClass.ACTIVE

    # Check the assertion evaluation spec
    assert monitor.info.assertionMonitor is not None  # Type narrowing
    assertion_evaluation_spec = monitor.info.assertionMonitor.assertions[0]
    assert assertion_evaluation_spec.assertion == str(assertion_urn)
    parameters = assertion_evaluation_spec.parameters
    assert parameters is not None  # Type narrowing
    assert (
        parameters.type
        == models.AssertionEvaluationParametersTypeClass.DATASET_FRESHNESS
    )
    freshness_parameters = parameters.datasetFreshnessParameters
    assert freshness_parameters is not None  # Type narrowing
    assert (
        freshness_parameters.sourceType
        == models.DatasetFreshnessSourceTypeClass.INFORMATION_SCHEMA
    )
    assert freshness_parameters.field is None

    # Check the settings
    assert monitor.info.assertionMonitor.settings is not None  # Type narrowing
    settings = monitor.info.assertionMonitor.settings.adjustmentSettings
    assert settings is not None  # Type narrowing
    assert settings.sensitivity is not None  # Type narrowing
    assert settings.sensitivity.level == 5
    assert settings.exclusionWindows == []
    assert (
        settings.trainingDataLookbackWindowDays
        == ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS
    )


def test_to_monitor_entity_with_smart_freshness_assertion_with_all_fields_set(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    # Arrange
    assertion_urn = AssertionUrn("urn:li:assertion:123")
    assertion_input = _SmartFreshnessAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=freshness_stub_entity_client,
        urn=assertion_urn,
        display_name="test_assertion",
        enabled=True,
        detection_mechanism=DetectionMechanism.LAST_MODIFIED_COLUMN(
            column_name="last_modified", additional_filter="id > 1000"
        ),
        sensitivity=InferenceSensitivity.HIGH,
        exclusion_windows=[
            FixedRangeExclusionWindow(
                start=datetime(2025, 1, 1),
                end=datetime(2025, 1, 10),
            )
        ],
        training_data_lookback_days=90,
        incident_behavior=[
            AssertionIncidentBehavior.RAISE_ON_FAIL,
            AssertionIncidentBehavior.RESOLVE_ON_PASS,
        ],
        tags=["urn:li:tag:my_tag_1", "urn:li:tag:my_tag_2"],
        **default_created_updated_params,
    )

    # Act
    monitor = assertion_input.to_monitor_entity(assertion_urn)

    # Assert
    assert (
        monitor.urn.entity
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)"
    )
    assert monitor.urn.id  # We don't care about the value, just that it's not empty
    assert monitor.info.status.mode == models.MonitorModeClass.ACTIVE

    # Check the assertion evaluation spec
    assert monitor.info.assertionMonitor is not None  # Type narrowing
    assertion_evaluation_spec = monitor.info.assertionMonitor.assertions[0]
    parameters = assertion_evaluation_spec.parameters
    assert parameters is not None  # Type narrowing
    assert (
        parameters.type
        == models.AssertionEvaluationParametersTypeClass.DATASET_FRESHNESS
    )
    freshness_parameters = parameters.datasetFreshnessParameters
    assert freshness_parameters is not None  # Type narrowing
    assert (
        freshness_parameters.sourceType
        == models.DatasetFreshnessSourceTypeClass.FIELD_VALUE
    )
    assert freshness_parameters.field is not None  # Type narrowing
    assert freshness_parameters.field.path == "last_modified"

    # Check the settings
    assert monitor.info.assertionMonitor.settings is not None  # Type narrowing
    settings = monitor.info.assertionMonitor.settings.adjustmentSettings
    assert settings is not None  # Type narrowing
    assert settings.sensitivity is not None  # Type narrowing
    assert settings.sensitivity.level == 10
    assert settings.exclusionWindows == [
        models.AssertionExclusionWindowClass(
            type=models.AssertionExclusionWindowTypeClass.FIXED_RANGE,
            displayName="Jan 1, 12:00 AM - Jan 10, 12:00 AM",
            fixedRange=models.AbsoluteTimeWindowClass(
                startTimeMillis=int(datetime(2025, 1, 1).timestamp() * 1000),
                endTimeMillis=int(datetime(2025, 1, 10).timestamp() * 1000),
            ),
        )
    ]
    assert settings.trainingDataLookbackWindowDays == 90


@pytest.mark.parametrize(
    "column, expected_error_message",
    [
        pytest.param(
            "last_modified_wrong_type",
            re.escape(
                "Column last_modified_wrong_type with type NUMBER does not have an allowed type for a last modified column in dataset urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD). Allowed types are ['DATE', 'TIME']."
            ),
            id="last_modified_wrong_type",
        ),
        pytest.param(
            "wrong_type_int_as_a_string",
            re.escape(
                "Column wrong_type_int_as_a_string with type STRING does not have an allowed type for a last modified column in dataset urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD). Allowed types are ['DATE', 'TIME']."
            ),
            id="wrong_type_int_as_a_string",
        ),
    ],
)
def test_to_assertion_and_monitor_entities_with_invalid_field_type(
    freshness_stub_entity_client: StubEntityClient,
    column: str,
    expected_error_message: str,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    assertion_input = _SmartFreshnessAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=freshness_stub_entity_client,
        detection_mechanism=DetectionMechanism.LAST_MODIFIED_COLUMN(
            column_name=column, additional_filter="id > 1000"
        ),
        **default_created_updated_params,
    )
    with pytest.raises(SDKUsageError, match=expected_error_message):
        assertion_input.to_assertion_and_monitor_entities()


def test_to_assertion_and_monitor_entities(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    assertion_input = _SmartFreshnessAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=freshness_stub_entity_client,
        **default_created_updated_params,
    )
    assertion, monitor = assertion_input.to_assertion_and_monitor_entities()
    assert assertion
    assert monitor
    assert assertion.urn  # We don't care about the value, just that it's not empty
    assert (
        monitor.urn.entity
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)"
    )
    assert monitor.urn.id  # We don't care about the value, just that it's not empty


@pytest.mark.parametrize(
    "training_data_lookback_days, expected_days, expected_raises",
    [
        pytest.param(30, 30, nullcontext(), id="valid days"),
        pytest.param(0, 0, nullcontext(), id="zero days"),
        pytest.param(
            -1,
            -1,
            pytest.raises(
                SDKUsageError, match="Training data lookback days must be non-negative"
            ),
            id="negative days",
        ),
        pytest.param("30", 30, nullcontext(), id="string days"),
        pytest.param(
            None,
            ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
            nullcontext(),
            id="default days",
        ),
        pytest.param(
            "invalid",
            None,
            pytest.raises(SDKUsageErrorWithExamples),
            id="invalid string",
        ),
    ],
)
def test_assertion_creation_with_training_data_lookback_days(
    training_data_lookback_days: Union[int, str, None],
    expected_days: int,
    expected_raises: ContextManager[Any],
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": freshness_stub_entity_client,
        **default_created_updated_params,
    }
    if training_data_lookback_days is not None:
        params["training_data_lookback_days"] = training_data_lookback_days  # type: ignore[typeddict-item]  # We're testing the error case
    with expected_raises:
        assertion = _SmartFreshnessAssertionInput(**params)
        assert assertion.training_data_lookback_days == expected_days


@pytest.mark.parametrize(
    "display_name, expected_name",
    [
        pytest.param("Custom Name", "Custom Name", id="custom name"),
        pytest.param(None, None, id="default name"),
        pytest.param("", "", id="empty name"),
    ],
)
def test_assertion_creation_with_display_name(
    display_name: Optional[str],
    expected_name: Optional[str],
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": freshness_stub_entity_client,
        **default_created_updated_params,
    }
    if display_name is not None:
        params["display_name"] = display_name
    assertion = _SmartFreshnessAssertionInput(**params)
    if expected_name is None:
        assert assertion.display_name.startswith(DEFAULT_NAME_PREFIX)
        assert (
            len(assertion.display_name)
            == len(DEFAULT_NAME_PREFIX) + DEFAULT_NAME_SUFFIX_LENGTH + 1
        )  # +1 for hyphen
    else:
        assert assertion.display_name == expected_name


@pytest.mark.parametrize(
    "monitor_mode, expected_mode",
    [
        pytest.param(True, models.MonitorModeClass.ACTIVE, id="enabled"),
        pytest.param(False, models.MonitorModeClass.INACTIVE, id="disabled"),
    ],
)
def test_monitor_entity_mode(
    monitor_mode: bool,
    expected_mode: models.MonitorModeClass,
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    assertion_input = _SmartFreshnessAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=freshness_stub_entity_client,
        enabled=monitor_mode,
        **default_created_updated_params,
    )
    monitor = assertion_input.to_monitor_entity(AssertionUrn("urn:li:assertion:123"))
    assert monitor.info.status.mode == expected_mode


@pytest.mark.parametrize(
    "tags, expected_tags",
    [
        pytest.param(
            "urn:li:tag:my_tag_1",
            [models.TagAssociationClass(tag="urn:li:tag:my_tag_1")],
            id="single string tag",
        ),
        pytest.param(
            ["urn:li:tag:my_tag_1", "urn:li:tag:my_tag_2"],
            [
                models.TagAssociationClass(tag="urn:li:tag:my_tag_1"),
                models.TagAssociationClass(tag="urn:li:tag:my_tag_2"),
            ],
            id="list of string tags",
        ),
        pytest.param(
            None,
            None,
            id="no tags",
        ),
        pytest.param(
            [],
            None,
            id="empty tag list",
        ),
    ],
)
def test_tag_conversion(
    tags: Optional[TagsInputType],
    expected_tags: Optional[TagsInputType],
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": freshness_stub_entity_client,
        **default_created_updated_params,
    }
    if tags is not None:
        params["tags"] = tags
    assertion = _SmartFreshnessAssertionInput(**params)
    assertion_entity = assertion.to_assertion_entity()
    assert assertion_entity.tags == expected_tags


def test_assertion_creation_with_combined_parameters(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": freshness_stub_entity_client,
        "display_name": "Combined Test Assertion",
        "enabled": True,
        "detection_mechanism": DetectionMechanism.LAST_MODIFIED_COLUMN(
            column_name="last_modified",
            additional_filter="id > 1000",
        ),
        "sensitivity": InferenceSensitivity.HIGH,
        "exclusion_windows": [
            FixedRangeExclusionWindow(
                start=datetime(2025, 1, 1),
                end=datetime(2025, 1, 10),
            ),
        ],
        "training_data_lookback_days": 90,
        "incident_behavior": [
            AssertionIncidentBehavior.RAISE_ON_FAIL,
            AssertionIncidentBehavior.RESOLVE_ON_PASS,
        ],
        "tags": ["urn:li:tag:my_tag_1", "urn:li:tag:my_tag_2"],
        **default_created_updated_params,
    }
    assertion = _SmartFreshnessAssertionInput(**params)
    assertion_entity = assertion.to_assertion_entity()
    assert assertion.urn
    monitor_entity = assertion.to_monitor_entity(assertion.urn)

    # Verify assertion entity
    assert assertion_entity.urn == params["urn"]
    assert assertion.display_name == params["display_name"]
    assert assertion_entity.on_success == [
        models.AssertionActionClass(
            type=models.AssertionActionTypeClass.RESOLVE_INCIDENT
        )
    ]
    assert assertion_entity.on_failure == [
        models.AssertionActionClass(type=models.AssertionActionTypeClass.RAISE_INCIDENT)
    ]
    assert assertion_entity.tags == [
        models.TagAssociationClass(tag="urn:li:tag:my_tag_1"),
        models.TagAssociationClass(tag="urn:li:tag:my_tag_2"),
    ]

    # Verify monitor entity
    assert monitor_entity.info.status.mode == models.MonitorModeClass.ACTIVE
    assert monitor_entity.info.assertionMonitor is not None
    assert monitor_entity.info.assertionMonitor.settings is not None
    settings = monitor_entity.info.assertionMonitor.settings.adjustmentSettings
    assert settings is not None
    assert settings.sensitivity is not None
    assert settings.sensitivity.level == 10
    assert settings.trainingDataLookbackWindowDays == 90
    assert settings.exclusionWindows is not None
    assert len(settings.exclusionWindows) == 1


def test_assertion_creation_with_invalid_dataset_urn(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    with pytest.raises(InvalidUrnError):
        _SmartFreshnessAssertionInput(
            urn=AssertionUrn("urn:li:assertion:123"),
            dataset_urn="invalid_dataset_urn",
            entity_client=freshness_stub_entity_client,
            detection_mechanism=DetectionMechanism.LAST_MODIFIED_COLUMN(
                column_name="last_modified"
            ),
            **default_created_updated_params,
        )


def test_assertion_creation_with_missing_required_parameters() -> None:
    with pytest.raises(TypeError):
        _SmartFreshnessAssertionInput(  # type: ignore[call-arg]  # We're testing the error case
            urn=AssertionUrn("urn:li:assertion:123"),
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        )


def test_assertion_creation_with_invalid_detection_mechanism(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    with pytest.raises(SDKUsageErrorWithExamples):
        _SmartFreshnessAssertionInput(
            urn=AssertionUrn("urn:li:assertion:123"),
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
            entity_client=freshness_stub_entity_client,
            **default_created_updated_params,
            detection_mechanism="invalid_mechanism",  # type: ignore
        )


class StubAssertionInput(_AssertionInput):
    """
    A stub implementation of _AssertionInput with abstract methods overridden to return None.
    This allows us to test the methods in _AssertionInput without having to implement the other abstract methods.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def _create_assertion_info(self) -> None:  # type: ignore[override]  # Not used
        return None

    def _create_monitor_info(  # type: ignore[override]  # Not used
        self,
        assertion_urn: AssertionUrn,
        status: models.MonitorStatusClass,
        schedule: models.CronScheduleClass,
        source_type: Union[str, models.DatasetFreshnessSourceTypeClass],
        field: Optional[FieldSpecType],
    ) -> None:
        return None

    def _convert_schedule(self) -> None:  # type: ignore[override]  # Not used
        return None

    def _get_assertion_evaluation_parameters(  # type: ignore[override]  # Not used
        self, source_type: str, field: Optional[models.FreshnessFieldSpecClass]
    ) -> models.AssertionEvaluationParametersClass:
        return None  # type: ignore[return-value]  # Not used

    def _convert_assertion_source_type_and_field(self) -> None:  # type: ignore[override]  # Not used
        return None

    def _create_field_spec(self) -> None:  # type: ignore[override]  # Not used
        return None

    def _assertion_type(self) -> str:
        return models.AssertionTypeClass.FRESHNESS


def test_assertion_creation_with_invalid_source_type(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    with pytest.raises(SDKUsageError):
        StubAssertionInput(
            urn=AssertionUrn("urn:li:assertion:123"),
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
            entity_client=freshness_stub_entity_client,
            **default_created_updated_params,
            source_type="INVALID",
        )


@pytest.mark.parametrize(
    "schedule, timezone, expected_raises",
    [
        # Valid cron expressions
        pytest.param(
            "0 * * * *",  # Every hour
            "UTC",
            nullcontext(),
            id="valid-hourly-cron",
        ),
        pytest.param(
            "0 0 * * *",  # Daily at midnight
            "UTC",
            nullcontext(),
            id="valid-daily-cron",
        ),
        pytest.param(
            "0 0 * * 0",  # Weekly on Sunday
            "UTC",
            nullcontext(),
            id="valid-weekly-cron",
        ),
        pytest.param(
            "0 0 1 * *",  # Monthly on 1st
            "UTC",
            nullcontext(),
            id="valid-monthly-cron",
        ),
        pytest.param(
            "0 0 1 1 *",  # Yearly on Jan 1st
            "UTC",
            nullcontext(),
            id="valid-yearly-cron",
        ),
        pytest.param(
            "*/15 * * * *",  # Every 15 minutes
            "UTC",
            nullcontext(),
            id="valid-minutes-cron",
        ),
        pytest.param(
            "0 0-23/2 * * *",  # Every 2 hours
            "UTC",
            nullcontext(),
            id="valid-range-cron",
        ),
        # Valid timezones
        pytest.param(
            "0 * * * *",
            "America/New_York",
            nullcontext(),
            id="valid-timezone-america",
        ),
        pytest.param(
            "0 * * * *",
            "Europe/London",
            nullcontext(),
            id="valid-timezone-europe",
        ),
        pytest.param(
            "0 * * * *",
            "Asia/Tokyo",
            nullcontext(),
            id="valid-timezone-asia",
        ),
        pytest.param(
            "0 * * * *",
            "Australia/Sydney",
            nullcontext(),
            id="valid-timezone-australia",
        ),
        # Invalid cron expressions
        pytest.param(
            "invalid",
            "UTC",
            pytest.raises(SDKUsageError, match="Invalid cron expression or timezone"),
            id="invalid-cron-expression",
        ),
        pytest.param(
            "0 * * *",  # Missing field
            "UTC",
            pytest.raises(SDKUsageError, match="Invalid cron expression or timezone"),
            id="invalid-cron-missing-field",
        ),
        pytest.param(
            "0 * * * * *",  # Extra field
            "UTC",
            pytest.raises(SDKUsageError, match="Invalid cron expression or timezone"),
            id="invalid-cron-extra-field",
        ),
        pytest.param(
            "99 * * * *",  # Invalid hour
            "UTC",
            pytest.raises(SDKUsageError, match="Invalid cron expression or timezone"),
            id="invalid-cron-hour",
        ),
        pytest.param(
            "0 99 * * *",  # Invalid minute
            "UTC",
            pytest.raises(SDKUsageError, match="Invalid cron expression or timezone"),
            id="invalid-cron-minute",
        ),
        pytest.param(
            "0 0 * * 7",  # sunday=7 is not POSIX.1-2017
            "UTC",
            pytest.raises(SDKUsageError, match="Invalid cron expression or timezone"),
            id="invalid-cron-sunday-7",
        ),
        pytest.param(
            "0 0 * * 6-7",  # sunday=7 is not POSIX.1-2017
            "UTC",
            pytest.raises(SDKUsageError, match="Invalid cron expression or timezone"),
            id="invalid-cron-sunday-7-range",
        ),
        # Invalid timezones
        pytest.param(
            "0 * * * *",
            "Invalid/Timezone",
            pytest.raises(SDKUsageError, match="Invalid cron expression or timezone"),
            id="invalid-timezone",
        ),
        pytest.param(
            "0 * * * *",
            "Not/A/Timezone",
            pytest.raises(SDKUsageError, match="Invalid cron expression or timezone"),
            id="invalid-timezone-format",
        ),
        pytest.param(
            "0 * * * *",
            "",  # Empty timezone
            nullcontext(),
            id="empty-timezone",
        ),
    ],
)
def test_validate_cron_schedule(
    schedule: str,
    timezone: str,
    expected_raises: ContextManager[Any],
) -> None:
    """Test the validation of cron schedule expressions and timezones."""
    with expected_raises:
        _validate_cron_schedule(schedule, timezone)


# Tests for _FreshnessAssertionInput


@pytest.mark.parametrize(
    "freshness_schedule_check_type, lookback_window, expected_raises",
    [
        pytest.param(
            None,
            None,
            nullcontext(),
            id="default - since_the_last_check with no lookback_window",
        ),
        pytest.param(
            models.FreshnessAssertionScheduleTypeClass.SINCE_THE_LAST_CHECK,
            None,
            nullcontext(),
            id="since_the_last_check with no lookback_window",
        ),
        pytest.param(
            "SINCE_THE_LAST_CHECK",
            None,
            nullcontext(),
            id="since_the_last_check (string) with no lookback_window",
        ),
        pytest.param(
            models.FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL,
            TimeWindowSize(multiple=1, unit=CalendarInterval.DAY),
            nullcontext(),
            id="fixed_interval with lookback_window (TimeWindowSize)",
        ),
        # Error cases
        pytest.param(
            models.FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL,
            None,
            pytest.raises(
                SDKUsageError,
                match="Fixed interval freshness assertions must have a lookback_window provided.",
            ),
            id="fixed_interval without lookback_window (error)",
        ),
        pytest.param(
            models.FreshnessAssertionScheduleTypeClass.SINCE_THE_LAST_CHECK,
            TimeWindowSize(multiple=1, unit=CalendarInterval.DAY),
            pytest.raises(
                SDKUsageError,
                match="Since the last check freshness assertions cannot have a lookback_window provided.",
            ),
            id="since_the_last_check with lookback_window (error)",
        ),
    ],
)
def test_freshness_assertion_input_with_schedule_type_and_lookback_window(
    freshness_schedule_check_type: Optional[
        Union[str, models.FreshnessAssertionScheduleTypeClass]
    ],
    lookback_window: Optional[Union[str, dict[str, Union[str, int]], TimeWindowSize]],
    expected_raises: ContextManager[Any],
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": freshness_stub_entity_client,
        **default_created_updated_params,
    }

    # Add freshness-specific parameters
    if freshness_schedule_check_type is not None:
        params["freshness_schedule_check_type"] = freshness_schedule_check_type  # type: ignore
    if lookback_window is not None:
        params["lookback_window"] = lookback_window  # type: ignore

    with expected_raises:
        assertion = _FreshnessAssertionInput(**params)  # type: ignore
        assert assertion.freshness_schedule_check_type == (
            freshness_schedule_check_type
            or models.FreshnessAssertionScheduleTypeClass.SINCE_THE_LAST_CHECK
        )
        if lookback_window:
            assert assertion.lookback_window is not None


@pytest.mark.parametrize(
    "detection_mechanism, expected_type",
    [
        pytest.param(
            DetectionMechanism.INFORMATION_SCHEMA,
            "information_schema",
            id="information_schema",
        ),
        pytest.param(
            DetectionMechanism.AUDIT_LOG,
            "audit_log",
            id="audit_log",
        ),
        pytest.param(
            DetectionMechanism.DATAHUB_OPERATION,
            "datahub_operation",
            id="datahub_operation",
        ),
        pytest.param(
            DetectionMechanism.LAST_MODIFIED_COLUMN(column_name="last_modified"),
            "last_modified_column",
            id="last_modified_column",
        ),
        pytest.param(
            DetectionMechanism.HIGH_WATERMARK_COLUMN(column_name="id"),
            "high_watermark_column",
            id="high_watermark_column",
        ),
        pytest.param(
            DetectionMechanism.HIGH_WATERMARK_COLUMN(
                column_name="id", additional_filter="id = 1"
            ),
            "high_watermark_column",
            id="high_watermark_column with filter",
        ),
    ],
)
def test_freshness_assertion_input_detection_mechanism_support(
    detection_mechanism: _DetectionMechanismTypes,
    expected_type: str,
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test that _FreshnessAssertionInput supports most detection mechanisms including HIGH_WATERMARK_COLUMN."""
    assertion = _FreshnessAssertionInput(
        urn=AssertionUrn("urn:li:assertion:123"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        detection_mechanism=detection_mechanism,
        entity_client=freshness_stub_entity_client,
        **default_created_updated_params,
    )
    assert assertion.detection_mechanism.type == expected_type


def test_freshness_assertion_input_uses_daily_schedule_by_default(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test that _FreshnessAssertionInput uses daily schedule by default."""
    assertion = _FreshnessAssertionInput(
        urn=AssertionUrn("urn:li:assertion:123"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=freshness_stub_entity_client,
        **default_created_updated_params,
    )

    # The default schedule should be daily ("0 0 * * *")
    schedule = assertion._convert_schedule()
    assert schedule.cron == "0 0 * * *"  # Daily at midnight
    assert schedule.timezone == str(tzlocal.get_localzone())


def test_freshness_assertion_input_uses_native_source_type(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test that _FreshnessAssertionInput uses NATIVE source type."""
    assertion = _FreshnessAssertionInput(
        urn=AssertionUrn("urn:li:assertion:123"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=freshness_stub_entity_client,
        **default_created_updated_params,
    )

    assert assertion.source_type == models.AssertionSourceTypeClass.NATIVE


def test_freshness_assertion_input_creates_schedule_with_all_fields(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test that _FreshnessAssertionInput creates assertion info with schedule configuration."""
    assertion = _FreshnessAssertionInput(
        urn=AssertionUrn("urn:li:assertion:123"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=freshness_stub_entity_client,
        freshness_schedule_check_type=models.FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL,
        lookback_window=TimeWindowSize(multiple=2, unit=CalendarInterval.HOUR),
        **default_created_updated_params,
    )

    assertion_info = assertion._create_assertion_info(filter=None)
    assert isinstance(assertion_info, models.FreshnessAssertionInfoClass)

    # Check schedule configuration
    assert assertion_info.schedule is not None
    assert (
        assertion_info.schedule.type
        == models.FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL
    )
    assert assertion_info.schedule.cron is not None
    assert assertion_info.schedule.cron.cron == "0 0 * * *"  # Daily default
    assert assertion_info.schedule.fixedInterval is not None
    assert assertion_info.schedule.fixedInterval.multiple == 2
    assert (
        assertion_info.schedule.fixedInterval.unit == models.CalendarIntervalClass.HOUR
    )


def test_freshness_assertion_input_creates_schedule_since_last_check(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test schedule creation for SINCE_THE_LAST_CHECK type."""
    assertion = _FreshnessAssertionInput(
        urn=AssertionUrn("urn:li:assertion:123"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=freshness_stub_entity_client,
        freshness_schedule_check_type=models.FreshnessAssertionScheduleTypeClass.SINCE_THE_LAST_CHECK,
        **default_created_updated_params,
    )

    assertion_info = assertion._create_assertion_info(filter=None)
    assert isinstance(assertion_info, models.FreshnessAssertionInfoClass)

    # Check schedule configuration
    assert assertion_info.schedule is not None
    assert (
        assertion_info.schedule.type
        == models.FreshnessAssertionScheduleTypeClass.SINCE_THE_LAST_CHECK
    )
    assert assertion_info.schedule.cron is not None
    assert (
        assertion_info.schedule.fixedInterval is None
    )  # Should be None for SINCE_THE_LAST_CHECK


@pytest.mark.parametrize(
    "lookback_window_input, expected_multiple, expected_unit",
    [
        pytest.param(
            models.TimeWindowSizeClass(
                multiple=1, unit=models.CalendarIntervalClass.DAY
            ),
            1,
            models.CalendarIntervalClass.DAY,
            id="model format",
        ),
        pytest.param(
            TimeWindowSize(multiple=3, unit=CalendarInterval.HOUR),
            3,
            models.CalendarIntervalClass.HOUR,
            id="TimeWindowSize object",
        ),
    ],
)
def test_freshness_assertion_input_lookback_window_parsing(
    lookback_window_input: Union[TimeWindowSizeInputTypes],
    expected_multiple: int,
    expected_unit: models.CalendarIntervalClass,
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test that lookback_window is correctly parsed from different input formats."""
    assertion = _FreshnessAssertionInput(
        urn=AssertionUrn("urn:li:assertion:123"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=freshness_stub_entity_client,
        freshness_schedule_check_type=models.FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL,
        lookback_window=lookback_window_input,
        **default_created_updated_params,
    )

    assert assertion.lookback_window is not None
    assert assertion.lookback_window.multiple == expected_multiple
    assert assertion.lookback_window.unit == expected_unit


def test_freshness_assertion_input_supports_high_watermark_detection_mechanism(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test that _FreshnessAssertionInput supports HIGH_WATERMARK_COLUMN detection mechanism."""
    assertion = _FreshnessAssertionInput(
        urn=AssertionUrn("urn:li:assertion:123"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=freshness_stub_entity_client,
        detection_mechanism=DetectionMechanism.HIGH_WATERMARK_COLUMN(
            column_name="last_modified",
            additional_filter="last_modified > '2021-01-01'",
        ),
        **default_created_updated_params,
    )

    source_type, field = assertion._convert_assertion_source_type_and_field()
    assert source_type == models.DatasetFreshnessSourceTypeClass.FIELD_VALUE
    assert field is not None
    assert isinstance(field, models.FreshnessFieldSpecClass)
    assert field.path == "last_modified"
    assert field.kind == models.FreshnessFieldKindClass.HIGH_WATERMARK


@dataclass
class SourceTypeValidityTestParams:
    """Test parameters for _is_source_type_valid test cases.

    Contains input parameters and expected output for the _is_source_type_valid function.
    """

    # Input parameters
    source_type: Type[_DetectionMechanismTypes]
    platform: str
    assertion_type: Union[models.AssertionTypeClass, str]
    # Expected output
    expected_result: bool
    # Test case description
    description: str
    # List of invalid source types for this test case
    invalid_source_types: set[DatasetSourceType] = dataclasses.field(
        default_factory=set
    )


@pytest.mark.parametrize(
    "params",
    [
        pytest.param(
            SourceTypeValidityTestParams(
                source_type=_InformationSchema,
                platform="databricks",
                assertion_type="all",
                expected_result=False,
                description="Exact match with all assertion type",
                invalid_source_types={
                    DatasetSourceType(
                        source_type=_InformationSchema,
                        platform="databricks",
                        assertion_type="all",
                    )
                },
            ),
            id="exact_match_all_assertion_type",
        ),
        pytest.param(
            SourceTypeValidityTestParams(
                source_type=_InformationSchema,
                platform="all",
                assertion_type="all",
                expected_result=False,
                description="All platform and assertion type",
                invalid_source_types={
                    DatasetSourceType(
                        source_type=_InformationSchema,
                        platform="all",
                        assertion_type="all",
                    )
                },
            ),
            id="all_platform_and_assertion_type",
        ),
        pytest.param(
            SourceTypeValidityTestParams(
                source_type=_InformationSchema,
                platform="databricks",
                assertion_type=models.AssertionTypeClass.FRESHNESS,
                expected_result=False,
                description="Exact match with specific assertion type",
                invalid_source_types={
                    DatasetSourceType(
                        source_type=_InformationSchema,
                        platform="databricks",
                        assertion_type=models.AssertionTypeClass.FRESHNESS,
                    )
                },
            ),
            id="exact_match_specific_assertion_type",
        ),
        pytest.param(
            SourceTypeValidityTestParams(
                source_type=_InformationSchema,
                platform="all",
                assertion_type=models.AssertionTypeClass.FRESHNESS,
                expected_result=False,
                description="All platform with specific assertion type",
                invalid_source_types={
                    DatasetSourceType(
                        source_type=_InformationSchema,
                        platform="all",
                        assertion_type=models.AssertionTypeClass.FRESHNESS,
                    )
                },
            ),
            id="all_platform_specific_assertion_type",
        ),
        pytest.param(
            SourceTypeValidityTestParams(
                source_type=_InformationSchema,
                platform="snowflake",
                assertion_type="all",
                expected_result=False,
                description="Platform match with assertion_type='all'",
                invalid_source_types={
                    DatasetSourceType(
                        source_type=_InformationSchema,
                        platform="snowflake",
                        assertion_type="all",
                    )
                },
            ),
            id="platform_match_assertion_type_all",
        ),
        pytest.param(
            SourceTypeValidityTestParams(
                source_type=_InformationSchema,
                platform="snowflake",
                assertion_type=models.AssertionTypeClass.FRESHNESS,
                expected_result=False,
                description="Exact match with both platform and assertion type",
                invalid_source_types={
                    DatasetSourceType(
                        source_type=_InformationSchema,
                        platform="snowflake",
                        assertion_type=models.AssertionTypeClass.FRESHNESS,
                    )
                },
            ),
            id="exact_match_both",
        ),
        pytest.param(
            SourceTypeValidityTestParams(
                source_type=_AuditLog,
                platform="databricks",
                assertion_type="all",
                expected_result=True,
                description="Different source type should be valid",
                invalid_source_types={
                    DatasetSourceType(
                        source_type=_InformationSchema,
                        platform="databricks",
                        assertion_type="all",
                    )
                },
            ),
            id="different_source_type",
        ),
    ],
)
def test_is_source_type_valid(params: SourceTypeValidityTestParams) -> None:
    """Test the _is_source_type_valid function with various input combinations."""
    dataset_source_type = DatasetSourceType(
        source_type=params.source_type,
        platform=params.platform,
        assertion_type=params.assertion_type,
    )
    assert (
        _is_source_type_valid(
            dataset_source_type, invalid_source_types=params.invalid_source_types
        )
        == params.expected_result
    ), params.description


# Tests for SQL Assertions


@pytest.mark.parametrize(
    "assertion_condition, parameters, expected_raises",
    [
        # Valid METRIC assertions
        pytest.param(
            SqlAssertionCondition.IS_GREATER_THAN,
            100,
            nullcontext(),
            id="metric_greater_than_single_value",
        ),
        pytest.param(
            "IS_GREATER_THAN",
            100.5,
            nullcontext(),
            id="metric_greater_than_float_string_params",
        ),
        pytest.param(
            SqlAssertionCondition.IS_WITHIN_A_RANGE,
            (10, 100),
            nullcontext(),
            id="metric_between_range",
        ),
        pytest.param(
            SqlAssertionCondition.IS_EQUAL_TO,
            42,
            nullcontext(),
            id="metric_equal_to",
        ),
        # Valid METRIC_CHANGE assertions
        pytest.param(
            SqlAssertionCondition.GROWS_AT_MOST_ABSOLUTE,
            10,
            nullcontext(),
            id="metric_change_absolute_less_than",
        ),
        pytest.param(
            "GROWS_AT_LEAST_PERCENTAGE",
            5.0,
            nullcontext(),
            id="metric_change_percentage_gte_string_params",
        ),
        pytest.param(
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_ABSOLUTE,
            (-10, 10),
            nullcontext(),
            id="metric_change_absolute_between",
        ),
        # Error cases - wrong parameters for operator
        pytest.param(
            SqlAssertionCondition.IS_WITHIN_A_RANGE,
            100,  # Should be tuple for BETWEEN
            pytest.raises(SDKUsageError, match="must be a tuple range for condition"),
            id="between_operator_single_value_error",
        ),
        pytest.param(
            SqlAssertionCondition.IS_GREATER_THAN,
            (10, 100),  # Should be single value for GREATER_THAN
            pytest.raises(
                SDKUsageError, match="must be a single value numeric for condition"
            ),
            id="single_value_operator_tuple_error",
        ),
    ],
)
def test_sql_assertion_criteria_validation(
    assertion_condition: Union[SqlAssertionCondition, str],
    parameters: Union[Union[float, int], tuple[Union[float, int], Union[float, int]]],
    expected_raises: ContextManager[Any],
) -> None:
    """Test SqlAssertionCriteria validation with various inputs."""
    criteria = SqlAssertionCriteria(
        condition=assertion_condition,
        parameters=parameters,
    )

    with expected_raises:
        SqlAssertionCriteria.validate(criteria)
        # The validation doesn't modify the original criteria, it just validates it
        # Note: criteria might have been converted by Pydantic if string inputs were used
        assert (
            criteria.condition == assertion_condition
            or str(criteria.condition.value) == assertion_condition  # type: ignore
        )
        assert criteria.parameters == parameters


@pytest.mark.parametrize(
    "assertion_condition, expected_model_type",
    [
        # METRIC type conditions
        pytest.param(
            SqlAssertionCondition.IS_EQUAL_TO,
            models.SqlAssertionTypeClass.METRIC,
            id="is_equal_to_metric",
        ),
        pytest.param(
            SqlAssertionCondition.IS_NOT_EQUAL_TO,
            models.SqlAssertionTypeClass.METRIC,
            id="is_not_equal_to_metric",
        ),
        pytest.param(
            SqlAssertionCondition.IS_GREATER_THAN,
            models.SqlAssertionTypeClass.METRIC,
            id="is_greater_than_metric",
        ),
        pytest.param(
            SqlAssertionCondition.IS_LESS_THAN,
            models.SqlAssertionTypeClass.METRIC,
            id="is_less_than_metric",
        ),
        pytest.param(
            SqlAssertionCondition.IS_WITHIN_A_RANGE,
            models.SqlAssertionTypeClass.METRIC,
            id="is_within_a_range_metric",
        ),
        # String versions
        pytest.param(
            "IS_EQUAL_TO",
            models.SqlAssertionTypeClass.METRIC,
            id="string_is_equal_to_metric",
        ),
        pytest.param(
            "IS_NOT_EQUAL_TO",
            models.SqlAssertionTypeClass.METRIC,
            id="string_is_not_equal_to_metric",
        ),
        pytest.param(
            "IS_GREATER_THAN",
            models.SqlAssertionTypeClass.METRIC,
            id="string_is_greater_than_metric",
        ),
        pytest.param(
            "IS_LESS_THAN",
            models.SqlAssertionTypeClass.METRIC,
            id="string_is_less_than_metric",
        ),
        pytest.param(
            "IS_WITHIN_A_RANGE",
            models.SqlAssertionTypeClass.METRIC,
            id="string_is_within_a_range_metric",
        ),
        # METRIC_CHANGE type conditions
        pytest.param(
            SqlAssertionCondition.GROWS_AT_LEAST_ABSOLUTE,
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            id="grows_at_least_absolute_metric_change",
        ),
        pytest.param(
            SqlAssertionCondition.GROWS_AT_LEAST_PERCENTAGE,
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            id="grows_at_least_percentage_metric_change",
        ),
        pytest.param(
            SqlAssertionCondition.GROWS_AT_MOST_ABSOLUTE,
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            id="grows_at_most_absolute_metric_change",
        ),
        pytest.param(
            SqlAssertionCondition.GROWS_AT_MOST_PERCENTAGE,
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            id="grows_at_most_percentage_metric_change",
        ),
        pytest.param(
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_ABSOLUTE,
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            id="grows_within_a_range_absolute_metric_change",
        ),
        pytest.param(
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_PERCENTAGE,
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            id="grows_within_a_range_percentage_metric_change",
        ),
        # String versions for metric change
        pytest.param(
            "GROWS_AT_LEAST_ABSOLUTE",
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            id="string_grows_at_least_absolute_metric_change",
        ),
        pytest.param(
            "GROWS_AT_LEAST_PERCENTAGE",
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            id="string_grows_at_least_percentage_metric_change",
        ),
        pytest.param(
            "GROWS_AT_MOST_ABSOLUTE",
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            id="string_grows_at_most_absolute_metric_change",
        ),
        pytest.param(
            "GROWS_AT_MOST_PERCENTAGE",
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            id="string_grows_at_most_percentage_metric_change",
        ),
        pytest.param(
            "GROWS_WITHIN_A_RANGE_ABSOLUTE",
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            id="string_grows_within_a_range_absolute_metric_change",
        ),
        pytest.param(
            "GROWS_WITHIN_A_RANGE_PERCENTAGE",
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            id="string_grows_within_a_range_percentage_metric_change",
        ),
    ],
)
def test_sql_assertion_criteria_get_type_from_condition(
    assertion_condition: Union[SqlAssertionCondition, str],
    expected_model_type: models.SqlAssertionTypeClass,
) -> None:
    """Test SqlAssertionCriteria.get_type_from_condition() conversion."""
    result = SqlAssertionCriteria.get_type_from_condition(assertion_condition)
    assert result == expected_model_type


@pytest.mark.parametrize(
    "condition, expected_model_type",
    [
        # Absolute change type conditions
        pytest.param(
            SqlAssertionCondition.GROWS_AT_MOST_ABSOLUTE,
            models.AssertionValueChangeTypeClass.ABSOLUTE,
            id="grows_at_most_absolute",
        ),
        pytest.param(
            SqlAssertionCondition.GROWS_AT_LEAST_ABSOLUTE,
            models.AssertionValueChangeTypeClass.ABSOLUTE,
            id="grows_at_least_absolute",
        ),
        pytest.param(
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_ABSOLUTE,
            models.AssertionValueChangeTypeClass.ABSOLUTE,
            id="grows_within_range_absolute",
        ),
        # Percentage change type conditions
        pytest.param(
            SqlAssertionCondition.GROWS_AT_MOST_PERCENTAGE,
            models.AssertionValueChangeTypeClass.PERCENTAGE,
            id="grows_at_most_percentage",
        ),
        pytest.param(
            SqlAssertionCondition.GROWS_AT_LEAST_PERCENTAGE,
            models.AssertionValueChangeTypeClass.PERCENTAGE,
            id="grows_at_least_percentage",
        ),
        pytest.param(
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_PERCENTAGE,
            models.AssertionValueChangeTypeClass.PERCENTAGE,
            id="grows_within_range_percentage",
        ),
        # String versions of change type conditions
        pytest.param(
            "GROWS_AT_MOST_ABSOLUTE",
            models.AssertionValueChangeTypeClass.ABSOLUTE,
            id="string_grows_at_most_absolute",
        ),
        pytest.param(
            "GROWS_AT_LEAST_ABSOLUTE",
            models.AssertionValueChangeTypeClass.ABSOLUTE,
            id="string_grows_at_least_absolute",
        ),
        pytest.param(
            "GROWS_WITHIN_A_RANGE_ABSOLUTE",
            models.AssertionValueChangeTypeClass.ABSOLUTE,
            id="string_grows_within_range_absolute",
        ),
        pytest.param(
            "GROWS_AT_MOST_PERCENTAGE",
            models.AssertionValueChangeTypeClass.PERCENTAGE,
            id="string_grows_at_most_percentage",
        ),
        pytest.param(
            "GROWS_AT_LEAST_PERCENTAGE",
            models.AssertionValueChangeTypeClass.PERCENTAGE,
            id="string_grows_at_least_percentage",
        ),
        pytest.param(
            "GROWS_WITHIN_A_RANGE_PERCENTAGE",
            models.AssertionValueChangeTypeClass.PERCENTAGE,
            id="string_grows_within_range_percentage",
        ),
        # Value-based conditions (no change type)
        pytest.param(
            SqlAssertionCondition.IS_EQUAL_TO,
            None,
            id="is_equal_to_no_change_type",
        ),
        pytest.param(
            SqlAssertionCondition.IS_NOT_EQUAL_TO,
            None,
            id="is_not_equal_to_no_change_type",
        ),
        pytest.param(
            SqlAssertionCondition.IS_GREATER_THAN,
            None,
            id="is_greater_than_no_change_type",
        ),
        pytest.param(
            SqlAssertionCondition.IS_LESS_THAN,
            None,
            id="is_less_than_no_change_type",
        ),
        pytest.param(
            SqlAssertionCondition.IS_WITHIN_A_RANGE,
            None,
            id="is_within_a_range_no_change_type",
        ),
        # String versions of value-based conditions
        pytest.param(
            "IS_EQUAL_TO",
            None,
            id="string_is_equal_to_no_change_type",
        ),
        pytest.param(
            "IS_NOT_EQUAL_TO",
            None,
            id="string_is_not_equal_to_no_change_type",
        ),
        pytest.param(
            "IS_GREATER_THAN",
            None,
            id="string_is_greater_than_no_change_type",
        ),
        pytest.param(
            "IS_LESS_THAN",
            None,
            id="string_is_less_than_no_change_type",
        ),
        pytest.param(
            "IS_WITHIN_A_RANGE",
            None,
            id="string_is_within_a_range_no_change_type",
        ),
    ],
)
def test_sql_assertion_criteria_get_change_type_from_condition(
    condition: Union[SqlAssertionCondition, str],
    expected_model_type: Optional[models.AssertionValueChangeTypeClass],
) -> None:
    """Test SqlAssertionCriteria.get_change_type_from_condition() conversion."""
    result = SqlAssertionCriteria.get_change_type_from_condition(condition)
    assert result == expected_model_type


@pytest.mark.parametrize(
    "condition, expected_model_operator",
    [
        # Value-based operators
        pytest.param(
            SqlAssertionCondition.IS_EQUAL_TO,
            models.AssertionStdOperatorClass.EQUAL_TO,
            id="is_equal_to_operator",
        ),
        pytest.param(
            SqlAssertionCondition.IS_NOT_EQUAL_TO,
            models.AssertionStdOperatorClass.NOT_EQUAL_TO,
            id="is_not_equal_to_operator",
        ),
        pytest.param(
            SqlAssertionCondition.IS_GREATER_THAN,
            models.AssertionStdOperatorClass.GREATER_THAN,
            id="is_greater_than_operator",
        ),
        pytest.param(
            SqlAssertionCondition.IS_LESS_THAN,
            models.AssertionStdOperatorClass.LESS_THAN,
            id="is_less_than_operator",
        ),
        pytest.param(
            SqlAssertionCondition.IS_WITHIN_A_RANGE,
            models.AssertionStdOperatorClass.BETWEEN,
            id="is_within_a_range_operator",
        ),
        # String versions of value-based operators
        pytest.param(
            "IS_EQUAL_TO",
            models.AssertionStdOperatorClass.EQUAL_TO,
            id="string_is_equal_to_operator",
        ),
        pytest.param(
            "IS_NOT_EQUAL_TO",
            models.AssertionStdOperatorClass.NOT_EQUAL_TO,
            id="string_is_not_equal_to_operator",
        ),
        pytest.param(
            "IS_GREATER_THAN",
            models.AssertionStdOperatorClass.GREATER_THAN,
            id="string_is_greater_than_operator",
        ),
        pytest.param(
            "IS_LESS_THAN",
            models.AssertionStdOperatorClass.LESS_THAN,
            id="string_is_less_than_operator",
        ),
        pytest.param(
            "IS_WITHIN_A_RANGE",
            models.AssertionStdOperatorClass.BETWEEN,
            id="string_is_within_a_range_operator",
        ),
        # Growth-based operators (absolute)
        pytest.param(
            SqlAssertionCondition.GROWS_AT_LEAST_ABSOLUTE,
            models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
            id="grows_at_least_absolute_operator",
        ),
        pytest.param(
            SqlAssertionCondition.GROWS_AT_MOST_ABSOLUTE,
            models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
            id="grows_at_most_absolute_operator",
        ),
        pytest.param(
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_ABSOLUTE,
            models.AssertionStdOperatorClass.BETWEEN,
            id="grows_within_a_range_absolute_operator",
        ),
        # Growth-based operators (percentage)
        pytest.param(
            SqlAssertionCondition.GROWS_AT_LEAST_PERCENTAGE,
            models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
            id="grows_at_least_percentage_operator",
        ),
        pytest.param(
            SqlAssertionCondition.GROWS_AT_MOST_PERCENTAGE,
            models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
            id="grows_at_most_percentage_operator",
        ),
        pytest.param(
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_PERCENTAGE,
            models.AssertionStdOperatorClass.BETWEEN,
            id="grows_within_a_range_percentage_operator",
        ),
        # String versions of growth-based operators
        pytest.param(
            "GROWS_AT_LEAST_ABSOLUTE",
            models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
            id="string_grows_at_least_absolute_operator",
        ),
        pytest.param(
            "GROWS_AT_MOST_ABSOLUTE",
            models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
            id="string_grows_at_most_absolute_operator",
        ),
        pytest.param(
            "GROWS_WITHIN_A_RANGE_ABSOLUTE",
            models.AssertionStdOperatorClass.BETWEEN,
            id="string_grows_within_a_range_absolute_operator",
        ),
        pytest.param(
            "GROWS_AT_LEAST_PERCENTAGE",
            models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
            id="string_grows_at_least_percentage_operator",
        ),
        pytest.param(
            "GROWS_AT_MOST_PERCENTAGE",
            models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
            id="string_grows_at_most_percentage_operator",
        ),
        pytest.param(
            "GROWS_WITHIN_A_RANGE_PERCENTAGE",
            models.AssertionStdOperatorClass.BETWEEN,
            id="string_grows_within_a_range_percentage_operator",
        ),
    ],
)
def test_sql_assertion_criteria_get_operator_from_condition(
    condition: Union[SqlAssertionCondition, str],
    expected_model_operator: models.AssertionStdOperatorClass,
) -> None:
    """Test SqlAssertionCriteria.get_operator_from_condition() conversion."""
    result = SqlAssertionCriteria.get_operator_from_condition(condition)
    assert result == expected_model_operator


@pytest.mark.parametrize(
    "parameters, expected_params_type",
    [
        pytest.param(
            100,
            "single_value",
            id="single_int_value",
        ),
        pytest.param(
            100.5,
            "single_value",
            id="single_float_value",
        ),
        pytest.param(
            (10, 100),
            "range",
            id="tuple_range",
        ),
        pytest.param(
            (10.5, 100.5),
            "range",
            id="tuple_float_range",
        ),
    ],
)
def test_sql_assertion_criteria_get_parameters(
    parameters: Union[Union[float, int], tuple[Union[float, int], Union[float, int]]],
    expected_params_type: str,
) -> None:
    """Test SqlAssertionCriteria.get_parameters() conversion."""
    result = SqlAssertionCriteria.get_parameters(parameters)
    assert isinstance(result, models.AssertionStdParametersClass)

    if expected_params_type == "single_value":
        assert result.value is not None
        assert result.value.type == models.AssertionStdParameterTypeClass.NUMBER
        assert result.value.value == str(parameters)
        assert result.minValue is None
        assert result.maxValue is None
    elif expected_params_type == "range":
        assert result.value is None
        assert result.minValue is not None
        assert result.maxValue is not None
        assert result.minValue.type == models.AssertionStdParameterTypeClass.NUMBER
        assert result.maxValue.type == models.AssertionStdParameterTypeClass.NUMBER
        # min is first element, max is second element
        assert result.minValue.value == str(parameters[0])  # type: ignore
        assert result.maxValue.value == str(parameters[1])  # type: ignore


def test_sql_assertion_input_creation_basic(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test basic _SqlAssertionInput creation."""
    criteria = SqlAssertionCriteria(
        condition=SqlAssertionCondition.IS_GREATER_THAN,
        parameters=100,
    )

    assertion_input = _SqlAssertionInput(
        urn=AssertionUrn("urn:li:assertion:sql_test"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_table,PROD)",
        entity_client=freshness_stub_entity_client,
        criteria=criteria,
        statement="SELECT COUNT(*) FROM test_table",
        **default_created_updated_params,
    )

    assert assertion_input.urn == AssertionUrn("urn:li:assertion:sql_test")
    assert (
        str(assertion_input.dataset_urn)
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_table,PROD)"
    )
    assert assertion_input.criteria == criteria
    assert assertion_input.statement == "SELECT COUNT(*) FROM test_table"
    assert assertion_input.source_type == models.AssertionSourceTypeClass.NATIVE


def test_sql_assertion_input_creation_with_all_parameters(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test _SqlAssertionInput creation with all parameters."""
    criteria = SqlAssertionCriteria(
        condition=SqlAssertionCondition.GROWS_WITHIN_A_RANGE_PERCENTAGE,
        parameters=(-10.0, 10.0),
    )

    assertion_input = _SqlAssertionInput(
        urn=AssertionUrn("urn:li:assertion:sql_test_full"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test_dataset.test_table,PROD)",
        entity_client=freshness_stub_entity_client,
        criteria=criteria,
        statement="SELECT AVG(value) FROM test_dataset.test_table WHERE created_date >= CURRENT_DATE()",
        display_name="Test SQL Assertion",
        enabled=True,
        schedule=models.CronScheduleClass(cron="0 */6 * * *", timezone="UTC"),
        incident_behavior=[
            AssertionIncidentBehavior.RAISE_ON_FAIL,
            AssertionIncidentBehavior.RESOLVE_ON_PASS,
        ],
        tags=["urn:li:tag:sql_assertion", "urn:li:tag:automated"],
        **default_created_updated_params,
    )

    assert assertion_input.display_name == "Test SQL Assertion"
    assert assertion_input.enabled is True
    assert assertion_input.schedule is not None
    assert assertion_input.schedule.cron == "0 */6 * * *"
    assert assertion_input.schedule.timezone == "UTC"
    assert len(assertion_input.incident_behavior) == 2  # type: ignore
    assert AssertionIncidentBehavior.RAISE_ON_FAIL in assertion_input.incident_behavior  # type: ignore
    assert (
        AssertionIncidentBehavior.RESOLVE_ON_PASS in assertion_input.incident_behavior  # type: ignore
    )
    assert assertion_input.tags == ["urn:li:tag:sql_assertion", "urn:li:tag:automated"]


def test_sql_assertion_input_create_assertion_info(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test _SqlAssertionInput._create_assertion_info() method."""
    criteria = SqlAssertionCriteria(
        condition=SqlAssertionCondition.IS_LESS_THAN,
        parameters=500,
    )

    assertion_input = _SqlAssertionInput(
        urn=AssertionUrn("urn:li:assertion:sql_info_test"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:postgres,public.users,PROD)",
        entity_client=freshness_stub_entity_client,
        criteria=criteria,
        statement="SELECT COUNT(*) FROM public.users WHERE active = true",
        **default_created_updated_params,
    )

    assertion_info = assertion_input._create_assertion_info(filter=None)

    assert isinstance(assertion_info, models.SqlAssertionInfoClass)
    assert assertion_info.type == models.SqlAssertionTypeClass.METRIC
    assert (
        assertion_info.entity
        == "urn:li:dataset:(urn:li:dataPlatform:postgres,public.users,PROD)"
    )
    assert (
        assertion_info.statement
        == "SELECT COUNT(*) FROM public.users WHERE active = true"
    )
    assert assertion_info.changeType is None
    assert assertion_info.operator == models.AssertionStdOperatorClass.LESS_THAN
    assert assertion_info.parameters.value is not None
    assert assertion_info.parameters.value.value == "500.0"


def test_sql_assertion_input_create_assertion_info_with_change_type(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test _SqlAssertionInput._create_assertion_info() with change type."""
    criteria = SqlAssertionCriteria(
        condition=SqlAssertionCondition.GROWS_WITHIN_A_RANGE_ABSOLUTE,
        parameters=(5, 50),
    )

    assertion_input = _SqlAssertionInput(
        urn=AssertionUrn("urn:li:assertion:sql_change_test"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:mysql,analytics.events,PROD)",
        entity_client=freshness_stub_entity_client,
        criteria=criteria,
        statement="SELECT COUNT(*) FROM analytics.events WHERE event_date = CURDATE()",
        **default_created_updated_params,
    )

    assertion_info = assertion_input._create_assertion_info(filter=None)

    assert isinstance(assertion_info, models.SqlAssertionInfoClass)
    assert assertion_info.type == models.SqlAssertionTypeClass.METRIC_CHANGE
    assert assertion_info.changeType == models.AssertionValueChangeTypeClass.ABSOLUTE
    assert assertion_info.operator == models.AssertionStdOperatorClass.BETWEEN
    assert assertion_info.parameters.minValue is not None
    assert assertion_info.parameters.maxValue is not None
    assert assertion_info.parameters.minValue.value == "5.0"  # min is first element
    assert assertion_info.parameters.maxValue.value == "50.0"  # max is second element


def test_sql_assertion_input_create_monitor_info(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test _SqlAssertionInput._create_monitor_info() method."""
    criteria = SqlAssertionCriteria(
        condition=SqlAssertionCondition.IS_GREATER_THAN,
        parameters=1000,
    )

    assertion_input = _SqlAssertionInput(
        urn=AssertionUrn("urn:li:assertion:sql_monitor_test"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:redshift,public.orders,PROD)",
        entity_client=freshness_stub_entity_client,
        criteria=criteria,
        statement="SELECT SUM(amount) FROM public.orders WHERE order_date >= CURRENT_DATE",
        **default_created_updated_params,
    )

    assertion_urn = AssertionUrn("urn:li:assertion:sql_monitor_test")
    status = models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE)
    schedule = models.CronScheduleClass(cron="0 12 * * *", timezone="UTC")

    monitor_info = assertion_input._create_monitor_info(assertion_urn, status, schedule)

    assert isinstance(monitor_info, models.MonitorInfoClass)
    assert monitor_info.type == models.MonitorTypeClass.ASSERTION
    assert monitor_info.status == status
    assert monitor_info.assertionMonitor is not None
    assert len(monitor_info.assertionMonitor.assertions) == 1

    assertion_spec = monitor_info.assertionMonitor.assertions[0]
    assert assertion_spec.assertion == str(assertion_urn)
    assert assertion_spec.schedule == schedule
    assert assertion_spec.parameters is not None
    assert (
        assertion_spec.parameters.type
        == models.AssertionEvaluationParametersTypeClass.DATASET_SQL
    )


def test_sql_assertion_input_get_assertion_evaluation_parameters(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test _SqlAssertionInput._get_assertion_evaluation_parameters() method."""
    criteria = SqlAssertionCriteria(
        condition=SqlAssertionCondition.IS_EQUAL_TO,
        parameters=42,
    )

    assertion_input = _SqlAssertionInput(
        urn=AssertionUrn("urn:li:assertion:sql_params_test"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:hive,default.test_table,PROD)",
        entity_client=freshness_stub_entity_client,
        criteria=criteria,
        statement="SELECT 42",
        **default_created_updated_params,
    )

    params = assertion_input._get_assertion_evaluation_parameters("unused", None)

    assert isinstance(params, models.AssertionEvaluationParametersClass)
    assert params.type == models.AssertionEvaluationParametersTypeClass.DATASET_SQL


def test_sql_assertion_input_convert_schedule_default(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test _SqlAssertionInput._convert_schedule() with default schedule."""
    criteria = SqlAssertionCriteria(
        condition=SqlAssertionCondition.IS_NOT_EQUAL_TO,
        parameters=0,
    )

    assertion_input = _SqlAssertionInput(
        urn=AssertionUrn("urn:li:assertion:sql_schedule_test"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:kafka,test_topic,PROD)",
        entity_client=freshness_stub_entity_client,
        criteria=criteria,
        statement="SELECT COUNT(*) FROM test_topic",
        **default_created_updated_params,
    )

    schedule = assertion_input._convert_schedule()

    assert isinstance(schedule, models.CronScheduleClass)
    assert schedule.cron == DEFAULT_EVERY_SIX_HOURS_SCHEDULE.cron
    assert schedule.timezone == DEFAULT_EVERY_SIX_HOURS_SCHEDULE.timezone


def test_sql_assertion_input_convert_schedule_custom(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test _SqlAssertionInput._convert_schedule() with custom schedule."""
    criteria = SqlAssertionCriteria(
        condition=SqlAssertionCondition.IS_GREATER_THAN,
        parameters=100,
    )

    custom_schedule = models.CronScheduleClass(
        cron="0 */4 * * *", timezone="America/New_York"
    )

    assertion_input = _SqlAssertionInput(
        urn=AssertionUrn("urn:li:assertion:sql_custom_schedule_test"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:s3,bucket.dataset,PROD)",
        entity_client=freshness_stub_entity_client,
        criteria=criteria,
        statement="SELECT COUNT(*) FROM s3_table",
        schedule=custom_schedule,
        **default_created_updated_params,
    )

    schedule = assertion_input._convert_schedule()

    assert isinstance(schedule, models.CronScheduleClass)
    assert schedule.cron == "0 */4 * * *"
    assert schedule.timezone == "America/New_York"


def test_sql_assertion_input_unused_methods(
    freshness_stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    """Test _SqlAssertionInput methods that are not used for SQL assertions."""
    criteria = SqlAssertionCriteria(
        condition=SqlAssertionCondition.IS_LESS_THAN,
        parameters=999,
    )

    assertion_input = _SqlAssertionInput(
        urn=AssertionUrn("urn:li:assertion:sql_unused_test"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:oracle,hr.employees,PROD)",
        entity_client=freshness_stub_entity_client,
        criteria=criteria,
        statement="SELECT COUNT(*) FROM hr.employees",
        **default_created_updated_params,
    )

    # Test methods that return placeholders for SQL assertions
    source_type, field = assertion_input._convert_assertion_source_type_and_field()
    assert source_type == "None"
    assert field is None

    filter_result = assertion_input._create_filter_from_detection_mechanism()
    assert filter_result is None

    assertion_type = assertion_input._assertion_type()
    assert assertion_type == models.AssertionTypeClass.SQL


def test_sql_assertion_condition_type_mapping() -> None:
    """Test that SqlAssertionCondition properly maps to model class values."""
    # Test METRIC conditions
    assert (
        SqlAssertionCriteria.get_type_from_condition(SqlAssertionCondition.IS_EQUAL_TO)
        == models.SqlAssertionTypeClass.METRIC
    )
    assert (
        SqlAssertionCriteria.get_type_from_condition(
            SqlAssertionCondition.IS_GREATER_THAN
        )
        == models.SqlAssertionTypeClass.METRIC
    )
    assert (
        SqlAssertionCriteria.get_type_from_condition(
            SqlAssertionCondition.IS_WITHIN_A_RANGE
        )
        == models.SqlAssertionTypeClass.METRIC
    )

    # Test METRIC_CHANGE conditions
    assert (
        SqlAssertionCriteria.get_type_from_condition(
            SqlAssertionCondition.GROWS_AT_LEAST_ABSOLUTE
        )
        == models.SqlAssertionTypeClass.METRIC_CHANGE
    )
    assert (
        SqlAssertionCriteria.get_type_from_condition(
            SqlAssertionCondition.GROWS_AT_MOST_ABSOLUTE
        )
        == models.SqlAssertionTypeClass.METRIC_CHANGE
    )
    assert (
        SqlAssertionCriteria.get_type_from_condition(
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_ABSOLUTE
        )
        == models.SqlAssertionTypeClass.METRIC_CHANGE
    )


def test_sql_assertion_condition_operator_mapping() -> None:
    """Test that SqlAssertionCondition properly maps to operators."""
    # Test operator mappings
    assert (
        SqlAssertionCriteria.get_operator_from_condition(
            SqlAssertionCondition.IS_EQUAL_TO
        )
        == models.AssertionStdOperatorClass.EQUAL_TO
    )
    assert (
        SqlAssertionCriteria.get_operator_from_condition(
            SqlAssertionCondition.IS_NOT_EQUAL_TO
        )
        == models.AssertionStdOperatorClass.NOT_EQUAL_TO
    )
    assert (
        SqlAssertionCriteria.get_operator_from_condition(
            SqlAssertionCondition.IS_GREATER_THAN
        )
        == models.AssertionStdOperatorClass.GREATER_THAN
    )
    assert (
        SqlAssertionCriteria.get_operator_from_condition(
            SqlAssertionCondition.IS_LESS_THAN
        )
        == models.AssertionStdOperatorClass.LESS_THAN
    )
    assert (
        SqlAssertionCriteria.get_operator_from_condition(
            SqlAssertionCondition.IS_WITHIN_A_RANGE
        )
        == models.AssertionStdOperatorClass.BETWEEN
    )
    assert (
        SqlAssertionCriteria.get_operator_from_condition(
            SqlAssertionCondition.GROWS_AT_LEAST_ABSOLUTE
        )
        == models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO
    )
    assert (
        SqlAssertionCriteria.get_operator_from_condition(
            SqlAssertionCondition.GROWS_AT_MOST_ABSOLUTE
        )
        == models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO
    )
    assert (
        SqlAssertionCriteria.get_operator_from_condition(
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_ABSOLUTE
        )
        == models.AssertionStdOperatorClass.BETWEEN
    )


@pytest.mark.parametrize(
    "assertion_type, operator, change_type, expected_condition, expected_exception",
    [
        # METRIC type assertions (value-based) - success cases
        pytest.param(
            models.SqlAssertionTypeClass.METRIC,
            models.AssertionStdOperatorClass.EQUAL_TO,
            None,
            SqlAssertionCondition.IS_EQUAL_TO,
            nullcontext(),
            id="metric_equal_to",
        ),
        pytest.param(
            models.SqlAssertionTypeClass.METRIC,
            models.AssertionStdOperatorClass.NOT_EQUAL_TO,
            None,
            SqlAssertionCondition.IS_NOT_EQUAL_TO,
            nullcontext(),
            id="metric_not_equal_to",
        ),
        pytest.param(
            models.SqlAssertionTypeClass.METRIC,
            models.AssertionStdOperatorClass.GREATER_THAN,
            None,
            SqlAssertionCondition.IS_GREATER_THAN,
            nullcontext(),
            id="metric_greater_than",
        ),
        pytest.param(
            models.SqlAssertionTypeClass.METRIC,
            models.AssertionStdOperatorClass.LESS_THAN,
            None,
            SqlAssertionCondition.IS_LESS_THAN,
            nullcontext(),
            id="metric_less_than",
        ),
        pytest.param(
            models.SqlAssertionTypeClass.METRIC,
            models.AssertionStdOperatorClass.BETWEEN,
            None,
            SqlAssertionCondition.IS_WITHIN_A_RANGE,
            nullcontext(),
            id="metric_between",
        ),
        # METRIC_CHANGE type assertions (growth-based with absolute change) - success cases
        pytest.param(
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
            models.AssertionValueChangeTypeClass.ABSOLUTE,
            SqlAssertionCondition.GROWS_AT_MOST_ABSOLUTE,
            nullcontext(),
            id="metric_change_grows_at_most_absolute",
        ),
        pytest.param(
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
            models.AssertionValueChangeTypeClass.ABSOLUTE,
            SqlAssertionCondition.GROWS_AT_LEAST_ABSOLUTE,
            nullcontext(),
            id="metric_change_grows_at_least_absolute",
        ),
        pytest.param(
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            models.AssertionStdOperatorClass.BETWEEN,
            models.AssertionValueChangeTypeClass.ABSOLUTE,
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_ABSOLUTE,
            nullcontext(),
            id="metric_change_grows_within_range_absolute",
        ),
        # METRIC_CHANGE type assertions (growth-based with percentage change) - success cases
        pytest.param(
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
            models.AssertionValueChangeTypeClass.PERCENTAGE,
            SqlAssertionCondition.GROWS_AT_MOST_PERCENTAGE,
            nullcontext(),
            id="metric_change_grows_at_most_percentage",
        ),
        pytest.param(
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
            models.AssertionValueChangeTypeClass.PERCENTAGE,
            SqlAssertionCondition.GROWS_AT_LEAST_PERCENTAGE,
            nullcontext(),
            id="metric_change_grows_at_least_percentage",
        ),
        pytest.param(
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            models.AssertionStdOperatorClass.BETWEEN,
            models.AssertionValueChangeTypeClass.PERCENTAGE,
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_PERCENTAGE,
            nullcontext(),
            id="metric_change_grows_within_range_percentage",
        ),
        # Error cases
        pytest.param(
            models.SqlAssertionTypeClass.METRIC,
            "UNSUPPORTED_OPERATOR",  # Invalid operator
            None,
            None,
            pytest.raises(ValueError, match="Unsupported combination"),
            id="metric_unsupported_operator_error",
        ),
        pytest.param(
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
            None,  # Missing required changeType
            None,
            pytest.raises(
                AssertionError,
                match="changeType must be present for METRIC_CHANGE assertions",
            ),
            id="metric_change_missing_change_type_error",
        ),
        pytest.param(
            models.SqlAssertionTypeClass.METRIC_CHANGE,
            "UNSUPPORTED_OPERATOR",  # Invalid operator for metric change
            models.AssertionValueChangeTypeClass.ABSOLUTE,
            None,
            pytest.raises(ValueError, match="Unsupported combination"),
            id="metric_change_unsupported_operator_error",
        ),
        pytest.param(
            "UNSUPPORTED_TYPE",  # Invalid assertion type
            models.AssertionStdOperatorClass.EQUAL_TO,
            None,
            None,
            pytest.raises(ValueError, match="Unsupported combination"),
            id="unsupported_assertion_type_error",
        ),
    ],
)
def test_sql_assertion_get_condition_from_model_assertion_info(
    assertion_type: Union[models.SqlAssertionTypeClass, str],
    operator: Union[models.AssertionStdOperatorClass, str],
    change_type: Optional[models.AssertionValueChangeTypeClass],
    expected_condition: Optional[SqlAssertionCondition],
    expected_exception: ContextManager[Any],
) -> None:
    """Test SqlAssertion._get_condition_from_model_assertion_info() conversion with success and error cases."""
    # Create mock assertion info
    assertion_info = models.SqlAssertionInfoClass(
        type=assertion_type,
        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_table,PROD)",
        statement="SELECT COUNT(*) FROM table",
        operator=operator,
        parameters=models.AssertionStdParametersClass(
            value=models.AssertionStdParameterClass(
                type=models.AssertionStdParameterTypeClass.NUMBER,
                value="100",
            )
        ),
        changeType=change_type,
    )

    # Test the method
    with expected_exception:
        result = SqlAssertion._get_condition_from_model_assertion_info(assertion_info)
        # Only assert the result if we expect success (no exception)
        if expected_condition is not None:
            assert result == expected_condition
