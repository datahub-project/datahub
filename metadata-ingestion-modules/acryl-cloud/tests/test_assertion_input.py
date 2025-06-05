import re
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Any, ContextManager, Optional, TypedDict, Union, cast

import pytest
from pydantic import ValidationError

import datahub.metadata.schema_classes as models
from acryl_datahub_cloud._sdk_extras.assertion_input import (
    ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
    DEFAULT_NAME_PREFIX,
    DEFAULT_NAME_SUFFIX_LENGTH,
    AssertionIncidentBehavior,
    DetectionMechanism,
    FixedRangeExclusionWindow,
    FixedRangeExclusionWindowInputTypes,
    InferenceSensitivity,
    SDKUsageErrorWithExamples,
    _AssertionInput,
    _DetectionMechanismTypes,
    _SmartFreshnessAssertionInput,
)
from acryl_datahub_cloud._sdk_extras.entities.assertion import (
    TagsInputType,
)
from acryl_datahub_cloud._sdk_extras.errors import SDKUsageError
from datahub.emitter.mce_builder import make_ts_millis
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, InvalidUrnError
from datahub.sdk.entity_client import EntityClient
from tests.conftest import StubEntityClient


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
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": stub_entity_client,
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
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    assertion = _SmartFreshnessAssertionInput(
        urn=AssertionUrn("urn:li:assertion:123"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        display_name="test_assertion",
        detection_mechanism=detection_mechanism_str,
        entity_client=stub_entity_client,
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
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": stub_entity_client,
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
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": stub_entity_client,
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
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": stub_entity_client,
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
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": stub_entity_client,
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
    stub_entity_client: StubEntityClient,
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
        entity_client=stub_entity_client,
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
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    detection_mechanism = DetectionMechanism.LAST_MODIFIED_COLUMN(
        column_name="column", additional_filter=additional_filter
    )
    assertion_input = _SmartFreshnessAssertionInput(
        urn=AssertionUrn("urn:li:assertion:123"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        detection_mechanism=detection_mechanism,
        entity_client=stub_entity_client,
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
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    assertion_urn = AssertionUrn(
        "urn:li:assertion:123"
    )  # This is created in the assertion, we are passing it in to the monitor since we are not creating the assertion in this test
    assertion_input = _SmartFreshnessAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=stub_entity_client,
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
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    # Arrange
    assertion_urn = AssertionUrn("urn:li:assertion:123")
    assertion_input = _SmartFreshnessAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=stub_entity_client,
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
                "Column last_modified_wrong_type with type NumberTypeClass does not have an allowed type for a last modified column in dataset urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD). Allowed types are ['DateTypeClass', 'TimeTypeClass']."
            ),
            id="last_modified_wrong_type",
        ),
        pytest.param(
            "wrong_type_int_as_a_string",
            re.escape(
                "Column wrong_type_int_as_a_string with type StringTypeClass does not have an allowed type for a last modified column in dataset urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD). Allowed types are ['DateTypeClass', 'TimeTypeClass']."
            ),
            id="wrong_type_int_as_a_string",
        ),
    ],
)
def test_to_assertion_and_monitor_entities_with_invalid_field_type(
    stub_entity_client: StubEntityClient,
    column: str,
    expected_error_message: str,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    assertion_input = _SmartFreshnessAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=stub_entity_client,
        detection_mechanism=DetectionMechanism.LAST_MODIFIED_COLUMN(
            column_name=column, additional_filter="id > 1000"
        ),
        **default_created_updated_params,
    )
    with pytest.raises(SDKUsageError, match=expected_error_message):
        assertion_input.to_assertion_and_monitor_entities()


def test_to_assertion_and_monitor_entities(
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    assertion_input = _SmartFreshnessAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=stub_entity_client,
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
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": stub_entity_client,
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
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": stub_entity_client,
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
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    assertion_input = _SmartFreshnessAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        entity_client=stub_entity_client,
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
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": stub_entity_client,
        **default_created_updated_params,
    }
    if tags is not None:
        params["tags"] = tags
    assertion = _SmartFreshnessAssertionInput(**params)
    assertion_entity = assertion.to_assertion_entity()
    assert assertion_entity.tags == expected_tags


def test_assertion_creation_with_combined_parameters(
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        "entity_client": stub_entity_client,
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
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    with pytest.raises(InvalidUrnError):
        _SmartFreshnessAssertionInput(
            urn=AssertionUrn("urn:li:assertion:123"),
            dataset_urn="invalid_dataset_urn",
            entity_client=stub_entity_client,
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
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    with pytest.raises(SDKUsageErrorWithExamples):
        _SmartFreshnessAssertionInput(
            urn=AssertionUrn("urn:li:assertion:123"),
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
            entity_client=stub_entity_client,
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

    def _convert_schedule(self) -> None:  # type: ignore[override]  # Not used
        return None

    def _convert_assertion_source_type_and_field(self) -> None:  # type: ignore[override]  # Not used
        return None

    def _create_field_spec(self) -> None:  # type: ignore[override]  # Not used
        return None


def test_assertion_creation_with_invalid_source_type(
    stub_entity_client: StubEntityClient,
    default_created_updated_params: CreatedUpdatedParams,
) -> None:
    with pytest.raises(SDKUsageError):
        StubAssertionInput(
            urn=AssertionUrn("urn:li:assertion:123"),
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
            entity_client=stub_entity_client,
            **default_created_updated_params,
            source_type="INVALID",
        )
