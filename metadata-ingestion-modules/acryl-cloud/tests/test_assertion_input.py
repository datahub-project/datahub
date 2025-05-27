from contextlib import nullcontext
from datetime import datetime
from typing import Any, ContextManager, TypedDict, Union, cast

import pytest
from pydantic import ValidationError

from acryl_datahub_cloud._sdk_extras.assertion_input import (
    AssertionIncidentBehavior,
    DetectionMechanism,
    FixedRangeExclusionWindow,
    FixedRangeExclusionWindowInputTypes,
    InferenceSensitivity,
    SDKUsageErrorWithExamples,
    _AssertionInput,
)
from datahub.metadata.urns import AssertionUrn, DatasetUrn


class AssertionInputParams(TypedDict, total=False):
    urn: Union[str, AssertionUrn]
    dataset_urn: Union[str, DatasetUrn]
    display_name: str
    detection_mechanism: Union[
        str, dict[str, str], DetectionMechanism.DETECTION_MECHANISM_TYPES
    ]
    sensitivity: Union[str, InferenceSensitivity]
    exclusion_windows: Union[
        dict[str, datetime],
        list[dict[str, datetime]],
        FixedRangeExclusionWindow,
        list[FixedRangeExclusionWindow],
    ]
    training_data_lookback_days: int
    incident_behavior: Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]


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
            DetectionMechanism.HIGH_WATER_MARK_COLUMN(column_name="id"),
            "high_water_mark_column",
            {"column_name": "id"},
            id="high_water_mark_column (column_name only)",
        ),
        pytest.param(
            DetectionMechanism.HIGH_WATER_MARK_COLUMN(
                column_name="id", additional_filter="id > 1000"
            ),
            "high_water_mark_column",
            {"column_name": "id", "additional_filter": "id > 1000"},
            id="high_water_mark_column (column_name + additional_filter)",
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
    detection_mechanism: DetectionMechanism.DETECTION_MECHANISM_TYPES,
    expected_type: str,
    expected_additional_kwargs: dict[str, str],
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
    }
    if detection_mechanism:
        params["detection_mechanism"] = detection_mechanism
    assertion = _AssertionInput(**params)
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
) -> None:
    assertion = _AssertionInput(
        urn=AssertionUrn("urn:li:assertion:123"),
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        display_name="test_assertion",
        detection_mechanism=detection_mechanism_str,
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
            {"type": "high_water_mark_column", "column_name": "id"},
            "high_water_mark_column",
            {"column_name": "id"},
            id="dict: high_water_mark_column (column_name only)",
        ),
        pytest.param(
            {
                "type": "high_water_mark_column",
                "column_name": "id",
                "additional_filter": "id > 1000",
            },
            "high_water_mark_column",
            {"column_name": "id", "additional_filter": "id > 1000"},
            id="dict: high_water_mark_column (column_name + additional_filter)",
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
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
    }
    if detection_mechanism_dict:
        params["detection_mechanism"] = detection_mechanism_dict
    assertion = _AssertionInput(**params)
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
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
    }
    if sensitivity:
        params["sensitivity"] = sensitivity
    with expected_raises:
        assertion = _AssertionInput(**params)
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
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
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
        assertion = _AssertionInput(**params)
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
) -> None:
    params: AssertionInputParams = {
        "urn": AssertionUrn("urn:li:assertion:123"),
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
    }
    if incident_behavior is not None:
        params["incident_behavior"] = cast(
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]],
            incident_behavior,
        )
    with expected_raises:
        assertion = _AssertionInput(**params)
        assert assertion.incident_behavior == expected_incident_behavior
