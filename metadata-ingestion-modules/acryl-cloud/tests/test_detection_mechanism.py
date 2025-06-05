import pytest

from acryl_datahub_cloud._sdk_extras.assertion_input import (
    DetectionMechanism,
    SDKUsageErrorWithExamples,
    _DetectionMechanismTypes,
)


@pytest.mark.parametrize(
    "mechanism_str, expected_type",
    [
        ("information_schema", "information_schema"),
        ("audit_log", "audit_log"),
        ("datahub_operation", "datahub_operation"),
    ],
)
def test_parse_detection_mechanism_with_str(
    mechanism_str: str, expected_type: str
) -> None:
    detection_mechanism = DetectionMechanism.parse(mechanism_str)
    assert detection_mechanism.type == expected_type


def test_parse_detection_mechanism_with_str_invalid_type() -> None:
    with pytest.raises(SDKUsageErrorWithExamples) as e:
        DetectionMechanism.parse("invalid")
    assert "Invalid detection mechanism type: invalid" in str(e.value)


@pytest.mark.parametrize(
    "mechanism_str",
    [
        "last_modified_column",
        "high_watermark_column",
    ],
)
def test_parse_detection_mechanism_with_str_missing_required_kwargs(
    mechanism_str: str,
) -> None:
    with pytest.raises(SDKUsageErrorWithExamples) as e:
        DetectionMechanism.parse(mechanism_str)
    assert (
        f"Detection mechanism type '{mechanism_str}' requires additional parameters:"
        in str(e.value)
    )


@pytest.mark.parametrize(
    "mechanism_dict, expected_type",
    [
        ({"type": "information_schema"}, "information_schema"),
        ({"type": "audit_log"}, "audit_log"),
        ({"type": "datahub_operation"}, "datahub_operation"),
        (
            {"type": "last_modified_column", "column_name": "last_modified"},
            "last_modified_column",
        ),
        (
            {
                "type": "last_modified_column",
                "column_name": "last_modified",
                "additional_filter": "last_modified > '2021-01-01'",
            },
            "last_modified_column",
        ),
        (
            {"type": "high_watermark_column", "column_name": "id"},
            "high_watermark_column",
        ),
        (
            {
                "type": "high_watermark_column",
                "column_name": "id",
                "additional_filter": "id > 1000",
            },
            "high_watermark_column",
        ),
    ],
)
def test_parse_detection_mechanism_with_dict(
    mechanism_dict: dict[str, str], expected_type: str
) -> None:
    detection_mechanism = DetectionMechanism.parse(mechanism_dict)
    assert detection_mechanism.type == expected_type

    # Check that additional kwargs if present are passed to the
    # detection mechanism e.g. column for last_modified_column
    if len(mechanism_dict) > 1:
        for key, value in mechanism_dict.items():
            if key != "type":
                assert getattr(detection_mechanism, key) == value


def test_parse_detection_mechanism_with_dict_missing_type() -> None:
    with pytest.raises(SDKUsageErrorWithExamples) as e:
        DetectionMechanism.parse({})
    assert (
        "Detection mechanism type is required if using a dict to create a DetectionMechanism"
        in str(e.value)
    )


def test_parse_detection_mechanism_with_dict_invalid_type() -> None:
    with pytest.raises(SDKUsageErrorWithExamples) as e:
        DetectionMechanism.parse({"type": "invalid"})
    assert "Invalid detection mechanism type: invalid" in str(e.value)


def test_parse_detection_mechanism_with_extra_unrecognized_kwargs_when_no_kwargs_are_expected() -> (
    None
):
    with pytest.raises(SDKUsageErrorWithExamples) as e:
        DetectionMechanism.parse({"type": "information_schema", "extra": "extra"})
    assert (
        "Invalid additional fields specified for detection mechanism 'information_schema': {'extra': 'extra'}"
        in str(e.value)
    )


def test_parse_detection_mechanism_with_extra_unrecognized_kwargs_and_missing_required_kwargs_when_some_kwargs_are_expected() -> (
    None
):
    with pytest.raises(SDKUsageErrorWithExamples) as e:
        DetectionMechanism.parse({"type": "last_modified_column", "extra": "extra"})
    assert (
        "Invalid detection mechanism type 'last_modified_column': {'extra': 'extra'}"
        in str(e.value)
    )


@pytest.mark.parametrize(
    "mechanism_instance, expected_type, expected_kwargs",
    [
        (DetectionMechanism.INFORMATION_SCHEMA, "information_schema", {}),
        (DetectionMechanism.AUDIT_LOG, "audit_log", {}),
        (DetectionMechanism.DATAHUB_OPERATION, "datahub_operation", {}),
        (
            DetectionMechanism.LAST_MODIFIED_COLUMN(column_name="last_modified"),
            "last_modified_column",
            {"column_name": "last_modified"},
        ),
        (
            DetectionMechanism.LAST_MODIFIED_COLUMN(
                column_name="last_modified",
                additional_filter="last_modified > '2021-01-01'",
            ),
            "last_modified_column",
            {
                "column_name": "last_modified",
                "additional_filter": "last_modified > '2021-01-01'",
            },
        ),
        (
            DetectionMechanism.HIGH_WATERMARK_COLUMN(column_name="id"),
            "high_watermark_column",
            {"column_name": "id"},
        ),
        (
            DetectionMechanism.HIGH_WATERMARK_COLUMN(
                column_name="id", additional_filter="id > 1000"
            ),
            "high_watermark_column",
            {"column_name": "id", "additional_filter": "id > 1000"},
        ),
    ],
)
def test_parse_detection_mechanism_with_detection_mechanism_instance(
    mechanism_instance: _DetectionMechanismTypes,
    expected_type: str,
    expected_kwargs: dict[str, str],
) -> None:
    detection_mechanism = DetectionMechanism.parse(mechanism_instance)
    assert detection_mechanism.type == expected_type
    for key, value in expected_kwargs.items():
        assert getattr(detection_mechanism, key) == value


def test_invalid_detection_mechanism_type() -> None:
    with pytest.raises(SDKUsageErrorWithExamples) as e:
        DetectionMechanism.parse(detection_mechanism_config=1)  # type: ignore[arg-type]  # Purposely testing an invalid type
    assert "Invalid detection mechanism: 1" in str(e.value)


def test_parse_detection_mechanism_with_class_object() -> None:
    with pytest.raises(SDKUsageErrorWithExamples) as e:
        DetectionMechanism.parse(
            DetectionMechanism.LAST_MODIFIED_COLUMN  # type: ignore[arg-type]  # Purposely testing an invalid type
        )
    assert (
        "Invalid detection mechanism: <class 'acryl_datahub_cloud._sdk_extras.assertion_input._LastModifiedColumn'>"
        in str(e.value)
    )


class NotADetectionMechanism:
    pass


def test_parse_detection_mechanism_with_unrelated_instance() -> None:
    with pytest.raises(SDKUsageErrorWithExamples) as e:
        DetectionMechanism.parse(NotADetectionMechanism())  # type: ignore[arg-type]  # Purposely testing an invalid type
    assert "Invalid detection mechanism:" in str(e.value)
