from unittest.mock import patch

from datahub_integrations.gen_ai.description_context import (
    ColumnMetadataInfo,
)
from datahub_integrations.gen_ai.description_v3 import FieldPathProcessor


def test_init() -> None:
    """Test FieldPathProcessor initialization."""
    column_infos = {
        "simple_field": ColumnMetadataInfo(column_name="simple_field"),
        "[version=2.0].[type=string].complex_field": ColumnMetadataInfo(
            column_name="[version=2.0].[type=string].complex_field"
        ),
    }

    processor = FieldPathProcessor(column_infos)

    assert processor.original_column_infos == column_infos
    assert processor.v1_column_infos == {}
    assert processor.v1_to_v2_mapping == {}
    assert processor.conversion_successful is False


def test_restore_v2_paths_successful_conversion() -> None:
    column_infos = {
        "[version=2.0].[type=string].field1": ColumnMetadataInfo(
            column_name="[version=2.0].[type=string].field1"
        ),
        "[version=2.0].[type=int].field2": ColumnMetadataInfo(
            column_name="[version=2.0].[type=int].field2"
        ),
    }

    processor = FieldPathProcessor(column_infos)
    result_fields = processor.simplify()  # This will set conversion_successful to True
    assert processor.conversion_successful is True
    assert result_fields == processor.v1_column_infos
    assert "field1" in result_fields
    assert result_fields["field1"].column_name == "field1"

    column_descriptions = {
        "field1": "Description for field1",
        "field2": "Description for field2",
    }

    result = processor.restore_v2_paths(column_descriptions)

    expected = {
        "[version=2.0].[type=string].field1": "Description for field1",
        "[version=2.0].[type=int].field2": "Description for field2",
    }
    assert result == expected


def test_restore_v2_paths_failed_conversion() -> None:
    column_infos = {
        "[version=2.0].[type=string].field1": ColumnMetadataInfo(
            column_name="[version=2.0].[type=string].field1"
        ),
        "[version=2.0].[type=int].field1": ColumnMetadataInfo(
            column_name="[version=2.0].[type=int].field1"
        ),
    }

    processor = FieldPathProcessor(column_infos)
    result = (
        processor.simplify()
    )  # This will set conversion_successful to False due to collision
    assert processor.conversion_successful is False
    assert result == processor.original_column_infos
    assert result == column_infos

    column_descriptions = {
        "[version=2.0].[type=string].field1": "Description for field1",
        "[version=2.0].[type=int].field1": "Description for field1 int",
    }

    result_fields = processor.restore_v2_paths(column_descriptions)

    # Should return unchanged descriptions when conversion failed
    assert result_fields == column_descriptions


def test_mixed_v1_and_v2_paths() -> None:
    column_infos = {
        "simple_field": ColumnMetadataInfo(column_name="simple_field"),
        "[version=2.0].[type=string].complex_field": ColumnMetadataInfo(
            column_name="[version=2.0].[type=string].complex_field"
        ),
        "another_simple": ColumnMetadataInfo(column_name="another_simple"),
    }

    processor = FieldPathProcessor(column_infos)
    result = processor.simplify()

    assert processor.conversion_successful is True
    assert len(result) == 3
    assert "simple_field" in result
    assert "complex_field" in result
    assert "another_simple" in result


def test_restore_v2_paths_missing_mapping() -> None:
    column_infos = {
        "[version=2.0].[type=string].field1": ColumnMetadataInfo(
            column_name="[version=2.0].[type=string].field1"
        ),
    }

    processor = FieldPathProcessor(column_infos)
    processor.simplify()

    # Manually remove a mapping to simulate missing mapping
    processor.v1_to_v2_mapping.pop("field1", None)

    column_descriptions = {
        "field1": "Description for field1",
    }

    with patch("datahub_integrations.gen_ai.description_v3.logger") as mock_logger:
        result = processor.restore_v2_paths(column_descriptions)

        mock_logger.warning.assert_called_once()
        assert (
            "No v2 mapping found for v1 field path"
            in mock_logger.warning.call_args[0][0]
        )
        assert result["field1"] == "Description for field1"  # Should keep original
