from unittest.mock import Mock

import pytest

from datahub.ingestion.source.vertexai.protobuf_utils import (
    extract_numeric_value,
    extract_protobuf_value,
)


class TestExtractProtobufValue:
    @pytest.mark.parametrize(
        "field_name,field_value,expected",
        [
            ("string_value", "hello world", "hello world"),
            ("number_value", 42.5, "42.5"),
            ("bool_value", True, "True"),
            ("bool_value", False, "False"),
        ],
    )
    def test_extract_value_by_field(self, field_name, field_value, expected):
        mock_value = Mock()
        # Delete other fields to ensure we're testing the right fallback path
        for field in ["string_value", "number_value", "bool_value"]:
            if field != field_name:
                delattr(mock_value, field)
        setattr(mock_value, field_name, field_value)

        result = extract_protobuf_value(mock_value)

        assert result == expected

    def test_extract_fallback_to_str(self):
        mock_value = Mock()
        del mock_value.string_value
        del mock_value.number_value
        del mock_value.bool_value

        result = extract_protobuf_value(mock_value)

        assert result is not None

    def test_extract_empty_string_falls_through(self):
        mock_value = Mock()
        mock_value.string_value = ""
        mock_value.number_value = 42

        result = extract_protobuf_value(mock_value)

        assert result == "42"


class TestExtractNumericValue:
    @pytest.mark.parametrize(
        "number_value,expected",
        [
            (3.14159, "3.14159"),
            (0, "0"),
            (-42.7, "-42.7"),
            (100, "100"),
        ],
    )
    def test_extract_number_field(self, number_value, expected):
        mock_value = Mock()
        mock_value.number_value = number_value

        result = extract_numeric_value(mock_value)

        assert result == expected

    @pytest.mark.parametrize(
        "string_value,expected",
        [
            ("123.456", "123.456"),
            ("0", "0"),
            ("-99.9", "-99.9"),
        ],
    )
    def test_extract_numeric_string(self, string_value, expected):
        mock_value = Mock()
        del mock_value.number_value
        mock_value.string_value = string_value

        result = extract_numeric_value(mock_value)

        assert result == expected

    def test_extract_non_numeric_string_returns_none(self):
        mock_value = Mock()
        del mock_value.number_value
        mock_value.string_value = "not a number"

        result = extract_numeric_value(mock_value)

        assert result is None

    @pytest.mark.parametrize(
        "str_repr,expected",
        [
            ("99.9", "99.9"),
            ("invalid", None),
        ],
    )
    def test_extract_fallback_string_representation(self, str_repr, expected):
        class CustomValue:
            def __str__(self):
                return str_repr

        mock_value = CustomValue()

        result = extract_numeric_value(mock_value)

        assert result == expected
