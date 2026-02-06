"""Unit tests for Google Sheets schema module."""

from datahub.ingestion.source.google_sheets.constants import NativeDataType
from datahub.ingestion.source.google_sheets.utils import (
    infer_column_type,
    is_date,
    is_number,
)
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NumberTypeClass,
)


class TestTypeInferenceUtils:
    """Tests for type inference utility functions in utils.py."""

    def test_infer_column_type_number(self):
        """Test infer_column_type for numeric columns."""
        data_rows = [["123"], ["456"], ["789.5"]]
        result = infer_column_type(data_rows, 0)

        assert isinstance(result.data_type, NumberTypeClass)
        assert result.native_type == NativeDataType.NUMBER.value

    def test_infer_column_type_boolean(self):
        """Test infer_column_type for boolean columns."""
        data_rows = [["true"], ["false"], ["yes"]]
        result = infer_column_type(data_rows, 0)

        assert isinstance(result.data_type, BooleanTypeClass)
        assert result.native_type == NativeDataType.BOOLEAN.value

    def test_infer_column_type_date(self):
        """Test infer_column_type for date columns."""
        data_rows = [["2024-01-15"], ["2024-02-20"], ["2024-03-10"]]
        result = infer_column_type(data_rows, 0)

        assert isinstance(result.data_type, DateTypeClass)
        assert result.native_type == NativeDataType.DATE.value

    def test_is_number_util(self):
        """Test is_number utility function."""
        assert is_number("123")
        assert is_number("123.45")
        assert is_number("-123.45")
        assert not is_number("abc")
        assert not is_number("")

    def test_is_date_util(self):
        """Test is_date utility function."""
        assert is_date("2024-01-15")
        assert is_date("01/15/2024")
        assert is_date("Jan 15, 2024")
        assert not is_date("not a date")
        assert not is_date("123")
