import pytest

from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    BIGQUERY_NUMERIC_TYPES,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.filter_builder import (
    FilterBuilder,
)


class TestFilterBuilderNumericTypes:
    def test_all_numeric_types_unquoted(self):
        for numeric_type in BIGQUERY_NUMERIC_TYPES:
            int_filter = FilterBuilder.create_safe_filter("col", "123", numeric_type)
            assert int_filter == "`col` = 123", numeric_type

            float_filter = FilterBuilder.create_safe_filter(
                "col", "99.99", numeric_type
            )
            assert float_filter == "`col` = 99.99", numeric_type

    def test_case_insensitive_numeric_types(self):
        filter_lower = FilterBuilder.create_safe_filter("col", "123", "int64")
        filter_upper = FilterBuilder.create_safe_filter("col", "123", "INT64")
        filter_mixed = FilterBuilder.create_safe_filter("col", "123", "Int64")

        assert filter_lower == filter_upper == filter_mixed == "`col` = 123"


class TestFilterBuilderEdgeCases:
    """Test FilterBuilder edge cases and error handling."""

    def test_quote_escaping_in_string_values(self):
        """Test that single quotes in values are properly escaped."""
        filter_expr = FilterBuilder.create_safe_filter("name", "O'Brien", "STRING")
        assert filter_expr == "`name` = 'O''Brien'"
        assert filter_expr.count("'") == 4  # Opening, 2 escaped, closing

    def test_invalid_numeric_value_raises(self):
        # A string value on a numeric column would build an invalid INT64 = STRING filter.
        with pytest.raises(ValueError, match="Non-numeric value"):
            FilterBuilder.create_safe_filter("int_col", "not_a_number", "INT64")

    def test_sql_injection_protection_semicolon(self):
        """Test that semicolons in values are rejected."""
        with pytest.raises(ValueError, match="Invalid value for filter"):
            FilterBuilder.create_safe_filter("col", "value; DROP TABLE", "STRING")

    def test_sql_injection_protection_comment(self):
        """Test that SQL comments in values are rejected."""
        with pytest.raises(ValueError, match="Invalid value for filter"):
            FilterBuilder.create_safe_filter("col", "value--comment", "STRING")

    def test_sql_injection_protection_multiline_comment(self):
        """Test that multiline comments in values are rejected."""
        with pytest.raises(ValueError, match="Invalid value for filter"):
            FilterBuilder.create_safe_filter("col", "value/*comment*/", "STRING")

    def test_invalid_column_name(self):
        """Test that invalid column names are rejected."""
        with pytest.raises(ValueError, match="Invalid column name for filter"):
            FilterBuilder.create_safe_filter("col; DROP TABLE", "value", "STRING")

    def test_negative_numeric_values(self):
        """Test that negative numeric values are handled correctly."""
        filter_expr = FilterBuilder.create_safe_filter("amount", "-123.45", "NUMERIC")
        assert filter_expr == "`amount` = -123.45"
        assert "'" not in filter_expr

    def test_zero_numeric_value(self):
        """Test that zero is handled correctly for numeric types."""
        filter_expr = FilterBuilder.create_safe_filter("count", "0", "INT64")
        assert filter_expr == "`count` = 0"
        assert "'" not in filter_expr

    def test_integer_value_object(self):
        """Test that integer value objects are converted correctly."""
        filter_expr = FilterBuilder.create_safe_filter("year", 2024, "INT64")
        assert filter_expr == "`year` = 2024"
        assert "'" not in filter_expr

    def test_float_value_object(self):
        """Test that float value objects are converted correctly."""
        filter_expr = FilterBuilder.create_safe_filter("ratio", 0.123, "FLOAT64")
        assert filter_expr == "`ratio` = 0.123"
        assert "'" not in filter_expr


class TestFilterBuilderPartitionIdConversion:
    """Test FilterBuilder partition ID to filter conversion."""

    def test_convert_yyyymmdd_partition_id(self):
        """Test conversion of YYYYMMDD partition ID to date filter."""
        filters = FilterBuilder.convert_partition_id_to_filters(
            "20250115", ["date_col"]
        )
        assert filters is not None
        assert len(filters) == 1
        assert filters[0] == "`date_col` = '2025-01-15'"

    def test_convert_yyyymmddhh_partition_id(self):
        """Test conversion of YYYYMMDDHH partition ID to date filter."""
        filters = FilterBuilder.convert_partition_id_to_filters(
            "2025011523", ["datetime_col"]
        )
        assert filters is not None
        assert len(filters) == 1
        assert filters[0] == "`datetime_col` = '2025-01-15'"

    def test_convert_multi_column_partition_id(self):
        """Test conversion of multi-column partition ID."""
        filters = FilterBuilder.convert_partition_id_to_filters(
            "year=2025$month=01$day=15", ["year", "month", "day"]
        )
        assert filters is not None
        assert len(filters) == 3
        assert "`year` = '2025'" in filters
        assert "`month` = '01'" in filters
        assert "`day` = '15'" in filters

    def test_convert_partition_id_with_non_required_columns(self):
        """Test that only required columns are included in filters."""
        filters = FilterBuilder.convert_partition_id_to_filters(
            "year=2025$month=01$day=15", ["year", "day"]
        )
        assert filters is not None
        assert len(filters) == 2
        assert "`year` = '2025'" in filters
        assert "`day` = '15'" in filters
        assert not any("month" in f for f in filters)

    def test_convert_simple_partition_id_single_column(self):
        """Test conversion of simple partition ID with single column."""
        filters = FilterBuilder.convert_partition_id_to_filters("2025", ["year"])
        assert filters is not None
        assert len(filters) == 1
        assert filters[0] == "`year` = '2025'"

    def test_convert_partition_id_multiple_columns_returns_none(self):
        """Test that complex multi-column scenarios return None."""
        filters = FilterBuilder.convert_partition_id_to_filters(
            "20250115", ["year", "month", "day"]
        )
        assert filters is None

    def test_convert_empty_partition_id(self):
        """Test that empty partition IDs create empty string filter."""
        filters = FilterBuilder.convert_partition_id_to_filters("", ["col"])
        assert filters is not None
        assert len(filters) == 1
        assert filters[0] == "`col` = ''"

    def test_convert_hive_style_with_numeric_columns(self):
        """Test Hive-style partition with numeric column types (unquoted)."""
        column_types = {
            "venue": "STRING",
            "product_type": "STRING",
            "date": "INT64",
        }
        filters = FilterBuilder.convert_partition_id_to_filters(
            "venue=okx$product_type=swap$date=20251201",
            ["venue", "product_type", "date"],
            column_types,
        )
        assert filters is not None
        assert len(filters) == 3
        assert "`venue` = 'okx'" in filters
        assert "`product_type` = 'swap'" in filters
        assert "`date` = 20251201" in filters

    def test_convert_hive_style_without_column_types(self):
        """Test Hive-style partition without column types defaults to quoted."""
        filters = FilterBuilder.convert_partition_id_to_filters(
            "venue=okx$product_type=swap$date=20251201",
            ["venue", "product_type", "date"],
        )
        assert filters is not None
        assert len(filters) == 3
        assert "`venue` = 'okx'" in filters
        assert "`product_type` = 'swap'" in filters
        assert "`date` = '20251201'" in filters


class TestFilterBuilderYearlyRange:
    """A bare-YYYY partition ID on a date/datetime/timestamp column is a yearly
    partition whose rows span the whole year, so it must become a half-open full-year
    range rather than an equality to Jan 1 (which would exclude the rest of the year)."""

    def test_year_on_date_column_is_full_year_range(self):
        filter_expr = FilterBuilder.create_safe_filter("event_date", "2025", "DATE")
        assert (
            filter_expr
            == "`event_date` >= '2025-01-01' AND `event_date` < '2026-01-01'"
        )

    def test_year_on_datetime_column_is_full_year_range(self):
        filter_expr = FilterBuilder.create_safe_filter("event_dt", "2025", "DATETIME")
        assert filter_expr == "`event_dt` >= '2025-01-01' AND `event_dt` < '2026-01-01'"

    def test_year_on_timestamp_column_wraps_bounds_in_timestamp(self):
        filter_expr = FilterBuilder.create_safe_filter("event_ts", "2025", "TIMESTAMP")
        assert filter_expr == (
            "`event_ts` >= TIMESTAMP('2025-01-01') "
            "AND `event_ts` < TIMESTAMP('2026-01-01')"
        )

    def test_year_on_string_column_is_plain_equality(self):
        # Without a date-typed column a bare YYYY is a real string value, not a year.
        filter_expr = FilterBuilder.create_safe_filter("year", "2025", "STRING")
        assert filter_expr == "`year` = '2025'"


class TestFilterBuilderFlexibleTimestamps:
    """A DATETIME/TIMESTAMP value sampled with a 'T' separator, fractional seconds, or a
    UTC offset is a valid literal BigQuery casts; it must pass through rather than being
    dropped to an unformattable-date error."""

    def test_timestamp_with_t_separator_and_offset(self):
        filter_expr = FilterBuilder.create_safe_filter(
            "ts", "2025-01-15T10:30:00.123456+00:00", "TIMESTAMP"
        )
        assert filter_expr == "`ts` = TIMESTAMP('2025-01-15T10:30:00.123456+00:00')"

    def test_datetime_with_fractional_seconds(self):
        filter_expr = FilterBuilder.create_safe_filter(
            "dt", "2025-01-15 10:30:00.123456", "DATETIME"
        )
        assert filter_expr == "`dt` = '2025-01-15 10:30:00.123456'"

    def test_timestamp_with_zulu_suffix(self):
        filter_expr = FilterBuilder.create_safe_filter(
            "ts", "2025-01-15T10:30:00Z", "TIMESTAMP"
        )
        assert filter_expr == "`ts` = TIMESTAMP('2025-01-15T10:30:00Z')"
