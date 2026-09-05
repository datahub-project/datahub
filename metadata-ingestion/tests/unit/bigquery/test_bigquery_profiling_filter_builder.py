from datetime import datetime, timedelta, timezone

import pytest

from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    BIGQUERY_NUMERIC_TYPES,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.filter_builder import (
    FilterBuilder,
)
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    validate_filter_expression,
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
        """Single quotes are backslash-escaped (\\'), which is the only quote escape
        BigQuery GoogleSQL accepts — a doubled quote ('') reads as two adjacent
        literals and fails at query time."""
        filter_expr = FilterBuilder.create_safe_filter("name", "O'Brien", "STRING")
        assert filter_expr == "`name` = 'O\\'Brien'"
        assert filter_expr.count("'") == 3  # opening, backslash-escaped, closing

    def test_invalid_numeric_value_raises(self):
        # A string value on a numeric column would build an invalid INT64 = STRING filter.
        with pytest.raises(ValueError, match="Non-numeric value"):
            FilterBuilder.create_safe_filter("int_col", "not_a_number", "INT64")

    def test_string_value_with_sql_punctuation_is_escaped_not_rejected(self):
        # ; -- /* # are not injection boundaries once the value lives inside a quoted
        # literal, so a legitimate STRING partition value carrying them is escaped into
        # the literal rather than dropped.
        assert (
            FilterBuilder.create_safe_filter("col", "value; DROP TABLE", "STRING")
            == "`col` = 'value; DROP TABLE'"
        )
        assert (
            FilterBuilder.create_safe_filter("col", "value--comment", "STRING")
            == "`col` = 'value--comment'"
        )
        assert (
            FilterBuilder.create_safe_filter("col", "value/*comment*/", "STRING")
            == "`col` = 'value/*comment*/'"
        )

    def test_real_world_string_values_with_punctuation_are_kept(self):
        # Regression for the blacklist that dropped values like `C#` (# line comment)
        # and `A--B` (-- line comment); both are ordinary STRING partition values.
        assert (
            FilterBuilder.create_safe_filter("lang", "C#", "STRING") == "`lang` = 'C#'"
        )
        assert (
            FilterBuilder.create_safe_filter("range", "A--B", "STRING")
            == "`range` = 'A--B'"
        )

    def test_invalid_column_name(self):
        """Test that invalid column names are rejected."""
        with pytest.raises(ValueError, match="Invalid column name for filter"):
            FilterBuilder.create_safe_filter("col; DROP TABLE", "value", "STRING")

    def test_column_name_with_trailing_newline_is_rejected(self):
        # VALID_COLUMN_NAME_PATTERN is $-anchored, so .match() would let a trailing
        # newline through; fullmatch closes that gap.
        with pytest.raises(ValueError, match="Invalid column name for filter"):
            FilterBuilder.create_safe_filter("col\n", "value", "STRING")

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

    def test_convert_hive_style_partial_columns_returns_none(self):
        # A Hive ID that constrains only some required partition columns would select
        # more than the target partition, so it returns None like the multi-column path
        # rather than emitting a partial filter.
        filters = FilterBuilder.convert_partition_id_to_filters(
            "year=2025$month=01", ["year", "month", "day"]
        )
        assert filters is None


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

    def test_year_9999_date_column_drops_unrepresentable_upper_bound(self):
        # (YYYY+1)-01-01 for 9999 is 10000-01-01, which BigQuery DATE cannot represent,
        # so the range degrades to a lower-bound-only scan of the final year.
        filter_expr = FilterBuilder.create_safe_filter("event_date", "9999", "DATE")
        assert filter_expr == "`event_date` >= '9999-01-01'"

    def test_year_9999_timestamp_column_drops_unrepresentable_upper_bound(self):
        filter_expr = FilterBuilder.create_safe_filter("event_ts", "9999", "TIMESTAMP")
        assert filter_expr == "`event_ts` >= TIMESTAMP('9999-01-01')"


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

    def test_datetime_with_t_separator_no_timezone_passes(self):
        # A DATETIME can hold a 'T'-separated, tz-free datetime literal.
        filter_expr = FilterBuilder.create_safe_filter(
            "dt", "2025-01-15T10:30:00", "DATETIME"
        )
        assert filter_expr == "`dt` = '2025-01-15T10:30:00'"

    def test_datetime_with_timezone_is_rejected(self):
        # DATETIME has no timezone; a tz-bearing literal would build an uncastable
        # comparison, so the value is rejected for the caller to skip/report.
        with pytest.raises(ValueError, match="Could not format date value"):
            FilterBuilder.create_safe_filter(
                "dt", "2025-01-15T10:30:00+00:00", "DATETIME"
            )

    def test_date_column_rejects_datetime_literal(self):
        # A DATE column cannot represent a time component; reject rather than emit
        # an invalid DATE = '...T...' predicate.
        with pytest.raises(ValueError, match="Could not format date value"):
            FilterBuilder.create_safe_filter("d", "2025-01-15T10:30:00Z", "DATE")

    def test_date_column_rejects_space_separated_datetime(self):
        with pytest.raises(ValueError, match="Could not format date value"):
            FilterBuilder.create_safe_filter("d", "2025-01-15 10:30:00", "DATE")


class TestFilterBuilderRangeLowerBound:
    def test_numeric_range_id_scans_whole_bucket(self):
        # A RANGE partition ID is the bucket's inclusive floor, so the whole bucket
        # must be scanned with `>=`, not matched exactly with `=`.
        assert (
            FilterBuilder.create_lower_bound_filter("bucket", "100", "INT64")
            == "`bucket` >= 100"
        )

    def test_convert_partition_id_uses_lower_bound_for_range(self):
        filters = FilterBuilder.convert_partition_id_to_filters(
            "100",
            ["bucket"],
            {"bucket": "INT64"},
            is_range_partition=True,
        )
        assert filters == ["`bucket` >= 100"]

    def test_convert_partition_id_uses_equality_when_not_range(self):
        filters = FilterBuilder.convert_partition_id_to_filters(
            "100",
            ["bucket"],
            {"bucket": "INT64"},
        )
        assert filters == ["`bucket` = 100"]

    def test_non_integer_range_bound_rejected(self):
        # RANGE partitioning is integer-only; a non-integer bound is rejected so the
        # caller skips/reports rather than emitting an unenforceable predicate.
        with pytest.raises(ValueError, match="must be an integer"):
            FilterBuilder.create_lower_bound_filter("bucket", "2025-01-01", "INT64")

    def test_invalid_column_name_rejected(self):
        with pytest.raises(ValueError, match="Invalid column name"):
            FilterBuilder.create_lower_bound_filter("bucket; DROP", "100", "INT64")

    def test_range_id_that_looks_like_a_date_is_not_date_normalized(self):
        # A digit RANGE bucket id with no column type must be scanned as an integer
        # floor, not rewritten to YYYY-MM-DD — the date rewrite would then fail int()
        # parsing in create_lower_bound_filter and drop the partition entirely.
        filters = FilterBuilder.convert_partition_id_to_filters(
            "20250115", ["bucket"], is_range_partition=True
        )
        assert filters == ["`bucket` >= 20250115"]


class TestFilterBuilderPartitionDatetime:
    def test_day_granularity_builds_half_open_range(self):
        result = FilterBuilder.create_partition_datetime_filter(
            "d", datetime(2025, 1, 15, 10, 30), "DATE", "DAY"
        )
        assert result == "`d` >= '2025-01-15' AND `d` < '2025-01-16'"

    def test_hour_granularity_timestamp_column(self):
        result = FilterBuilder.create_partition_datetime_filter(
            "ts", datetime(2025, 1, 15, 10, 30), "TIMESTAMP", "HOUR"
        )
        assert result == (
            "`ts` >= TIMESTAMP('2025-01-15 10:00:00') "
            "AND `ts` < TIMESTAMP('2025-01-15 11:00:00')"
        )

    def test_month_granularity_datetime_column(self):
        result = FilterBuilder.create_partition_datetime_filter(
            "dt", datetime(2025, 12, 5), "DATETIME", "MONTH"
        )
        assert result == (
            "`dt` >= '2025-12-01 00:00:00' AND `dt` < '2026-01-01 00:00:00'"
        )

    def test_year_9999_has_no_upper_bound(self):
        result = FilterBuilder.create_partition_datetime_filter(
            "d", datetime(9999, 6, 1), "DATE", "YEAR"
        )
        assert result == "`d` >= '9999-01-01'"

    def test_invalid_column_name_raises(self):
        with pytest.raises(ValueError, match="Invalid column name"):
            FilterBuilder.create_partition_datetime_filter(
                "d; DROP", datetime(2025, 1, 15), "DATE", "DAY"
            )

    def test_day_partition_at_datetime_max_has_no_upper_bound(self):
        # The last representable day would overflow when adding a day; fall back to a
        # lower-bound-only predicate instead of raising OverflowError.
        result = FilterBuilder.create_partition_datetime_filter(
            "d", datetime(9999, 12, 31), "DATE", "DAY"
        )
        assert result == "`d` >= '9999-12-31'"

    def test_hour_partition_at_datetime_max_has_no_upper_bound(self):
        result = FilterBuilder.create_partition_datetime_filter(
            "ts", datetime(9999, 12, 31, 23), "TIMESTAMP", "HOUR"
        )
        assert result == "`ts` >= TIMESTAMP('9999-12-31 23:00:00')"

    def test_month_partition_at_datetime_max_has_no_upper_bound(self):
        result = FilterBuilder.create_partition_datetime_filter(
            "dt", datetime(9999, 12, 15), "DATETIME", "MONTH"
        )
        assert result == "`dt` >= '9999-12-01 00:00:00'"

    def test_timestamp_tzaware_normalized_to_utc_boundary(self):
        # BigQuery partitions TIMESTAMP on UTC boundaries. A tz-aware instant must be
        # converted to UTC before flooring: 10:30+05:30 is 05:00 UTC, so the HOUR
        # partition is [05:00, 06:00) UTC, not [10:00, 11:00) local.
        moment = datetime(
            2025, 1, 15, 10, 30, tzinfo=timezone(timedelta(hours=5, minutes=30))
        )
        result = FilterBuilder.create_partition_datetime_filter(
            "ts", moment, "TIMESTAMP", "HOUR"
        )
        assert result == (
            "`ts` >= TIMESTAMP('2025-01-15 05:00:00+00:00') "
            "AND `ts` < TIMESTAMP('2025-01-15 06:00:00+00:00')"
        )

    def test_low_year_is_zero_padded_to_four_digits(self):
        # Years < 1000 must render as four digits; C strftime("%Y") is not guaranteed to
        # zero-pad them, and BigQuery rejects a non-four-digit year literal.
        result = FilterBuilder.create_partition_datetime_filter(
            "d", datetime(5, 3, 7), "DATE", "DAY"
        )
        assert result == "`d` >= '0005-03-07' AND `d` < '0005-03-08'"

    def test_full_year_range_low_year_end_is_padded(self):
        # The exclusive end (year+1) must also be four digits: 0999 -> "1000-01-01".
        result = FilterBuilder.create_safe_filter("d", "0999", "DATE")
        assert result == "`d` >= '0999-01-01' AND `d` < '1000-01-01'"

    def test_datetime_column_stays_timezone_free(self):
        # DATETIME has no timezone in BigQuery; an offset on the input must not leak in.
        moment = datetime(2025, 1, 15, 10, 30, tzinfo=timezone(timedelta(hours=-8)))
        result = FilterBuilder.create_partition_datetime_filter(
            "dt", moment, "DATETIME", "HOUR"
        )
        assert result == (
            "`dt` >= '2025-01-15 10:00:00' AND `dt` < '2025-01-15 11:00:00'"
        )


class TestFilterBuilderCompactPartitionIdRanges:
    """A compact partition-id (YYYYMM / YYYYMMDDHH, or YYYYMMDD on a sub-day
    DATETIME/TIMESTAMP column) denotes a whole time-unit bucket and must become a
    granularity-aware half-open range, not an equality to the bucket start."""

    def test_month_id_on_date_column_is_month_range(self):
        result = FilterBuilder.create_safe_filter("d", "202501", "DATE")
        assert result == "`d` >= '2025-01-01' AND `d` < '2025-02-01'"

    def test_hour_id_on_timestamp_column_is_hour_range(self):
        result = FilterBuilder.create_safe_filter("ts", "2025011510", "TIMESTAMP")
        assert result == (
            "`ts` >= TIMESTAMP('2025-01-15 10:00:00') "
            "AND `ts` < TIMESTAMP('2025-01-15 11:00:00')"
        )

    def test_day_id_on_datetime_column_is_day_range(self):
        result = FilterBuilder.create_safe_filter("dt", "20250115", "DATETIME")
        assert result == (
            "`dt` >= '2025-01-15 00:00:00' AND `dt` < '2025-01-16 00:00:00'"
        )

    def test_day_id_on_date_column_stays_equality(self):
        # A DATE day-partition equals exactly that date; no range needed.
        result = FilterBuilder.create_safe_filter("d", "20250115", "DATE")
        assert result == "`d` = '2025-01-15'"

    def test_hour_id_on_date_column_falls_back_to_day_equality(self):
        # A 10-digit YYYYMMDDHH id on a DATE column must not build an hourly range: DATE
        # formatting drops the hour, so both bounds render to the same YYYY-MM-DD and
        # the predicate matches zero rows. It must fall back to a day equality instead.
        result = FilterBuilder.create_safe_filter("d", "2025011510", "DATE")
        assert result == "`d` = '2025-01-15'"


class TestFilterBuilderBooleanValues:
    def test_true_value_emits_unquoted_keyword(self):
        assert (
            FilterBuilder.create_safe_filter("flag", "true", "BOOL") == "`flag` = TRUE"
        )

    def test_false_value_emits_unquoted_keyword(self):
        assert (
            FilterBuilder.create_safe_filter("flag", "False", "BOOLEAN")
            == "`flag` = FALSE"
        )

    def test_non_boolean_value_raises(self):
        with pytest.raises(ValueError, match="Non-boolean value"):
            FilterBuilder.create_safe_filter("flag", "maybe", "BOOL")


class TestFilterBuilderStringEscaping:
    def test_backslash_is_escaped_not_rejected(self):
        # A legitimate value with a backslash must be encoded, not dropped.
        assert (
            FilterBuilder.create_safe_filter("path", "a\\b", "STRING")
            == "`path` = 'a\\\\b'"
        )

    def test_quote_with_punctuation_is_backslash_escaped_and_validates(self):
        # A value carrying both a quote and a statement/comment character must escape
        # the quote as \' (not ''), otherwise BigQuery rejects the doubled-quote literal
        # while the security validator masks it and lets it through. Verify the emitted
        # predicate uses \' and still passes validate_filter_expression.
        filter_expr = FilterBuilder.create_safe_filter("col", "a';DROP", "STRING")
        assert filter_expr == "`col` = 'a\\';DROP'"
        assert validate_filter_expression(filter_expr) is True
