from datahub.ingestion.source.ge_data_profiler import (
    _is_complex_data_type,
    _quote_column_name,
    _safe_convert_value,
)


class TestGEProfilerHelpers:
    """Test the helper functions added to fix profiling issues."""

    def test_quote_column_name(self):
        """Test column name quoting for different database dialects."""
        # Test Snowflake - should use double quotes
        assert _quote_column_name("default", "snowflake") == '"default"'
        assert _quote_column_name("when", "snowflake") == '"when"'
        assert _quote_column_name("normal_column", "snowflake") == '"normal_column"'

        # Test Databricks - should use backticks
        assert _quote_column_name("default", "databricks") == "`default`"
        assert _quote_column_name("when", "databricks") == "`when`"

        # Test BigQuery - should use backticks
        assert _quote_column_name("default", "bigquery") == "`default`"

        # Test PostgreSQL - should use double quotes
        assert _quote_column_name("default", "postgresql") == '"default"'

        # Test MySQL - should use backticks
        assert _quote_column_name("default", "mysql") == "`default`"

        # Test unknown dialect - should default to double quotes
        assert _quote_column_name("default", "unknown_db") == '"default"'

    def test_safe_convert_value(self):
        """Test safe value conversion for different data types."""
        from datetime import date, datetime, time, timedelta

        # Test None values
        assert _safe_convert_value(None) == "None"

        # Test regular values
        assert _safe_convert_value("hello") == "hello"
        assert _safe_convert_value(123) == "123"
        assert _safe_convert_value(3.14) == "3.14"

        # Test datetime objects
        dt = datetime(2023, 1, 1, 12, 30, 45)
        assert _safe_convert_value(dt) == "2023-01-01T12:30:45"

        # Test date objects
        d = date(2023, 1, 1)
        assert _safe_convert_value(d) == "2023-01-01"

        # Test time objects
        t = time(12, 30, 45)
        assert _safe_convert_value(t) == "12:30:45"

        # Test timedelta objects
        td = timedelta(hours=2, minutes=30)
        assert _safe_convert_value(td) == "9000.0"  # total_seconds()

    def test_is_complex_data_type(self):
        """Test complex data type detection."""
        # Test complex types that should be detected
        assert _is_complex_data_type("MAP<STRING, STRING>")
        assert _is_complex_data_type("STRUCT<field1: STRING, field2: INT>")
        assert _is_complex_data_type("OBJECT")
        assert _is_complex_data_type("ARRAY<STRING>")
        assert _is_complex_data_type("JSON")
        assert _is_complex_data_type("VARIANT")

        # Test case variations
        assert _is_complex_data_type("map<string, string>")
        assert _is_complex_data_type("struct<field1: string>")
        assert _is_complex_data_type("object")

        # Test simple types that should not be detected
        assert not _is_complex_data_type("STRING")
        assert not _is_complex_data_type("INTEGER")
        assert not _is_complex_data_type("FLOAT")
        assert not _is_complex_data_type("BOOLEAN")
        assert not _is_complex_data_type("TIMESTAMP")
        assert not _is_complex_data_type("VARCHAR(255)")

        # Test edge cases
        assert not _is_complex_data_type("")
        assert _is_complex_data_type("STRING_MAP")  # contains 'MAP'
        assert _is_complex_data_type("STRUCTURED_DATA")  # contains 'STRUCT'
