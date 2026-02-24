"""Unit tests for type mapping."""

import sqlalchemy.types as sa_types

from datahub.ingestion.source.sqlalchemy_profiler.type_mapping import (
    ProfilerDataType,
    _get_column_types_to_ignore,
    get_column_profiler_type,
    resolve_profiler_type_with_fallback,
    should_profile_column,
)


class TestTypeMapping:
    """Test cases for type mapping functions."""

    def test_get_column_profiler_type_int(self):
        """Test integer type detection."""
        assert (
            get_column_profiler_type(sa_types.Integer(), "postgresql")
            == ProfilerDataType.INT
        )
        assert (
            get_column_profiler_type(sa_types.BigInteger(), "postgresql")
            == ProfilerDataType.INT
        )
        assert (
            get_column_profiler_type(sa_types.SmallInteger(), "postgresql")
            == ProfilerDataType.INT
        )

    def test_get_column_profiler_type_float(self):
        """Test float type detection."""
        assert (
            get_column_profiler_type(sa_types.Float(), "postgresql")
            == ProfilerDataType.FLOAT
        )
        assert (
            get_column_profiler_type(sa_types.Numeric(), "postgresql")
            == ProfilerDataType.FLOAT
        )
        assert (
            get_column_profiler_type(sa_types.DECIMAL(), "postgresql")
            == ProfilerDataType.FLOAT
        )

    def test_get_column_profiler_type_string(self):
        """Test string type detection."""
        assert (
            get_column_profiler_type(sa_types.String(), "postgresql")
            == ProfilerDataType.STRING
        )
        assert (
            get_column_profiler_type(sa_types.Text(), "postgresql")
            == ProfilerDataType.STRING
        )
        assert (
            get_column_profiler_type(sa_types.VARCHAR(50), "postgresql")
            == ProfilerDataType.STRING
        )

    def test_get_column_profiler_type_datetime(self):
        """Test datetime type detection."""
        assert (
            get_column_profiler_type(sa_types.DateTime(), "postgresql")
            == ProfilerDataType.DATETIME
        )
        assert (
            get_column_profiler_type(sa_types.Date(), "postgresql")
            == ProfilerDataType.DATETIME
        )
        assert (
            get_column_profiler_type(sa_types.Time(), "postgresql")
            == ProfilerDataType.DATETIME
        )

    def test_get_column_profiler_type_boolean(self):
        """Test boolean type detection."""
        assert (
            get_column_profiler_type(sa_types.Boolean(), "postgresql")
            == ProfilerDataType.BOOLEAN
        )

    def test_get_column_profiler_type_unknown(self):
        """Test unknown type detection."""
        # ARRAY and JSON should return UNKNOWN
        assert (
            get_column_profiler_type(sa_types.ARRAY(sa_types.Integer()), "postgresql")
            == ProfilerDataType.UNKNOWN
        )
        assert (
            get_column_profiler_type(sa_types.JSON(), "postgresql")
            == ProfilerDataType.UNKNOWN
        )

    def test_should_profile_column(self):
        """Test column profiling decision."""
        # Should profile regular columns
        assert (
            should_profile_column(sa_types.Integer(), "postgresql", "id", True) is True
        )
        assert (
            should_profile_column(sa_types.String(), "postgresql", "name", True) is True
        )

        # Should not profile nested fields if disabled
        assert (
            should_profile_column(
                sa_types.Integer(), "postgresql", "nested.field", False
            )
            is False
        )
        assert (
            should_profile_column(
                sa_types.Integer(), "postgresql", "nested.field", True
            )
            is True
        )

        # Should not profile complex types
        assert (
            should_profile_column(
                sa_types.ARRAY(sa_types.Integer()), "postgresql", "arr", True
            )
            is False
        )
        assert (
            should_profile_column(sa_types.JSON(), "postgresql", "json_col", True)
            is False
        )

    def test_get_column_types_to_ignore(self):
        """Test database-specific type filtering."""
        # PostgreSQL
        ignored = _get_column_types_to_ignore("postgresql")
        assert "JSON" in ignored or "JSONB" in ignored

        # BigQuery
        ignored = _get_column_types_to_ignore("bigquery")
        assert "ARRAY" in ignored
        assert "STRUCT" in ignored
        assert "GEOGRAPHY" in ignored
        assert "JSON" in ignored

        # Snowflake
        ignored = _get_column_types_to_ignore("snowflake")
        assert "GEOGRAPHY" in ignored
        assert "GEOMETRY" in ignored
        assert "OBJECT" in ignored
        assert "ARRAY" in ignored

        # Other databases
        ignored = _get_column_types_to_ignore("mysql")
        # May be empty or contain some types
        assert isinstance(ignored, list)

    def test_resolve_profiler_type_with_fallback(self):
        """Test type resolution with fallback."""
        # For UNKNOWN types, should attempt fallback
        col_type = sa_types.ARRAY(sa_types.Integer())
        result = resolve_profiler_type_with_fallback(
            col_type, "postgresql", "ARRAY(INTEGER)"
        )
        # Should still return UNKNOWN if fallback doesn't help
        assert result == ProfilerDataType.UNKNOWN

    def test_should_profile_column_nested_disabled(self):
        """Test column profiling decision with nested fields disabled."""
        # Should not profile nested fields when disabled
        assert (
            should_profile_column(
                sa_types.Integer(),
                "postgresql",
                "nested.field",
                profile_nested_fields=False,
            )
            is False
        )

        # Should profile regular fields
        assert (
            should_profile_column(
                sa_types.Integer(), "postgresql", "id", profile_nested_fields=False
            )
            is True
        )

    def test_should_profile_column_nested_enabled(self):
        """Test column profiling decision with nested fields enabled."""
        # Should profile nested fields when enabled
        assert (
            should_profile_column(
                sa_types.Integer(),
                "postgresql",
                "nested.field",
                profile_nested_fields=True,
            )
            is True
        )

    def test_should_profile_column_complex_types(self):
        """Test that complex types are excluded."""
        # ARRAY types should be excluded
        assert (
            should_profile_column(
                sa_types.ARRAY(sa_types.Integer()),
                "postgresql",
                "arr",
                profile_nested_fields=True,
            )
            is False
        )

        # JSON types should be excluded
        assert (
            should_profile_column(
                sa_types.JSON(), "postgresql", "json_col", profile_nested_fields=True
            )
            is False
        )

    def test_get_column_profiler_type_all_types(self):
        """Test type detection for all supported types."""
        # Test all integer variants
        assert (
            get_column_profiler_type(sa_types.Integer(), "postgresql")
            == ProfilerDataType.INT
        )
        assert (
            get_column_profiler_type(sa_types.BigInteger(), "postgresql")
            == ProfilerDataType.INT
        )
        assert (
            get_column_profiler_type(sa_types.SmallInteger(), "postgresql")
            == ProfilerDataType.INT
        )

        # Test all float variants
        assert (
            get_column_profiler_type(sa_types.Float(), "postgresql")
            == ProfilerDataType.FLOAT
        )
        assert (
            get_column_profiler_type(sa_types.Numeric(), "postgresql")
            == ProfilerDataType.FLOAT
        )
        assert (
            get_column_profiler_type(sa_types.DECIMAL(), "postgresql")
            == ProfilerDataType.FLOAT
        )

        # Test all string variants
        assert (
            get_column_profiler_type(sa_types.String(), "postgresql")
            == ProfilerDataType.STRING
        )
        assert (
            get_column_profiler_type(sa_types.Text(), "postgresql")
            == ProfilerDataType.STRING
        )
        assert (
            get_column_profiler_type(sa_types.VARCHAR(50), "postgresql")
            == ProfilerDataType.STRING
        )
        assert (
            get_column_profiler_type(sa_types.CHAR(10), "postgresql")
            == ProfilerDataType.STRING
        )

        # Test all datetime variants
        assert (
            get_column_profiler_type(sa_types.DateTime(), "postgresql")
            == ProfilerDataType.DATETIME
        )
        assert (
            get_column_profiler_type(sa_types.Date(), "postgresql")
            == ProfilerDataType.DATETIME
        )
        assert (
            get_column_profiler_type(sa_types.Time(), "postgresql")
            == ProfilerDataType.DATETIME
        )
        assert (
            get_column_profiler_type(sa_types.TIMESTAMP(), "postgresql")
            == ProfilerDataType.DATETIME
        )

        # Test boolean
        assert (
            get_column_profiler_type(sa_types.Boolean(), "postgresql")
            == ProfilerDataType.BOOLEAN
        )
