"""Unit tests for ProfilingContext validation."""

import pytest

from datahub.ingestion.source.sqlalchemy_profiler.profiling_context import (
    ProfilingContext,
)


class TestProfilingContextValidation:
    """Test ProfilingContext input validation."""

    def test_valid_with_table_and_schema(self):
        """ProfilingContext should accept table and schema parameters."""
        context = ProfilingContext(
            schema="public",
            table="users",
            custom_sql=None,
            pretty_name="public.users",
        )
        assert context.schema == "public"
        assert context.table == "users"
        assert context.custom_sql is None

    def test_valid_with_custom_sql(self):
        """ProfilingContext should accept custom_sql parameter."""
        context = ProfilingContext(
            schema=None,
            table=None,
            custom_sql="SELECT * FROM users WHERE active = true",
            pretty_name="users_active",
        )
        assert context.table is None
        assert context.custom_sql == "SELECT * FROM users WHERE active = true"

    def test_invalid_with_both_table_and_custom_sql(self):
        """ProfilingContext should reject both table and custom_sql."""
        with pytest.raises(
            ValueError,
            match="ProfilingContext cannot have both 'table' and 'custom_sql' specified",
        ):
            ProfilingContext(
                schema="public",
                table="users",
                custom_sql="SELECT * FROM users LIMIT 100",
                pretty_name="public.users",
            )

    def test_invalid_without_table_or_custom_sql(self):
        """ProfilingContext should reject missing both table and custom_sql."""
        with pytest.raises(
            ValueError,
            match="ProfilingContext requires either \\('table' with 'schema'\\) or 'custom_sql'",
        ):
            ProfilingContext(
                schema="public",
                table=None,
                custom_sql=None,
                pretty_name="public.unknown",
            )

    def test_invalid_table_without_schema(self):
        """ProfilingContext should reject table without schema."""
        with pytest.raises(
            ValueError,
            match="ProfilingContext requires either \\('table' with 'schema'\\) or 'custom_sql'",
        ):
            ProfilingContext(
                schema=None,
                table="users",
                custom_sql=None,
                pretty_name="users",
            )

    def test_invalid_schema_without_table(self):
        """ProfilingContext should reject schema without table."""
        with pytest.raises(
            ValueError,
            match="ProfilingContext requires either \\('table' with 'schema'\\) or 'custom_sql'",
        ):
            ProfilingContext(
                schema="public",
                table=None,
                custom_sql=None,
                pretty_name="public",
            )

    def test_valid_sample_percentage_zero(self):
        """ProfilingContext should accept sample_percentage=0."""
        context = ProfilingContext(
            schema="public",
            table="users",
            custom_sql=None,
            pretty_name="public.users",
            sample_percentage=0,
        )
        assert context.sample_percentage == 0

    def test_valid_sample_percentage_hundred(self):
        """ProfilingContext should accept sample_percentage=100."""
        context = ProfilingContext(
            schema="public",
            table="users",
            custom_sql=None,
            pretty_name="public.users",
            sample_percentage=100,
        )
        assert context.sample_percentage == 100

    def test_valid_sample_percentage_fifty(self):
        """ProfilingContext should accept sample_percentage=50."""
        context = ProfilingContext(
            schema="public",
            table="users",
            custom_sql=None,
            pretty_name="public.users",
            sample_percentage=50.5,
        )
        assert context.sample_percentage == 50.5

    def test_invalid_sample_percentage_negative(self):
        """ProfilingContext should reject negative sample_percentage."""
        with pytest.raises(
            ValueError, match="sample_percentage must be in \\[0, 100\\], got -1"
        ):
            ProfilingContext(
                schema="public",
                table="users",
                custom_sql=None,
                pretty_name="public.users",
                sample_percentage=-1,
            )

    def test_invalid_sample_percentage_over_hundred(self):
        """ProfilingContext should reject sample_percentage > 100."""
        with pytest.raises(
            ValueError, match="sample_percentage must be in \\[0, 100\\], got 101"
        ):
            ProfilingContext(
                schema="public",
                table="users",
                custom_sql=None,
                pretty_name="public.users",
                sample_percentage=101,
            )
