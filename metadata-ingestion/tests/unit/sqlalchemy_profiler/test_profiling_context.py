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
            pretty_name="public.users",
            table="users",
            schema="public",
            custom_sql=None,
        )
        assert context.schema == "public"
        assert context.table == "users"
        assert context.custom_sql is None

    def test_valid_table_without_schema(self):
        """ProfilingContext should accept table without schema (two-tier databases)."""
        context = ProfilingContext(
            pretty_name="users",
            table="users",
            schema=None,
            custom_sql=None,
        )
        assert context.schema is None
        assert context.table == "users"
        assert context.custom_sql is None

    def test_valid_with_custom_sql(self):
        """ProfilingContext should accept both table and custom_sql."""
        context = ProfilingContext(
            pretty_name="users_active",
            table="users",
            schema="public",
            custom_sql="SELECT * FROM users WHERE active = true",
        )
        assert context.table == "users"
        assert context.custom_sql == "SELECT * FROM users WHERE active = true"

    def test_valid_with_both_table_and_custom_sql(self):
        """ProfilingContext should accept both table and custom_sql (adapters handle precedence)."""
        context = ProfilingContext(
            pretty_name="public.users",
            table="users",
            schema="public",
            custom_sql="SELECT * FROM users LIMIT 100",
        )
        assert context.schema == "public"
        assert context.table == "users"
        assert context.custom_sql == "SELECT * FROM users LIMIT 100"

    def test_invalid_empty_table(self):
        """ProfilingContext should reject empty table string."""
        with pytest.raises(
            ValueError,
            match="table must be a non-empty string",
        ):
            ProfilingContext(
                pretty_name="public.unknown",
                table="",
                schema="public",
                custom_sql=None,
            )

    def test_valid_sample_percentage_zero(self):
        """ProfilingContext should accept sample_percentage=0."""
        context = ProfilingContext(
            pretty_name="public.users",
            table="users",
            schema="public",
            custom_sql=None,
            sample_percentage=0,
        )
        assert context.sample_percentage == 0

    def test_valid_sample_percentage_hundred(self):
        """ProfilingContext should accept sample_percentage=100."""
        context = ProfilingContext(
            pretty_name="public.users",
            table="users",
            schema="public",
            custom_sql=None,
            sample_percentage=100,
        )
        assert context.sample_percentage == 100

    def test_valid_sample_percentage_fifty(self):
        """ProfilingContext should accept sample_percentage=50."""
        context = ProfilingContext(
            pretty_name="public.users",
            table="users",
            schema="public",
            custom_sql=None,
            sample_percentage=50.5,
        )
        assert context.sample_percentage == 50.5

    def test_invalid_sample_percentage_negative(self):
        """ProfilingContext should reject negative sample_percentage."""
        with pytest.raises(
            ValueError, match="sample_percentage must be in \\[0, 100\\], got -1"
        ):
            ProfilingContext(
                pretty_name="public.users",
                table="users",
                schema="public",
                custom_sql=None,
                sample_percentage=-1,
            )

    def test_invalid_sample_percentage_over_hundred(self):
        """ProfilingContext should reject sample_percentage > 100."""
        with pytest.raises(
            ValueError, match="sample_percentage must be in \\[0, 100\\], got 101"
        ):
            ProfilingContext(
                pretty_name="public.users",
                table="users",
                schema="public",
                custom_sql=None,
                sample_percentage=101,
            )
