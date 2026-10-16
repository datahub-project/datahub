"""Unit tests for ProfilingContext validation."""

import pytest

from datahub.ingestion.source.sqlalchemy_profiler.profiling_context import (
    AppliedProfile,
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

    def test_requested_partition_is_reported_as_applied(self):
        """The source narrows to the partition before any adapter runs."""
        context = ProfilingContext(
            pretty_name="public.events",
            table="events",
            schema="public",
            partition="20230906",
        )

        assert context.applied.partition == "20230906"
        assert not context.applied.sampled


class TestAppliedProfile:
    @pytest.mark.parametrize("percentage", [0, 50.5, 100])
    def test_accepts_a_percentage_in_range(self, percentage):
        assert AppliedProfile(sample_percentage=percentage).sampled

    @pytest.mark.parametrize("percentage", [-1, 101])
    def test_rejects_a_percentage_out_of_range(self, percentage):
        with pytest.raises(
            ValueError, match=r"sample_percentage must be in \[0, 100\]"
        ):
            AppliedProfile(sample_percentage=percentage)

    def test_a_fixed_size_sample_is_a_sample(self):
        # The fraction is unknown when the row count is, so sample_rows is the
        # only thing reported -- it still has to read as sampled.
        assert AppliedProfile(sample_rows=10_000).sampled

    def test_a_bound_is_not_a_sample(self):
        assert not AppliedProfile(limit=100, offset=25).sampled
