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

        # The partition is the source's choice and stays on the context;
        # AppliedProfile carries only what the adapter decided.
        assert context.partition == "20230906"
        assert context.applied == AppliedProfile()
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

    def test_rejects_a_percentage_out_of_range_on_assignment(self):
        # Adapters report by assigning, never by constructing, so the check has
        # to hold on that path -- at construction every field is still None.
        applied = AppliedProfile()
        with pytest.raises(ValueError, match=r"sample_percentage must be in"):
            applied.sample_percentage = 101

    def test_rejects_both_kinds_of_sample_at_once(self):
        # Two spellings of one reduction. `sampled` collapses them, so a
        # disagreement between the two could never surface anywhere.
        with pytest.raises(ValueError, match="either by fraction or by fixed"):
            AppliedProfile(sample_percentage=1.5, sample_rows=10_000)

    @pytest.mark.parametrize("limit", [0, -1])
    def test_rejects_a_limit_that_bounds_nothing(self, limit):
        # Mirrors ProfilingConfig.limit's PositiveInt on the applied side.
        with pytest.raises(ValueError, match="limit must be > 0"):
            AppliedProfile(limit=limit)

    def test_rejects_an_offset_reported_without_a_limit(self):
        # Alone it would render a spec claiming a bound, while the central
        # warning -- which compares per field -- would count it honoured.
        with pytest.raises(ValueError, match="bounds nothing on its own"):
            AppliedProfile(offset=25)

    def test_an_adapter_reports_a_limit_before_its_offset(self):
        # The order both adapters already use, and the only one the invariant
        # allows.
        applied = AppliedProfile()
        applied.limit = 100
        applied.offset = 25

        assert (applied.limit, applied.offset) == (100, 25)
        assert not applied.sampled
