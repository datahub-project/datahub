from datetime import datetime, timezone
from typing import List, Optional, Tuple
from unittest.mock import MagicMock, Mock

import time_machine

from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_profiling import (
    DremioProfiler,
    ProfileTarget,
    build_profile_target,
)
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport


def _target(
    name: str = "tbl",
    columns: Optional[List[Tuple[str, str]]] = None,
    urn: str = "urn:li:dataset:(urn:li:dataPlatform:dremio,x,PROD)",
) -> ProfileTarget:
    cols = columns if columns is not None else [("col1", "integer")]
    return ProfileTarget(
        dataset_urn=urn,
        full_table_name='"src"."schema"."' + name + '"',
        resource_name=name,
        columns=cols,
    )


# Anchor for time-machine so skip-threshold offsets are stable on slow CI.
_FROZEN_NOW = datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc)


def _epoch_ms_offset(seconds: float) -> int:
    return round((_FROZEN_NOW.timestamp() - seconds) * 1000)


def make_profiler(**profiling_kwargs):
    """Create a DremioProfiler with default config, optionally overriding profiling fields."""
    config = DremioSourceConfig(
        hostname="localhost",
        tls=False,
        authentication_method="PAT",
        password="token",
        profiling=profiling_kwargs or {},
    )
    report = Mock(spec=DremioSourceReport)
    api = Mock()
    return DremioProfiler(config=config, report=report, api_operations=api)


class TestBuildProfileSql:
    def test_always_includes_row_and_column_count(self):
        profiler = make_profiler(profile_table_level_only=True)
        sql = profiler._build_profile_sql('"test"."table"', [("col1", "integer")])
        assert "COUNT(*) AS row_count" in sql
        assert "column_count" in sql

    def test_table_level_only_omits_column_metrics(self):
        profiler = make_profiler(profile_table_level_only=True)
        sql = profiler._build_profile_sql('"test"."table"', [("col1", "integer")])
        assert "col1" not in sql

    def test_numeric_column_gets_mean_and_stddev(self):
        profiler = make_profiler(
            profile_table_level_only=False,
            include_field_mean_value=True,
            include_field_stddev_value=True,
        )
        sql = profiler._build_profile_sql('"test"."table"', [("amount", "double")])
        assert "AVG" in sql
        assert "STDDEV" in sql

    def test_string_column_does_not_get_mean_or_stddev(self):
        profiler = make_profiler(
            profile_table_level_only=False,
            include_field_mean_value=True,
            include_field_stddev_value=True,
        )
        sql = profiler._build_profile_sql(
            '"test"."table"', [("name", "character varying")]
        )
        assert "AVG" not in sql
        assert "STDDEV" not in sql

    def test_limit_appended_when_set(self):
        profiler = make_profiler(profile_table_level_only=True, limit=1000, offset=500)
        sql = profiler._build_profile_sql('"test"."table"', [])
        assert "LIMIT 1000" in sql
        assert "OFFSET 500" in sql

    def test_no_limit_offset_when_not_configured(self):
        profiler = make_profiler(profile_table_level_only=True, limit=None, offset=None)
        sql = profiler._build_profile_sql('"test"."table"', [])
        assert "LIMIT" not in sql
        assert "OFFSET" not in sql

    def test_column_name_with_special_chars_is_quoted(self):
        profiler = make_profiler(
            profile_table_level_only=False,
            include_field_null_count=True,
        )
        sql = profiler._build_profile_sql('"test"."table"', [("my-col", "integer")])
        assert '"my-col"' in sql


class TestChunkColumns:
    def test_empty_columns_returns_empty_list(self):
        profiler = make_profiler()
        assert profiler._chunk_columns([]) == []

    def test_fewer_than_max_stays_single_chunk(self):
        profiler = make_profiler()
        cols = [("col", "integer")] * 10
        chunks = profiler._chunk_columns(cols)
        assert len(chunks) == 1
        assert len(chunks[0]) == 10

    def test_splits_correctly_at_max_boundary(self):
        profiler = make_profiler()
        cols = [("col", "integer")] * (DremioProfiler.MAX_COLUMNS_PER_QUERY + 1)
        chunks = profiler._chunk_columns(cols)
        assert len(chunks) == 2
        assert len(chunks[0]) == DremioProfiler.MAX_COLUMNS_PER_QUERY
        assert len(chunks[1]) == 1


class TestCombineProfileResults:
    def test_single_chunk_passthrough(self):
        profiler = make_profiler()
        result = profiler._combine_profile_results(
            [
                {
                    "row_count": 100,
                    "column_count": 5,
                    "column_stats": {"col1": {"min": 0}},
                }
            ]
        )
        assert result["row_count"] == 100
        assert result["column_count"] == 5
        assert "col1" in result["column_stats"]

    def test_column_stats_merged_across_chunks(self):
        profiler = make_profiler()
        result = profiler._combine_profile_results(
            [
                {"row_count": 100, "column_count": 800, "column_stats": {"col1": {}}},
                {"row_count": 100, "column_count": 50, "column_stats": {"col801": {}}},
            ]
        )
        assert "col1" in result["column_stats"]
        assert "col801" in result["column_stats"]
        assert result["column_count"] == 850
        # row_count must not scale with chunk count — each chunk reruns COUNT(*).
        assert result["row_count"] == 100


class TestProfilingStateHandler:
    """Tests that the ProfilingHandler state integration records timestamps correctly."""

    def _make_profiler_with_state_handler(
        self,
    ) -> Tuple[DremioProfiler, Mock, Mock]:
        config = DremioSourceConfig(
            hostname="localhost",
            tls=False,
            authentication_method="PAT",
            password="token",
        )
        report = MagicMock()
        api = Mock()
        state_handler = Mock()
        profiler = DremioProfiler(
            config=config,
            report=report,
            api_operations=api,
            state_handler=state_handler,
        )
        return profiler, api, state_handler

    def test_add_to_state_called_after_successful_profile(self):
        profiler, api_mock, state_mock = self._make_profiler_with_state_handler()
        dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.src.schema.tbl,PROD)"
        )
        target = _target(urn=dataset_urn)

        api_mock.execute_query.return_value = [{"row_count": 10, "column_count": 1}]

        workunits = list(profiler.get_workunits(target))

        assert len(workunits) == 1
        state_mock.add_to_state.assert_called_once()
        urn_arg, ts_arg = state_mock.add_to_state.call_args[0]
        assert urn_arg == dataset_urn
        assert isinstance(ts_arg, int) and ts_arg > 0

    def test_add_to_state_not_called_when_no_state_handler(self):
        """Profiler without a state handler should not raise and should still yield the profile."""
        config = DremioSourceConfig(
            hostname="localhost",
            tls=False,
            authentication_method="PAT",
            password="token",
        )
        api_mock = Mock()
        profiler = DremioProfiler(
            config=config,
            report=MagicMock(),
            api_operations=api_mock,
            state_handler=None,
        )
        api_mock.execute_query.return_value = [{"row_count": 5, "column_count": 1}]

        workunits = list(profiler.get_workunits(_target()))
        assert len(workunits) == 1

    def test_add_to_state_not_called_when_no_columns(self):
        """No profile is emitted for empty-column tables, so state should not be updated."""
        profiler, _api_mock, state_mock = self._make_profiler_with_state_handler()

        workunits = list(profiler.get_workunits(_target(columns=[])))

        assert workunits == []
        state_mock.add_to_state.assert_not_called()

    # ------------------------------------------------------------------
    # Profiling-skip tests (min_time_between_profiles_hours)
    # ------------------------------------------------------------------

    def _make_profiler_with_updated_since(
        self, days: float
    ) -> Tuple[DremioProfiler, Mock, Mock]:
        """Profiler with state handler and profile_if_updated_since_days set."""
        config = DremioSourceConfig(
            hostname="localhost",
            tls=False,
            authentication_method="PAT",
            password="token",
            profiling={"profile_if_updated_since_days": days},
        )
        report = MagicMock()
        api = Mock()
        state_handler = Mock()
        profiler = DremioProfiler(
            config=config,
            report=report,
            api_operations=api,
            state_handler=state_handler,
        )
        return profiler, api, state_handler

    @time_machine.travel(_FROZEN_NOW, tick=False)
    def test_profiling_skipped_when_profiled_within_threshold(self):
        """When last_profiled is within profile_if_updated_since_days, skip it."""
        profiler, _api_mock, state_mock = self._make_profiler_with_updated_since(1.0)
        target = _target()

        # Profiled 1 hour ago → within 1-day threshold
        last_profiled_ms = _epoch_ms_offset(3600)
        state_mock.get_last_profiled.return_value = last_profiled_ms

        workunits = list(profiler.get_workunits(target))

        assert workunits == []
        # State must be carried forward so the next run retains the original timestamp
        state_mock.add_to_state.assert_called_once_with(
            target.dataset_urn, last_profiled_ms
        )

    @time_machine.travel(_FROZEN_NOW, tick=False)
    def test_profiling_proceeds_when_past_threshold(self):
        """When last_profiled is older than profile_if_updated_since_days, profile normally."""
        profiler, api_mock, state_mock = self._make_profiler_with_updated_since(1.0)

        # Profiled 30 hours ago → outside 1-day threshold
        last_profiled_ms = _epoch_ms_offset(30 * 3600)
        state_mock.get_last_profiled.return_value = last_profiled_ms
        api_mock.execute_query.return_value = [{"row_count": 10, "column_count": 1}]

        workunits = list(profiler.get_workunits(_target()))

        assert len(workunits) == 1

    def test_profiling_proceeds_when_never_profiled_before(self):
        """When no previous profile timestamp exists, always profile regardless of threshold."""
        profiler, api_mock, state_mock = self._make_profiler_with_updated_since(1.0)

        state_mock.get_last_profiled.return_value = None
        api_mock.execute_query.return_value = [{"row_count": 5, "column_count": 1}]

        workunits = list(profiler.get_workunits(_target()))

        assert len(workunits) == 1

    @time_machine.travel(_FROZEN_NOW, tick=False)
    def test_profiling_always_runs_when_threshold_not_configured(self):
        """Without profile_if_updated_since_days, profiling always runs (default behaviour)."""
        profiler, api_mock, state_mock = self._make_profiler_with_state_handler()

        # Simulate profiled very recently (5 minutes ago)
        state_mock.get_last_profiled.return_value = _epoch_ms_offset(300)
        api_mock.execute_query.return_value = [{"row_count": 5, "column_count": 1}]

        workunits = list(profiler.get_workunits(_target()))

        assert len(workunits) == 1


class TestBuildProfileTarget:
    def test_projects_path_resource_name_columns(self):
        dataset = Mock()
        dataset.resource_name = "tbl"
        dataset.path = ["src", "schema"]
        col_a = Mock()
        col_a.name = "a"
        col_a.data_type = "integer"
        col_b = Mock()
        col_b.name = "b"
        col_b.data_type = "varchar"
        dataset.columns = [col_a, col_b]

        target = build_profile_target(
            dataset, "urn:li:dataset:(urn:li:dataPlatform:dremio,x,PROD)"
        )

        assert target.dataset_urn == (
            "urn:li:dataset:(urn:li:dataPlatform:dremio,x,PROD)"
        )
        assert target.resource_name == "tbl"
        assert target.full_table_name == '"src"."schema"."tbl"'
        assert target.columns == [("a", "integer"), ("b", "varchar")]
        # No back-reference to the heavy DremioDataset.
        assert not hasattr(target, "_dataset_response")
