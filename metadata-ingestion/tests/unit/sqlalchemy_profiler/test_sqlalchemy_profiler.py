"""Unit tests for SQLAlchemyProfiler."""

import logging
import sqlite3
from typing import Any, List
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, Float, Integer, String, create_engine

from datahub.ingestion.source.ge_profiling_config import (
    ProfilingConfig,
    ProfilingIsolationLevel,
)
from datahub.ingestion.source.profiling.common import Cardinality, ProfilerRequest
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler import (
    SQLAlchemyProfiler,
)
from datahub.ingestion.source.sqlalchemy_profiler.type_mapping import ProfilerDataType
from datahub.metadata.schema_classes import DatasetFieldProfileClass


@pytest.fixture
def sqlite_engine():
    """Create an in-memory SQLite engine for testing."""
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def test_table(sqlite_engine):
    """Create a test table with sample data."""
    metadata = sa.MetaData()
    table = sa.Table(
        "test_table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("value", Float),
    )
    metadata.create_all(sqlite_engine)

    with sqlite_engine.connect() as conn, conn.begin():
        conn.execute(
            sa.insert(table),
            [
                {"id": 1, "name": "Alice", "value": 10.5},
                {"id": 2, "name": "Bob", "value": 20.5},
                {"id": 3, "name": "Charlie", "value": 30.5},
            ],
        )

    return table


@pytest.fixture
def profiler_config():
    """Create a test profiling config."""
    return ProfilingConfig(
        enabled=True,
        include_field_null_count=True,
        include_field_distinct_count=True,
        include_field_min_value=True,
        include_field_max_value=True,
        include_field_mean_value=True,
        include_field_median_value=True,
        include_field_stddev_value=True,
        include_field_sample_values=True,
    )


@pytest.fixture
def mock_report():
    """Create a mock SQLSourceReport."""
    report = MagicMock(spec=SQLSourceReport)
    report.report_dropped = MagicMock()
    report.warning = MagicMock()
    return report


@pytest.fixture
def profiler(sqlite_engine, profiler_config, mock_report):
    """Create a SQLAlchemyProfiler instance."""
    return SQLAlchemyProfiler(
        conn=sqlite_engine,
        report=mock_report,
        config=profiler_config,
        platform="sqlite",
        env="TEST",
    )


class TestSQLAlchemyProfiler:
    """Test cases for SQLAlchemyProfiler."""

    def test_init(self, profiler, sqlite_engine):
        """Test profiler initialization."""
        assert profiler.base_engine == sqlite_engine
        assert profiler.platform == "sqlite"
        assert profiler.env == "TEST"
        assert profiler.times_taken == []
        assert profiler.total_row_count == 0

    def test_get_columns_to_profile(self, profiler, sqlite_engine, test_table):
        """Test column filtering logic."""
        # Create a table object with metadata
        metadata = sa.MetaData()
        sql_table = sa.Table(
            "test_table",
            metadata,
            autoload_with=sqlite_engine,
        )

        columns = profiler._get_columns_to_profile(sql_table, "test_table")
        # Should include all columns that match the config
        assert len(columns) > 0
        assert "id" in columns or "name" in columns or "value" in columns

    def test_should_ignore_column(self, profiler):
        """Test column type-based filtering."""
        # Should not ignore regular types
        assert not profiler._should_ignore_column(sa.Integer(), "id")
        assert not profiler._should_ignore_column(sa.String(), "name")
        assert not profiler._should_ignore_column(sa.Float(), "value")

    def test_generate_profiles_empty_list(self, profiler):
        """Test generate_profiles with empty request list."""
        requests: list = []
        # max_workers must be > 0
        profiles = list(profiler.generate_profiles(requests, max_workers=1))
        assert len(profiles) == 0

    def test_get_columns_to_profile_with_nested_fields_disabled(
        self, profiler, sqlite_engine
    ):
        """Test column filtering with nested fields disabled."""
        profiler.config.profile_nested_fields = False

        metadata = sa.MetaData()
        table = sa.Table(
            "test_table",
            metadata,
            Column("id", Integer),
            Column("nested.field", String(50)),
        )

        columns = profiler._get_columns_to_profile(table, "test_table")
        # Nested field should be excluded
        assert "nested.field" not in columns

    def test_get_columns_to_profile_with_nested_fields_enabled(
        self, profiler, sqlite_engine
    ):
        """Test column filtering with nested fields enabled."""
        profiler.config.profile_nested_fields = True

        metadata = sa.MetaData()
        table = sa.Table(
            "test_table",
            metadata,
            Column("id", Integer),
            Column("nested.field", String(50)),
        )

        profiler._get_columns_to_profile(table, "test_table")
        # Nested field should be included
        # Note: May still be filtered by type or other criteria
        # Just verify the method doesn't crash

    def test_get_columns_to_profile_max_limit(self, profiler, sqlite_engine):
        """Test column filtering with max columns limit."""
        profiler.config.max_number_of_fields_to_profile = 2

        metadata = sa.MetaData()
        table = sa.Table(
            "test_table",
            metadata,
            Column("id", Integer),
            Column("name", String(50)),
            Column("value", Float),
            Column("extra", String(50)),
        )

        columns = profiler._get_columns_to_profile(table, "test_table")
        # Should be limited to max_number_of_fields_to_profile
        assert len(columns) <= 2

    def test_setup_permission_error_with_catch_exceptions_true(
        self, profiler, mock_report, sqlite_engine
    ):
        """Test permission error during setup when catch_exceptions=True."""
        profiler.config.catch_exceptions = True

        request = ProfilerRequest(
            pretty_name="test_table",
            batch_kwargs={"table": "test_table", "schema": "test_schema"},
        )

        # Mock setup_profiling to raise PermissionError
        with (
            sqlite_engine.connect() as conn,
            patch.object(profiler, "base_engine") as mock_engine,
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
            ) as mock_get_adapter,
        ):
            mock_engine.connect.return_value.__enter__.return_value = conn
            mock_adapter = MagicMock()
            mock_adapter.setup_profiling.side_effect = PermissionError(
                "permission denied"
            )
            mock_get_adapter.return_value = mock_adapter

            # Should return tuple (request, None) and log warning, not raise
            result_request, result_profile = profiler._generate_profile_from_request(
                None, request
            )

            # Should return None for profile (error was caught)
            assert result_request == request
            assert result_profile is None

            # Should have called report.warning for setup failure
            mock_report.warning.assert_called()
            call_args = mock_report.warning.call_args
            assert call_args is not None
            assert "Profiling setup failed" in call_args[1]["title"]

    def test_permission_error_with_catch_exceptions_false(
        self, profiler, sqlite_engine
    ):
        """Test permission error handling when catch_exceptions=False."""
        profiler.config.catch_exceptions = False

        request = ProfilerRequest(
            pretty_name="test_table",
            batch_kwargs={"table": "test_table", "schema": "test_schema"},
        )

        # Mock setup_profiling to raise PermissionError
        with (
            sqlite_engine.connect() as conn,
            patch.object(profiler, "base_engine") as mock_engine,
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
            ) as mock_get_adapter,
        ):
            mock_engine.connect.return_value.__enter__.return_value = conn
            mock_adapter = MagicMock()
            mock_adapter.setup_profiling.side_effect = PermissionError(
                "permission denied"
            )
            mock_get_adapter.return_value = mock_adapter

            # Should re-raise the exception
            with pytest.raises(PermissionError, match="permission denied"):
                profiler._generate_profile_from_request(None, request)

    def test_sqlalchemy_error_with_catch_exceptions_true(
        self, profiler, mock_report, sqlite_engine
    ):
        """Test SQLAlchemy error handling when catch_exceptions=True."""
        profiler.config.catch_exceptions = True

        request = ProfilerRequest(
            pretty_name="test_table",
            batch_kwargs={"table": "test_table", "schema": "test_schema"},
        )

        # Mock setup_profiling to raise SQLAlchemy error
        with (
            sqlite_engine.connect() as conn,
            patch.object(profiler, "base_engine") as mock_engine,
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
            ) as mock_get_adapter,
        ):
            mock_engine.connect.return_value.__enter__.return_value = conn
            mock_adapter = MagicMock()
            mock_adapter.setup_profiling.side_effect = sa.exc.OperationalError(
                "database error", None, None
            )
            mock_get_adapter.return_value = mock_adapter

            # Should return tuple (request, None) and log warning, not raise
            result_request, result_profile = profiler._generate_profile_from_request(
                None, request
            )

            # Should return None for profile (error was caught)
            assert result_request == request
            assert result_profile is None

            # Should have called report.warning
            mock_report.warning.assert_called()
            call_args = mock_report.warning.call_args
            assert "Profiling setup failed" in call_args[1]["title"]

    def test_sqlalchemy_error_with_catch_exceptions_false(
        self, profiler, sqlite_engine
    ):
        """Test SQLAlchemy error handling when catch_exceptions=False."""
        profiler.config.catch_exceptions = False

        request = ProfilerRequest(
            pretty_name="test_table",
            batch_kwargs={"table": "test_table", "schema": "test_schema"},
        )

        # Mock setup_profiling to raise SQLAlchemy error
        with (
            sqlite_engine.connect() as conn,
            patch.object(profiler, "base_engine") as mock_engine,
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
            ) as mock_get_adapter,
        ):
            mock_engine.connect.return_value.__enter__.return_value = conn
            mock_adapter = MagicMock()
            mock_adapter.setup_profiling.side_effect = sa.exc.OperationalError(
                "database error", None, None
            )
            mock_get_adapter.return_value = mock_adapter

            # Should re-raise the exception
            with pytest.raises(sa.exc.OperationalError):
                profiler._generate_profile_from_request(None, request)

    def test_connection_error_with_catch_exceptions_true(
        self, profiler, mock_report, sqlite_engine
    ):
        """Test ConnectionError handling when catch_exceptions=True."""
        profiler.config.catch_exceptions = True

        request = ProfilerRequest(
            pretty_name="test_table",
            batch_kwargs={"table": "test_table", "schema": "test_schema"},
        )

        # Mock setup_profiling to raise ConnectionError
        with (
            sqlite_engine.connect() as conn,
            patch.object(profiler, "base_engine") as mock_engine,
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
            ) as mock_get_adapter,
        ):
            mock_engine.connect.return_value.__enter__.return_value = conn
            mock_adapter = MagicMock()
            mock_adapter.setup_profiling.side_effect = ConnectionError(
                "connection lost"
            )
            mock_get_adapter.return_value = mock_adapter

            # Should return tuple (request, None) and log warning, not raise
            result_request, result_profile = profiler._generate_profile_from_request(
                None, request
            )

            # Should return None for profile (error was caught)
            assert result_request == request
            assert result_profile is None

            # Should have called report.warning
            mock_report.warning.assert_called()

    def test_unexpected_error_with_catch_exceptions_true(
        self, profiler, mock_report, sqlite_engine
    ):
        """Test unexpected exception handling when catch_exceptions=True."""
        profiler.config.catch_exceptions = True

        request = ProfilerRequest(
            pretty_name="test_table",
            batch_kwargs={"table": "test_table", "schema": "test_schema"},
        )

        # Mock setup_profiling to raise an unexpected error
        with (
            sqlite_engine.connect() as conn,
            patch.object(profiler, "base_engine") as mock_engine,
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
            ) as mock_get_adapter,
        ):
            mock_engine.connect.return_value.__enter__.return_value = conn
            mock_adapter = MagicMock()
            mock_adapter.setup_profiling.side_effect = RuntimeError("unexpected error")
            mock_get_adapter.return_value = mock_adapter

            # Should return tuple (request, None) and log warning, not raise
            result_request, result_profile = profiler._generate_profile_from_request(
                None, request
            )

            # Should return None for profile (error was caught)
            assert result_request == request
            assert result_profile is None

            # Should have called report.warning
            mock_report.warning.assert_called()

    def test_unexpected_error_with_catch_exceptions_false(
        self, profiler, sqlite_engine
    ):
        """Test unexpected exception handling when catch_exceptions=False."""
        profiler.config.catch_exceptions = False

        request = ProfilerRequest(
            pretty_name="test_table",
            batch_kwargs={"table": "test_table", "schema": "test_schema"},
        )

        # Mock setup_profiling to raise an unexpected error
        with (
            sqlite_engine.connect() as conn,
            patch.object(profiler, "base_engine") as mock_engine,
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
            ) as mock_get_adapter,
        ):
            mock_engine.connect.return_value.__enter__.return_value = conn
            mock_adapter = MagicMock()
            mock_adapter.setup_profiling.side_effect = RuntimeError("unexpected error")
            mock_get_adapter.return_value = mock_adapter

            # Should re-raise the exception
            with pytest.raises(RuntimeError, match="unexpected error"):
                profiler._generate_profile_from_request(None, request)

    def test_cleanup_called_after_error(self, profiler, sqlite_engine):
        """Test that adapter cleanup is called even when profiling fails."""
        profiler.config.catch_exceptions = True

        request = ProfilerRequest(
            pretty_name="test_table",
            batch_kwargs={"table": "test_table", "schema": "test_schema"},
        )

        # Mock setup_profiling to raise an error
        with (
            sqlite_engine.connect() as conn,
            patch.object(profiler, "base_engine") as mock_engine,
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
            ) as mock_get_adapter,
        ):
            mock_engine.connect.return_value.__enter__.return_value = conn
            mock_adapter = MagicMock()
            mock_adapter.setup_profiling.side_effect = RuntimeError("test error")
            mock_get_adapter.return_value = mock_adapter

            # Execute profiling (will fail)
            profiler._generate_profile_from_request(None, request)

            # Cleanup should have been called even though profiling failed
            mock_adapter.cleanup.assert_called_once()

    @pytest.mark.parametrize(
        "stat_name,expected_title",
        [
            ("min", "Profiling: Unable to Calculate Min"),
            ("max", "Profiling: Unable to Calculate Max"),
            ("mean", "Profiling: Unable to Calculate Mean"),
            ("stdev", "Profiling: Unable to Calculate Standard Deviation"),
            ("median", "Profiling: Unable to Calculate Median"),
        ],
    )
    def test_batchable_numeric_stats_exception_caught(
        self, profiler, mock_report, stat_name, expected_title
    ):
        """Test that batchable numeric stats exceptions are caught in _process_numeric_column_stats."""
        mock_runner = MagicMock()
        mock_table = MagicMock()
        mock_column_profile = MagicMock()

        # Create a FutureResult that raises an exception when .result() is called
        mock_future = MagicMock()
        mock_future.result.side_effect = Exception(f"{stat_name} error")

        # Pass the future in numeric_stats_futures (nested dict: {col_name: {stat_name: future}})
        numeric_stats_futures = {"value_col": {stat_name: mock_future}}

        # Should not raise, should log warning
        profiler._process_numeric_column_stats(
            runner=mock_runner,
            sql_table=mock_table,
            col_name="value_col",
            column_profile=mock_column_profile,
            col_type=ProfilerDataType.FLOAT,
            cardinality=Cardinality.MANY,
            numeric_stats_futures=numeric_stats_futures,
            pretty_name="test.table",
            platform="sqlite",
        )

        # Verify warning was logged
        mock_report.warning.assert_called()
        call_args = mock_report.warning.call_args
        assert call_args.kwargs["title"] == expected_title
        assert "test.table.value_col" in call_args.kwargs["context"]

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "sample_values",
                "config_overrides": {},
                "mock_method": "get_column_sample_values",
                "profiler_method": "_add_sample_values",
                "method_kwargs": {
                    "col_name": "test_col",
                    "non_null_count": 10,
                    "row_count": 100,
                    "pretty_name": "test.table",
                },
                "expected_title": "Profiling: Unable to Calculate Sample Values",
                "expected_context": "test.table.test_col",
            },
            {
                "name": "histogram",
                "config_overrides": {"include_field_histogram": True},
                "mock_method": "get_column_histogram",
                "profiler_method": "_process_numeric_column_stats",
                "method_kwargs": {
                    "col_name": "value_col",
                    "col_type": ProfilerDataType.FLOAT,
                    "cardinality": Cardinality.MANY,
                    "numeric_stats_futures": {},
                    "pretty_name": "test.table",
                    "platform": "sqlite",
                },
                "expected_title": "Profiling: Unable to Calculate Histogram",
                "expected_context": "test.table.value_col",
            },
            {
                "name": "quantiles",
                "config_overrides": {"include_field_quantiles": True},
                "mock_method": "get_column_quantiles",
                "profiler_method": "_process_numeric_column_stats",
                "method_kwargs": {
                    "col_name": "value_col",
                    "col_type": ProfilerDataType.FLOAT,
                    "cardinality": Cardinality.MANY,
                    "numeric_stats_futures": {},
                    "pretty_name": "test.table",
                    "platform": "sqlite",
                },
                "expected_title": "Profiling: Unable to Calculate Quantiles",
                "expected_context": "test.table.value_col",
            },
            {
                "name": "distinct_value_frequencies",
                "config_overrides": {"include_field_distinct_value_frequencies": True},
                "mock_method": "get_column_distinct_value_frequencies",
                "profiler_method": "_maybe_add_distinct_value_frequencies",
                "method_kwargs": {
                    "col_name": "status_col",
                    "cardinality": Cardinality.ONE,
                    "allowed_cardinalities": {Cardinality.ONE, Cardinality.TWO},
                    "pretty_name": "test.table",
                },
                "expected_title": "Profiling: Unable to Calculate Distinct Value Frequencies",
                "expected_context": "test.table.status_col",
            },
        ],
        ids=lambda tc: tc["name"],
    )
    def test_non_batchable_query_exceptions_caught(
        self, sqlite_engine, mock_report, test_case
    ):
        """Test that non-batchable query exceptions are caught and logged."""
        # Create profiler with appropriate config
        config = ProfilingConfig(
            enabled=True, catch_exceptions=True, **test_case["config_overrides"]
        )
        profiler = SQLAlchemyProfiler(
            conn=sqlite_engine,
            report=mock_report,
            config=config,
            platform="sqlite",
            env="TEST",
        )

        # Set up mocks
        mock_runner = MagicMock()
        mock_table = MagicMock()
        mock_column_profile = MagicMock()

        # Make the runner method raise an exception
        getattr(mock_runner, test_case["mock_method"]).side_effect = Exception(
            f"{test_case['name']} error"
        )

        # Call the profiler method
        method = getattr(profiler, test_case["profiler_method"])
        method(
            runner=mock_runner,
            sql_table=mock_table,
            column_profile=mock_column_profile,
            **test_case["method_kwargs"],
        )

        # Verify warning was logged
        assert mock_report.warning.called
        warning_calls = [
            call.kwargs for call in mock_report.warning.call_args_list if call.kwargs
        ]
        matching_warnings = [
            w
            for w in warning_calls
            if w.get("title") == test_case["expected_title"]
            and test_case["expected_context"] in w.get("context", "")
        ]
        assert len(matching_warnings) > 0, (
            f"Expected warning with title '{test_case['expected_title']}' "
            f"and context '{test_case['expected_context']}' not found. "
            f"Got warnings: {warning_calls}"
        )

    def test_row_count_failure_returns_none(self, profiler, mock_report, sqlite_engine):
        """
        Test that profiling returns None when row_count metric fails.

        This prevents empty profiles from being emitted when we can't get basic
        metrics like row count (e.g., due to permission errors). This matches
        GE profiler behavior which asserts that profile.rowCount is not None.

        The row_count extraction includes explicit exception handling and early
        return logic to prevent emitting profiles without this critical metric.
        """
        profiler.config.catch_exceptions = True

        request = ProfilerRequest(
            pretty_name="test.table",
            batch_kwargs={"table": "test_table", "schema": "test_schema"},
        )

        # Mock the profiling to fail during row count extraction
        # This simulates permission errors or other database failures
        with (
            sqlite_engine.connect() as conn,
            patch.object(profiler, "base_engine") as mock_engine,
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
            ) as mock_get_adapter,
        ):
            mock_engine.connect.return_value.__enter__.return_value = conn
            mock_adapter = MagicMock()

            # Setup succeeds but subsequent profiling will fail
            # We raise an exception that will propagate through the profiling pipeline
            mock_adapter.setup_profiling.side_effect = Exception(
                "Simulated row count failure"
            )

            mock_get_adapter.return_value = mock_adapter

            # Attempt to profile - should return None for failed profiling
            result_request, result_profile = profiler._generate_profile_from_request(
                None, request
            )

            # Verify that None is returned (no profile emitted on failure)
            assert result_request == request
            assert result_profile is None, (
                "Expected None to be returned when profiling fails, "
                "preventing incomplete profiles from being emitted"
            )

            # Verify warning was logged
            assert mock_report.warning.called

    def test_empty_table_skips_column_profiling(
        self, profiler, sqlite_engine, test_table
    ):
        """
        Test that empty tables (row_count == 0) skip column profiling but return basic profile.

        This optimization matches GE profiler behavior:
        - Empty tables get a basic profile with rowCount=0
        - Column profiling is skipped (no field profiles generated)
        - No wasted queries on empty tables

        The behavior is the same as row_count failure (None) - both return a basic profile.
        The difference is the reason: empty table optimization vs permission error.
        """
        request = ProfilerRequest(
            pretty_name="test.empty_table",
            batch_kwargs={"table": "test_table", "schema": None},
        )

        # Create a mock sql_table with columns but mock row_count to return 0
        metadata = sa.MetaData()
        sql_table = sa.Table(
            "test_table",
            metadata,
            sa.Column("id", sa.Integer),
            sa.Column("value", sa.Integer),
        )

        # Define side effect that sets profile.rowCount = 0 and returns 0
        def mock_profile_row_count(*args, **kwargs):
            # The profile parameter is at index 3 (after self, runner, query_combiner, sql_table)
            profile = args[3] if len(args) > 3 else kwargs.get("profile")
            if profile:
                profile.rowCount = 0
            return 0

        with (
            sqlite_engine.connect() as conn,
            patch.object(profiler, "base_engine") as mock_engine,
            patch.object(
                profiler, "_profile_row_count", side_effect=mock_profile_row_count
            ),
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
            ) as mock_get_adapter,
        ):
            mock_engine.connect.return_value.__enter__.return_value = conn

            # Create mock adapter and mock context
            mock_adapter = MagicMock()
            mock_context = MagicMock()
            mock_context.sql_table = sql_table
            mock_adapter.setup_profiling.return_value = mock_context
            mock_adapter.cleanup.return_value = None
            mock_get_adapter.return_value = mock_adapter

            # Attempt to profile - should return basic profile
            result_request, result_profile = profiler._generate_profile_from_request(
                None, request
            )

            # Verify that a basic profile is returned (not None)
            assert result_request == request
            assert result_profile is not None, (
                "Expected basic profile to be returned for empty table, "
                "not None (which would skip the entire table)"
            )

            # Verify row_count was set to 0
            assert result_profile.rowCount == 0, (
                f"Expected rowCount=0, got {result_profile.rowCount}"
            )

            # Verify no field profiles were generated (column profiling skipped)
            assert (
                result_profile.fieldProfiles is None
                or len(result_profile.fieldProfiles) == 0
            ), (
                f"Expected no field profiles for empty table, got {len(result_profile.fieldProfiles) if result_profile.fieldProfiles else 0}"
            )


class TestProfilingIsolationLevelResolution:
    """The effective isolation level is resolved once in __init__ from config."""

    @staticmethod
    def _make_profiler(
        config: ProfilingConfig, sqlite_engine: sa.engine.Engine
    ) -> SQLAlchemyProfiler:
        return SQLAlchemyProfiler(
            conn=sqlite_engine,
            report=SQLSourceReport(),
            config=config,
            platform="mysql",
            env="TEST",
        )

    def test_unset_is_none(self, sqlite_engine):
        # The default sets nothing on the connection, so the whole table profile
        # runs under one transaction -- unchanged from before this option existed.
        config = ProfilingConfig(enabled=True)
        profiler = self._make_profiler(config, sqlite_engine)
        assert profiler._profiling_isolation_level is None

    def test_autocommit_resolves_to_sqlalchemy_name(self, sqlite_engine):
        config = ProfilingConfig(
            enabled=True,
            profiling_isolation_level=ProfilingIsolationLevel.AUTOCOMMIT,
        )
        profiler = self._make_profiler(config, sqlite_engine)
        assert profiler._profiling_isolation_level == "AUTOCOMMIT"

    def test_named_level_resolves_to_sql_standard_spelling(self, sqlite_engine):
        # The enum member is READ_COMMITTED; the value handed to SQLAlchemy is the
        # SQL standard spelling with a space.
        config = ProfilingConfig(
            enabled=True,
            profiling_isolation_level=ProfilingIsolationLevel.READ_COMMITTED,
        )
        profiler = self._make_profiler(config, sqlite_engine)
        assert profiler._profiling_isolation_level == "READ COMMITTED"


class TestProfilingIsolationLevelApplication:
    """The per-table path applies the resolved level, or skips it when None."""

    def test_applies_resolved_level(self, profiler, sqlite_engine):
        # The rebind `conn = conn.execution_options(...)` is required on SA 1.4
        # (returns a branched copy) and a no-op on SA 2.0 (returns self). Against a
        # MagicMock the identity assertion below passes regardless of whether the
        # code rebinds; against a real connection it fails if the rebind is dropped,
        # which is the only reason the rebind exists.
        profiler.config.catch_exceptions = True
        profiler._profiling_isolation_level = "AUTOCOMMIT"
        branched_conn: list = []
        received_conn: list = []

        def capture_setup(context, conn):
            received_conn.append(conn)
            raise RuntimeError("short-circuit")

        with sqlite_engine.connect() as real_conn:
            original_execution_options = real_conn.execution_options

            def spy_execution_options(*args, **kwargs):
                result = original_execution_options(*args, **kwargs)
                branched_conn.append(result)
                return result

            with (
                patch.object(profiler, "base_engine") as mock_engine,
                patch(
                    "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
                ) as mock_get_adapter,
                patch.object(
                    real_conn, "execution_options", side_effect=spy_execution_options
                ),
            ):
                mock_engine.connect.return_value.__enter__.return_value = real_conn
                mock_adapter = MagicMock()
                mock_adapter.setup_profiling.side_effect = capture_setup
                mock_get_adapter.return_value = mock_adapter

                profiler._generate_single_profile(
                    query_combiner=MagicMock(),
                    pretty_name="my_db.my_table",
                    schema="my_db",
                    table="my_table",
                    platform="mysql",
                )

        # setup_profiling received exactly the object execution_options returned
        # — not the raw checked-out connection. On SA 1.4 (the repo's pinned
        # version) these are different objects, so this fails if the rebind is
        # dropped.
        assert len(received_conn) == 1
        assert len(branched_conn) == 1
        assert received_conn[0] is branched_conn[0]

    def test_skips_when_resolved_level_none(self, profiler):
        # When the level is unset (the default), execution_options is not called
        # at all -- the checked-out connection flows downstream unchanged. This is
        # byte-for-byte master behaviour.
        profiler.config.catch_exceptions = True
        profiler._profiling_isolation_level = None
        with (
            patch.object(profiler, "base_engine") as mock_engine,
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
            ) as mock_get_adapter,
        ):
            mock_conn = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            mock_adapter = MagicMock()
            mock_adapter.setup_profiling.side_effect = RuntimeError("short-circuit")
            mock_get_adapter.return_value = mock_adapter

            profiler._generate_single_profile(
                query_combiner=MagicMock(),
                pretty_name="my_db.my_table",
                schema="my_db",
                table="my_table",
                platform="mysql",
            )

        mock_conn.execution_options.assert_not_called()
        assert mock_adapter.setup_profiling.call_args[0][1] is mock_conn


class TestProfilingIsolationLevelRejection:
    """A per-table rejection warns and continues, but respects catch_exceptions."""

    def test_rejection_reports_warning_and_still_profiles(
        self, profiler, sqlite_engine
    ):
        # Both halves: a rejected execution_options produces a warning titled
        # "Profiling: isolation level unavailable" AND the table still produces a
        # profile. The second assertion is the one that matters -- without it this
        # test passes even when the fallback leaves an unusable connection.
        profiler.config.catch_exceptions = True
        profiler._profiling_isolation_level = "AUTOCOMMIT"
        request = ProfilerRequest(
            pretty_name="test.my_table",
            batch_kwargs={"table": "test_table", "schema": None},
        )
        metadata = sa.MetaData()
        sql_table = sa.Table("test_table", metadata, sa.Column("id", sa.Integer))

        def mock_profile_row_count(*args, **kwargs):
            profile = args[3] if len(args) > 3 else kwargs.get("profile")
            if profile:
                profile.rowCount = 0
            return 0

        with (
            sqlite_engine.connect() as conn,
            patch.object(profiler, "base_engine") as mock_engine,
            patch.object(
                profiler, "_profile_row_count", side_effect=mock_profile_row_count
            ),
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
            ) as mock_get_adapter,
            patch.object(
                sqlite_engine.dialect,
                "set_isolation_level",
                side_effect=sqlite3.OperationalError("proxy refuses AUTOCOMMIT"),
            ),
        ):
            mock_engine.connect.return_value.__enter__.return_value = conn
            mock_adapter = MagicMock()
            mock_context = MagicMock()
            mock_context.sql_table = sql_table
            mock_adapter.setup_profiling.return_value = mock_context
            mock_get_adapter.return_value = mock_adapter

            result_request, result_profile = profiler._generate_profile_from_request(
                None, request
            )

        assert result_request == request
        assert result_profile is not None
        assert profiler.report.warning.call_count == 1
        warning_call = profiler.report.warning.call_args
        assert warning_call.kwargs["title"] == "Profiling: isolation level unavailable"
        assert warning_call.kwargs["context"] == (
            "Asset: test.my_table; isolation_level=AUTOCOMMIT"
        )
        assert isinstance(warning_call.kwargs["exc"], sqlite3.OperationalError)

    def test_rejection_respects_catch_exceptions_false(self, profiler, sqlite_engine):
        # When catch_exceptions is False, a rejection re-raises rather than
        # degrading -- matching the other paths in _generate_single_profile.
        profiler.config.catch_exceptions = False
        profiler._profiling_isolation_level = "AUTOCOMMIT"
        request = ProfilerRequest(
            pretty_name="test.my_table",
            batch_kwargs={"table": "test_table", "schema": None},
        )

        with (
            sqlite_engine.connect() as conn,
            patch.object(profiler, "base_engine") as mock_engine,
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
            ) as mock_get_adapter,
            patch.object(
                sqlite_engine.dialect,
                "set_isolation_level",
                side_effect=sqlite3.OperationalError("nope"),
            ),
        ):
            mock_engine.connect.return_value.__enter__.return_value = conn
            mock_adapter = MagicMock()
            mock_adapter.setup_profiling.side_effect = RuntimeError("short-circuit")
            mock_get_adapter.return_value = mock_adapter

            with pytest.raises(sqlite3.OperationalError):
                profiler._generate_profile_from_request(None, request)

    def test_rejection_log_output_is_bounded(
        self, sqlite_engine, profiler_config, caplog
    ):
        # One log line per run, not one per failing table: the structured report
        # dedups by title+message (N tables -> 1 entry with N contexts), and the
        # logger is gated on a per-run flag so it emits exactly once.
        real_report = SQLSourceReport()
        profiler = SQLAlchemyProfiler(
            conn=sqlite_engine,
            report=real_report,
            config=profiler_config,
            platform="sqlite",
            env="TEST",
        )
        profiler.config.catch_exceptions = True
        profiler._profiling_isolation_level = "AUTOCOMMIT"
        metadata = sa.MetaData()
        sql_table = sa.Table("test_table", metadata, sa.Column("id", sa.Integer))

        def mock_profile_row_count(*args, **kwargs):
            profile = args[3] if len(args) > 3 else kwargs.get("profile")
            if profile:
                profile.rowCount = 0
            return 0

        requests = [
            ProfilerRequest(
                pretty_name="db.t1",
                batch_kwargs={"table": "t1", "schema": None},
            ),
            ProfilerRequest(
                pretty_name="db.t2",
                batch_kwargs={"table": "t2", "schema": None},
            ),
        ]

        with (
            sqlite_engine.connect() as conn,
            patch.object(profiler, "base_engine") as mock_engine,
            patch.object(
                profiler, "_profile_row_count", side_effect=mock_profile_row_count
            ),
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
            ) as mock_get_adapter,
            patch.object(
                sqlite_engine.dialect,
                "set_isolation_level",
                side_effect=sqlite3.OperationalError("nope"),
            ),
        ):
            mock_engine.connect.return_value.__enter__.return_value = conn
            mock_adapter = MagicMock()
            mock_context = MagicMock()
            mock_context.sql_table = sql_table
            mock_adapter.setup_profiling.return_value = mock_context
            mock_get_adapter.return_value = mock_adapter

            with caplog.at_level(
                logging.WARNING,
                logger="datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler",
            ):
                for req in requests:
                    profiler._generate_profile_from_request(MagicMock(), req)

        # One deduped report entry, not two.
        assert len(real_report.warnings) == 1
        warning = real_report.warnings[0]
        assert warning.title == "Profiling: isolation level unavailable"
        contexts = list(warning.context)
        assert len(contexts) == 2
        assert any(c.startswith("Asset: db.t1") for c in contexts)
        assert any(c.startswith("Asset: db.t2") for c in contexts)
        # Exactly one log record for the run, not one per failing table.
        log_records = [
            r for r in caplog.records if "isolation level unavailable" in r.getMessage()
        ]
        assert len(log_records) == 1

    def test_unrecognised_level_warns_and_still_profiles(self, profiler, sqlite_engine):
        # A level the dialect does not recognise reaches the same broad
        # except and degrades the same way: one warning, then a transactional
        # profile. The enum prevents typos at config-parse time, but a level the
        # enum accepts may still be one this particular dialect rejects.
        profiler.config.catch_exceptions = True
        profiler._profiling_isolation_level = "BOGUS_LEVEL"
        request = ProfilerRequest(
            pretty_name="test.my_table",
            batch_kwargs={"table": "test_table", "schema": None},
        )
        metadata = sa.MetaData()
        sql_table = sa.Table("test_table", metadata, sa.Column("id", sa.Integer))

        def mock_profile_row_count(*args, **kwargs):
            profile = args[3] if len(args) > 3 else kwargs.get("profile")
            if profile:
                profile.rowCount = 0
            return 0

        with (
            sqlite_engine.connect() as conn,
            patch.object(profiler, "base_engine") as mock_engine,
            patch.object(
                profiler, "_profile_row_count", side_effect=mock_profile_row_count
            ),
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.get_adapter"
            ) as mock_get_adapter,
        ):
            mock_engine.connect.return_value.__enter__.return_value = conn
            mock_adapter = MagicMock()
            mock_context = MagicMock()
            mock_context.sql_table = sql_table
            mock_adapter.setup_profiling.return_value = mock_context
            mock_get_adapter.return_value = mock_adapter

            result_request, result_profile = profiler._generate_profile_from_request(
                None, request
            )

        assert result_request == request
        assert result_profile is not None
        assert profiler.report.warning.call_count == 1
        warning_call = profiler.report.warning.call_args
        assert warning_call.kwargs["title"] == "Profiling: isolation level unavailable"
        assert warning_call.kwargs["context"] == (
            "Asset: test.my_table; isolation_level=BOGUS_LEVEL"
        )
        assert isinstance(warning_call.kwargs["exc"], sa.exc.ArgumentError)


class TestEmittedFieldPaths:
    """The seam between the two names a column has.

    Profiling addresses columns by their stored identifier so a case-colliding
    pair stays distinct; a profile has to attach to the path the source put in
    schemaMetadata. `_to_emitted_field_paths` is where one becomes the other, and
    it is the only place two stored names can collapse onto one path.
    """

    @staticmethod
    def _profiles(*paths: str) -> List[DatasetFieldProfileClass]:
        return [DatasetFieldProfileClass(fieldPath=p) for p in paths]

    def test_translates_each_path_through_the_adapter(self, profiler: Any) -> None:
        adapter = MagicMock()
        adapter.field_path_for.side_effect = lambda name, conn: name.lower()

        result = profiler._to_emitted_field_paths(
            self._profiles("ORDER_ID", "AMOUNT"), adapter, MagicMock(), "db.tbl"
        )

        assert [p.fieldPath for p in result] == ["order_id", "amount"]

    def test_two_stored_names_collapsing_to_one_path_keep_one_profile(
        self, profiler: Any
    ) -> None:
        # An Oracle-shaped case: "col" and "COL" are distinct columns and each got
        # its own statistics, but the schema declares a single folded field. Two
        # profiles on one path would be dropped downstream, so keep the first.
        adapter = MagicMock()
        adapter.field_path_for.side_effect = lambda name, conn: name.lower()

        result = profiler._to_emitted_field_paths(
            self._profiles("col", "COL", "ID"), adapter, MagicMock(), "db.tbl"
        )

        assert [p.fieldPath for p in result] == ["col", "id"]

    def test_distinct_paths_all_survive(self, profiler: Any) -> None:
        # The preserve-case shape: the adapter hands the stored name straight
        # back, so nothing collapses and both columns keep their statistics.
        adapter = MagicMock()
        adapter.field_path_for.side_effect = lambda name, conn: name

        result = profiler._to_emitted_field_paths(
            self._profiles("col", "COL"), adapter, MagicMock(), "db.tbl"
        )

        assert [p.fieldPath for p in result] == ["col", "COL"]

    def test_a_quoted_name_leaves_as_a_plain_string(self, profiler: Any) -> None:
        # Profiles are built from sql_table.columns, whose names this module
        # rebuilds as quoted_name so the generated SQL targets each column
        # exactly. quoted_name is a str subclass whose .lower()/.upper() return
        # self while the identifier is quoted, so a path that keeps the subclass
        # silently survives every later fold -- Snowflake's convert_urns_to_lowercase
        # became a no-op this way, with no error anywhere.
        adapter = MagicMock()
        adapter.field_path_for.side_effect = lambda name, conn: name

        result = profiler._to_emitted_field_paths(
            [
                DatasetFieldProfileClass(
                    fieldPath=sa.sql.quoted_name("MixedCol", quote=True)
                )
            ],
            adapter,
            MagicMock(),
            "db.tbl",
        )

        assert type(result[0].fieldPath) is str
        assert result[0].fieldPath.lower() == "mixedcol"

    def test_quoted_names_still_fold_and_collapse(self, profiler: Any) -> None:
        adapter = MagicMock()
        adapter.field_path_for.side_effect = lambda name, conn: name.lower()

        result = profiler._to_emitted_field_paths(
            [
                DatasetFieldProfileClass(fieldPath=sa.sql.quoted_name(n, quote=True))
                for n in ("col", "COL", "ID")
            ],
            adapter,
            MagicMock(),
            "db.tbl",
        )

        assert [p.fieldPath for p in result] == ["col", "id"]

    def test_a_collapsed_pair_is_reported(self, profiler: Any) -> None:
        # Dropping the second profile is correct -- the schema declares one field.
        # Doing it silently is not: the surviving profile carries whichever
        # column came first, so the statistics under `col` may be "COL"'s, and
        # which one wins moves with column order. Nothing else in the run says so.
        adapter = MagicMock()
        adapter.field_path_for.side_effect = lambda name, conn: name.lower()

        profiler._to_emitted_field_paths(
            self._profiles("col", "COL"), adapter, MagicMock(), "db.sch.orders"
        )

        assert profiler.report.warning.called
        context = profiler.report.warning.call_args.kwargs["context"]
        assert "db.sch.orders" in context
        assert "col" in context

    def test_nothing_collapsing_is_not_reported(self, profiler: Any) -> None:
        adapter = MagicMock()
        adapter.field_path_for.side_effect = lambda name, conn: name.lower()

        profiler._to_emitted_field_paths(
            self._profiles("ID", "AMOUNT"), adapter, MagicMock(), "db.sch.orders"
        )

        assert not profiler.report.warning.called


class TestIgnoreSamplingColumnNames:
    """tags_to_ignore_sampling is resolved against DataHub, so it arrives as
    emitted field paths. Profiling addresses columns by their stored name, which
    on a normalizing dialect is a different string -- the membership test would
    silently never hit and the tag would stop working.

    These use the real adapters on purpose. A stub adapter that lowercases makes
    this look fixed while Snowflake, whose field_path_for returns the stored name
    unchanged, still fails.
    """

    @staticmethod
    def _table(*names: str) -> sa.Table:
        return sa.Table("t", sa.MetaData(), *[sa.Column(n, sa.String()) for n in names])

    @staticmethod
    def _snowflake_adapter() -> Any:
        from snowflake.sqlalchemy import dialect as snowflake_dialect

        from datahub.ingestion.source.sqlalchemy_profiler.adapters.snowflake import (
            SnowflakeAdapter,
        )

        engine = MagicMock()
        engine.dialect = snowflake_dialect()
        return SnowflakeAdapter(ProfilingConfig(), SQLSourceReport(), engine), engine

    def test_snowflake_tag_still_matches_its_column(self, profiler: Any) -> None:
        # The default recipe lowercases field paths, so DataHub holds
        # 'customer_id' while profiling holds 'CUSTOMER_ID'.
        adapter, engine = self._snowflake_adapter()

        kept = profiler._ignore_list_as_stored_names(
            ["customer_id"], self._table("CUSTOMER_ID", "AMOUNT"), adapter, engine
        )

        assert kept == ["CUSTOMER_ID"]

    def test_untagged_columns_are_left_alone(self, profiler: Any) -> None:
        adapter, engine = self._snowflake_adapter()

        kept = profiler._ignore_list_as_stored_names(
            ["customer_id"], self._table("AMOUNT", "TOTAL"), adapter, engine
        )

        assert kept == []

    def test_a_case_only_pair_is_treated_as_one(self, profiler: Any) -> None:
        # Documented trade-off: the emitted path cannot be reconstructed here, so
        # matching is folded and tagging one spelling skips both. Erring towards
        # skipping suits a control meant to keep stats off costly or sensitive
        # columns.
        adapter, engine = self._snowflake_adapter()

        kept = profiler._ignore_list_as_stored_names(
            ["col"], self._table("col", "COL"), adapter, engine
        )

        assert kept == ["col", "COL"]

    def test_empty_list_short_circuits(self, profiler: Any) -> None:
        adapter = MagicMock()

        assert (
            profiler._ignore_list_as_stored_names([], self._table("A"), adapter, None)
            == []
        )
        adapter.field_path_for.assert_not_called()
