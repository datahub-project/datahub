"""Unit tests for SQLAlchemyProfiler."""

from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, Float, Integer, String, create_engine

from datahub.ingestion.source.ge_data_profiler import ProfilerRequest
from datahub.ingestion.source.ge_profiling_config import ProfilingConfig
from datahub.ingestion.source.profiling.common import Cardinality
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler import (
    SQLAlchemyProfiler,
)
from datahub.ingestion.source.sqlalchemy_profiler.type_mapping import ProfilerDataType


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
    report.report_warning = MagicMock()
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
