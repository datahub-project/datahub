"""Unit tests for QueryCombinerRunner - focuses on query combining behavior."""

import math
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, Float, Integer, String, create_engine, event

from datahub.ingestion.source.ge_profiling_config import (
    ProfilingConfig,
)
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.adapters import get_adapter
from datahub.ingestion.source.sqlalchemy_profiler.query_combiner_runner import (
    QueryCombinerRunner,
)
from datahub.utilities.sqlalchemy_query_combiner import SQLAlchemyQueryCombiner


def _is_single_row_query_method(query):
    """Simple version for testing."""
    import traceback

    SINGLE_ROW_QUERY_METHODS = {
        "get_row_count",
        "get_column_min",
        "get_column_max",
        "get_column_mean",
        "get_column_stdev",
        "get_column_unique_count",
        "get_column_non_null_count",
        "get_column_median",
    }

    stack = traceback.extract_stack()
    return any(frame.name in SINGLE_ROW_QUERY_METHODS for frame in reversed(stack))


def _register_sqlite_functions(dbapi_conn, connection_record):
    """Register custom aggregate functions for SQLite to support stddev and median."""

    class StdDevAggregate:
        """SQLite aggregate for standard deviation."""

        def __init__(self):
            self.values = []

        def step(self, value):
            if value is not None:
                self.values.append(value)

        def finalize(self):
            if len(self.values) == 0:
                return None
            if len(self.values) == 1:
                return None
            mean = sum(self.values) / len(self.values)
            variance = sum((x - mean) ** 2 for x in self.values) / (
                len(self.values) - 1
            )
            return math.sqrt(variance)

    class MedianAggregate:
        """SQLite aggregate for MEDIAN."""

        def __init__(self):
            self.values = []

        def step(self, value):
            if value is not None:
                self.values.append(value)

        def finalize(self):
            if len(self.values) == 0:
                return None

            sorted_values = sorted(self.values)
            n = len(sorted_values)

            # For even number of values, return average of two middle values
            # For odd number, return the middle value
            if n % 2 == 0:
                mid1 = sorted_values[n // 2 - 1]
                mid2 = sorted_values[n // 2]
                return (mid1 + mid2) / 2.0
            else:
                return sorted_values[n // 2]

    dbapi_conn.create_aggregate("stddev", 1, StdDevAggregate)
    dbapi_conn.create_aggregate("median", 1, MedianAggregate)


@pytest.fixture
def sqlite_engine():
    """Create an in-memory SQLite engine with custom functions for testing."""
    engine = create_engine("sqlite:///:memory:")
    # Register custom functions on connect
    event.listen(engine, "connect", _register_sqlite_functions)
    return engine


@pytest.fixture
def test_adapter(sqlite_engine):
    """Create a generic adapter for testing."""
    config = ProfilingConfig()
    report = SQLSourceReport()
    return get_adapter("sqlite", config, report, sqlite_engine)


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


class TestQueryCombinerRunner:
    """Test cases for QueryCombinerRunner - focuses on query combining behavior."""

    def test_runner_returns_future_results(
        self, sqlite_engine, test_adapter, test_table
    ):
        """Test that runner returns FutureResult objects."""
        with sqlite_engine.connect() as conn:
            # Create mock query combiner
            mock_combiner = MagicMock()
            mock_combiner.run = MagicMock(side_effect=lambda func: func())

            runner = QueryCombinerRunner(
                conn, "sqlite", test_adapter, query_combiner=mock_combiner
            )

            # Methods return FutureResult objects
            count_future = runner.get_row_count(test_table)
            min_future = runner.get_column_min(test_table, "value")

            # Verify they are FutureResult instances
            from datahub.ingestion.source.sqlalchemy_profiler.query_combiner_runner import (
                FutureResult,
            )

            assert isinstance(count_future, FutureResult)
            assert isinstance(min_future, FutureResult)

            # Extract results
            assert count_future.result() == 3
            assert min_future.result() == 10.5

    def test_runner_with_query_combiner(self, sqlite_engine, test_adapter, test_table):
        """Test that runner invokes query combiner when provided."""
        with sqlite_engine.connect() as conn:
            # Create mock query combiner
            mock_combiner = MagicMock()
            mock_combiner.run = MagicMock(side_effect=lambda func: func())

            runner = QueryCombinerRunner(
                conn, "sqlite", test_adapter, query_combiner=mock_combiner
            )

            # Schedule a query
            count_future = runner.get_row_count(test_table)

            # Verify query combiner was invoked
            assert mock_combiner.run.called
            assert mock_combiner.run.call_count == 1

            # Extract result
            assert count_future.result() == 3

    def test_multiple_queries_invoke_combiner_multiple_times(
        self, sqlite_engine, test_adapter, test_table
    ):
        """Test that each method invokes query combiner."""
        with sqlite_engine.connect() as conn:
            # Create mock query combiner
            mock_combiner = MagicMock()
            mock_combiner.run = MagicMock(side_effect=lambda func: func())

            runner = QueryCombinerRunner(
                conn, "sqlite", test_adapter, query_combiner=mock_combiner
            )

            # Schedule multiple queries
            runner.get_row_count(test_table)
            runner.get_column_min(test_table, "value")
            runner.get_column_max(test_table, "value")
            runner.get_column_mean(test_table, "value")

            # Verify query combiner was invoked for each query (but not flushed)
            assert mock_combiner.run.call_count == 4

    def test_query_combiner_can_batch_queries(
        self, sqlite_engine, test_adapter, test_table
    ):
        """Test that query combiner can accumulate and batch queries."""
        with sqlite_engine.connect() as conn:
            # Create a realistic query combiner mock that accumulates queries
            accumulated_queries = []

            def accumulate_query(func):
                accumulated_queries.append(func)
                # In real query combiner, this would batch queries
                # For testing, just execute immediately
                return func()

            mock_combiner = MagicMock()
            mock_combiner.run = MagicMock(side_effect=accumulate_query)

            runner = QueryCombinerRunner(
                conn, "sqlite", test_adapter, query_combiner=mock_combiner
            )

            # Execute multiple queries
            runner.get_row_count(test_table)
            runner.get_column_min(test_table, "value")
            runner.get_column_max(test_table, "value")

            # Verify all queries were accumulated
            assert len(accumulated_queries) == 3
            assert mock_combiner.run.call_count == 3

    def test_decorated_methods(self, sqlite_engine, test_adapter, test_table):
        """Test that expected methods are decorated with query combiner."""
        with sqlite_engine.connect() as conn:
            mock_combiner = MagicMock()
            mock_combiner.run = MagicMock(side_effect=lambda func: func())

            runner = QueryCombinerRunner(
                conn, "sqlite", test_adapter, query_combiner=mock_combiner
            )

            # Test all decorated methods
            runner.get_row_count(test_table)
            assert mock_combiner.run.called
            mock_combiner.run.reset_mock()

            runner.get_column_non_null_count(test_table, "value")
            assert mock_combiner.run.called
            mock_combiner.run.reset_mock()

            runner.get_column_min(test_table, "value")
            assert mock_combiner.run.called
            mock_combiner.run.reset_mock()

            runner.get_column_max(test_table, "value")
            assert mock_combiner.run.called
            mock_combiner.run.reset_mock()

            runner.get_column_mean(test_table, "value")
            assert mock_combiner.run.called
            mock_combiner.run.reset_mock()

            runner.get_column_stdev(test_table, "value")
            assert mock_combiner.run.called
            mock_combiner.run.reset_mock()

            runner.get_column_unique_count(test_table, "value")
            assert mock_combiner.run.called
            mock_combiner.run.reset_mock()

            # Note: get_column_median will return None for SQLite (no PERCENTILE_CONT)
            # but the decorator should still be invoked
            runner.get_column_median(test_table, "value")
            assert mock_combiner.run.called

    def test_non_decorated_methods(self, sqlite_engine, test_adapter, test_table):
        """Test that non-decorated methods don't invoke query combiner."""
        with sqlite_engine.connect() as conn:
            mock_combiner = MagicMock()
            mock_combiner.run = MagicMock(side_effect=lambda func: func())

            runner = QueryCombinerRunner(
                conn, "sqlite", test_adapter, query_combiner=mock_combiner
            )

            # Methods that aren't decorated (they may execute multiple queries)
            runner.get_column_quantiles(test_table, "value", [0.5])
            runner.get_column_histogram(test_table, "value", num_buckets=5)
            runner.get_column_value_frequencies(test_table, "name", top_k=10)
            runner.get_column_distinct_value_frequencies(test_table, "name")
            runner.get_column_sample_values(test_table, "name", limit=5)

            # Query combiner should NOT be invoked for these methods
            assert not mock_combiner.run.called

    def test_query_combiner_receives_correct_function(
        self, sqlite_engine, test_adapter, test_table
    ):
        """Test that query combiner receives callable that executes the query."""
        with sqlite_engine.connect() as conn:
            captured_func = None

            def capture_func(func):
                nonlocal captured_func
                captured_func = func
                return func()

            mock_combiner = MagicMock()
            mock_combiner.run = MagicMock(side_effect=capture_func)

            runner = QueryCombinerRunner(
                conn, "sqlite", test_adapter, query_combiner=mock_combiner
            )

            result_future = runner.get_row_count(test_table)

            # Verify the function was captured and callable
            assert captured_func is not None
            assert callable(captured_func)

            # Extract result from future
            assert result_future.result() == 3

    def test_delegates_to_adapter(self, sqlite_engine, test_adapter, test_table):
        """Test that runner delegates all actual query logic to adapter."""
        with sqlite_engine.connect() as conn:
            mock_combiner = MagicMock()
            mock_combiner.run = MagicMock(side_effect=lambda func: func())

            runner = QueryCombinerRunner(
                conn, "sqlite", test_adapter, query_combiner=mock_combiner
            )

            # All these calls should delegate to adapter methods
            count_future = runner.get_row_count(test_table)
            assert count_future.result() == 3

            non_null_future = runner.get_column_non_null_count(test_table, "name")
            assert non_null_future.result() == 3

            min_future = runner.get_column_min(test_table, "value")
            assert min_future.result() == 10.5

            max_future = runner.get_column_max(test_table, "value")
            assert max_future.result() == 30.5

    def test_queries_are_batched_with_multiple_ctes(
        self, sqlite_engine, test_adapter, test_table, caplog
    ):
        """
        CRITICAL TEST: Verify queries are batched into one SQL with multiple CTEs.

        This test prevents regression where each query was flushed individually,
        defeating the purpose of query batching.
        """
        with (
            sqlite_engine.connect() as conn,
            SQLAlchemyQueryCombiner(
                enabled=True,
                catch_exceptions=False,
                is_single_row_query_method=_is_single_row_query_method,
                serial_execution_fallback_enabled=True,
            ).activate() as query_combiner,
        ):
            runner = QueryCombinerRunner(conn, "sqlite", test_adapter, query_combiner)

            # Schedule multiple queries
            row_count_future = runner.get_row_count(test_table)
            min_future = runner.get_column_min(test_table, "value")
            max_future = runner.get_column_max(test_table, "value")
            mean_future = runner.get_column_mean(test_table, "value")

            # Flush once - should batch all 4 queries together
            query_combiner.flush()

            # Extract results
            assert row_count_future.result() == 3
            assert min_future.result() == 10.5
            assert max_future.result() == 30.5
            assert mean_future.result() == 20.5

            # CRITICAL: Verify batching actually happened
            # Check query combiner report
            assert query_combiner.report.combined_queries_issued >= 1, (
                "No combined queries were issued - batching is not working!"
            )
            assert query_combiner.report.queries_combined == 4, (
                f"Expected 4 queries to be combined, got {query_combiner.report.queries_combined}"
            )

            # Verify we see a combined query with multiple CTEs in logs
            # Combined queries have format: WITH cte1 AS (...), cte2 AS (...), ...
            combined_query_found = False
            for record in caplog.records:
                if "Executing combined query" in record.message:
                    combined_query_found = True
                    # Count CTEs in the query (should have multiple)
                    cte_count = record.message.count(" AS \n(SELECT ")
                    assert cte_count >= 2, (
                        f"Combined query should have multiple CTEs, found {cte_count}"
                    )
                    break

            assert combined_query_found, (
                "No combined query found in logs - queries may be executing individually!"
            )

    def test_strategic_batching_with_multiple_flush_points(
        self, sqlite_engine, test_adapter, test_table
    ):
        """
        Test strategic batching: multiple flush points for different stages.

        This mimics the real profiler behavior where we batch queries in stages:
        - Stage 1: Row count + column cardinality
        - Stage 2: Numeric stats (min/max/mean/stdev)
        """
        with (
            sqlite_engine.connect() as conn,
            SQLAlchemyQueryCombiner(
                enabled=True,
                catch_exceptions=False,
                is_single_row_query_method=_is_single_row_query_method,
                serial_execution_fallback_enabled=True,
            ).activate() as query_combiner,
        ):
            runner = QueryCombinerRunner(conn, "sqlite", test_adapter, query_combiner)

            # Stage 1: Schedule row count and cardinality queries
            row_count_future = runner.get_row_count(test_table)
            non_null_id_future = runner.get_column_non_null_count(test_table, "id")
            non_null_value_future = runner.get_column_non_null_count(
                test_table, "value"
            )
            unique_id_future = runner.get_column_unique_count(
                test_table, "id", use_approx=False
            )

            # Flush stage 1
            query_combiner.flush()

            # Extract stage 1 results
            row_count = row_count_future.result()
            assert row_count == 3
            assert non_null_id_future.result() == 3
            assert non_null_value_future.result() == 3
            assert unique_id_future.result() == 3

            # Stage 2: Schedule numeric stats
            min_future = runner.get_column_min(test_table, "value")
            max_future = runner.get_column_max(test_table, "value")
            mean_future = runner.get_column_mean(test_table, "value")

            # Flush stage 2
            query_combiner.flush()

            # Extract stage 2 results
            assert min_future.result() == 10.5
            assert max_future.result() == 30.5
            assert mean_future.result() == 20.5

            # Verify batching happened in both stages
            assert query_combiner.report.combined_queries_issued >= 2
            assert (
                query_combiner.report.queries_combined == 7
            )  # 4 in stage 1, 3 in stage 2

    def test_flush_required_before_result(
        self, sqlite_engine, test_adapter, test_table
    ):
        """Test that calling result() before flush() raises ValueError."""
        with (
            sqlite_engine.connect() as conn,
            SQLAlchemyQueryCombiner(
                enabled=True,
                catch_exceptions=False,
                is_single_row_query_method=_is_single_row_query_method,
                serial_execution_fallback_enabled=True,
            ).activate() as query_combiner,
        ):
            runner = QueryCombinerRunner(conn, "sqlite", test_adapter, query_combiner)

            # Schedule query
            row_count_future = runner.get_row_count(test_table)

            # Try to get result before flush
            with pytest.raises(ValueError):
                row_count_future.result()

            # Now flush and it should work
            query_combiner.flush()
            assert row_count_future.result() == 3

    def test_no_batching_regression(
        self, sqlite_engine, test_adapter, test_table, caplog
    ):
        """
        REGRESSION TEST: Ensure we don't regress to one-query-per-flush pattern.

        This test would FAIL with the old @_run_with_query_combiner decorator
        that called flush() after every method.
        """
        with (
            sqlite_engine.connect() as conn,
            SQLAlchemyQueryCombiner(
                enabled=True,
                catch_exceptions=False,
                is_single_row_query_method=_is_single_row_query_method,
                serial_execution_fallback_enabled=True,
            ).activate() as query_combiner,
        ):
            runner = QueryCombinerRunner(conn, "sqlite", test_adapter, query_combiner)

            # Schedule 5 queries
            futures = [
                runner.get_row_count(test_table),
                runner.get_column_min(test_table, "value"),
                runner.get_column_max(test_table, "value"),
                runner.get_column_mean(test_table, "value"),
                runner.get_column_non_null_count(test_table, "value"),
            ]

            # Single flush for all 5 queries
            query_combiner.flush()

            # Extract all results
            for future in futures:
                future.result()

            # CRITICAL ASSERTION: Should have 1 combined query, not 5 separate ones
            assert query_combiner.report.combined_queries_issued == 1, (
                f"Expected 1 combined query, got {query_combiner.report.combined_queries_issued}. Batching regression detected!"
            )

            assert query_combiner.report.uncombined_queries_issued == 0, (
                f"Found {query_combiner.report.uncombined_queries_issued} uncombined queries. Batching is not working!"
            )

            assert query_combiner.report.queries_combined == 5, (
                f"Expected 5 queries to be combined, got {query_combiner.report.queries_combined}"
            )

    def test_query_exception_propagates_as_sql_error(
        self, sqlite_engine, test_adapter, test_table
    ):
        """
        Test that flush() raises SQLAlchemy errors when fallback is disabled.

        When serial_execution_fallback_enabled=False, flush() should raise
        the actual SQLAlchemy error immediately when batch execution fails,
        rather than falling back to serial execution.

        This test verifies that:
        1. flush() raises the actual SQLAlchemy error when batch execution fails
        2. The error message contains details about the SQL problem
        3. The exception type is correct (not a generic Python error)
        """
        with (
            sqlite_engine.connect() as conn,
            SQLAlchemyQueryCombiner(
                enabled=True,
                catch_exceptions=False,  # Fail fast on SQL errors
                is_single_row_query_method=_is_single_row_query_method,
                serial_execution_fallback_enabled=False,  # Disable fallback so flush() raises
            ).activate() as query_combiner,
        ):
            runner = QueryCombinerRunner(conn, "sqlite", test_adapter, query_combiner)

            # Schedule a query that will fail (non-existent column)
            _ = runner.get_column_min(test_table, "nonexistent_column")

            # flush() should raise the actual SQL exception when batch execution fails
            with pytest.raises(
                (sa.exc.SQLAlchemyError, sa.exc.OperationalError)
            ) as exc_info:
                query_combiner.flush()

            # Verify it's the actual SQL error with meaningful details
            assert "nonexistent_column" in str(exc_info.value).lower()
            assert isinstance(
                exc_info.value, (sa.exc.SQLAlchemyError, sa.exc.OperationalError)
            )

    def test_future_result_value_error_when_not_flushed(
        self, sqlite_engine, test_adapter, test_table
    ):
        """
        Test that calling result() before flush() raises a clear ValueError.

        This test verifies that users get a helpful error message when they
        forget to call flush() before accessing results. The error should
        clearly indicate that flush() needs to be called first.
        """
        with (
            sqlite_engine.connect() as conn,
            SQLAlchemyQueryCombiner(
                enabled=True,
                catch_exceptions=False,
                is_single_row_query_method=_is_single_row_query_method,
                serial_execution_fallback_enabled=True,
            ).activate() as query_combiner,
        ):
            runner = QueryCombinerRunner(conn, "sqlite", test_adapter, query_combiner)

            # Schedule a query but don't flush
            future = runner.get_row_count(test_table)

            # Calling result() before flush() should raise ValueError with helpful message
            with pytest.raises(
                ValueError, match="Result not available yet.*flush\\(\\)"
            ):
                future.result()

            # After flush(), result should work
            query_combiner.flush()
            assert future.result() == 3

    def test_exception_stored_in_future_result(
        self, sqlite_engine, test_adapter, test_table
    ):
        """
        Test that exceptions during greenlet execution are stored and re-raised.

        When a batchable query fails inside the greenlet:
        - The exception is caught and stored in the result container
        - Calling result() should re-raise the actual database exception
        - NOT raise a misleading ValueError("Result not available yet...")
        """
        with (
            sqlite_engine.connect() as conn,
            SQLAlchemyQueryCombiner(
                enabled=True,
                catch_exceptions=False,
                is_single_row_query_method=_is_single_row_query_method,
                serial_execution_fallback_enabled=True,  # Enable fallback for individual query execution
            ).activate() as query_combiner,
        ):
            runner = QueryCombinerRunner(conn, "sqlite", test_adapter, query_combiner)

            # Schedule a query that will fail (non-existent column)
            # This will trigger fallback execution where exception is caught and stored
            future = runner.get_column_min(test_table, "nonexistent_column")

            # Flush will catch the exception during fallback execution
            # The exception should be stored in the container, not lost
            query_combiner.flush()

            # result() should re-raise the actual database exception
            # NOT "ValueError: Result not available yet..."
            with pytest.raises(
                (sa.exc.SQLAlchemyError, sa.exc.OperationalError)
            ) as exc_info:
                future.result()

            # Verify it's the actual SQL error with meaningful details
            error_msg = str(exc_info.value).lower()
            assert "nonexistent_column" in error_msg or "no such column" in error_msg
            assert isinstance(
                exc_info.value, (sa.exc.SQLAlchemyError, sa.exc.OperationalError)
            )

    def test_multiple_futures_with_mixed_success_and_failure(
        self, sqlite_engine, test_adapter, test_table
    ):
        """
        Test that when some queries succeed and others fail, each future correctly reports its state.

        This ensures that:
        1. Successful queries return their values
        2. Failed queries raise their exceptions
        3. One failure doesn't contaminate other futures
        """
        with (
            sqlite_engine.connect() as conn,
            SQLAlchemyQueryCombiner(
                enabled=True,
                catch_exceptions=False,
                is_single_row_query_method=_is_single_row_query_method,
                serial_execution_fallback_enabled=True,
            ).activate() as query_combiner,
        ):
            runner = QueryCombinerRunner(conn, "sqlite", test_adapter, query_combiner)

            # Schedule mixed queries: some will succeed, some will fail
            success_future_1 = runner.get_row_count(test_table)
            failure_future = runner.get_column_min(test_table, "nonexistent_column")
            success_future_2 = runner.get_column_max(test_table, "value")

            # Flush - fallback will execute each query individually
            query_combiner.flush()

            # Successful futures should return values
            assert success_future_1.result() == 3
            assert success_future_2.result() == 30.5

            # Failed future should raise the actual exception
            with pytest.raises((sa.exc.SQLAlchemyError, sa.exc.OperationalError)):
                failure_future.result()

    def test_exception_details_preserved(self, sqlite_engine, test_adapter, test_table):
        """
        Test that the full exception details (type, message, traceback) are preserved.

        When an exception occurs during query execution, the complete exception
        should be available to the caller, not a wrapped or modified version.
        """
        with (
            sqlite_engine.connect() as conn,
            SQLAlchemyQueryCombiner(
                enabled=True,
                catch_exceptions=False,
                is_single_row_query_method=_is_single_row_query_method,
                serial_execution_fallback_enabled=True,
            ).activate() as query_combiner,
        ):
            runner = QueryCombinerRunner(conn, "sqlite", test_adapter, query_combiner)

            # Trigger a specific type of exception
            future = runner.get_column_min(test_table, "nonexistent_column")
            query_combiner.flush()

            # Verify the exception is exactly what SQLAlchemy raised
            try:
                future.result()
                pytest.fail("Expected exception to be raised")
            except Exception as e:
                # Should be an SQLAlchemy exception, not ValueError
                assert not isinstance(e, ValueError)
                assert isinstance(e, (sa.exc.SQLAlchemyError, sa.exc.OperationalError))

                # Exception message should contain meaningful details
                error_str = str(e).lower()
                assert (
                    "nonexistent_column" in error_str or "no such column" in error_str
                ), f"Exception message doesn't contain column name: {error_str}"

    def test_future_result_repr_shows_state(
        self, sqlite_engine, test_adapter, test_table
    ):
        """
        Test that FutureResult.__repr__ correctly shows the current state.

        The repr should distinguish between:
        - pending: query not yet executed
        - error: query failed with exception
        - value: query succeeded with result
        """
        with (
            sqlite_engine.connect() as conn,
            SQLAlchemyQueryCombiner(
                enabled=True,
                catch_exceptions=False,
                is_single_row_query_method=_is_single_row_query_method,
                serial_execution_fallback_enabled=True,
            ).activate() as query_combiner,
        ):
            runner = QueryCombinerRunner(conn, "sqlite", test_adapter, query_combiner)

            # State 1: Pending (before flush)
            pending_future = runner.get_row_count(test_table)
            repr_str = repr(pending_future)
            assert "pending" in repr_str
            assert "error" not in repr_str

            # State 2: Success (after successful flush)
            query_combiner.flush()
            success_repr = repr(pending_future)
            assert "value=" in success_repr
            assert "3" in success_repr  # The actual row count
            assert "error" not in success_repr
            assert "pending" not in success_repr

            # State 3: Error (after failed query)
            error_future = runner.get_column_min(test_table, "nonexistent_column")
            query_combiner.flush()
            error_repr = repr(error_future)
            assert "error=" in error_repr
            assert "pending" not in error_repr
            # Should show exception type or message
            assert (
                "OperationalError" in error_repr
                or "SQLAlchemyError" in error_repr
                or "nonexistent" in error_repr.lower()
            )
