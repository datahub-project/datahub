"""Unit tests for QueryCombinerRunner - focuses on query combining behavior."""

import math
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, Float, Integer, String, create_engine, event

from datahub.ingestion.source.ge_profiling_config import ProfilingConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.adapters import get_adapter
from datahub.ingestion.source.sqlalchemy_profiler.query_combiner_runner import (
    QueryCombinerRunner,
)


def _register_sqlite_functions(dbapi_conn, connection_record):
    """Register custom aggregate functions for SQLite to support stddev."""

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

    dbapi_conn.create_aggregate("stddev", 1, StdDevAggregate)


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

    def test_runner_without_query_combiner(
        self, sqlite_engine, test_adapter, test_table
    ):
        """Test that runner works without query combiner."""
        with sqlite_engine.connect() as conn:
            runner = QueryCombinerRunner(
                conn, "sqlite", test_adapter, query_combiner=None
            )

            # Should execute queries directly without query combiner
            count = runner.get_row_count(test_table)
            assert count == 3

            min_val = runner.get_column_min(test_table, "value")
            assert min_val == 10.5

    def test_runner_with_query_combiner(self, sqlite_engine, test_adapter, test_table):
        """Test that runner invokes query combiner when provided."""
        with sqlite_engine.connect() as conn:
            # Create mock query combiner
            mock_combiner = MagicMock()
            mock_combiner.run = MagicMock(side_effect=lambda func: func())

            runner = QueryCombinerRunner(
                conn, "sqlite", test_adapter, query_combiner=mock_combiner
            )

            # Execute a query
            count = runner.get_row_count(test_table)

            # Verify query combiner was invoked
            assert mock_combiner.run.called
            assert mock_combiner.run.call_count == 1
            assert count == 3

    def test_multiple_queries_invoke_combiner_multiple_times(
        self, sqlite_engine, test_adapter, test_table
    ):
        """Test that each decorated method invokes query combiner."""
        with sqlite_engine.connect() as conn:
            # Create mock query combiner
            mock_combiner = MagicMock()
            mock_combiner.run = MagicMock(side_effect=lambda func: func())

            runner = QueryCombinerRunner(
                conn, "sqlite", test_adapter, query_combiner=mock_combiner
            )

            # Execute multiple queries
            runner.get_row_count(test_table)
            runner.get_column_min(test_table, "value")
            runner.get_column_max(test_table, "value")
            runner.get_column_mean(test_table, "value")

            # Verify query combiner was invoked for each query
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

            result = runner.get_row_count(test_table)

            # Verify the function was captured and callable
            assert captured_func is not None
            assert callable(captured_func)
            assert result == 3

    def test_delegates_to_adapter(self, sqlite_engine, test_adapter, test_table):
        """Test that runner delegates all actual query logic to adapter."""
        with sqlite_engine.connect() as conn:
            runner = QueryCombinerRunner(conn, "sqlite", test_adapter)

            # All these calls should delegate to adapter methods
            count = runner.get_row_count(test_table)
            assert count == 3

            non_null = runner.get_column_non_null_count(test_table, "name")
            assert non_null == 3

            min_val = runner.get_column_min(test_table, "value")
            assert min_val == 10.5

            max_val = runner.get_column_max(test_table, "value")
            assert max_val == 30.5
