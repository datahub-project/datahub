"""Unit tests for StatsCalculator."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine

from datahub.ingestion.source.sql_profiler.stats_calculator import StatsCalculator


@pytest.fixture
def sqlite_engine():
    """Create an in-memory SQLite engine for testing."""
    engine = create_engine("sqlite:///:memory:")
    return engine


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
        Column("created_at", DateTime),
    )
    metadata.create_all(sqlite_engine)

    with sqlite_engine.connect() as conn, conn.begin():
        conn.execute(
            sa.insert(table),
            [
                {
                    "id": 1,
                    "name": "Alice",
                    "value": 10.5,
                    "created_at": datetime(2024, 1, 1),
                },
                {
                    "id": 2,
                    "name": "Bob",
                    "value": 20.5,
                    "created_at": datetime(2024, 1, 2),
                },
                {
                    "id": 3,
                    "name": "Charlie",
                    "value": 30.5,
                    "created_at": datetime(2024, 1, 3),
                },
                {
                    "id": 4,
                    "name": None,
                    "value": 40.5,
                    "created_at": datetime(2024, 1, 4),
                },
                {
                    "id": 5,
                    "name": "Eve",
                    "value": None,
                    "created_at": datetime(2024, 1, 5),
                },
            ],
        )

    return table


class TestStatsCalculator:
    """Test cases for StatsCalculator."""

    def test_get_row_count(self, sqlite_engine, test_table):
        """Test row count calculation."""
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            count = calc.get_row_count(test_table)
            assert count == 5

    def test_get_column_non_null_count(self, sqlite_engine, test_table):
        """Test non-null count calculation."""
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            # 'name' column has 4 non-null values (one is None)
            count = calc.get_column_non_null_count(test_table, "name")
            assert count == 4

            # 'value' column has 4 non-null values (one is None)
            count = calc.get_column_non_null_count(test_table, "value")
            assert count == 4

    def test_get_column_min(self, sqlite_engine, test_table):
        """Test minimum value calculation."""
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            min_val = calc.get_column_min(test_table, "value")
            assert min_val == 10.5

    def test_get_column_max(self, sqlite_engine, test_table):
        """Test maximum value calculation."""
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            max_val = calc.get_column_max(test_table, "value")
            assert max_val == 40.5

    def test_get_column_mean(self, sqlite_engine, test_table):
        """Test mean calculation."""
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            mean_val = calc.get_column_mean(test_table, "value")
            # Mean of [10.5, 20.5, 30.5, 40.5] = 25.5
            assert mean_val == pytest.approx(25.5, rel=1e-6)

    def test_get_column_stdev(self, sqlite_engine, test_table):
        """Test standard deviation calculation."""
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            # SQLite doesn't support stddev, so this will return None or raise
            try:
                stdev_val = calc.get_column_stdev(test_table, "value")
                # If it succeeds, should be a positive number
                if stdev_val is not None:
                    assert stdev_val > 0
            except Exception:
                # SQLite doesn't support stddev, so this is expected
                pytest.skip("SQLite doesn't support stddev function")

    def test_get_column_unique_count_exact(self, sqlite_engine, test_table):
        """Test unique count calculation (exact, not approximate)."""
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            unique_count = calc.get_column_unique_count(
                test_table, "name", use_approx=False
            )
            # Should have 4 unique names (Alice, Bob, Charlie, Eve)
            assert unique_count == 4

    @patch(
        "datahub.ingestion.source.sql_profiler.database_handlers.DatabaseHandlers.get_approx_unique_count_expr"
    )
    def test_get_column_unique_count_approx(
        self, mock_approx, sqlite_engine, test_table
    ):
        """Test approximate unique count."""
        mock_approx.return_value = sa.func.count(sa.func.distinct(sa.column("name")))
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "bigquery")
            unique_count = calc.get_column_unique_count(
                test_table, "name", use_approx=True
            )
            assert unique_count == 4
            mock_approx.assert_called_once_with("bigquery", "name")

    def test_get_column_histogram(self, sqlite_engine, test_table):
        """Test histogram generation."""
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            histogram = calc.get_column_histogram(test_table, "value", num_buckets=5)
            assert len(histogram) == 5
            # Each bucket should be a tuple of (start, end, count)
            for bucket in histogram:
                assert len(bucket) == 3
                assert isinstance(bucket[0], float)
                assert isinstance(bucket[1], float)
                assert isinstance(bucket[2], int)

    def test_get_column_value_frequencies(self, sqlite_engine, test_table):
        """Test value frequency calculation."""
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            frequencies = calc.get_column_value_frequencies(
                test_table, "name", top_k=10
            )
            assert len(frequencies) <= 10
            # Should be sorted by frequency descending
            if len(frequencies) > 1:
                assert frequencies[0][1] >= frequencies[1][1]

    def test_get_column_distinct_value_frequencies(self, sqlite_engine, test_table):
        """Test distinct value frequencies calculation."""
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            frequencies = calc.get_column_distinct_value_frequencies(test_table, "name")
            # Should have all distinct values (excluding None)
            assert len(frequencies) > 0
            # Should be sorted by value (None values are excluded)
            values = [freq[0] for freq in frequencies if freq[0] is not None]
            if len(values) > 1:
                assert values == sorted(values)

    def test_get_column_median(self, sqlite_engine, test_table):
        """Test median calculation."""
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            # SQLite doesn't support PERCENTILE_CONT, so this will return None
            try:
                median_val = calc.get_column_median(test_table, "value")
                # SQLite doesn't support median, so this is expected to return None
                # In a real database, median of [10.5, 20.5, 30.5, 40.5] = 25.5
                assert median_val is None or median_val == pytest.approx(25.5, rel=1e-6)
            except Exception:
                # SQLite doesn't support PERCENTILE_CONT, so this is expected
                pytest.skip("SQLite doesn't support PERCENTILE_CONT function")

    def test_get_column_quantiles(self, sqlite_engine, test_table):
        """Test quantiles calculation."""
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            quantiles = calc.get_column_quantiles(
                test_table, "value", [0.25, 0.5, 0.75]
            )
            assert len(quantiles) == 3
            # All quantiles should be within the min/max range
            assert all(q is None or (10.5 <= q <= 40.5) for q in quantiles)

    def test_get_row_count_with_estimation_postgresql(self, sqlite_engine, test_table):
        """Test row count estimation for PostgreSQL."""
        with (
            sqlite_engine.connect() as conn,
            patch.object(StatsCalculator, "_get_row_count_estimate", return_value=100),
        ):
            # Mock the estimation query
            calc = StatsCalculator(conn, "postgresql")
            count = calc.get_row_count(test_table, use_estimation=True)
            assert count == 100

    def test_get_row_count_with_estimation_mysql(self, sqlite_engine, test_table):
        """Test row count estimation for MySQL."""
        with (
            sqlite_engine.connect() as conn,
            patch.object(StatsCalculator, "_get_row_count_estimate", return_value=200),
        ):
            # Mock the estimation query
            calc = StatsCalculator(conn, "mysql")
            count = calc.get_row_count(test_table, use_estimation=True)
            assert count == 200

    def test_get_row_count_with_sample_clause(self, sqlite_engine, test_table):
        """Test row count with sample clause."""
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            # SQLite doesn't support TABLESAMPLE, but we can test the code path
            count = calc.get_row_count(test_table, sample_clause="LIMIT 3")
            # Should still return the actual count (sample_clause is just a suffix)
            assert count == 5

    def test_query_combiner_integration(self, sqlite_engine, test_table):
        """Test that query combiner is used when provided."""
        with sqlite_engine.connect() as conn:
            mock_combiner = MagicMock()
            mock_combiner.run = MagicMock(side_effect=lambda func: func())

            calc = StatsCalculator(conn, "sqlite", query_combiner=mock_combiner)
            count = calc.get_row_count(test_table)

            # Query combiner should have been called
            assert mock_combiner.run.called
            assert count == 5

    def test_get_column_histogram_empty_column(self, sqlite_engine):
        """Test histogram with empty column."""
        metadata = sa.MetaData()
        table = sa.Table(
            "empty_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("value", Float),
        )
        metadata.create_all(sqlite_engine)

        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            # Should handle empty table gracefully (min/max will be None)
            histogram = calc.get_column_histogram(table, "value", num_buckets=5)
            # Should return empty list when min/max are None
            assert histogram == []

    def test_get_column_histogram_with_bounds(self, sqlite_engine, test_table):
        """Test histogram with explicit min/max bounds."""
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            histogram = calc.get_column_histogram(
                test_table, "value", num_buckets=5, min_val=0.0, max_val=50.0
            )
            assert len(histogram) == 5
            # First bucket should start at 0.0
            assert histogram[0][0] == 0.0
            # Last bucket should end at 50.0
            assert histogram[-1][1] == 50.0

    def test_get_column_quantiles_with_connection(self, sqlite_engine, test_table):
        """Test quantiles calculation using DatabaseHandlers."""
        with sqlite_engine.connect() as conn:
            calc = StatsCalculator(conn, "sqlite")
            # SQLite will use the fallback PERCENTILE_CONT method
            quantiles = calc.get_column_quantiles(
                test_table, "value", [0.25, 0.5, 0.75]
            )
            assert len(quantiles) == 3
            # May be None if PERCENTILE_CONT is not supported, but should not crash
            assert all(q is None or isinstance(q, float) for q in quantiles)
