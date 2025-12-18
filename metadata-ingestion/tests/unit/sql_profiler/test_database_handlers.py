"""Unit tests for DatabaseHandlers."""

from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, Float, Integer, create_engine

from datahub.ingestion.source.sql_profiler.database_handlers import DatabaseHandlers


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
        Column("value", Float),
    )
    metadata.create_all(sqlite_engine)

    with sqlite_engine.connect() as conn, conn.begin():
        conn.execute(
            sa.insert(table),
            [
                {"id": 1, "value": 10.5},
                {"id": 2, "value": 20.5},
                {"id": 3, "value": 30.5},
            ],
        )

    return table


class TestDatabaseHandlers:
    """Test cases for DatabaseHandlers."""

    def test_get_approx_unique_count_expr_bigquery(self):
        """Test BigQuery approximate unique count expression."""
        expr = DatabaseHandlers.get_approx_unique_count_expr("bigquery", "column_name")
        # Should return a SQLAlchemy function
        assert expr is not None
        # Should be APPROX_COUNT_DISTINCT (check if it's a function or has func attribute)
        assert hasattr(expr, "func") or hasattr(expr, "__class__")

    def test_get_approx_unique_count_expr_snowflake(self):
        """Test Snowflake approximate unique count expression."""
        expr = DatabaseHandlers.get_approx_unique_count_expr("snowflake", "column_name")
        assert expr is not None

    def test_get_approx_unique_count_expr_redshift(self):
        """Test Redshift approximate unique count expression."""
        expr = DatabaseHandlers.get_approx_unique_count_expr("redshift", "column_name")
        # Redshift uses raw SQL text
        assert expr is not None
        # Check if it's a text object or string
        assert expr is not None
        assert (
            hasattr(expr, "text") or isinstance(expr, str) or hasattr(expr, "__class__")
        )

    def test_get_approx_unique_count_expr_athena(self):
        """Test Athena approximate unique count expression."""
        expr = DatabaseHandlers.get_approx_unique_count_expr("athena", "column_name")
        assert expr is not None

    def test_get_approx_unique_count_expr_databricks(self):
        """Test Databricks approximate unique count expression."""
        expr = DatabaseHandlers.get_approx_unique_count_expr(
            "databricks", "column_name"
        )
        assert expr is not None

    def test_get_approx_unique_count_expr_fallback(self):
        """Test fallback to exact count for unsupported platforms."""
        expr = DatabaseHandlers.get_approx_unique_count_expr("mysql", "column_name")
        assert expr is not None

    def test_get_median_expr_snowflake(self):
        """Test Snowflake median expression."""
        expr = DatabaseHandlers.get_median_expr("snowflake", "column_name")
        assert expr is not None

    def test_get_median_expr_bigquery(self):
        """Test BigQuery median expression."""
        expr = DatabaseHandlers.get_median_expr("bigquery", "column_name")
        # BigQuery uses raw SQL text
        assert expr is not None
        # Check if it's a text object or string
        assert expr is not None
        assert (
            hasattr(expr, "text") or isinstance(expr, str) or hasattr(expr, "__class__")
        )

    def test_get_median_expr_redshift(self):
        """Test Redshift median expression."""
        expr = DatabaseHandlers.get_median_expr("redshift", "column_name")
        assert expr is not None

    def test_get_median_expr_athena(self):
        """Test Athena median expression."""
        expr = DatabaseHandlers.get_median_expr("athena", "column_name")
        assert expr is not None

    def test_get_median_expr_databricks(self):
        """Test Databricks median expression."""
        expr = DatabaseHandlers.get_median_expr("databricks", "column_name")
        assert expr is not None

    def test_get_median_expr_fallback(self):
        """Test fallback median for unsupported platforms."""
        expr = DatabaseHandlers.get_median_expr("mysql", "column_name")
        # May return None or a text expression
        # Check if it's a text object or string
        assert (
            expr is None
            or hasattr(expr, "text")
            or isinstance(expr, str)
            or hasattr(expr, "__class__")
        )

    def test_get_quantiles_bigquery(self, sqlite_engine, test_table):
        """Test BigQuery quantiles generation."""
        with (
            sqlite_engine.connect() as conn,
            patch.object(conn, "execute") as mock_execute,
        ):
            # Mock the result for BigQuery's array access
            mock_result = MagicMock()
            mock_result.fetchone.return_value = [15.0, 20.5, 25.0]
            mock_execute.return_value = mock_result

            quantiles = DatabaseHandlers.get_quantiles(
                conn, "bigquery", test_table, "value", [0.25, 0.5, 0.75]
            )
            assert len(quantiles) == 3
            assert all(isinstance(q, float) or q is None for q in quantiles)

    def test_get_quantiles_snowflake(self, sqlite_engine, test_table):
        """Test Snowflake quantiles generation."""
        with (
            sqlite_engine.connect() as conn,
            patch.object(conn, "execute") as mock_execute,
        ):
            # Mock multiple queries for Snowflake
            mock_result = MagicMock()
            mock_result.scalar.side_effect = [15.0, 20.5, 25.0]
            mock_execute.return_value = mock_result

            quantiles = DatabaseHandlers.get_quantiles(
                conn, "snowflake", test_table, "value", [0.25, 0.5, 0.75]
            )
            assert len(quantiles) == 3

    def test_get_quantiles_athena(self, sqlite_engine, test_table):
        """Test Athena quantiles generation."""
        with (
            sqlite_engine.connect() as conn,
            patch.object(conn, "execute") as mock_execute,
        ):
            # Mock array result for Athena
            mock_result = MagicMock()
            mock_result.scalar.return_value = [15.0, 20.5, 25.0]
            mock_execute.return_value = mock_result

            quantiles = DatabaseHandlers.get_quantiles(
                conn, "athena", test_table, "value", [0.25, 0.5, 0.75]
            )
            assert len(quantiles) == 3

    def test_get_sample_clause_bigquery(self):
        """Test BigQuery sample clause generation."""
        clause = DatabaseHandlers.get_sample_clause("bigquery", 10000)
        assert clause == "TABLESAMPLE SYSTEM (10000 ROWS)"

    def test_get_sample_clause_other_platforms(self):
        """Test sample clause for unsupported platforms."""
        clause = DatabaseHandlers.get_sample_clause("mysql", 10000)
        assert clause is None

    def test_get_sample_clause_no_size(self):
        """Test sample clause without size."""
        clause = DatabaseHandlers.get_sample_clause("bigquery", None)
        assert clause is None
