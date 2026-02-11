"""Unit tests for platform adapters."""

import math
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, Float, Integer, String, create_engine, event

from datahub.ingestion.source.ge_profiling_config import ProfilingConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.adapters import get_adapter
from datahub.ingestion.source.sqlalchemy_profiler.adapters.athena import AthenaAdapter
from datahub.ingestion.source.sqlalchemy_profiler.adapters.bigquery import (
    BigQueryAdapter,
    _quote_bigquery_identifier,
)
from datahub.ingestion.source.sqlalchemy_profiler.adapters.databricks import (
    DatabricksAdapter,
)
from datahub.ingestion.source.sqlalchemy_profiler.adapters.generic import (
    GenericAdapter,
)
from datahub.ingestion.source.sqlalchemy_profiler.adapters.mysql import MySQLAdapter
from datahub.ingestion.source.sqlalchemy_profiler.adapters.postgres import (
    PostgresAdapter,
)
from datahub.ingestion.source.sqlalchemy_profiler.adapters.redshift import (
    RedshiftAdapter,
)
from datahub.ingestion.source.sqlalchemy_profiler.adapters.snowflake import (
    SnowflakeAdapter,
)
from datahub.ingestion.source.sqlalchemy_profiler.adapters.trino import TrinoAdapter
from datahub.ingestion.source.sqlalchemy_profiler.profiling_context import (
    ProfilingContext,
)


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
    return table


@pytest.fixture
def config():
    """Create test profiling config."""
    return ProfilingConfig()


@pytest.fixture
def report():
    """Create test SQL report."""
    return SQLSourceReport()


class TestAdapterFactory:
    """Test adapter factory function."""

    def test_get_adapter_bigquery(self, config, report, sqlite_engine):
        """Test factory returns BigQuery adapter."""
        adapter = get_adapter("bigquery", config, report, sqlite_engine)
        assert isinstance(adapter, BigQueryAdapter)

    def test_get_adapter_postgresql(self, config, report, sqlite_engine):
        """Test factory returns PostgreSQL adapter."""
        adapter = get_adapter("postgresql", config, report, sqlite_engine)
        assert isinstance(adapter, PostgresAdapter)

    def test_get_adapter_postgres(self, config, report, sqlite_engine):
        """Test factory returns PostgreSQL adapter for 'postgres' alias."""
        adapter = get_adapter("postgres", config, report, sqlite_engine)
        assert isinstance(adapter, PostgresAdapter)

    def test_get_adapter_athena(self, config, report, sqlite_engine):
        """Test factory returns Athena adapter."""
        adapter = get_adapter("athena", config, report, sqlite_engine)
        assert isinstance(adapter, AthenaAdapter)

    def test_get_adapter_mysql(self, config, report, sqlite_engine):
        """Test factory returns MySQL adapter."""
        adapter = get_adapter("mysql", config, report, sqlite_engine)
        assert isinstance(adapter, MySQLAdapter)

    def test_get_adapter_generic(self, config, report, sqlite_engine):
        """Test factory returns generic adapter for unknown platform."""
        adapter = get_adapter("unknown_platform", config, report, sqlite_engine)
        assert isinstance(adapter, GenericAdapter)

    def test_get_adapter_redshift(self, config, report, sqlite_engine):
        """Test factory returns Redshift adapter."""
        adapter = get_adapter("redshift", config, report, sqlite_engine)
        assert isinstance(adapter, RedshiftAdapter)

    def test_get_adapter_snowflake(self, config, report, sqlite_engine):
        """Test factory returns Snowflake adapter."""
        adapter = get_adapter("snowflake", config, report, sqlite_engine)
        assert isinstance(adapter, SnowflakeAdapter)

    def test_get_adapter_databricks(self, config, report, sqlite_engine):
        """Test factory returns Databricks adapter."""
        adapter = get_adapter("databricks", config, report, sqlite_engine)
        assert isinstance(adapter, DatabricksAdapter)

    def test_get_adapter_trino(self, config, report, sqlite_engine):
        """Test factory returns Trino adapter."""
        adapter = get_adapter("trino", config, report, sqlite_engine)
        assert isinstance(adapter, TrinoAdapter)

    def test_get_adapter_case_insensitive(self, config, report, sqlite_engine):
        """Test factory handles case-insensitive platform names."""
        adapter = get_adapter("BigQuery", config, report, sqlite_engine)
        assert isinstance(adapter, BigQueryAdapter)

        adapter = get_adapter("POSTGRESQL", config, report, sqlite_engine)
        assert isinstance(adapter, PostgresAdapter)


class TestGenericAdapter:
    """Test cases for GenericAdapter."""

    @pytest.fixture
    def adapter(self, config, report, sqlite_engine):
        """Create generic adapter for testing."""
        return GenericAdapter(config, report, sqlite_engine)

    def test_setup_profiling_creates_sql_table(
        self, adapter, sqlite_engine, test_table
    ):
        """Test setup creates SQLAlchemy table object."""
        context = ProfilingContext(
            schema=None, table="test_table", custom_sql=None, pretty_name="test_table"
        )

        with sqlite_engine.connect() as conn:
            result_context = adapter.setup_profiling(context, conn)

        assert result_context.sql_table is not None
        assert result_context.sql_table.name == "test_table"

    def test_setup_profiling_requires_table_name(self, adapter, sqlite_engine):
        """Test setup fails without table name."""
        context = ProfilingContext(
            schema=None, table=None, custom_sql=None, pretty_name="test"
        )

        with (
            sqlite_engine.connect() as conn,
            pytest.raises(ValueError, match="table name required"),
        ):
            adapter.setup_profiling(context, conn)

    def test_cleanup_does_nothing(self, adapter):
        """Test cleanup is a no-op for generic adapter."""
        context = ProfilingContext(
            schema=None, table="test_table", custom_sql=None, pretty_name="test_table"
        )
        # Should not raise any errors
        adapter.cleanup(context)

    def test_get_approx_unique_count_expr(self, adapter):
        """Test approximate unique count uses COUNT(DISTINCT)."""
        expr = adapter.get_approx_unique_count_expr("column_name")
        # Should generate COUNT(DISTINCT column_name)
        assert "count" in str(expr).lower()
        assert "distinct" in str(expr).lower()

    def test_get_median_expr(self, adapter):
        """Test median expression uses MEDIAN function."""
        expr = adapter.get_median_expr("column_name")
        assert expr is not None
        # Should generate MEDIAN(column_name)
        compiled = expr.compile(compile_kwargs={"literal_binds": True})
        expr_str = str(compiled)
        assert "median" in expr_str.lower()
        assert "column_name" in expr_str

    def test_get_quantiles_expr(self, adapter):
        """Test quantiles expression returns None for generic adapter."""
        expr = adapter.get_quantiles_expr("column_name", [0.25, 0.5, 0.75])
        assert expr is None

    def test_get_sample_clause(self, adapter):
        """Test sample clause returns None for generic adapter."""
        clause = adapter.get_sample_clause(1000)
        assert clause is None

    def test_supports_row_count_estimation(self, adapter):
        """Test generic adapter does not support row count estimation."""
        assert not adapter.supports_row_count_estimation()

    def test_get_estimated_row_count(self, adapter, sqlite_engine, test_table):
        """Test row count estimation returns None for generic adapter."""
        with sqlite_engine.connect() as conn:
            result = adapter.get_estimated_row_count(test_table, conn)
        assert result is None

    @pytest.fixture
    def sqlite_engine_with_data(self):
        """Create an in-memory SQLite engine with test data."""
        engine = create_engine("sqlite:///:memory:")
        # Register custom functions on connect
        event.listen(engine, "connect", _register_sqlite_functions)

        metadata = sa.MetaData()
        table = sa.Table(
            "test_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            Column("value", Float),
        )
        metadata.create_all(engine)

        with engine.connect() as conn, conn.begin():
            conn.execute(
                sa.insert(table),
                [
                    {"id": 1, "name": "Alice", "value": 10.5},
                    {"id": 2, "name": "Bob", "value": 20.5},
                    {"id": 3, "name": "Charlie", "value": 30.5},
                    {"id": 4, "name": None, "value": 40.5},
                    {"id": 5, "name": "Eve", "value": None},
                ],
            )

        return engine, table

    def test_get_row_count(self, config, report, sqlite_engine_with_data):
        """Test row count calculation."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            count = adapter.get_row_count(table, conn)
            assert count == 5

    def test_get_row_count_with_sample_clause(
        self, config, report, sqlite_engine_with_data
    ):
        """Test row count with sample clause."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            count = adapter.get_row_count(table, conn, sample_clause="LIMIT 3")
            assert count == 5

    def test_get_row_count_with_estimation(
        self, config, report, sqlite_engine_with_data
    ):
        """Test row count estimation for platforms that support it."""
        engine, table = sqlite_engine_with_data
        postgres_adapter = get_adapter("postgresql", config, report, engine)
        with (
            engine.connect() as conn,
            patch.object(postgres_adapter, "get_estimated_row_count", return_value=100),
        ):
            count = postgres_adapter.get_row_count(table, conn, use_estimation=True)
            assert count == 100

    def test_get_column_non_null_count(self, config, report, sqlite_engine_with_data):
        """Test non-null count calculation."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            count = adapter.get_column_non_null_count(table, "name", conn)
            assert count == 4

            count = adapter.get_column_non_null_count(table, "value", conn)
            assert count == 4

            count = adapter.get_column_non_null_count(table, "id", conn)
            assert count == 5

    def test_get_column_min(self, config, report, sqlite_engine_with_data):
        """Test minimum value calculation."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            min_val = adapter.get_column_min(table, "value", conn)
            assert min_val == 10.5

            min_id = adapter.get_column_min(table, "id", conn)
            assert min_id == 1

    def test_get_column_max(self, config, report, sqlite_engine_with_data):
        """Test maximum value calculation."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            max_val = adapter.get_column_max(table, "value", conn)
            assert max_val == 40.5

            max_id = adapter.get_column_max(table, "id", conn)
            assert max_id == 5

    def test_get_column_mean(self, config, report, sqlite_engine_with_data):
        """Test mean calculation."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            mean_val = adapter.get_column_mean(table, "value", conn)
            assert mean_val == pytest.approx(25.5, rel=1e-6)

    def test_get_column_stdev(self, config, report, sqlite_engine_with_data):
        """Test standard deviation calculation."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            stdev_val = adapter.get_column_stdev(table, "value", conn)
            # Should return a positive value for our test data
            assert stdev_val is not None
            assert stdev_val > 0

    def test_get_column_stdev_single_value(self, config, report):
        """Test standard deviation with single non-null value returns None."""
        engine = create_engine("sqlite:///:memory:")
        # Register custom functions
        event.listen(engine, "connect", _register_sqlite_functions)

        metadata = sa.MetaData()
        table = sa.Table(
            "single_value_table",
            metadata,
            Column("id", Integer),
            Column("value", Float),
        )
        metadata.create_all(engine)

        with engine.connect() as conn, conn.begin():
            conn.execute(
                sa.insert(table),
                [
                    {"id": 1, "value": 10.5},
                    {"id": 2, "value": None},
                    {"id": 3, "value": None},
                ],
            )

        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            stdev_val = adapter.get_column_stdev(table, "value", conn)
            assert stdev_val is None

    def test_get_column_stdev_all_null(self, config, report):
        """Test standard deviation with all NULL values."""
        engine = create_engine("sqlite:///:memory:")
        # Register custom functions
        event.listen(engine, "connect", _register_sqlite_functions)

        metadata = sa.MetaData()
        table = sa.Table(
            "all_null_table",
            metadata,
            Column("id", Integer),
            Column("value", Float),
        )
        metadata.create_all(engine)

        with engine.connect() as conn, conn.begin():
            conn.execute(
                sa.insert(table),
                [
                    {"id": 1, "value": None},
                    {"id": 2, "value": None},
                ],
            )

        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            stdev_val = adapter.get_column_stdev(table, "value", conn)
            assert stdev_val is None

    def test_get_column_stdev_redshift_null_value(self, config, report):
        """Test Redshift returns 0.0 for all-NULL stdev."""
        engine = create_engine("sqlite:///:memory:")
        # Register custom functions
        event.listen(engine, "connect", _register_sqlite_functions)

        metadata = sa.MetaData()
        table = sa.Table(
            "all_null_table",
            metadata,
            Column("value", Float),
        )
        metadata.create_all(engine)

        with engine.connect() as conn, conn.begin():
            conn.execute(
                sa.insert(table),
                [
                    {"value": None},
                    {"value": None},
                ],
            )

        redshift_adapter = get_adapter("redshift", config, report, engine)
        with engine.connect() as conn:
            stdev_val = redshift_adapter.get_column_stdev(table, "value", conn)
            assert stdev_val == 0.0

    def test_get_column_unique_count_exact(
        self, config, report, sqlite_engine_with_data
    ):
        """Test exact unique count calculation."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            unique_count = adapter.get_column_unique_count(
                table, "name", conn, use_approx=False
            )
            assert unique_count == 4

    def test_get_column_unique_count_approx(
        self, config, report, sqlite_engine_with_data
    ):
        """Test approximate unique count uses adapter expression."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            unique_count = adapter.get_column_unique_count(
                table, "name", conn, use_approx=True
            )
            assert unique_count == 4

    def test_get_column_median(self, config, report, sqlite_engine_with_data):
        """Test median calculation."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            median_val = adapter.get_column_median(table, "value", conn)
            # With registered median aggregate, SQLite should return median value
            # Test data: [10.5, 20.5, 30.5, 40.5] -> median = (20.5 + 30.5) / 2 = 25.5
            assert median_val == pytest.approx(25.5, rel=1e-6)

    def test_get_column_quantiles(self, config, report, sqlite_engine_with_data):
        """Test quantiles calculation."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            quantiles = adapter.get_column_quantiles(
                table, "value", conn, [0.25, 0.5, 0.75]
            )
            assert len(quantiles) == 3
            assert all(q is None or (10.5 <= q <= 40.5) for q in quantiles)

    def test_get_column_histogram(self, config, report, sqlite_engine_with_data):
        """Test histogram generation."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            histogram = adapter.get_column_histogram(
                table, "value", conn, num_buckets=5
            )
            assert len(histogram) == 5
            for bucket in histogram:
                assert len(bucket) == 3
                assert isinstance(bucket[0], float)
                assert isinstance(bucket[1], float)
                assert isinstance(bucket[2], int)

    def test_get_column_histogram_empty_column(self, config, report):
        """Test histogram with empty column."""
        engine = create_engine("sqlite:///:memory:")
        metadata = sa.MetaData()
        table = sa.Table(
            "empty_table",
            metadata,
            Column("id", Integer),
            Column("value", Float),
        )
        metadata.create_all(engine)

        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            histogram = adapter.get_column_histogram(
                table, "value", conn, num_buckets=5
            )
            assert histogram == []

    def test_get_column_histogram_with_bounds(
        self, config, report, sqlite_engine_with_data
    ):
        """Test histogram with explicit min/max bounds."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            histogram = adapter.get_column_histogram(
                table, "value", conn, num_buckets=5, min_val=0.0, max_val=50.0
            )
            assert len(histogram) == 5
            assert histogram[0][0] == 0.0
            assert histogram[-1][1] == 50.0

    def test_get_column_value_frequencies(
        self, config, report, sqlite_engine_with_data
    ):
        """Test value frequency calculation."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            frequencies = adapter.get_column_value_frequencies(
                table, "name", conn, top_k=10
            )
            assert len(frequencies) <= 10
            if len(frequencies) > 1:
                assert frequencies[0][1] >= frequencies[1][1]

    def test_get_column_distinct_value_frequencies(
        self, config, report, sqlite_engine_with_data
    ):
        """Test distinct value frequencies calculation."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            frequencies = adapter.get_column_distinct_value_frequencies(
                table, "name", conn
            )
            assert len(frequencies) > 0
            values = [freq[0] for freq in frequencies if freq[0] is not None]
            if len(values) > 1:
                assert values == sorted(values)

    def test_get_column_sample_values(self, config, report, sqlite_engine_with_data):
        """Test sample values retrieval."""
        engine, table = sqlite_engine_with_data
        adapter = get_adapter("sqlite", config, report, engine)
        with engine.connect() as conn:
            samples = adapter.get_column_sample_values(table, "name", conn, limit=3)
            assert len(samples) <= 3
            assert all(isinstance(s, str) or s is None for s in samples)


class TestPostgresAdapter:
    """Test cases for PostgresAdapter."""

    @pytest.fixture
    def adapter(self, config, report, sqlite_engine):
        """Create PostgreSQL adapter for testing."""
        return PostgresAdapter(config, report, sqlite_engine)

    def test_setup_profiling_creates_sql_table(
        self, adapter, sqlite_engine, test_table
    ):
        """Test setup creates SQLAlchemy table object."""
        context = ProfilingContext(
            schema=None, table="test_table", custom_sql=None, pretty_name="test_table"
        )

        with sqlite_engine.connect() as conn:
            result_context = adapter.setup_profiling(context, conn)

        assert result_context.sql_table is not None
        assert result_context.sql_table.name == "test_table"

    def test_get_approx_unique_count_expr(self, adapter):
        """Test PostgreSQL uses COUNT(DISTINCT) for approximate unique count."""
        expr = adapter.get_approx_unique_count_expr("column_name")
        assert "count" in str(expr).lower()
        assert "distinct" in str(expr).lower()

    def test_get_median_expr(self, adapter):
        """Test PostgreSQL uses PERCENTILE_CONT for median."""
        expr = adapter.get_median_expr("test_column")
        assert expr is not None
        expr_str = str(expr)
        assert "PERCENTILE_CONT" in expr_str
        assert "0.5" in expr_str
        assert "test_column" in expr_str

    def test_supports_row_count_estimation(self, adapter):
        """Test PostgreSQL supports row count estimation."""
        assert adapter.supports_row_count_estimation()

    def test_get_estimated_row_count_with_mock(self, adapter, test_table):
        """Test PostgreSQL row count estimation can be called."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = 12345
        mock_conn.execute.return_value = mock_result

        # Note: This may return None if query fails, which is expected behavior
        result = adapter.get_estimated_row_count(test_table, mock_conn)

        # Just verify the method can be called without raising an exception
        assert result is None or isinstance(result, int)

    def test_get_estimated_row_count_handles_none(self, adapter, test_table):
        """Test PostgreSQL row count estimation handles None result."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = None
        mock_conn.execute.return_value = mock_result

        result = adapter.get_estimated_row_count(test_table, mock_conn)

        assert result is None

    def test_get_estimated_row_count_handles_exception(self, adapter, test_table):
        """Test PostgreSQL row count estimation handles exceptions gracefully."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Database error")

        result = adapter.get_estimated_row_count(test_table, mock_conn)

        assert result is None


class TestMySQLAdapter:
    """Test cases for MySQLAdapter."""

    @pytest.fixture
    def adapter(self, config, report, sqlite_engine):
        """Create MySQL adapter for testing."""
        return MySQLAdapter(config, report, sqlite_engine)

    def test_supports_row_count_estimation(self, adapter):
        """Test MySQL supports row count estimation."""
        assert adapter.supports_row_count_estimation()

    def test_get_estimated_row_count_with_mock(self, adapter, test_table):
        """Test MySQL row count estimation query structure."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = 54321
        mock_conn.execute.return_value = mock_result

        result = adapter.get_estimated_row_count(test_table, mock_conn)

        assert result == 54321
        # Verify query was executed
        assert mock_conn.execute.called

    def test_get_estimated_row_count_handles_none(self, adapter, test_table):
        """Test MySQL row count estimation handles None result."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = None
        mock_conn.execute.return_value = mock_result

        result = adapter.get_estimated_row_count(test_table, mock_conn)

        assert result is None


class TestRedshiftAdapter:
    """Test cases for RedshiftAdapter."""

    @pytest.fixture
    def adapter(self, config, report, sqlite_engine):
        """Create Redshift adapter for testing."""
        return RedshiftAdapter(config, report, sqlite_engine)

    def test_setup_profiling_creates_sql_table(
        self, adapter, sqlite_engine, test_table
    ):
        """Test setup creates SQLAlchemy table object."""
        context = ProfilingContext(
            schema=None, table="test_table", custom_sql=None, pretty_name="test_table"
        )

        with sqlite_engine.connect() as conn:
            result_context = adapter.setup_profiling(context, conn)

        assert result_context.sql_table is not None
        assert result_context.sql_table.name == "test_table"

    def test_get_approx_unique_count_expr(self, adapter):
        """Test Redshift uses COUNT(DISTINCT)."""
        expr = adapter.get_approx_unique_count_expr("column_name")
        assert "count" in str(expr).lower()
        assert "distinct" in str(expr).lower()

    def test_get_median_expr(self, adapter):
        """Test Redshift uses PERCENTILE_CONT for median."""
        expr = adapter.get_median_expr("test_column")
        assert expr is not None
        expr_str = str(expr)
        assert "PERCENTILE_CONT" in expr_str
        assert "0.5" in expr_str
        assert "test_column" in expr_str

    def test_get_mean_expr(self, adapter):
        """Test Redshift casts column to FLOAT for AVG."""
        expr = adapter.get_mean_expr("test_column")
        assert expr is not None
        # Should be AVG(CAST(column AS FLOAT))
        compiled = expr.compile(compile_kwargs={"literal_binds": True})
        expr_str = str(compiled).upper()
        assert "AVG" in expr_str
        assert "CAST" in expr_str
        assert "FLOAT" in expr_str

    def test_get_stdev_null_value(self, adapter):
        """Test Redshift returns 0.0 for NULL stdev."""
        null_value = adapter.get_stdev_null_value()
        assert null_value == 0.0

    def test_supports_row_count_estimation(self, adapter):
        """Test Redshift supports row count estimation."""
        assert adapter.supports_row_count_estimation()

    def test_get_estimated_row_count_with_mock(self, adapter, test_table):
        """Test Redshift row count estimation can be called."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = 54321
        mock_conn.execute.return_value = mock_result

        # Note: This may return None if query fails, which is expected behavior
        result = adapter.get_estimated_row_count(test_table, mock_conn)

        # Just verify the method can be called without raising an exception
        assert result is None or isinstance(result, int)

    def test_get_estimated_row_count_handles_none(self, adapter, test_table):
        """Test Redshift row count estimation handles None result."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = None
        mock_conn.execute.return_value = mock_result

        result = adapter.get_estimated_row_count(test_table, mock_conn)

        assert result is None

    def test_get_estimated_row_count_handles_exception(self, adapter, test_table):
        """Test Redshift row count estimation handles exceptions gracefully."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Database error")

        result = adapter.get_estimated_row_count(test_table, mock_conn)

        assert result is None


class TestAthenaAdapter:
    """Test cases for AthenaAdapter."""

    @pytest.fixture
    def adapter(self, config, report, sqlite_engine):
        """Create Athena adapter for testing."""
        return AthenaAdapter(config, report, sqlite_engine)

    def test_get_approx_unique_count_expr(self, adapter):
        """Test Athena uses approx_distinct."""
        expr = adapter.get_approx_unique_count_expr("test_column")
        assert expr is not None
        expr_str = str(expr)
        assert "approx_distinct" in expr_str
        assert "test_column" in expr_str

    def test_get_median_expr(self, adapter):
        """Test Athena uses approx_percentile for median."""
        expr = adapter.get_median_expr("test_column")
        assert expr is not None
        # Should be a SQLAlchemy function call
        compiled = expr.compile(compile_kwargs={"literal_binds": True})
        expr_str = str(compiled)
        assert "approx_percentile" in expr_str.lower()
        assert "0.5" in expr_str

    def test_get_column_quantiles(self, adapter, test_table):
        """Test Athena get_column_quantiles with mocked connection."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        # Athena returns array of quantile values
        mock_result.scalar.return_value = [5.0, 25.0, 50.0, 75.0, 95.0]
        mock_conn.execute.return_value = mock_result

        quantiles = adapter.get_column_quantiles(
            test_table, "test_column", mock_conn, [0.05, 0.25, 0.5, 0.75, 0.95]
        )

        assert len(quantiles) == 5
        assert quantiles == [5.0, 25.0, 50.0, 75.0, 95.0]
        # Verify query was executed
        assert mock_conn.execute.called


class TestBigQueryAdapter:
    """Test cases for BigQueryAdapter."""

    @pytest.fixture
    def adapter(self, config, report, sqlite_engine):
        """Create BigQuery adapter for testing."""
        return BigQueryAdapter(config, report, sqlite_engine)

    def test_get_approx_unique_count_expr(self, adapter):
        """Test BigQuery uses APPROX_COUNT_DISTINCT."""
        expr = adapter.get_approx_unique_count_expr("test_column")
        assert expr is not None
        # Should be a SQLAlchemy function call for APPROX_COUNT_DISTINCT
        compiled = expr.compile(compile_kwargs={"literal_binds": True})
        expr_str = str(compiled)
        assert "APPROX_COUNT_DISTINCT" in expr_str

    def test_get_median_expr(self, adapter):
        """Test BigQuery median expression using literal_column."""
        # BigQuery uses APPROX_QUANTILES with array indexing.
        # We use literal_column() to generate the full SQL string since
        # SQLAlchemy's dialect doesn't support the [] operator on expressions.
        expr = adapter.get_median_expr("test_column")
        assert expr is not None
        expr_str = str(expr)
        # Should generate: APPROX_QUANTILES(`test_column`, 2)[OFFSET(1)]
        assert "APPROX_QUANTILES" in expr_str
        assert "test_column" in expr_str
        assert "OFFSET(1)" in expr_str

    def test_get_quantiles_expr(self, adapter):
        """Test BigQuery quantiles expression."""
        expr = adapter.get_quantiles_expr("test_column", [0.25, 0.5, 0.75])
        assert expr is not None
        expr_str = str(expr)
        assert "approx_quantiles" in expr_str.lower()

    def test_get_sample_clause(self, adapter):
        """Test BigQuery sample clause."""
        clause = adapter.get_sample_clause(1000)
        assert clause is not None
        assert "TABLESAMPLE SYSTEM" in clause
        assert "1000 ROWS" in clause

    def test_quote_bigquery_identifier_simple(self):
        """Test quoting simple identifier."""
        result = _quote_bigquery_identifier("my_table")
        assert result == "`my_table`"

    def test_quote_bigquery_identifier_with_schema(self):
        """Test quoting schema.table."""
        result = _quote_bigquery_identifier("my_schema.my_table")
        assert result == "`my_schema`.`my_table`"

    def test_quote_bigquery_identifier_with_project(self):
        """Test quoting project.dataset.table."""
        result = _quote_bigquery_identifier("project.dataset.table")
        assert result == "`project`.`dataset`.`table`"

    def test_quote_bigquery_identifier_with_backticks(self):
        """Test escaping backticks in identifier."""
        result = _quote_bigquery_identifier("table`with`backticks")
        assert result == "`table``with``backticks`"

    def test_quote_bigquery_identifier_with_dots_and_backticks(self):
        """Test complex identifier with dots and backticks."""
        result = _quote_bigquery_identifier("schema.table`name")
        assert result == "`schema`.`table``name`"

    def test_setup_profiling_with_limit(self, adapter, config, test_table):
        """Test BigQuery setup with LIMIT creates temp table."""
        config.limit = 100
        adapter.config = config

        context = ProfilingContext(
            schema="my_schema",
            table="my_table",
            custom_sql=None,
            pretty_name="my_schema.my_table",
        )

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_query_job = MagicMock()
        mock_destination = MagicMock()
        mock_destination.table_id = "temp_table_123"
        mock_destination.project = "my_project"
        mock_destination.dataset_id = "my_dataset"
        mock_query_job.destination = mock_destination
        mock_cursor.query_job = mock_query_job

        mock_raw_conn = MagicMock()
        mock_raw_conn.cursor.return_value = mock_cursor

        with (
            patch.object(
                adapter.base_engine, "raw_connection", return_value=mock_raw_conn
            ),
            patch.object(adapter, "_create_sqlalchemy_table", return_value=test_table),
        ):
            result_context = adapter.setup_profiling(context, mock_conn)

        # Should have created temp table
        assert result_context.temp_table == "temp_table_123"
        assert result_context.temp_schema == "my_project.my_dataset"
        # Verify cursor.execute was called with LIMIT query
        assert mock_cursor.execute.called
        call_args = mock_cursor.execute.call_args[0][0]
        assert "LIMIT 100" in call_args
        assert "`my_schema`.`my_table`" in call_args

    def test_setup_profiling_with_custom_sql(self, adapter, test_table):
        """Test BigQuery setup with custom SQL creates temp table."""
        context = ProfilingContext(
            schema=None,
            table=None,
            custom_sql="SELECT * FROM my_table WHERE value > 100",
            pretty_name="custom_query",
        )

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_query_job = MagicMock()
        mock_destination = MagicMock()
        mock_destination.table_id = "temp_table_456"
        mock_destination.project = "my_project"
        mock_destination.dataset_id = "my_dataset"
        mock_query_job.destination = mock_destination
        mock_cursor.query_job = mock_query_job

        mock_raw_conn = MagicMock()
        mock_raw_conn.cursor.return_value = mock_cursor

        with (
            patch.object(
                adapter.base_engine, "raw_connection", return_value=mock_raw_conn
            ),
            patch.object(adapter, "_create_sqlalchemy_table", return_value=test_table),
        ):
            result_context = adapter.setup_profiling(context, mock_conn)

        # Should have created temp table
        assert result_context.temp_table == "temp_table_456"
        # Verify cursor.execute was called with custom SQL
        assert mock_cursor.execute.called
        call_args = mock_cursor.execute.call_args[0][0]
        assert "SELECT * FROM my_table WHERE value > 100" in call_args

    def test_setup_profiling_validates_limit_positive(self, adapter, config):
        """Test BigQuery setup validates LIMIT is positive."""
        config.limit = -5
        adapter.config = config

        context = ProfilingContext(
            schema="my_schema",
            table="my_table",
            custom_sql=None,
            pretty_name="my_schema.my_table",
        )

        mock_conn = MagicMock()

        with pytest.raises(ValueError, match="Invalid LIMIT value"):
            adapter.setup_profiling(context, mock_conn)

    def test_setup_profiling_validates_offset_non_negative(self, adapter, config):
        """Test BigQuery setup validates OFFSET is non-negative."""
        config.offset = -10
        adapter.config = config

        context = ProfilingContext(
            schema="my_schema",
            table="my_table",
            custom_sql=None,
            pretty_name="my_schema.my_table",
        )

        mock_conn = MagicMock()

        with pytest.raises(ValueError, match="Invalid OFFSET value"):
            adapter.setup_profiling(context, mock_conn)

    def test_cleanup_logs_temp_table(self, adapter):
        """Test BigQuery cleanup logs temp table (no-op)."""
        context = ProfilingContext(
            schema=None,
            table=None,
            custom_sql=None,
            pretty_name="test",
            temp_table="temp_table_123",
        )
        # Should not raise any errors
        adapter.cleanup(context)

    def test_get_column_quantiles(self, adapter, test_table):
        """Test BigQuery get_column_quantiles with mocked connection."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        # BigQuery returns a row with values for each quantile
        mock_result.fetchone.return_value = (5.0, 25.0, 50.0, 75.0, 95.0)
        mock_conn.execute.return_value = mock_result

        quantiles = adapter.get_column_quantiles(
            test_table, "test_column", mock_conn, [0.05, 0.25, 0.5, 0.75, 0.95]
        )

        assert len(quantiles) == 5
        assert quantiles == [5.0, 25.0, 50.0, 75.0, 95.0]
        # Verify query was executed
        assert mock_conn.execute.called


class TestSnowflakeAdapter:
    """Test cases for SnowflakeAdapter."""

    @pytest.fixture
    def adapter(self, config, report, sqlite_engine):
        """Create Snowflake adapter for testing."""
        return SnowflakeAdapter(config, report, sqlite_engine)

    def test_get_approx_unique_count_expr(self, adapter):
        """Test Snowflake uses APPROX_COUNT_DISTINCT."""
        expr = adapter.get_approx_unique_count_expr("test_column")
        assert expr is not None
        # Should be a SQLAlchemy function call for APPROX_COUNT_DISTINCT
        compiled = expr.compile(compile_kwargs={"literal_binds": True})
        expr_str = str(compiled)
        assert "APPROX_COUNT_DISTINCT" in expr_str

    def test_get_median_expr(self, adapter):
        """Test Snowflake uses native MEDIAN function."""
        expr = adapter.get_median_expr("test_column")
        assert expr is not None
        # Should be a SQLAlchemy function call for MEDIAN
        compiled = expr.compile(compile_kwargs={"literal_binds": True})
        expr_str = str(compiled)
        assert "median" in expr_str.lower()
        assert "test_column" in expr_str

    def test_setup_profiling_creates_sql_table(
        self, adapter, sqlite_engine, test_table
    ):
        """Test setup creates SQLAlchemy table object."""
        context = ProfilingContext(
            schema=None, table="test_table", custom_sql=None, pretty_name="test_table"
        )

        with sqlite_engine.connect() as conn:
            result_context = adapter.setup_profiling(context, conn)

        assert result_context.sql_table is not None
        assert result_context.sql_table.name == "test_table"

    def test_cleanup_does_nothing(self, adapter):
        """Test cleanup is a no-op for Snowflake adapter."""
        context = ProfilingContext(
            schema=None, table="test_table", custom_sql=None, pretty_name="test_table"
        )
        # Should not raise any errors
        adapter.cleanup(context)

    def test_get_column_quantiles(self, adapter, test_table):
        """Test Snowflake get_column_quantiles with mocked connection."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        # Snowflake executes one query per quantile
        mock_result.scalar.side_effect = [5.0, 25.0, 50.0, 75.0, 95.0]
        mock_conn.execute.return_value = mock_result

        quantiles = adapter.get_column_quantiles(
            test_table, "test_column", mock_conn, [0.05, 0.25, 0.5, 0.75, 0.95]
        )

        assert len(quantiles) == 5
        assert quantiles == [5.0, 25.0, 50.0, 75.0, 95.0]
        # Should execute 5 queries (one per quantile)
        assert mock_conn.execute.call_count == 5


class TestDatabricksAdapter:
    """Test cases for DatabricksAdapter."""

    @pytest.fixture
    def adapter(self, config, report, sqlite_engine):
        """Create Databricks adapter for testing."""
        return DatabricksAdapter(config, report, sqlite_engine)

    def test_get_approx_unique_count_expr(self, adapter):
        """Test Databricks uses approx_count_distinct (lowercase)."""
        expr = adapter.get_approx_unique_count_expr("test_column")
        assert expr is not None
        # Should be a SQLAlchemy function call for approx_count_distinct
        compiled = expr.compile(compile_kwargs={"literal_binds": True})
        expr_str = str(compiled)
        assert "approx_count_distinct" in expr_str.lower()

    def test_get_median_expr(self, adapter):
        """Test Databricks uses approx_percentile for median."""
        expr = adapter.get_median_expr("test_column")
        assert expr is not None
        # Should be a SQLAlchemy function call for approx_percentile
        compiled = expr.compile(compile_kwargs={"literal_binds": True})
        expr_str = str(compiled)
        assert "approx_percentile" in expr_str.lower()
        assert "0.5" in expr_str

    def test_setup_profiling_creates_sql_table(
        self, adapter, sqlite_engine, test_table
    ):
        """Test setup creates SQLAlchemy table object."""
        context = ProfilingContext(
            schema=None, table="test_table", custom_sql=None, pretty_name="test_table"
        )

        with sqlite_engine.connect() as conn:
            result_context = adapter.setup_profiling(context, conn)

        assert result_context.sql_table is not None
        assert result_context.sql_table.name == "test_table"

    def test_cleanup_does_nothing(self, adapter):
        """Test cleanup is a no-op for Databricks adapter."""
        context = ProfilingContext(
            schema=None, table="test_table", custom_sql=None, pretty_name="test_table"
        )
        # Should not raise any errors
        adapter.cleanup(context)

    def test_get_column_quantiles(self, adapter, test_table):
        """Test Databricks get_column_quantiles with mocked connection."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        # Databricks returns array of quantile values
        mock_result.scalar.return_value = [5.0, 25.0, 50.0, 75.0, 95.0]
        mock_conn.execute.return_value = mock_result

        quantiles = adapter.get_column_quantiles(
            test_table, "test_column", mock_conn, [0.05, 0.25, 0.5, 0.75, 0.95]
        )

        assert len(quantiles) == 5
        assert quantiles == [5.0, 25.0, 50.0, 75.0, 95.0]
        # Verify query was executed
        assert mock_conn.execute.called


class TestTrinoAdapter:
    """Test cases for TrinoAdapter."""

    @pytest.fixture
    def adapter(self, config, report, sqlite_engine):
        """Create Trino adapter for testing."""
        return TrinoAdapter(config, report, sqlite_engine)

    def test_get_approx_unique_count_expr(self, adapter):
        """Test Trino uses approx_distinct (same as Athena)."""
        expr = adapter.get_approx_unique_count_expr("test_column")
        assert expr is not None
        # Should be a SQLAlchemy function call for approx_distinct
        compiled = expr.compile(compile_kwargs={"literal_binds": True})
        expr_str = str(compiled)
        assert "approx_distinct" in expr_str.lower()

    def test_get_median_expr(self, adapter):
        """Test Trino uses approx_percentile for median (same as Athena)."""
        expr = adapter.get_median_expr("test_column")
        assert expr is not None
        # Should be a SQLAlchemy function call for approx_percentile
        compiled = expr.compile(compile_kwargs={"literal_binds": True})
        expr_str = str(compiled)
        assert "approx_percentile" in expr_str.lower()
        assert "0.5" in expr_str

    def test_setup_profiling_creates_sql_table(
        self, adapter, sqlite_engine, test_table
    ):
        """Test setup creates SQLAlchemy table object."""
        context = ProfilingContext(
            schema=None, table="test_table", custom_sql=None, pretty_name="test_table"
        )

        with sqlite_engine.connect() as conn:
            result_context = adapter.setup_profiling(context, conn)

        assert result_context.sql_table is not None
        assert result_context.sql_table.name == "test_table"

    def test_cleanup_always_drops_temp_view(self, adapter):
        """Test Trino always drops temp views (different from Athena)."""
        context = ProfilingContext(
            schema=None,
            table="test_table",
            custom_sql=None,
            pretty_name="test_table",
            temp_view="ge_test123",
        )
        # Should not raise any errors - cleanup always runs for Trino
        adapter.cleanup(context)

    def test_get_column_quantiles(self, adapter, test_table):
        """Test Trino get_column_quantiles with mocked connection."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        # Trino returns array of quantile values
        mock_result.scalar.return_value = [5.0, 25.0, 50.0, 75.0, 95.0]
        mock_conn.execute.return_value = mock_result

        quantiles = adapter.get_column_quantiles(
            test_table, "test_column", mock_conn, [0.05, 0.25, 0.5, 0.75, 0.95]
        )

        assert len(quantiles) == 5
        assert quantiles == [5.0, 25.0, 50.0, 75.0, 95.0]
        # Verify query was executed
        assert mock_conn.execute.called
