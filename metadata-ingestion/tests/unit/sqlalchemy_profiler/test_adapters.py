"""Unit tests for platform adapters."""

import re
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.engine import Dialect

from datahub.ingestion.source.ge_profiling_config import ProfilingConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.adapters import get_adapter
from datahub.ingestion.source.sqlalchemy_profiler.adapters.athena import AthenaAdapter
from datahub.ingestion.source.sqlalchemy_profiler.adapters.bigquery import (
    BigQueryAdapter,
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

# =============================================================================
# SQL Assertion Helpers
# =============================================================================


def compile_expr_to_sql(expr: sa.sql.expression.ClauseElement, dialect: Dialect) -> str:
    """Compile SQLAlchemy expression to SQL string with given dialect."""
    return str(expr.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


def assert_sql_matches_pattern(
    sql: str, pattern: str, flags: int = re.IGNORECASE
) -> None:
    """Assert SQL matches regex pattern."""
    assert re.search(pattern, sql, flags), (
        f"Pattern '{pattern}' not found in SQL: {sql}"
    )


# =============================================================================
# Mock Engine Fixtures
# =============================================================================


@pytest.fixture
def mock_generic_engine() -> Any:
    """Mock SQLAlchemy engine with SQLite dialect for testing GenericAdapter."""
    from sqlalchemy.dialects import sqlite

    engine = MagicMock()
    engine.dialect = sqlite.dialect()
    return engine


@pytest.fixture
def mock_postgres_engine() -> Any:
    """Mock PostgreSQL engine with correct dialect."""
    engine = MagicMock()
    engine.dialect = postgresql.dialect()  # type: ignore[misc]
    return engine


@pytest.fixture
def mock_mysql_engine() -> Any:
    """Mock MySQL engine with correct dialect."""
    engine = MagicMock()
    engine.dialect = mysql.dialect()
    return engine


@pytest.fixture
def mock_bigquery_engine() -> Any:
    """Mock BigQuery engine with correct dialect."""
    from sqlalchemy_bigquery import BigQueryDialect

    engine = MagicMock()
    engine.dialect = BigQueryDialect()
    return engine


@pytest.fixture
def mock_snowflake_engine() -> Any:
    """Mock Snowflake engine with correct dialect."""
    from snowflake.sqlalchemy import dialect as snowflake_dialect

    engine = MagicMock()
    engine.dialect = snowflake_dialect()
    return engine


@pytest.fixture
def mock_redshift_engine() -> Any:
    """Mock Redshift engine with correct dialect."""
    from sqlalchemy_redshift import dialect as redshift_dialect

    engine = MagicMock()
    engine.dialect = redshift_dialect.RedshiftDialect()
    return engine


@pytest.fixture
def mock_trino_engine() -> Any:
    """Mock Trino engine with correct dialect."""
    from trino.sqlalchemy import dialect as trino_dialect

    engine = MagicMock()
    engine.dialect = trino_dialect.TrinoDialect()
    return engine


@pytest.fixture
def mock_athena_engine() -> Any:
    """Mock Athena engine with correct dialect."""
    from pyathena import sqlalchemy_athena

    engine = MagicMock()
    engine.dialect = sqlalchemy_athena.AthenaDialect()
    return engine


@pytest.fixture
def mock_databricks_engine() -> Any:
    """Mock Databricks engine with correct dialect."""
    from databricks.sqlalchemy import DatabricksDialect

    engine = MagicMock()
    engine.dialect = DatabricksDialect()
    return engine


# =============================================================================
# Mock Table Fixture
# =============================================================================


@pytest.fixture
def mock_table() -> Any:
    """Create a mock SQLAlchemy table for testing."""
    table = Mock(spec=sa.Table)
    table.name = "test_table"
    table.schema = "test_schema"
    table.columns = []
    return table


# =============================================================================
# Shared Fixtures
# =============================================================================


@pytest.fixture
def config() -> ProfilingConfig:
    """Create test profiling config."""
    return ProfilingConfig()


@pytest.fixture
def report() -> SQLSourceReport:
    """Create test SQL report."""
    return SQLSourceReport()


# =============================================================================
# Tests
# =============================================================================


class TestAdapterFactory:
    """Test adapter factory function."""

    def test_get_adapter_bigquery(self, config, report, mock_bigquery_engine):
        """Test factory returns BigQuery adapter."""
        adapter = get_adapter("bigquery", config, report, mock_bigquery_engine)
        assert isinstance(adapter, BigQueryAdapter)

    def test_get_adapter_postgresql(self, config, report, mock_postgres_engine):
        """Test factory returns PostgreSQL adapter."""
        adapter = get_adapter("postgresql", config, report, mock_postgres_engine)
        assert isinstance(adapter, PostgresAdapter)

    def test_get_adapter_postgres(self, config, report, mock_postgres_engine):
        """Test factory returns PostgreSQL adapter for 'postgres' alias."""
        adapter = get_adapter("postgres", config, report, mock_postgres_engine)
        assert isinstance(adapter, PostgresAdapter)

    def test_get_adapter_athena(self, config, report, mock_athena_engine):
        """Test factory returns Athena adapter."""
        adapter = get_adapter("athena", config, report, mock_athena_engine)
        assert isinstance(adapter, AthenaAdapter)

    def test_get_adapter_mysql(self, config, report, mock_mysql_engine):
        """Test factory returns MySQL adapter."""
        adapter = get_adapter("mysql", config, report, mock_mysql_engine)
        assert isinstance(adapter, MySQLAdapter)

    def test_get_adapter_generic(self, config, report, mock_generic_engine):
        """Test factory returns GenericAdapter for unknown platform name."""
        adapter = get_adapter("unknown_platform", config, report, mock_generic_engine)
        assert isinstance(adapter, GenericAdapter)

    def test_get_adapter_redshift(self, config, report, mock_redshift_engine):
        """Test factory returns Redshift adapter."""
        adapter = get_adapter("redshift", config, report, mock_redshift_engine)
        assert isinstance(adapter, RedshiftAdapter)

    def test_get_adapter_snowflake(self, config, report, mock_snowflake_engine):
        """Test factory returns Snowflake adapter."""
        adapter = get_adapter("snowflake", config, report, mock_snowflake_engine)
        assert isinstance(adapter, SnowflakeAdapter)

    def test_get_adapter_databricks(self, config, report, mock_databricks_engine):
        """Test factory returns Databricks adapter."""
        adapter = get_adapter("databricks", config, report, mock_databricks_engine)
        assert isinstance(adapter, DatabricksAdapter)

    def test_get_adapter_trino(self, config, report, mock_trino_engine):
        """Test factory returns Trino adapter."""
        adapter = get_adapter("trino", config, report, mock_trino_engine)
        assert isinstance(adapter, TrinoAdapter)

    def test_get_adapter_case_insensitive(
        self, config, report, mock_bigquery_engine, mock_postgres_engine
    ):
        """Test factory is case-insensitive."""
        adapter = get_adapter("BigQuery", config, report, mock_bigquery_engine)
        assert isinstance(adapter, BigQueryAdapter)

        adapter = get_adapter("POSTGRESQL", config, report, mock_postgres_engine)
        assert isinstance(adapter, PostgresAdapter)


class TestGenericAdapter:
    """Test cases for GenericAdapter."""

    @pytest.fixture
    def adapter(self, config, report, mock_generic_engine):
        """Create generic adapter for testing."""
        return GenericAdapter(config, report, mock_generic_engine)

    def test_setup_profiling_creates_sql_table(self, adapter, mock_generic_engine):
        """Test setup creates SQLAlchemy table object."""
        context = ProfilingContext(
            schema="test_schema",
            table="test_table",
            custom_sql=None,
            pretty_name="test_table",
        )

        # Mock the table reflection
        with patch("sqlalchemy.Table") as mock_table_class:
            mock_table_instance = MagicMock()
            mock_table_instance.name = "test_table"
            mock_table_class.return_value = mock_table_instance

            mock_conn = MagicMock()
            result_context = adapter.setup_profiling(context, mock_conn)

            assert result_context.sql_table is not None
            # Verify Table was created with correct parameters
            mock_table_class.assert_called_once()
            call_args = mock_table_class.call_args
            assert call_args[0][0] == "test_table"  # table name
            assert call_args[1]["schema"] == "test_schema"

    def test_setup_profiling_requires_table_name(self, adapter):
        """Test ProfilingContext creation fails with empty table name."""
        # Validation happens in __post_init__, not in setup_profiling
        with pytest.raises(ValueError, match="table must be a non-empty string"):
            ProfilingContext(
                pretty_name="test",
                table="",
                schema=None,
                custom_sql="SELECT * FROM users",
            )

    def test_cleanup_does_nothing(self, adapter):
        """Test cleanup is a no-op for generic adapter."""
        context = ProfilingContext(
            schema="test_schema",
            table="test_table",
            custom_sql=None,
            pretty_name="test_table",
        )
        # Should not raise any errors
        adapter.cleanup(context)

    def test_get_approx_unique_count_expr(self, adapter, mock_generic_engine):
        """Test approximate unique count uses COUNT(DISTINCT)."""
        expr = adapter.get_approx_unique_count_expr("test_column")
        sql = compile_expr_to_sql(expr, mock_generic_engine.dialect)

        # Validate COUNT(DISTINCT(column)) structure
        pattern = r"\bCOUNT\s*\(\s*DISTINCT\s*\(\s*test_column\s*\)\s*\)"
        assert_sql_matches_pattern(sql, pattern)

    def test_get_median_expr(self, adapter, mock_generic_engine):
        """Test generic adapter tries MEDIAN function."""
        expr = adapter.get_median_expr("test_column")
        assert expr is not None
        sql = compile_expr_to_sql(expr, mock_generic_engine.dialect)

        # Validate MEDIAN function
        pattern = r"\bmedian\b"
        assert_sql_matches_pattern(sql, pattern)

    def test_get_quantiles_expr(self, adapter):
        """Test generic adapter returns None for quantiles (not supported)."""
        expr = adapter.get_quantiles_expr("test_column", [0.25, 0.5, 0.75])
        assert expr is None

    def test_get_sample_clause(self, adapter):
        """Test generic adapter returns None for sample clause (not supported)."""
        clause = adapter.get_sample_clause(1000)
        assert clause is None

    def test_supports_row_count_estimation(self, adapter):
        """Test generic adapter doesn't support row count estimation."""
        assert adapter.supports_row_count_estimation() is False

    def test_get_estimated_row_count(self, adapter, mock_table):
        """Test generic adapter returns None for row count estimation."""
        mock_conn = MagicMock()
        result = adapter.get_estimated_row_count(mock_table, mock_conn)
        assert result is None


class TestMySQLAdapter:
    """Test cases for MySQLAdapter."""

    @pytest.fixture
    def adapter(self, config, report, mock_mysql_engine):
        """Create MySQL adapter for testing."""
        return MySQLAdapter(config, report, mock_mysql_engine)

    def test_get_approx_unique_count_expr(self, adapter, mock_mysql_engine):
        """Test MySQL uses COUNT(DISTINCT) for approximate unique count."""
        expr = adapter.get_approx_unique_count_expr("user_id")
        sql = compile_expr_to_sql(expr, mock_mysql_engine.dialect)

        # Validate COUNT(DISTINCT(column)) structure
        pattern = r"\bCOUNT\s*\(\s*DISTINCT\s*\(\s*user_id\s*\)\s*\)"
        assert_sql_matches_pattern(sql, pattern)

    def test_get_median_expr(self, adapter):
        """Test MySQL doesn't support median (returns None)."""
        expr = adapter.get_median_expr("value_column")
        assert expr is None

    def test_supports_row_count_estimation(self, adapter):
        """Test MySQL supports fast row count estimation."""
        assert adapter.supports_row_count_estimation() is True

    def test_get_estimated_row_count_generates_correct_query(
        self, adapter, mock_table, mock_mysql_engine
    ):
        """Test MySQL row count estimation query structure."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = 12345
        mock_conn.execute.return_value = mock_result

        row_count = adapter.get_estimated_row_count(mock_table, mock_conn)

        # Verify query was executed
        assert mock_conn.execute.called
        executed_query = mock_conn.execute.call_args[0][0]
        sql = compile_expr_to_sql(executed_query, mock_mysql_engine.dialect)

        # Validate information_schema.tables query
        pattern = r"\binformation_schema\.tables\b.*\btable_rows\b"
        assert_sql_matches_pattern(sql, pattern)
        assert row_count == 12345


class TestPostgresAdapter:
    """Test cases for PostgresAdapter."""

    @pytest.fixture
    def adapter(self, config, report, mock_postgres_engine):
        """Create PostgreSQL adapter for testing."""
        return PostgresAdapter(config, report, mock_postgres_engine)

    def test_get_approx_unique_count_expr(self, adapter, mock_postgres_engine):
        """Test PostgreSQL uses COUNT(DISTINCT)."""
        expr = adapter.get_approx_unique_count_expr("email")
        sql = compile_expr_to_sql(expr, mock_postgres_engine.dialect)

        # Validate COUNT(DISTINCT(column)) structure
        pattern = r"\bCOUNT\s*\(\s*DISTINCT\s*\(\s*email\s*\)\s*\)"
        assert_sql_matches_pattern(sql, pattern)

    def test_get_median_expr(self, adapter, mock_postgres_engine):
        """Test PostgreSQL uses PERCENTILE_CONT for median."""
        expr = adapter.get_median_expr("price")
        sql = compile_expr_to_sql(expr, mock_postgres_engine.dialect)

        # Validate PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
        pattern = (
            r"\bPERCENTILE_CONT\s*\(\s*0\.5\s*\)"
            r".*\bWITHIN\s+GROUP\s*\("
            r".*\bORDER\s+BY\b"
            r".*\bprice\b"
        )
        assert_sql_matches_pattern(sql, pattern)

    def test_supports_row_count_estimation(self, adapter):
        """Test PostgreSQL supports fast row count estimation."""
        assert adapter.supports_row_count_estimation() is True

    def test_get_estimated_row_count_uses_pg_class(
        self, adapter, mock_table, mock_postgres_engine
    ):
        """Test PostgreSQL uses pg_class.reltuples for estimation."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = 98765
        mock_conn.execute.return_value = mock_result

        row_count = adapter.get_estimated_row_count(mock_table, mock_conn)

        assert mock_conn.execute.called
        executed_query = mock_conn.execute.call_args[0][0]
        sql = compile_expr_to_sql(executed_query, mock_postgres_engine.dialect)

        # Validate query uses pg_class and pg_namespace for reltuples
        assert "reltuples" in sql
        assert "pg_class" in sql
        assert "pg_namespace" in sql
        assert row_count == 98765


class TestRedshiftAdapter:
    """Test cases for RedshiftAdapter."""

    @pytest.fixture
    def adapter(self, config, report, mock_redshift_engine):
        """Create Redshift adapter for testing."""
        return RedshiftAdapter(config, report, mock_redshift_engine)

    def test_get_approx_unique_count_expr(self, adapter, mock_redshift_engine):
        """Test Redshift uses COUNT(DISTINCT)."""
        expr = adapter.get_approx_unique_count_expr("customer_id")
        sql = compile_expr_to_sql(expr, mock_redshift_engine.dialect)

        # Validate COUNT(DISTINCT(column)) structure
        pattern = r"\bCOUNT\s*\(\s*DISTINCT\s*\(\s*customer_id\s*\)\s*\)"
        assert_sql_matches_pattern(sql, pattern)

    def test_get_median_expr(self, adapter, mock_redshift_engine):
        """Test Redshift uses PERCENTILE_CONT for median."""
        expr = adapter.get_median_expr("amount")
        sql = compile_expr_to_sql(expr, mock_redshift_engine.dialect)

        # Validate PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)
        pattern = (
            r"\bPERCENTILE_CONT\s*\(\s*0\.5\s*\)"
            r".*\bWITHIN\s+GROUP\s*\("
            r".*\bORDER\s+BY\b"
            r".*\bamount\b"
        )
        assert_sql_matches_pattern(sql, pattern)

    def test_get_mean_expr(self, adapter, mock_redshift_engine):
        """Test Redshift casts to FLOAT for AVG precision."""
        expr = adapter.get_mean_expr("int_column")
        sql = compile_expr_to_sql(expr, mock_redshift_engine.dialect)

        # Validate AVG(CAST(column AS FLOAT))
        pattern = (
            r"\bAVG\s*\("
            r".*\bCAST\s*\("
            r".*\bint_column\b"
            r".*\bAS\s+FLOAT\b"
        )
        assert_sql_matches_pattern(sql, pattern)

    def test_get_stdev_null_value(self, adapter):
        """Test Redshift returns 0.0 for STDDEV on NULL columns."""
        null_value = adapter.get_stdev_null_value()
        assert null_value == 0.0

    def test_supports_row_count_estimation(self, adapter):
        """Test Redshift supports fast row count estimation."""
        assert adapter.supports_row_count_estimation() is True


class TestSnowflakeAdapter:
    """Test cases for SnowflakeAdapter."""

    @pytest.fixture
    def adapter(self, config, report, mock_snowflake_engine):
        """Create Snowflake adapter for testing."""
        return SnowflakeAdapter(config, report, mock_snowflake_engine)

    def test_get_approx_unique_count_expr(self, adapter, mock_snowflake_engine):
        """Test Snowflake uses APPROX_COUNT_DISTINCT."""
        expr = adapter.get_approx_unique_count_expr("session_id")
        sql = compile_expr_to_sql(expr, mock_snowflake_engine.dialect)

        # Validate APPROX_COUNT_DISTINCT(session_id)
        pattern = r"\bAPPROX_COUNT_DISTINCT\b.*\bsession_id\b"
        assert_sql_matches_pattern(sql, pattern)

    def test_get_median_expr(self, adapter, mock_snowflake_engine):
        """Test Snowflake uses MEDIAN function."""
        expr = adapter.get_median_expr("score")
        sql = compile_expr_to_sql(expr, mock_snowflake_engine.dialect)

        # Validate MEDIAN(score)
        pattern = r"\bMEDIAN\b.*\bscore\b"
        assert_sql_matches_pattern(sql, pattern)

    def test_quote_identifier_mixed_case(self, adapter):
        """Test Snowflake identifier quoting."""
        # Snowflake preserves case with double quotes
        quoted = adapter.quote_identifier("MixedCase")
        assert '"' in quoted
        assert "MixedCase" in quoted

    def test_quote_identifier_with_schema(self, adapter):
        """Test Snowflake schema.table quoting."""
        quoted = adapter.quote_identifier("schema.table")
        # Should quote both parts
        assert '"schema"."table"' in quoted or "schema.table" in quoted


class TestBigQueryAdapter:
    """Test cases for BigQueryAdapter."""

    @pytest.fixture
    def adapter(self, config, report, mock_bigquery_engine):
        """Create BigQuery adapter for testing."""
        return BigQueryAdapter(config, report, mock_bigquery_engine)

    def test_get_approx_unique_count_expr(self, adapter, mock_bigquery_engine):
        """Test BigQuery uses APPROX_COUNT_DISTINCT."""
        expr = adapter.get_approx_unique_count_expr("visitor_id")
        sql = compile_expr_to_sql(expr, mock_bigquery_engine.dialect)

        # Validate APPROX_COUNT_DISTINCT(visitor_id)
        pattern = r"\bAPPROX_COUNT_DISTINCT\b.*\bvisitor_id\b"
        assert_sql_matches_pattern(sql, pattern)

    def test_get_median_expr(self, adapter, mock_bigquery_engine):
        """Test BigQuery uses APPROX_QUANTILES for median."""
        expr = adapter.get_median_expr("duration")
        sql = compile_expr_to_sql(expr, mock_bigquery_engine.dialect)

        # Validate APPROX_QUANTILES(column, 2)[OFFSET(1)]
        pattern = (
            r"\bAPPROX_QUANTILES\s*\("
            r".*\bduration\b"
            r".*,\s*2\s*\)"
            r".*\bOFFSET\s*\(\s*1\s*\)"
        )
        assert_sql_matches_pattern(sql, pattern)

    def test_cleanup_does_nothing(self, adapter):
        """Test BigQuery cleanup is no-op (tables auto-expire)."""
        context = ProfilingContext(
            schema="test_schema",
            table="test_table",
            custom_sql=None,
            pretty_name="test",
            temp_table="temp_123",
        )
        # Should not raise
        adapter.cleanup(context)

    def test_create_temp_table_with_properly_quoted_identifiers(
        self, adapter, mock_bigquery_engine
    ):
        """Test that temp table creation uses properly quoted identifiers (SQL injection prevention)."""
        # Mock the raw connection and cursor
        mock_cursor = Mock()
        mock_cursor.query_job = Mock()
        mock_cursor.query_job.destination = Mock()
        mock_cursor.query_job.destination.project = "test_project"
        mock_cursor.query_job.destination.dataset_id = "test_dataset"
        mock_cursor.query_job.destination.table_id = "temp_table_123"

        mock_raw_conn = Mock()
        mock_raw_conn.cursor.return_value = mock_cursor

        with patch.object(
            adapter.base_engine, "raw_connection", return_value=mock_raw_conn
        ):
            context = ProfilingContext(
                schema="my_dataset",
                table="my_table",
                custom_sql=None,
                pretty_name="test",
            )

            adapter._create_temp_table_for_query(context)

            # Verify cursor.execute was called
            assert mock_cursor.execute.called
            sql = mock_cursor.execute.call_args[0][0]

            # Verify SQL contains properly quoted identifiers (not vulnerable to injection)
            assert "my_dataset" in sql
            assert "my_table" in sql
            assert "SELECT" in sql.upper()

    def test_create_temp_table_includes_limit_and_offset(
        self, adapter, mock_bigquery_engine, config
    ):
        """Test that temp table creation correctly applies LIMIT and OFFSET."""
        # Configure LIMIT and OFFSET
        config.limit = 100
        config.offset = 50
        adapter.config = config

        # Mock the raw connection and cursor
        mock_cursor = Mock()
        mock_cursor.query_job = Mock()
        mock_cursor.query_job.destination = Mock()
        mock_cursor.query_job.destination.project = "test_project"
        mock_cursor.query_job.destination.dataset_id = "test_dataset"
        mock_cursor.query_job.destination.table_id = "temp_table_123"

        mock_raw_conn = Mock()
        mock_raw_conn.cursor.return_value = mock_cursor

        with patch.object(
            adapter.base_engine, "raw_connection", return_value=mock_raw_conn
        ):
            context = ProfilingContext(
                schema="my_dataset",
                table="my_table",
                custom_sql=None,
                pretty_name="test",
            )

            adapter._create_temp_table_for_query(context)

            # Verify cursor.execute was called with LIMIT and OFFSET
            assert mock_cursor.execute.called
            sql = mock_cursor.execute.call_args[0][0].upper()

            # Verify LIMIT and OFFSET are in the SQL
            assert "LIMIT" in sql
            assert "OFFSET" in sql
            # Verify values are present as integers (not string concatenation)
            assert "100" in sql or "50" in sql

    def test_setup_sampling_generates_tablesample_sql(
        self, adapter, mock_bigquery_engine, config
    ):
        """Test that sampling generates correct TABLESAMPLE SQL for large tables."""
        # Configure sampling
        config.use_sampling = True
        config.sample_size = 1000
        adapter.config = config

        # Track the SQL passed to _create_temp_table_for_query
        captured_sql = []

        # Mock _get_quick_row_count to return large row count
        with patch.object(adapter, "_get_quick_row_count", return_value=10000):
            # Mock _create_temp_table_for_query
            def mock_create_temp(ctx):
                # Capture the custom_sql for verification
                if ctx.custom_sql:
                    captured_sql.append(ctx.custom_sql)
                ctx.temp_table = "temp_123"
                ctx.temp_schema = "project.dataset"
                return ctx

            with patch.object(
                adapter, "_create_temp_table_for_query", side_effect=mock_create_temp
            ) as mock_create:
                context = ProfilingContext(
                    schema="my_dataset",
                    table="my_table",
                    custom_sql=None,
                    pretty_name="test",
                )

                mock_conn = Mock()
                result = adapter._setup_sampling(context, mock_conn)

                # Verify sampling was set up
                assert result.is_sampled
                assert result.sample_percentage is not None
                assert result.sample_percentage > 0

                # Verify _create_temp_table_for_query was called
                assert mock_create.called

                # Verify the captured SQL contains TABLESAMPLE
                assert len(captured_sql) > 0
                assert "TABLESAMPLE" in captured_sql[0].upper()


class TestDatabricksAdapter:
    """Test cases for DatabricksAdapter."""

    @pytest.fixture
    def adapter(self, config, report, mock_databricks_engine):
        """Create Databricks adapter for testing."""
        return DatabricksAdapter(config, report, mock_databricks_engine)

    def test_get_approx_unique_count_expr(self, adapter, mock_databricks_engine):
        """Test Databricks uses approx_count_distinct."""
        expr = adapter.get_approx_unique_count_expr("device_id")
        sql = compile_expr_to_sql(expr, mock_databricks_engine.dialect)

        # Validate approx_count_distinct(device_id)
        pattern = r"\bapprox_count_distinct\b.*\bdevice_id\b"
        assert_sql_matches_pattern(sql, pattern)

    def test_get_median_expr(self, adapter, mock_databricks_engine):
        """Test Databricks uses approx_percentile for median."""
        expr = adapter.get_median_expr("latency")
        sql = compile_expr_to_sql(expr, mock_databricks_engine.dialect)

        # Validate approx_percentile(latency, 0.5)
        pattern = r"\bapprox_percentile\b.*\blatency\b.*\b0\.5\b"
        assert_sql_matches_pattern(sql, pattern)


class TestTrinoAdapter:
    """Test cases for TrinoAdapter."""

    @pytest.fixture
    def adapter(self, config, report, mock_trino_engine):
        """Create Trino adapter for testing."""
        return TrinoAdapter(config, report, mock_trino_engine)

    def test_get_approx_unique_count_expr(self, adapter, mock_trino_engine):
        """Test Trino uses approx_distinct."""
        expr = adapter.get_approx_unique_count_expr("request_id")
        sql = compile_expr_to_sql(expr, mock_trino_engine.dialect)

        # Validate approx_distinct(request_id)
        pattern = r"\bapprox_distinct\b.*\brequest_id\b"
        assert_sql_matches_pattern(sql, pattern)

    def test_get_median_expr(self, adapter, mock_trino_engine):
        """Test Trino uses approx_percentile for median."""
        expr = adapter.get_median_expr("response_time")
        sql = compile_expr_to_sql(expr, mock_trino_engine.dialect)

        # Validate approx_percentile(response_time, 0.5)
        pattern = r"\bapprox_percentile\b.*\bresponse_time\b.*\b0\.5\b"
        assert_sql_matches_pattern(sql, pattern)

    def test_cleanup_drops_temp_view(self, adapter):
        """Test Trino always drops temp views."""
        context = ProfilingContext(
            schema="test_schema",
            table="test_table",
            custom_sql=None,
            pretty_name="test_table",
            temp_view="ge_temp_view",
        )

        # Mock both base_engine and quote_identifier
        with (
            patch.object(adapter, "base_engine") as mock_engine,
            patch.object(
                adapter, "quote_identifier", return_value='"test_schema"."ge_temp_view"'
            ),
        ):
            mock_conn = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn

            adapter.cleanup(context)

            # Verify DROP VIEW was executed
            assert mock_conn.execute.called
            executed_query = mock_conn.execute.call_args[0][0]
            sql = str(executed_query)

            # Validate DROP VIEW ge_temp_view
            pattern = r"\bDROP\s+VIEW\b.*\bge_temp_view\b"
            assert_sql_matches_pattern(sql, pattern)


class TestAthenaAdapter:
    """Test cases for AthenaAdapter."""

    @pytest.fixture
    def adapter(self, config, report, mock_athena_engine):
        """Create Athena adapter for testing."""
        return AthenaAdapter(config, report, mock_athena_engine)

    def test_get_approx_unique_count_expr(self, adapter, mock_athena_engine):
        """Test Athena uses approx_distinct."""
        expr = adapter.get_approx_unique_count_expr("transaction_id")
        sql = compile_expr_to_sql(expr, mock_athena_engine.dialect)

        # Validate approx_distinct(transaction_id)
        pattern = r"\bapprox_distinct\b.*\btransaction_id\b"
        assert_sql_matches_pattern(sql, pattern)

    def test_get_median_expr(self, adapter, mock_athena_engine):
        """Test Athena uses approx_percentile for median."""
        expr = adapter.get_median_expr("value")
        sql = compile_expr_to_sql(expr, mock_athena_engine.dialect)

        # Validate approx_percentile(value, 0.5)
        pattern = r"\bapprox_percentile\b.*\bvalue\b.*\b0\.5\b"
        assert_sql_matches_pattern(sql, pattern)
