"""Integration tests for Postgres query-based lineage extraction.

Tests pg_stat_statements integration, SQL injection prevention, and lineage generation.
"""

import subprocess
import time

import pytest
import sqlalchemy as sa
from sqlalchemy import text

from datahub.ingestion.source.sql.postgres.lineage import (
    PostgresLineageExtractor,
    PostgresQueryEntry,
)
from datahub.ingestion.source.sql.postgres.query import PostgresQuery
from datahub.ingestion.source.sql.postgres.source import PostgresConfig
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from tests.test_helpers.docker_helpers import wait_for_port

POSTGRES_PORT = 5432


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/postgres"


def is_postgres_up(container_name: str) -> bool:
    """Check if postgres is responsive on a container."""
    cmd = f"docker logs {container_name} 2>&1 | grep 'PostgreSQL init process complete; ready for start up.'"
    ret = subprocess.run(cmd, shell=True)
    return ret.returncode == 0


@pytest.fixture(scope="module")
def postgres_runner(docker_compose_runner, test_resources_dir):
    """Start Postgres container with pg_stat_statements enabled."""
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "postgres"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testpostgres",
            POSTGRES_PORT,
            timeout=120,
            checker=lambda: is_postgres_up("testpostgres"),
        )
        yield docker_services


@pytest.fixture(scope="module")
def postgres_connection(postgres_runner):
    """Create SQLAlchemy connection to test Postgres instance."""
    engine = sa.create_engine(
        "postgresql://postgres:example@localhost:5432/postgres",
        isolation_level="AUTOCOMMIT",
    )

    with engine.connect() as conn:
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_stat_statements"))
            conn.execute(text("SELECT pg_stat_statements_reset()"))
        except Exception as e:
            pytest.skip(f"Failed to enable pg_stat_statements: {e}")

        conn.execute(text("DROP SCHEMA IF EXISTS lineage_test CASCADE"))
        conn.execute(text("CREATE SCHEMA lineage_test"))

        conn.execute(
            text(
                """
            CREATE TABLE lineage_test.source_orders (
                order_id INT PRIMARY KEY,
                customer_id INT,
                order_date DATE,
                total_amount DECIMAL(10,2)
            )
        """
            )
        )

        conn.execute(
            text(
                """
            CREATE TABLE lineage_test.source_customers (
                customer_id INT PRIMARY KEY,
                customer_name VARCHAR(100),
                country VARCHAR(50)
            )
        """
            )
        )

        conn.execute(
            text(
                """
            INSERT INTO lineage_test.source_orders VALUES
            (1, 101, '2024-01-01', 150.00),
            (2, 102, '2024-01-02', 200.00),
            (3, 101, '2024-01-03', 75.50)
        """
            )
        )

        conn.execute(
            text(
                """
            INSERT INTO lineage_test.source_customers VALUES
            (101, 'Alice Smith', 'USA'),
            (102, 'Bob Johnson', 'Canada')
        """
            )
        )

        yield conn

        conn.execute(text("DROP SCHEMA IF EXISTS lineage_test CASCADE"))


@pytest.mark.integration
class TestPostgresLineageIntegration:
    """Integration tests for Postgres lineage extraction."""

    def test_pg_stat_statements_enabled(self, postgres_connection):
        """Test that pg_stat_statements extension is properly enabled."""
        result = postgres_connection.execute(
            text(PostgresQuery.check_pg_stat_statements_enabled())
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] is True, "pg_stat_statements extension should be enabled"

    def test_query_history_extraction_basic(self, postgres_connection):
        """Test basic query history extraction from pg_stat_statements."""
        postgres_connection.execute(
            text("SELECT * FROM lineage_test.source_orders WHERE order_id = 1")
        )
        postgres_connection.execute(
            text("SELECT * FROM lineage_test.source_customers WHERE customer_id = 101")
        )

        time.sleep(0.5)

        query, params = PostgresQuery.get_query_history(limit=100, min_calls=1)
        result = postgres_connection.execute(text(query), params)

        queries = list(result)
        assert len(queries) > 0, "Should extract at least some queries"

        for row in queries:
            assert "query_id" in row
            assert "query_text" in row
            assert "execution_count" in row
            assert row["execution_count"] >= 1

    def test_lineage_from_insert_select(self, postgres_connection):
        """Test lineage extraction from INSERT...SELECT queries."""
        postgres_connection.execute(text("SELECT pg_stat_statements_reset()"))

        postgres_connection.execute(
            text(
                """
            CREATE TABLE lineage_test.target_customer_orders AS
            SELECT
                c.customer_id,
                c.customer_name,
                COUNT(o.order_id) as order_count,
                SUM(o.total_amount) as total_spent
            FROM lineage_test.source_customers c
            JOIN lineage_test.source_orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.customer_name
        """
            )
        )

        time.sleep(0.5)

        query, params = PostgresQuery.get_query_history(limit=100, min_calls=1)
        result = postgres_connection.execute(text(query), params)

        queries = list(result)

        ctas_queries = [
            q
            for q in queries
            if "target_customer_orders" in q["query_text"]
            and "CREATE TABLE" in q["query_text"].upper()
        ]

        assert len(ctas_queries) > 0, "Should find CREATE TABLE AS query"

        ctas_query_text = ctas_queries[0]["query_text"]
        assert "source_customers" in ctas_query_text
        assert "source_orders" in ctas_query_text

    def test_query_exclude_patterns(self, postgres_connection):
        """Test that exclude patterns filter out system queries."""
        postgres_connection.execute(text("SELECT * FROM lineage_test.source_orders"))
        postgres_connection.execute(text("SELECT version()"))
        postgres_connection.execute(text("SHOW server_version"))

        time.sleep(0.5)

        query, params = PostgresQuery.get_query_history(limit=100, min_calls=1)
        result = postgres_connection.execute(text(query), params)

        queries = [q["query_text"] for q in result]

        show_queries = [q for q in queries if q.upper().startswith("SHOW")]
        assert len(show_queries) == 0, (
            "SHOW queries should be filtered by default exclusions"
        )

    def test_sql_injection_prevention_database_filter(self, postgres_connection):
        """Test that SQL injection in database parameter is prevented."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            PostgresQuery.get_query_history(database="postgres'; DROP TABLE users; --")

    def test_sql_injection_prevention_exclude_patterns(self, postgres_connection):
        """Test that SQL injection in exclude patterns is safely parameterized."""
        malicious_pattern = "'; DROP TABLE lineage_test.source_orders; --"

        query, params = PostgresQuery.get_query_history(
            exclude_patterns=[malicious_pattern]
        )

        result = postgres_connection.execute(text(query), params)
        list(result)
        check_result = postgres_connection.execute(
            text(
                """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'lineage_test'
                AND table_name = 'source_orders'
            )
        """
            )
        )
        assert check_result.fetchone()[0] is True, "Table should still exist"

    def test_lineage_extractor_prerequisites(self, postgres_connection):
        """Test that PostgresLineageExtractor can check prerequisites."""
        config = PostgresConfig(
            username="postgres",
            password="example",
            host_port="localhost:5432",
            database="postgres",
        )

        report = type("Report", (), {"report_warning": lambda *args, **kwargs: None})()
        aggregator = SqlParsingAggregator(
            platform="postgres", generate_lineage=True, generate_queries=True
        )

        extractor = PostgresLineageExtractor(
            config=config,
            connection=postgres_connection,
            report=report,  # type: ignore[arg-type]
            sql_aggregator=aggregator,
        )

        is_ready, message = extractor.check_prerequisites()

        assert is_ready is True, f"Prerequisites should be met: {message}"
        assert "Prerequisites met" in message

    def test_query_entry_dataclass(self):
        """Test PostgresQueryEntry dataclass functionality."""
        entry = PostgresQueryEntry(
            query_id="12345",
            query_text="SELECT * FROM test_table",
            execution_count=50,
            total_exec_time_ms=5000.0,
            user_name="testuser",
            database_name="testdb",
        )

        assert entry.query_id == "12345"
        assert entry.avg_exec_time_ms == 100.0  # 5000/50

        entry_zero = PostgresQueryEntry(
            query_id="67890",
            query_text="SELECT 1",
            execution_count=0,
            total_exec_time_ms=0.0,
            user_name="testuser",
            database_name="testdb",
        )
        assert entry_zero.avg_exec_time_ms == 0.0

    def test_query_history_with_min_calls_filter(self, postgres_connection):
        """Test that min_calls filter works correctly."""
        postgres_connection.execute(text("SELECT pg_stat_statements_reset()"))

        for _ in range(5):
            postgres_connection.execute(
                text("SELECT * FROM lineage_test.source_orders")
            )

        postgres_connection.execute(text("SELECT * FROM lineage_test.source_customers"))

        time.sleep(0.5)

        query, params = PostgresQuery.get_query_history(limit=100, min_calls=3)
        result = postgres_connection.execute(text(query), params)

        queries = list(result)

        for q in queries:
            assert q["execution_count"] >= 3

    def test_queries_by_type_filter(self, postgres_connection):
        """Test filtering queries by SQL command type."""
        postgres_connection.execute(text("SELECT pg_stat_statements_reset()"))

        postgres_connection.execute(
            text("SELECT * FROM lineage_test.source_orders WHERE order_id = 1")
        )
        postgres_connection.execute(
            text(
                "INSERT INTO lineage_test.source_orders VALUES (999, 999, '2024-01-01', 99.99)"
            )
        )
        postgres_connection.execute(
            text("DELETE FROM lineage_test.source_orders WHERE order_id = 999")
        )

        time.sleep(0.5)

        query, params = PostgresQuery.get_queries_by_type(
            query_type="INSERT", limit=100
        )
        result = postgres_connection.execute(text(query), params)

        queries = list(result)

        for q in queries:
            assert q["query_text"].upper().startswith("INSERT")

    def test_extension_not_installed_scenario(self, postgres_connection):
        """Test behavior when pg_stat_statements extension is not installed."""
        config = PostgresConfig(
            username="postgres",
            password="example",
            host_port="localhost:5432",
            database="postgres",
        )

        class MockReport:
            def __init__(self):
                self.failures = []

            def report_failure(self, message, context=None, **kwargs):  # type: ignore[no-untyped-def]
                self.failures.append({"message": message, "context": context})

        report = MockReport()
        aggregator = SqlParsingAggregator(
            platform="postgres", generate_lineage=True, generate_queries=True
        )

        from unittest.mock import MagicMock

        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = [False]  # Extension not enabled
        mock_conn.execute.return_value = mock_result

        extractor = PostgresLineageExtractor(
            config=config,
            connection=mock_conn,
            report=report,  # type: ignore[arg-type]
            sql_aggregator=aggregator,
        )

        is_ready, message = extractor.check_prerequisites()

        assert is_ready is False
        assert "not installed" in message
        assert "CREATE EXTENSION pg_stat_statements" in message

    def test_permission_denied_scenario(self, postgres_connection):
        """Test behavior when user lacks pg_read_all_stats permission."""
        config = PostgresConfig(
            username="postgres",
            password="example",
            host_port="localhost:5432",
            database="postgres",
        )

        class MockReport:
            def __init__(self):
                self.failures = []

            def report_failure(self, message, context=None, **kwargs):  # type: ignore[no-untyped-def]
                self.failures.append({"message": message, "context": context})

        report = MockReport()
        aggregator = SqlParsingAggregator(
            platform="postgres", generate_lineage=True, generate_queries=True
        )

        from unittest.mock import MagicMock

        mock_conn = MagicMock()

        mock_extension_result = MagicMock()
        mock_extension_result.fetchone.return_value = [True]

        mock_permission_result = MagicMock()
        mock_permission_result.fetchone.return_value = [False, False]

        mock_conn.execute.side_effect = [mock_extension_result, mock_permission_result]

        extractor = PostgresLineageExtractor(
            config=config,
            connection=mock_conn,
            report=report,  # type: ignore[arg-type]
            sql_aggregator=aggregator,
        )

        is_ready, message = extractor.check_prerequisites()

        assert is_ready is False
        assert "Insufficient permissions" in message
        assert "pg_read_all_stats" in message

    def test_connection_loss_during_extraction(self, postgres_connection):
        """Test graceful handling of connection loss during query extraction."""
        config = PostgresConfig(
            username="postgres",
            password="example",
            host_port="localhost:5432",
            database="postgres",
            include_query_lineage=True,
        )

        class MockReport:
            def __init__(self):
                self.failures = []
                self.num_queries_extracted = 0

            def report_failure(self, message, context=None, **kwargs):  # type: ignore[no-untyped-def]
                self.failures.append({"message": message, "context": context})

        report = MockReport()
        aggregator = SqlParsingAggregator(
            platform="postgres", generate_lineage=True, generate_queries=True
        )

        from unittest.mock import MagicMock

        from sqlalchemy.exc import OperationalError

        mock_conn = MagicMock()

        mock_extension_result = MagicMock()
        mock_extension_result.fetchone.return_value = [True]
        mock_permission_result = MagicMock()
        mock_permission_result.fetchone.return_value = [True, False]

        mock_conn.execute.side_effect = [
            mock_extension_result,
            mock_permission_result,
            OperationalError("connection lost", None, None),
        ]

        extractor = PostgresLineageExtractor(
            config=config,
            connection=mock_conn,
            report=report,  # type: ignore[arg-type]
            sql_aggregator=aggregator,
        )

        queries = extractor.extract_query_history()

        assert len(queries) == 0
        assert len(report.failures) > 0
        assert any(
            "query_history_extraction_failed" in f["key"] for f in report.failures
        )

    def test_malformed_query_text_handling(self, postgres_connection):
        """Test handling of queries with unusual or malformed text."""
        malformed_queries = [
            "SELECT * FROM table WHERE name = 'O''Brien'",  # Escaped quotes
            "SELECT * FROM users WHERE comment LIKE '%—unicode—%'",  # Unicode dash
            "SELECT 'DROP TABLE users; --' as fake_injection",  # SQL in string literal
        ]

        for malformed_query in malformed_queries:
            try:
                postgres_connection.execute(text(malformed_query))
            except Exception:
                # Some queries may fail due to missing tables, that's OK
                pass

        time.sleep(0.5)

        query, params = PostgresQuery.get_query_history(limit=100, min_calls=1)
        result = postgres_connection.execute(text(query), params)

        queries = list(result)

        for q in queries:
            assert "query_text" in q
            assert isinstance(q["query_text"], str)
            assert "query_id" in q
            assert "execution_count" in q

    def test_usage_statistics_flag_enabled(self, postgres_connection):
        """Test that include_usage_statistics flag generates usage statistics."""
        postgres_connection.execute(text("SELECT pg_stat_statements_reset()"))

        for _ in range(10):
            postgres_connection.execute(
                text("SELECT * FROM lineage_test.source_orders")
            )

        for _ in range(5):
            postgres_connection.execute(
                text("SELECT * FROM lineage_test.source_customers")
            )

        time.sleep(0.5)

        config = PostgresConfig(
            username="postgres",
            password="example",
            host_port="localhost:5432",
            database="postgres",
            include_query_lineage=True,
            include_usage_statistics=True,
        )

        class MockReport:
            def __init__(self):
                self.failures = []
                self.num_queries_extracted = 0
                self.num_queries_parsed = 0
                self.num_queries_parse_failures = 0

            def report_failure(self, message, context=None, **kwargs):  # type: ignore[no-untyped-def]
                self.failures.append({"message": message, "context": context})

        report = MockReport()

        aggregator = SqlParsingAggregator(
            platform="postgres",
            generate_lineage=True,
            generate_queries=True,
            generate_usage_statistics=True,
        )

        extractor = PostgresLineageExtractor(
            config=config,
            connection=postgres_connection,
            report=report,  # type: ignore[arg-type]
            sql_aggregator=aggregator,
        )

        queries = extractor.extract_query_history()
        assert len(queries) > 0, "Should extract queries"

        extractor.populate_lineage_from_queries()

        mcps = list(aggregator.gen_metadata())

        assert len(mcps) > 0, "Should generate metadata change proposals"

        dataset_usage_aspects = [
            mcp
            for mcp in mcps
            if hasattr(mcp, "aspect")
            and mcp.aspect.__class__.__name__ == "DatasetUsageStatistics"
        ]

        assert len(dataset_usage_aspects) > 0, (
            "Should generate DatasetUsageStatistics aspects when include_usage_statistics=True"
        )

    def test_usage_statistics_flag_disabled(self, postgres_connection):
        """Test that usage statistics are not generated when flag is disabled."""
        postgres_connection.execute(text("SELECT pg_stat_statements_reset()"))

        for _ in range(10):
            postgres_connection.execute(
                text("SELECT * FROM lineage_test.source_orders")
            )

        time.sleep(0.5)

        config = PostgresConfig(
            username="postgres",
            password="example",
            host_port="localhost:5432",
            database="postgres",
            include_query_lineage=True,
            include_usage_statistics=False,
        )

        class MockReport:
            def __init__(self):
                self.failures = []
                self.num_queries_extracted = 0
                self.num_queries_parsed = 0
                self.num_queries_parse_failures = 0

            def report_failure(self, message, context=None, **kwargs):  # type: ignore[no-untyped-def]
                self.failures.append({"message": message, "context": context})

        report = MockReport()

        aggregator = SqlParsingAggregator(
            platform="postgres",
            generate_lineage=True,
            generate_queries=True,
            generate_usage_statistics=False,
        )

        extractor = PostgresLineageExtractor(
            config=config,
            connection=postgres_connection,
            report=report,  # type: ignore[arg-type]
            sql_aggregator=aggregator,
        )

        queries = extractor.extract_query_history()
        assert len(queries) > 0, "Should extract queries"

        extractor.populate_lineage_from_queries()

        mcps = list(aggregator.gen_metadata())

        dataset_usage_aspects = [
            mcp
            for mcp in mcps
            if hasattr(mcp, "aspect")
            and mcp.aspect.__class__.__name__ == "DatasetUsageStatistics"
        ]

        assert len(dataset_usage_aspects) == 0, (
            "Should not generate DatasetUsageStatistics aspects when include_usage_statistics=False"
        )
