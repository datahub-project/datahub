import subprocess
import time

import pytest
import sqlalchemy as sa
from sqlalchemy import text

from datahub.ingestion.source.sql.mssql.lineage import (
    MSSQLLineageExtractor,
    MSSQLQueryEntry,
)
from datahub.ingestion.source.sql.mssql.query import MSSQLQuery
from datahub.ingestion.source.sql.mssql.source import SQLServerConfig
from datahub.ingestion.source.sql.sql_common import SQLSourceReport
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from tests.test_helpers.docker_helpers import wait_for_port

MSSQL_PORT = 1433


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/sql_server"


def is_mssql_up(container_name: str) -> bool:
    """Check if SQL Server is responsive on a container."""
    cmd = f"docker logs {container_name} 2>&1 | grep 'SQL Server is now ready for client connections'"
    ret = subprocess.run(cmd, shell=True)
    return ret.returncode == 0


@pytest.fixture(scope="module")
def mssql_runner(docker_compose_runner, test_resources_dir):
    """Start SQL Server container with Query Store enabled."""
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "sql-server"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testsqlserver",
            MSSQL_PORT,
            timeout=120,
            checker=lambda: is_mssql_up("testsqlserver"),
        )
        time.sleep(5)  # Extra time for SQL Server to fully initialize
        yield docker_services


@pytest.fixture(scope="module")
def mssql_connection(mssql_runner):
    """Create SQLAlchemy connection to test SQL Server instance."""
    engine = sa.create_engine(
        "mssql+pyodbc://sa:test!Password@localhost:21433/master?"
        "driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes",
        isolation_level="AUTOCOMMIT",
    )

    with engine.connect() as conn:
        # Create test database with Query Store enabled
        conn.execute(text("DROP DATABASE IF EXISTS lineage_test"))
        conn.execute(text("CREATE DATABASE lineage_test"))
        conn.execute(
            text(
                """
            ALTER DATABASE lineage_test SET QUERY_STORE = ON
            (
                OPERATION_MODE = READ_WRITE,
                DATA_FLUSH_INTERVAL_SECONDS = 60,
                INTERVAL_LENGTH_MINUTES = 1,
                MAX_STORAGE_SIZE_MB = 100,
                QUERY_CAPTURE_MODE = AUTO
            )
        """
            )
        )

        conn.execute(text("USE lineage_test"))

        # Create test schema and tables
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

        conn.execute(text("USE master"))
        conn.execute(text("DROP DATABASE IF EXISTS lineage_test"))


@pytest.mark.integration
class TestMSSQLLineageIntegration:
    """Integration tests for MSSQL lineage extraction."""

    def test_query_store_enabled(self, mssql_connection):
        """Test that Query Store is properly enabled."""
        result = mssql_connection.execute(MSSQLQuery.check_query_store_enabled())
        row = result.fetchone()
        assert row is not None
        assert row["is_enabled"] == 1, "Query Store should be enabled"

    def test_sql_server_version_detection(self, mssql_connection):
        """Test SQL Server version detection."""
        result = mssql_connection.execute(MSSQLQuery.get_mssql_version())
        row = result.fetchone()
        assert row is not None
        assert "version" in row
        assert row["major_version"] >= 13, "Should be SQL Server 2016 or later"

    def test_query_history_extraction_basic(self, mssql_connection):
        """Test basic query history extraction from Query Store."""
        # Execute test queries
        mssql_connection.execute(
            text("SELECT * FROM lineage_test.source_orders WHERE order_id = 1")
        )
        mssql_connection.execute(
            text("SELECT * FROM lineage_test.source_customers WHERE customer_id = 101")
        )

        # Wait for Query Store to capture queries
        time.sleep(2)

        query, params = MSSQLQuery.get_query_history_from_query_store(
            database="lineage_test", limit=100, min_calls=1, exclude_patterns=None
        )
        result = mssql_connection.execute(query, params)

        queries = list(result)
        assert len(queries) > 0, "Should extract at least some queries"

        for row in queries:
            assert "query_id" in row
            assert "query_text" in row
            assert "execution_count" in row
            assert row["execution_count"] >= 1

    def test_lineage_from_select_into(self, mssql_connection):
        """Test lineage extraction from SELECT INTO queries."""
        mssql_connection.execute(
            text("DROP TABLE IF EXISTS lineage_test.target_summary")
        )

        mssql_connection.execute(
            text(
                """
            SELECT
                c.customer_id,
                c.customer_name,
                COUNT(o.order_id) as order_count,
                SUM(o.total_amount) as total_spent
            INTO lineage_test.target_summary
            FROM lineage_test.source_customers c
            JOIN lineage_test.source_orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.customer_name
        """
            )
        )

        time.sleep(2)

        query, params = MSSQLQuery.get_query_history_from_query_store(
            database="lineage_test", limit=100, min_calls=1, exclude_patterns=None
        )
        result = mssql_connection.execute(query, params)

        queries = list(result)

        select_into_queries = [
            q
            for q in queries
            if "target_summary" in q["query_text"]
            and "SELECT" in q["query_text"].upper()
            and "INTO" in q["query_text"].upper()
        ]

        assert len(select_into_queries) > 0, "Should find SELECT INTO query"

        select_into_query_text = select_into_queries[0]["query_text"]
        assert "source_customers" in select_into_query_text
        assert "source_orders" in select_into_query_text

    def test_query_exclude_patterns(self, mssql_connection):
        """Test that exclude patterns filter out system queries."""
        mssql_connection.execute(text("SELECT * FROM lineage_test.source_orders"))
        mssql_connection.execute(text("SELECT @@VERSION"))
        mssql_connection.execute(text("SELECT * FROM sys.databases"))

        time.sleep(2)

        query, params = MSSQLQuery.get_query_history_from_query_store(
            database="lineage_test",
            limit=100,
            min_calls=1,
            exclude_patterns=["%sys.%", "%@@%"],
        )
        result = mssql_connection.execute(query, params)

        queries = [q["query_text"] for q in result]

        sys_queries = [q for q in queries if "sys." in q.lower()]
        assert len(sys_queries) == 0, "sys.* queries should be filtered"

        version_queries = [q for q in queries if "@@VERSION" in q]
        assert len(version_queries) == 0, "@@VERSION queries should be filtered"

    def test_sql_injection_prevention_exclude_patterns(self, mssql_connection):
        """Test that SQL injection in exclude patterns is safely parameterized."""
        malicious_pattern = "'; DROP TABLE lineage_test.source_orders; --"

        query, params = MSSQLQuery.get_query_history_from_query_store(
            database="lineage_test",
            limit=10,
            min_calls=1,
            exclude_patterns=[malicious_pattern],
        )

        # Execute the query - if injection worked, the table would be dropped
        result = mssql_connection.execute(query, params)
        list(result)

        # Verify table still exists
        check_result = mssql_connection.execute(
            text(
                """
            SELECT CASE WHEN EXISTS (
                SELECT * FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'lineage_test'
                AND TABLE_NAME = 'source_orders'
            ) THEN 1 ELSE 0 END AS table_exists
        """
            )
        )
        assert check_result.fetchone()[0] == 1, "Table should still exist"

    def test_dmv_permissions_check(self, mssql_connection):
        """Test DMV permissions check query."""
        result = mssql_connection.execute(MSSQLQuery.check_dmv_permissions())
        row = result.fetchone()
        assert row is not None
        assert "has_view_server_state" in row
        assert row["has_view_server_state"] in (0, 1)

    def test_lineage_extractor_prerequisites(self, mssql_connection):
        """Test that MSSQLLineageExtractor can check prerequisites."""
        config = SQLServerConfig(
            username="sa",
            password="test!Password",
            host_port="localhost:21433",
            database="lineage_test",
        )

        report = SQLSourceReport()
        aggregator = SqlParsingAggregator(
            platform="mssql", generate_lineage=True, generate_queries=True
        )

        extractor = MSSQLLineageExtractor(
            config=config,
            connection=mssql_connection,
            report=report,
            sql_aggregator=aggregator,
        )

        is_ready, message, method = extractor.check_prerequisites()

        assert is_ready is True, f"Prerequisites should be met: {message}"
        assert method in ("query_store", "dmv")

    def test_query_entry_dataclass(self):
        """Test MSSQLQueryEntry dataclass functionality."""
        entry = MSSQLQueryEntry(
            query_id="12345",
            query_text="SELECT * FROM test_table",
            execution_count=50,
            total_exec_time_ms=5000.0,
            user_name="testuser",
            database_name="testdb",
        )

        assert entry.query_id == "12345"
        assert entry.avg_exec_time_ms == 100.0  # 5000/50

        entry_zero = MSSQLQueryEntry(
            query_id="67890",
            query_text="SELECT 1",
            execution_count=0,
            total_exec_time_ms=0.0,
            user_name="testuser",
            database_name="testdb",
        )
        assert entry_zero.avg_exec_time_ms == 0.0

    def test_query_history_with_min_calls_filter(self, mssql_connection):
        """Test that min_calls filter works correctly."""
        # Execute query multiple times
        for _ in range(5):
            mssql_connection.execute(
                text("SELECT * FROM lineage_test.source_orders WHERE order_id > 0")
            )

        mssql_connection.execute(
            text("SELECT * FROM lineage_test.source_customers WHERE customer_id > 0")
        )

        time.sleep(2)

        query, params = MSSQLQuery.get_query_history_from_query_store(
            database="lineage_test", limit=100, min_calls=3, exclude_patterns=None
        )
        result = mssql_connection.execute(query, params)

        queries = list(result)

        for q in queries:
            assert q["execution_count"] >= 3

    def test_query_store_disabled_scenario(self, mssql_connection):
        """Test behavior when Query Store is disabled."""
        # Create temporary database without Query Store
        mssql_connection.execute(text("CREATE DATABASE test_no_qs"))
        mssql_connection.execute(text("USE test_no_qs"))

        config = SQLServerConfig(
            username="sa",
            password="test!Password",
            host_port="localhost:21433",
            database="test_no_qs",
        )

        report = SQLSourceReport()
        aggregator = SqlParsingAggregator(
            platform="mssql", generate_lineage=True, generate_queries=True
        )

        extractor = MSSQLLineageExtractor(
            config=config,
            connection=mssql_connection,
            report=report,
            sql_aggregator=aggregator,
        )

        is_ready, message, method = extractor.check_prerequisites()

        # Should fall back to DMV
        assert method == "dmv", "Should fall back to DMV when Query Store disabled"

        # Cleanup
        mssql_connection.execute(text("USE master"))
        mssql_connection.execute(text("DROP DATABASE test_no_qs"))

    def test_malformed_query_text_handling(self, mssql_connection):
        """Test handling of queries with unusual or malformed text."""
        malformed_queries = [
            "SELECT * FROM lineage_test.source_orders WHERE customer_name = 'O''Brien'",
            "SELECT * FROM lineage_test.source_customers WHERE comment LIKE '%—unicode—%'",
            "SELECT 'DROP TABLE users; --' as fake_injection",
        ]

        for malformed_query in malformed_queries:
            try:
                mssql_connection.execute(text(malformed_query))
            except Exception:
                # Some queries may fail due to missing columns, that's OK
                pass

        time.sleep(2)

        query, params = MSSQLQuery.get_query_history_from_query_store(
            database="lineage_test", limit=100, min_calls=1, exclude_patterns=None
        )
        result = mssql_connection.execute(query, params)

        queries = list(result)

        for q in queries:
            assert "query_text" in q
            assert isinstance(q["query_text"], str)
            assert "query_id" in q
            assert "execution_count" in q

    def test_dmv_fallback_extraction(self, mssql_connection):
        """Test query extraction from DMVs as fallback."""
        # DMVs should work regardless of Query Store
        query, params = MSSQLQuery.get_query_history_from_dmv(
            database="lineage_test", limit=100, min_calls=1, exclude_patterns=None
        )
        result = mssql_connection.execute(query, params)

        queries = list(result)
        # DMV may have no results depending on cache, so just verify query executes
        assert isinstance(queries, list)

    def test_text_clause_wrapping(self, mssql_connection):
        """Test that all SQL queries return TextClause for SQL Alchemy 2.0 compatibility."""
        from sqlalchemy.sql.elements import TextClause

        # Check all query methods return TextClause
        assert isinstance(MSSQLQuery.check_query_store_enabled(), TextClause)
        assert isinstance(MSSQLQuery.check_dmv_permissions(), TextClause)
        assert isinstance(MSSQLQuery.get_mssql_version(), TextClause)

        query, _ = MSSQLQuery.get_query_history_from_query_store(
            database="test", limit=10, min_calls=1, exclude_patterns=None
        )
        assert isinstance(query, TextClause)

        query, _ = MSSQLQuery.get_query_history_from_dmv(
            database="test", limit=10, min_calls=1, exclude_patterns=None
        )
        assert isinstance(query, TextClause)

    def test_end_to_end_lineage_aggregation(self, mssql_connection):
        """Test complete lineage extraction and aggregation pipeline."""
        # Create tables for lineage test
        mssql_connection.execute(text("DROP TABLE IF EXISTS lineage_test.etl_source"))
        mssql_connection.execute(text("DROP TABLE IF EXISTS lineage_test.etl_staging"))
        mssql_connection.execute(text("DROP TABLE IF EXISTS lineage_test.etl_final"))

        mssql_connection.execute(
            text(
                """
            CREATE TABLE lineage_test.etl_source (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                value DECIMAL(10,2)
            )
        """
            )
        )

        mssql_connection.execute(
            text(
                """
            CREATE TABLE lineage_test.etl_staging (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                value DECIMAL(10,2),
                processed_date DATE
            )
        """
            )
        )

        mssql_connection.execute(
            text(
                """
            CREATE TABLE lineage_test.etl_final (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                total_value DECIMAL(10,2)
            )
        """
            )
        )

        # Execute lineage-generating queries
        mssql_connection.execute(
            text(
                """
            INSERT INTO lineage_test.etl_staging (id, name, value, processed_date)
            SELECT id, name, value, GETDATE()
            FROM lineage_test.etl_source
        """
            )
        )

        mssql_connection.execute(
            text(
                """
            INSERT INTO lineage_test.etl_final (id, name, total_value)
            SELECT id, name, SUM(value)
            FROM lineage_test.etl_staging
            GROUP BY id, name
        """
            )
        )

        time.sleep(2)

        config = SQLServerConfig(
            username="sa",
            password="test!Password",
            host_port="localhost:21433",
            database="lineage_test",
            include_query_lineage=True,
            max_queries_to_extract=100,
            min_query_calls=1,
        )

        report = SQLSourceReport()
        aggregator = SqlParsingAggregator(
            platform="mssql", generate_lineage=True, generate_queries=True
        )

        extractor = MSSQLLineageExtractor(
            config=config,
            connection=mssql_connection,
            report=report,
            sql_aggregator=aggregator,
        )

        # Test the complete pipeline
        extractor.populate_lineage_from_queries()

        # Verify queries were extracted
        assert report.num_queries_extracted > 0, "Should have extracted queries"

        # Cleanup
        aggregator.close()

    def test_lineage_with_parse_failures(self, mssql_connection):
        """Test that lineage extraction continues gracefully when some queries fail to parse."""
        config = SQLServerConfig(
            username="sa",
            password="test!Password",
            host_port="localhost:21433",
            database="lineage_test",
            include_query_lineage=True,
        )

        report = SQLSourceReport()
        aggregator = SqlParsingAggregator(
            platform="mssql", generate_lineage=True, generate_queries=True
        )

        # Execute some valid and some potentially problematic queries
        try:
            mssql_connection.execute(text("SELECT * FROM lineage_test.source_orders"))
            mssql_connection.execute(
                text("SELECT @@VERSION, @@SERVERNAME, @@LANGUAGE")
            )  # System variables
        except Exception:
            pass

        time.sleep(2)

        extractor = MSSQLLineageExtractor(
            config=config,
            connection=mssql_connection,
            report=report,
            sql_aggregator=aggregator,
        )

        # Should not raise exception even if some queries can't be parsed
        extractor.populate_lineage_from_queries()

        # Cleanup
        aggregator.close()

    def test_connection_loss_during_extraction(self, mssql_connection):
        """Test graceful handling of connection loss during query extraction."""
        config = SQLServerConfig(
            username="sa",
            password="test!Password",
            host_port="localhost:21433",
            database="lineage_test",
            include_query_lineage=True,
        )

        report = SQLSourceReport()
        aggregator = SqlParsingAggregator(
            platform="mssql", generate_lineage=True, generate_queries=True
        )

        from unittest.mock import MagicMock

        from sqlalchemy.exc import OperationalError

        mock_conn = MagicMock()

        # Simulate connection loss during query extraction
        mock_conn.execute.side_effect = OperationalError("connection lost", None, None)

        extractor = MSSQLLineageExtractor(
            config=config,
            connection=mock_conn,
            report=report,
            sql_aggregator=aggregator,
        )

        queries = extractor.extract_query_history()

        # Should return empty list and log failure, not crash
        assert len(queries) == 0
        assert len(report.failures) > 0

        # Cleanup
        aggregator.close()
