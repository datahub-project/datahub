from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.sql.postgres_lineage import (
    PostgresLineageExtractor,
    PostgresQueryEntry,
)
from datahub.ingestion.source.sql.postgres_query import PostgresQuery


class TestPostgresQuery:
    """Test PostgresQuery SQL generation."""

    def test_check_pg_stat_statements_enabled(self):
        """Test extension check query generation."""
        query = PostgresQuery.check_pg_stat_statements_enabled()

        assert "pg_extension" in query
        assert "pg_stat_statements" in query
        assert "enabled" in query.lower()

    def test_get_query_history_basic(self):
        """Test basic query history SQL generation."""
        query = PostgresQuery.get_query_history(limit=100)

        assert "pg_stat_statements" in query
        assert "LIMIT 100" in query
        assert "query" in query.lower()
        assert "calls" in query.lower()

    def test_get_query_history_with_filters(self):
        """Test query history with database and min_calls filters."""
        query = PostgresQuery.get_query_history(
            database="production",
            limit=500,
            min_calls=10,
        )

        assert "datname = 'production'" in query
        assert "calls >= 10" in query
        assert "LIMIT 500" in query

    def test_get_query_history_with_exclusions(self):
        """Test query history with exclude patterns."""
        query = PostgresQuery.get_query_history(
            limit=100,
            exclude_patterns=["%temp_%", "%staging%"],
        )

        assert "NOT ILIKE '%%temp_%%'" in query
        assert "NOT ILIKE '%%staging%%'" in query

    def test_get_queries_by_type(self):
        """Test filtering queries by SQL command type."""
        query = PostgresQuery.get_queries_by_type(
            query_type="INSERT",
            database="testdb",
            limit=100,
        )

        assert "INSERT" in query
        assert "datname = 'testdb'" in query


class TestPostgresLineageExtractor:
    """Test PostgresLineageExtractor functionality."""

    @pytest.fixture
    def mock_connection(self):
        """Mock database connection."""
        conn = MagicMock()
        return conn

    @pytest.fixture
    def mock_config(self):
        """Mock Postgres config."""
        config = MagicMock()
        config.database = "testdb"
        config.include_query_lineage = True
        config.max_queries_to_extract = 1000
        config.min_query_calls = 1
        config.query_exclude_patterns = None
        config.include_usage_statistics = False
        return config

    @pytest.fixture
    def mock_sql_aggregator(self):
        """Mock SQL parsing aggregator."""
        aggregator = MagicMock()
        return aggregator

    @pytest.fixture
    def lineage_extractor(self, mock_config, mock_connection, mock_sql_aggregator):
        """Create lineage extractor instance."""
        report = MagicMock()
        return PostgresLineageExtractor(
            config=mock_config,
            connection=mock_connection,
            report=report,
            sql_aggregator=mock_sql_aggregator,
        )

    def test_check_prerequisites_success(self, lineage_extractor, mock_connection):
        """Test successful prerequisites check."""
        mock_result = MagicMock()
        mock_result.fetchone.return_value = [True]
        mock_connection.execute.return_value = mock_result

        is_ready, message = lineage_extractor.check_prerequisites()

        assert is_ready is True
        assert "Prerequisites met" in message

    def test_check_prerequisites_extension_not_installed(
        self, lineage_extractor, mock_connection
    ):
        """Test prerequisites check when extension not installed."""
        mock_result = MagicMock()
        mock_result.fetchone.return_value = [False]
        mock_connection.execute.return_value = mock_result

        is_ready, message = lineage_extractor.check_prerequisites()

        assert is_ready is False
        assert "not installed" in message

    def test_extract_query_history(self, lineage_extractor, mock_connection):
        """Test query history extraction."""
        lineage_extractor.check_prerequisites = MagicMock(
            return_value=(True, "Prerequisites met")
        )

        mock_result = MagicMock()
        mock_result.__iter__.return_value = iter(
            [
                {
                    "query_id": "12345",
                    "query_text": "INSERT INTO target SELECT * FROM source",
                    "execution_count": 100,
                    "total_exec_time_ms": 5000.0,
                    "user_name": "testuser",
                    "database_name": "testdb",
                },
                {
                    "query_id": "67890",
                    "query_text": "CREATE TABLE new_table AS SELECT * FROM old_table",
                    "execution_count": 5,
                    "total_exec_time_ms": 1000.0,
                    "user_name": "admin",
                    "database_name": "testdb",
                },
            ]
        )
        mock_connection.execute.return_value = mock_result

        queries = lineage_extractor.extract_query_history()

        assert len(queries) == 2
        assert isinstance(queries[0], PostgresQueryEntry)
        assert queries[0].query_id == "12345"
        assert "INSERT INTO" in queries[0].query_text
        assert queries[0].execution_count == 100

    def test_populate_lineage_from_queries(
        self, lineage_extractor, mock_sql_aggregator
    ):
        """Test lineage population from queries."""
        test_queries = [
            PostgresQueryEntry(
                query_id="1",
                query_text="INSERT INTO target SELECT * FROM source",
                execution_count=10,
                total_exec_time_ms=100.0,
                user_name="user1",
                database_name="testdb",
            )
        ]

        lineage_extractor.extract_query_history = MagicMock(return_value=test_queries)

        lineage_extractor.populate_lineage_from_queries(discovered_tables=set())

        assert mock_sql_aggregator.add_observed_query.called
        call_args = mock_sql_aggregator.add_observed_query.call_args
        assert "INSERT INTO target" in call_args.kwargs["query"]

    def test_populate_lineage_disabled(self, lineage_extractor, mock_sql_aggregator):
        """Test that lineage population skips when disabled."""
        lineage_extractor.config.include_query_lineage = False

        lineage_extractor.populate_lineage_from_queries(discovered_tables=set())

        assert not mock_sql_aggregator.add_observed_query.called


class TestPostgresQueryEntry:
    """Test PostgresQueryEntry dataclass."""

    def test_avg_exec_time_calculation(self):
        """Test average execution time calculation."""
        entry = PostgresQueryEntry(
            query_id="123",
            query_text="SELECT * FROM test",
            execution_count=10,
            total_exec_time_ms=1000.0,
            user_name="testuser",
            database_name="testdb",
        )

        assert entry.avg_exec_time_ms == 100.0

    def test_avg_exec_time_zero_executions(self):
        """Test average execution time with zero executions."""
        entry = PostgresQueryEntry(
            query_id="123",
            query_text="SELECT * FROM test",
            execution_count=0,
            total_exec_time_ms=0.0,
            user_name="testuser",
            database_name="testdb",
        )

        assert entry.avg_exec_time_ms == 0.0
