from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.sql.postgres.lineage import (
    PostgresLineageExtractor,
    PostgresQueryEntry,
)
from datahub.ingestion.source.sql.postgres.query import PostgresQuery


class TestPostgresQuery:
    def test_check_pg_stat_statements_enabled(self):
        query = PostgresQuery.check_pg_stat_statements_enabled()

        assert "pg_extension" in query
        assert "pg_stat_statements" in query
        assert "enabled" in query.lower()

    def test_get_query_history_basic(self):
        query, params = PostgresQuery.get_query_history(limit=100)

        assert "pg_stat_statements" in query
        assert "LIMIT :limit" in query
        assert "query" in query.lower()
        assert "calls" in query.lower()
        assert params["limit"] == 100
        assert params["min_calls"] == 1

    def test_get_query_history_with_filters(self):
        query, params = PostgresQuery.get_query_history(
            database="production",
            limit=500,
            min_calls=10,
        )

        assert "datname = :database" in query
        assert "calls >= :min_calls" in query
        assert "LIMIT :limit" in query
        assert params["database"] == "production"
        assert params["min_calls"] == 10
        assert params["limit"] == 500

    def test_get_query_history_with_exclusions(self):
        query, params = PostgresQuery.get_query_history(
            limit=100,
            exclude_patterns=["%temp_%", "%staging%"],
        )

        # Default exclusions (0-4) and user patterns (5-6) are all parameterized
        assert "NOT ILIKE :exclude_pattern_5" in query
        assert "NOT ILIKE :exclude_pattern_6" in query
        assert params["exclude_pattern_5"] == "%%temp_%%"
        assert params["exclude_pattern_6"] == "%%staging%%"

    def test_get_queries_by_type(self):
        query, params = PostgresQuery.get_queries_by_type(
            query_type="INSERT",
            database="testdb",
            limit=100,
        )

        assert "ILIKE :query_type_pattern" in query
        assert "datname = :database" in query
        assert params["query_type_pattern"] == "INSERT%"
        assert params["database"] == "testdb"
        assert params["limit"] == 100


class TestPostgresLineageExtractor:
    @pytest.fixture
    def mock_connection(self):
        conn = MagicMock()
        return conn

    @pytest.fixture
    def mock_config(self):
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
        aggregator = MagicMock()
        return aggregator

    @pytest.fixture
    def lineage_extractor(self, mock_config, mock_connection, mock_sql_aggregator):
        report = MagicMock()
        return PostgresLineageExtractor(
            config=mock_config,
            connection=mock_connection,
            report=report,
            sql_aggregator=mock_sql_aggregator,
        )

    def test_check_prerequisites_success(self, lineage_extractor, mock_connection):
        # First call checks extension is installed, second call checks permissions
        mock_result_extension = MagicMock()
        mock_result_extension.fetchone.return_value = [True]

        mock_result_permissions = MagicMock()
        mock_result_permissions.fetchone.return_value = [
            True,
            True,
        ]  # has_stats_role, is_superuser

        mock_connection.execute.side_effect = [
            mock_result_extension,
            mock_result_permissions,
        ]

        is_ready, message = lineage_extractor.check_prerequisites()

        assert is_ready is True
        assert "Prerequisites met" in message

    def test_check_prerequisites_extension_not_installed(
        self, lineage_extractor, mock_connection
    ):
        mock_result = MagicMock()
        mock_result.fetchone.return_value = [False]
        mock_connection.execute.return_value = mock_result

        is_ready, message = lineage_extractor.check_prerequisites()

        assert is_ready is False
        assert "not installed" in message

    def test_check_prerequisites_permission_check_no_row(
        self, lineage_extractor, mock_connection
    ):
        mock_result_extension = MagicMock()
        mock_result_extension.fetchone.return_value = [True]

        mock_result_permissions = MagicMock()
        mock_result_permissions.fetchone.return_value = None

        mock_connection.execute.side_effect = [
            mock_result_extension,
            mock_result_permissions,
        ]

        is_ready, message = lineage_extractor.check_prerequisites()

        assert is_ready is False
        assert "Could not verify" in message

    def test_check_prerequisites_permission_check_exception(
        self, lineage_extractor, mock_connection
    ):
        from sqlalchemy.exc import DatabaseError

        mock_result_extension = MagicMock()
        mock_result_extension.fetchone.return_value = [True]

        mock_connection.execute.side_effect = [
            mock_result_extension,
            DatabaseError("connection failed", None, None),
        ]

        is_ready, message = lineage_extractor.check_prerequisites()

        assert is_ready is False
        assert "Permission check failed" in message

    def test_extract_query_history(self, lineage_extractor, mock_connection):
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
        observed_query = call_args[0][0]
        assert "INSERT INTO target" in observed_query.query

    def test_populate_lineage_disabled(self, lineage_extractor, mock_sql_aggregator):
        lineage_extractor.config.include_query_lineage = False

        lineage_extractor.populate_lineage_from_queries(discovered_tables=set())

        assert not mock_sql_aggregator.add_observed_query.called


class TestPostgresQueryEntry:
    def test_avg_exec_time_calculation(self):
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
        entry = PostgresQueryEntry(
            query_id="123",
            query_text="SELECT * FROM test",
            execution_count=0,
            total_exec_time_ms=0.0,
            user_name="testuser",
            database_name="testdb",
        )

        assert entry.avg_exec_time_ms == 0.0
