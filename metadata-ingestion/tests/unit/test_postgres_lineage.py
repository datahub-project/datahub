from typing import cast
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import DatabaseError, ProgrammingError

from datahub.ingestion.source.sql.postgres.lineage import (
    PostgresLineageExtractor,
    PostgresQueryEntry,
)
from datahub.ingestion.source.sql.postgres.query import PostgresQuery
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.sql_parsing.sqlglot_lineage import SqlUnderstandingError


class TestPostgresQuery:
    def test_check_pg_stat_statements_enabled(self):
        query = PostgresQuery.check_pg_stat_statements_enabled()
        query_str = str(query)

        assert "pg_extension" in query_str
        assert "pg_stat_statements" in query_str
        assert "enabled" in query_str.lower()

    def test_get_query_history_basic(self):
        query, params = PostgresQuery.get_query_history(limit=100)
        query_str = str(query)

        assert "pg_stat_statements" in query_str
        assert "LIMIT :limit" in query_str
        assert "query" in query_str.lower()
        assert "calls" in query_str.lower()
        assert params["limit"] == 100
        assert params["min_calls"] == 1

    def test_get_query_history_with_filters(self):
        query, params = PostgresQuery.get_query_history(
            database="production",
            limit=500,
            min_calls=10,
        )
        query_str = str(query)

        assert "datname = :database" in query_str
        assert "calls >= :min_calls" in query_str
        assert "LIMIT :limit" in query_str
        assert params["database"] == "production"
        assert params["min_calls"] == 10
        assert params["limit"] == 500

    def test_get_query_history_with_exclusions(self):
        query, params = PostgresQuery.get_query_history(
            limit=100,
            exclude_patterns=["%temp_%", "%staging%"],
        )
        query_str = str(query)

        assert "NOT ILIKE :exclude_pattern_5" in query_str
        assert "NOT ILIKE :exclude_pattern_6" in query_str
        assert params["exclude_pattern_5"] == "%%temp_%%"
        assert params["exclude_pattern_6"] == "%%staging%%"


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
        mock_result_version = MagicMock()
        mock_result_version.fetchone.return_value = [130000]

        mock_result_extension = MagicMock()
        mock_result_extension.fetchone.return_value = [True]

        mock_result_permissions = MagicMock()
        mock_result_permissions.fetchone.return_value = [
            True,
            True,
        ]

        mock_connection.execute.side_effect = [
            mock_result_version,
            mock_result_extension,
            mock_result_permissions,
        ]

        is_ready, message = lineage_extractor.check_prerequisites()

        assert is_ready is True
        assert "Prerequisites met" in message

    def test_check_prerequisites_version_too_old(
        self, lineage_extractor, mock_connection
    ):
        mock_result_version = MagicMock()
        mock_result_version.fetchone.return_value = [120000]

        mock_connection.execute.return_value = mock_result_version

        is_ready, message = lineage_extractor.check_prerequisites()

        assert is_ready is False
        assert "PostgreSQL version 12.0 detected" in message
        assert "requires PostgreSQL 13+" in message
        assert "total_exec_time" in message

    def test_check_prerequisites_extension_not_installed(
        self, lineage_extractor, mock_connection
    ):
        mock_result_version = MagicMock()
        mock_result_version.fetchone.return_value = [130000]

        mock_result_extension = MagicMock()
        mock_result_extension.fetchone.return_value = [False]

        mock_connection.execute.side_effect = [
            mock_result_version,
            mock_result_extension,
        ]

        is_ready, message = lineage_extractor.check_prerequisites()

        assert is_ready is False
        assert "not installed" in message

    def test_check_prerequisites_permission_check_no_row(
        self, lineage_extractor, mock_connection
    ):
        mock_result_version = MagicMock()
        mock_result_version.fetchone.return_value = [130000]

        mock_result_extension = MagicMock()
        mock_result_extension.fetchone.return_value = [True]

        mock_result_permissions = MagicMock()
        mock_result_permissions.fetchone.return_value = None

        mock_connection.execute.side_effect = [
            mock_result_version,
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

        mock_result_version = MagicMock()
        mock_result_version.fetchone.return_value = [130000]

        mock_result_extension = MagicMock()
        mock_result_extension.fetchone.return_value = [True]

        mock_connection.execute.side_effect = [
            mock_result_version,
            mock_result_extension,
            DatabaseError("connection failed", None, Exception("connection failed")),
        ]

        is_ready, message = lineage_extractor.check_prerequisites()

        assert is_ready is False
        assert "Permission check failed" in message

    def test_extract_query_history(self, lineage_extractor, mock_connection):
        lineage_extractor.check_prerequisites = MagicMock(
            return_value=(True, "Prerequisites met")
        )

        mock_result = MagicMock()
        mock_result.mappings.return_value = [
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

        lineage_extractor.populate_lineage_from_queries()

        assert mock_sql_aggregator.add_observed_query.called
        call_args = mock_sql_aggregator.add_observed_query.call_args
        observed_query = call_args[0][0]
        assert "INSERT INTO target" in observed_query.query

    def test_populate_lineage_disabled(self, lineage_extractor, mock_sql_aggregator):
        lineage_extractor.config.include_query_lineage = False

        lineage_extractor.populate_lineage_from_queries()

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


class TestPostgresLineageExtractorFailures:
    """Realistic degradations: an old server, a missing extension, a denied
    role, and queries sqlglot cannot understand must each degrade cleanly."""

    @pytest.fixture
    def report(self) -> SQLSourceReport:
        return SQLSourceReport()

    @pytest.fixture
    def extractor(self, report: SQLSourceReport) -> PostgresLineageExtractor:
        config = MagicMock()
        config.database = "testdb"
        config.include_query_lineage = True
        config.max_queries_to_extract = 1000
        config.min_query_calls = 1
        config.query_exclude_patterns = None
        return PostgresLineageExtractor(
            config=config,
            connection=MagicMock(),
            report=report,
            sql_aggregator=MagicMock(),
        )

    @staticmethod
    def _row(*values: object) -> MagicMock:
        result = MagicMock()
        result.fetchone.return_value = list(values)
        return result

    def test_version_check_failure_does_not_block_extraction(
        self, extractor: PostgresLineageExtractor
    ) -> None:
        # The version query is advisory: when it fails the extension and
        # permission checks still decide readiness.
        connection = cast(MagicMock, extractor.connection)
        connection.execute.side_effect = [
            DatabaseError("no pg_catalog access", None, Exception()),
            self._row(True),
            self._row(True, False),
        ]

        is_ready, _ = extractor.check_prerequisites()

        assert is_ready

    def test_extension_check_failure_is_not_ready(
        self, extractor: PostgresLineageExtractor
    ) -> None:
        connection = cast(MagicMock, extractor.connection)
        connection.execute.side_effect = [
            self._row(130000),
            DatabaseError("relation pg_extension denied", None, Exception()),
        ]

        is_ready, message = extractor.check_prerequisites()

        assert not is_ready
        assert "Extension check failed" in message

    def test_missing_stats_role_is_not_ready(
        self, extractor: PostgresLineageExtractor
    ) -> None:
        connection = cast(MagicMock, extractor.connection)
        connection.execute.side_effect = [
            self._row(130000),
            self._row(True),
            self._row(False, False),
        ]

        is_ready, message = extractor.check_prerequisites()

        assert not is_ready
        assert "pg_read_all_stats" in message

    def test_extract_query_history_reports_unmet_prerequisites(
        self, extractor: PostgresLineageExtractor, report: SQLSourceReport
    ) -> None:
        with patch.object(
            extractor, "check_prerequisites", return_value=(False, "not installed")
        ):
            assert extractor.extract_query_history() == []

        assert len(report.failures) == 1
        assert "pg_stat_statements_not_ready" in str(report.failures[0])
        cast(MagicMock, extractor.connection).execute.assert_not_called()

    def test_extract_query_history_reports_query_failure(
        self, extractor: PostgresLineageExtractor, report: SQLSourceReport
    ) -> None:
        connection = cast(MagicMock, extractor.connection)
        connection.execute.side_effect = ProgrammingError(
            "column total_exec_time does not exist", None, Exception()
        )
        with patch.object(
            extractor, "check_prerequisites", return_value=(True, "Prerequisites met")
        ):
            assert extractor.extract_query_history() == []

        assert len(report.failures) == 1
        assert "query_history_extraction_failed" in str(report.failures[0])

    def test_unparseable_queries_are_counted_not_fatal(
        self, extractor: PostgresLineageExtractor, report: SQLSourceReport
    ) -> None:
        def entry(query_id: str, text: str) -> PostgresQueryEntry:
            return PostgresQueryEntry(
                query_id=query_id,
                query_text=text,
                execution_count=1,
                total_exec_time_ms=1.0,
                user_name=None,
                database_name="testdb",
            )

        aggregator = cast(MagicMock, extractor.sql_aggregator)
        aggregator.add_observed_query.side_effect = [
            SqlUnderstandingError("unsupported"),
            None,
            ValueError("bad urn"),
        ]
        with patch.object(
            extractor,
            "extract_query_history",
            return_value=[
                entry("1", "MERGE INTO t USING s ON ..."),
                entry("2", "INSERT INTO t SELECT * FROM s"),
                entry("3", "CREATE TABLE x AS SELECT 1"),
            ],
        ):
            extractor.populate_lineage_from_queries()

        assert aggregator.add_observed_query.call_count == 3
        assert report.num_queries_parsed == 1
        assert report.num_queries_parse_failures == 2
