from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.sql.clickhouse.clickhouse_queries import (
    CLICKHOUSE_DATETIME_FORMAT,
    DEFAULT_TEMP_TABLES_PATTERNS,
    ClickHouseQueriesExtractor,
    ClickHouseQueriesExtractorConfig,
    ClickHouseQueriesSourceConfig,
)


class TestClickHouseQueriesExtractorConfig:
    def test_default_config(self):
        config = ClickHouseQueriesExtractorConfig()

        assert config.include_lineage is True
        assert config.include_queries is True
        assert config.include_usage_statistics is True
        assert config.include_operations is True
        assert config.query_log_table == "system.query_log"
        assert config.top_n_queries == 10

    def test_temporary_tables_patterns(self):
        config = ClickHouseQueriesExtractorConfig()
        patterns = config._compiled_temporary_tables_pattern

        assert len(patterns) == len(DEFAULT_TEMP_TABLES_PATTERNS)

        # Test pattern matching
        assert patterns[0].match("_temp_table")  # ^_.*
        assert patterns[1].match("db.tmp_staging")  # .*\.tmp_.*
        assert patterns[2].match("db.temp_data")  # .*\.temp_.*
        assert patterns[3].match("db._inner_mv")  # .*\._inner.*

        # Non-matching cases
        assert not patterns[0].match("normal_table")
        assert not patterns[1].match("db.regular_table")

    def test_custom_temp_patterns(self):
        config = ClickHouseQueriesExtractorConfig(
            temporary_tables_pattern=[r"^staging_.*", r".*_backup$"]
        )
        patterns = config._compiled_temporary_tables_pattern

        assert len(patterns) == 2
        assert patterns[0].match("staging_data")
        assert patterns[1].match("users_backup")
        assert not patterns[0].match("production_data")


class TestClickHouseQueriesSourceConfig:
    def test_inherits_clickhouse_config(self):
        config = ClickHouseQueriesSourceConfig(
            host_port="localhost:9000",
            username="test_user",
            include_lineage=False,
        )

        assert config.host_port == "localhost:9000"
        assert config.username == "test_user"
        assert config.include_lineage is False
        assert config.include_usage_statistics is True  # default


class TestClickHouseQueriesExtractor:
    @pytest.fixture
    def extractor_config(self):
        return ClickHouseQueriesExtractorConfig(
            pushdown_deny_usernames=["system", "default"],
        )

    @pytest.fixture
    def connection_config(self):
        return ClickHouseQueriesSourceConfig(
            host_port="localhost:8123",
            username="test",
        )

    def test_is_temp_table(self, extractor_config, connection_config):
        report = MagicMock()
        extractor = ClickHouseQueriesExtractor(
            config=extractor_config,
            connection_config=connection_config,
            structured_report=report,
        )

        # Test temp table detection
        assert extractor._is_temp_table("_temp_data")
        assert extractor._is_temp_table("db.tmp_staging")
        assert extractor._is_temp_table("db.temp_cache")
        assert extractor._is_temp_table("db._inner_mv_table")

        # Test non-temp tables
        assert not extractor._is_temp_table("production.users")
        assert not extractor._is_temp_table("analytics.events")

    def test_build_query_log_query_basic(self, extractor_config, connection_config):
        report = MagicMock()
        extractor = ClickHouseQueriesExtractor(
            config=extractor_config,
            connection_config=connection_config,
            structured_report=report,
        )

        query = extractor._build_query_log_query()

        # Check essential parts of the query
        assert "system.query_log" in query
        assert "type = 'QueryFinish'" in query
        assert "is_initial_query = 1" in query
        assert "'Insert'" in query
        assert "'Create'" in query
        assert "'Select'" in query
        assert "user != 'system'" in query
        assert "user != 'default'" in query

    def test_build_query_log_query_custom_table(self, connection_config):
        config = ClickHouseQueriesExtractorConfig(
            query_log_table="custom_db.query_log_view",
        )
        report = MagicMock()
        extractor = ClickHouseQueriesExtractor(
            config=config,
            connection_config=connection_config,
            structured_report=report,
        )

        query = extractor._build_query_log_query()
        assert "custom_db.query_log_view" in query

    def test_parse_query_log_row(self, extractor_config, connection_config):
        report = MagicMock()
        extractor = ClickHouseQueriesExtractor(
            config=extractor_config,
            connection_config=connection_config,
            structured_report=report,
        )

        row = {
            "query_id": "abc123",
            "query": "INSERT INTO target SELECT * FROM source",
            "query_kind": "Insert",
            "user": "analyst",
            "event_time": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            "current_database": "production",
            "normalized_query_hash": "12345",
        }

        result = extractor._parse_query_log_row(row)

        assert result is not None
        assert result.query == "INSERT INTO target SELECT * FROM source"
        assert result.session_id == "abc123"
        assert result.user is not None
        assert str(result.user) == "urn:li:corpuser:analyst"
        assert result.default_db == "production"
        assert result.query_hash == "12345"

    def test_parse_query_log_row_select(self, extractor_config, connection_config):
        report = MagicMock()
        extractor = ClickHouseQueriesExtractor(
            config=extractor_config,
            connection_config=connection_config,
            structured_report=report,
        )

        row = {
            "query_id": "xyz789",
            "query": "SELECT * FROM users",
            "query_kind": "Select",
            "user": "reader",
            "event_time": datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc),
            "current_database": "analytics",
            "normalized_query_hash": "67890",
        }

        result = extractor._parse_query_log_row(row)

        assert result is not None
        assert extractor.report.num_usage_queries == 1
        assert extractor.report.num_lineage_queries == 0

    def test_parse_query_log_row_missing_user(
        self, extractor_config, connection_config
    ):
        report = MagicMock()
        extractor = ClickHouseQueriesExtractor(
            config=extractor_config,
            connection_config=connection_config,
            structured_report=report,
        )

        row = {
            "query_id": "abc123",
            "query": "SELECT 1",
            "query_kind": "Select",
            "user": "",
            "event_time": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            "current_database": "default",
            "normalized_query_hash": "11111",
        }

        result = extractor._parse_query_log_row(row)

        assert result is not None
        assert result.user is None


class TestDateTimeFormat:
    def test_datetime_format(self):
        dt = datetime(2024, 1, 15, 10, 30, 45)
        formatted = dt.strftime(CLICKHOUSE_DATETIME_FORMAT)
        assert formatted == "2024-01-15 10:30:45"
