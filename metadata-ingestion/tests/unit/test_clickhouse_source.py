from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.sql.clickhouse import (
    ClickHouseConfig,
    get_view_definition,
)
from datahub.ingestion.source.sql.clickhouse.source import ClickHouseSource


def test_clickhouse_uri_https():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "database": "db",
            "uri_opts": {"protocol": "https"},
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "clickhouse://user:password@host:1111/db?protocol=https"
    )


def test_clickhouse_uri_native():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "scheme": "clickhouse+native",
        }
    )
    assert config.get_sql_alchemy_url() == "clickhouse+native://user:password@host:1111"


def test_clickhouse_uri_native_secure():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "database": "db",
            "scheme": "clickhouse+native",
            "uri_opts": {"secure": True},
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "clickhouse+native://user:password@host:1111/db?secure=True"
    )


def test_clickhouse_uri_default_password():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "host_port": "host:1111",
            "database": "db",
            "scheme": "clickhouse+native",
        }
    )
    assert config.get_sql_alchemy_url() == "clickhouse+native://user@host:1111/db"


def test_clickhouse_uri_native_secure_backward_compatibility():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "database": "db",
            "scheme": "clickhouse+native",
            "secure": True,
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "clickhouse+native://user:password@host:1111/db?secure=True"
    )


def test_clickhouse_uri_https_backward_compatibility():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "database": "db",
            "protocol": "https",
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "clickhouse://user:password@host:1111/db?protocol=https"
    )


def test_get_view_definition_with_schema():
    """Test that get_view_definition extracts view SQL from system.tables."""
    mock_dialect = MagicMock()
    mock_connection = MagicMock()

    expected_view_sql = (
        "CREATE VIEW db1.test_view AS SELECT col1, col2 FROM db1.source_table"
    )
    mock_connection.execute.return_value.fetchone.return_value = (expected_view_sql,)

    result = get_view_definition(
        mock_dialect, mock_connection, "test_view", schema="db1"
    )

    assert result == expected_view_sql
    mock_connection.execute.assert_called_once()


def test_get_view_definition_returns_empty_when_not_found():
    """Test that get_view_definition returns empty string when view not found."""
    mock_dialect = MagicMock()
    mock_connection = MagicMock()

    mock_connection.execute.return_value.fetchone.return_value = None

    result = get_view_definition(
        mock_dialect, mock_connection, "nonexistent_view", schema="db1"
    )

    assert result == ""


def test_get_view_definition_without_schema():
    """Test get_view_definition works without schema."""
    mock_dialect = MagicMock()
    mock_connection = MagicMock()

    expected_view_sql = "CREATE VIEW test_view AS SELECT * FROM source"
    mock_connection.execute.return_value.fetchone.return_value = (expected_view_sql,)

    result = get_view_definition(
        mock_dialect, mock_connection, "test_view", schema=None
    )

    assert result == expected_view_sql


# Query log extraction tests


def test_query_log_config_defaults():
    """Test that query log config options have correct defaults."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
        }
    )

    assert config.include_query_log_lineage is False
    assert config.include_usage_statistics is False
    assert config.include_query_log_operations is False
    assert config.query_log_table == "system.query_log"
    assert config.top_n_queries == 10


def test_query_log_config_enabled():
    """Test that query log config options can be enabled."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "include_query_log_lineage": True,
            "include_usage_statistics": True,
            "query_log_table": "custom.query_log_view",
            "query_log_deny_usernames": ["system", "default"],
        }
    )

    assert config.include_query_log_lineage is True
    assert config.include_usage_statistics is True
    assert config.query_log_table == "custom.query_log_view"
    assert config.query_log_deny_usernames == ["system", "default"]


def test_temporary_tables_pattern():
    """Test that temporary tables patterns are compiled correctly."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
        }
    )

    patterns = config._compiled_temporary_tables_pattern

    # Default patterns
    assert len(patterns) == 4

    # Test pattern matching
    assert patterns[0].match("_temp_table")  # ^_.*
    assert patterns[1].match("db.tmp_staging")  # .*\.tmp_.*
    assert patterns[2].match("db.temp_data")  # .*\.temp_.*
    assert patterns[3].match("db._inner_mv")  # .*\._inner.*

    # Non-matching cases
    assert not patterns[0].match("normal_table")
    assert not patterns[1].match("db.regular_table")


def test_query_log_table_validation_valid():
    """Test that valid query_log_table values are accepted."""
    # Standard table name
    config = ClickHouseConfig.model_validate(
        {"host_port": "localhost:8123", "query_log_table": "system.query_log"}
    )
    assert config.query_log_table == "system.query_log"

    # Custom view
    config = ClickHouseConfig.model_validate(
        {"host_port": "localhost:8123", "query_log_table": "monitoring.query_log_view"}
    )
    assert config.query_log_table == "monitoring.query_log_view"

    # Simple table name
    config = ClickHouseConfig.model_validate(
        {"host_port": "localhost:8123", "query_log_table": "query_log"}
    )
    assert config.query_log_table == "query_log"


def test_query_log_table_validation_invalid():
    """Test that invalid query_log_table values are rejected (SQL injection prevention)."""

    # SQL injection attempt with semicolon
    with pytest.raises(ValueError, match="Invalid query_log_table"):
        ClickHouseConfig.model_validate(
            {
                "host_port": "localhost:8123",
                "query_log_table": "system.query_log; DROP TABLE users;--",
            }
        )

    # SQL injection with quotes
    with pytest.raises(ValueError, match="Invalid query_log_table"):
        ClickHouseConfig.model_validate(
            {"host_port": "localhost:8123", "query_log_table": "system'--"}
        )

    # SQL injection with parentheses
    with pytest.raises(ValueError, match="Invalid query_log_table"):
        ClickHouseConfig.model_validate(
            {"host_port": "localhost:8123", "query_log_table": "system.query_log()"}
        )


def test_query_log_deny_usernames_validation_valid():
    """Test that valid usernames are accepted."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "query_log_deny_usernames": [
                "system",
                "default",
                "admin-user",
                "test_user123",
            ],
        }
    )
    assert config.query_log_deny_usernames == [
        "system",
        "default",
        "admin-user",
        "test_user123",
    ]


def test_query_log_deny_usernames_validation_invalid():
    """Test that invalid usernames are rejected (SQL injection prevention)."""

    # SQL injection attempt
    with pytest.raises(ValueError, match="Invalid username"):
        ClickHouseConfig.model_validate(
            {
                "host_port": "localhost:8123",
                "query_log_deny_usernames": ["system'; DROP TABLE users;--"],
            }
        )

    # Username with quotes
    with pytest.raises(ValueError, match="Invalid username"):
        ClickHouseConfig.model_validate(
            {"host_port": "localhost:8123", "query_log_deny_usernames": ["user'name"]}
        )


# Query log extraction method tests


def test_is_temp_table():
    """Test that _is_temp_table correctly identifies temporary tables."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
        }
    )

    # Create a mock source with the config
    with patch.object(ClickHouseSource, "__init__", lambda x, y, z: None):
        source = ClickHouseSource.__new__(ClickHouseSource)
        source.config = config

        # Tables that should match temporary patterns
        assert source._is_temp_table("_temp_table")
        assert source._is_temp_table("db.tmp_staging")
        assert source._is_temp_table("db.temp_data")
        assert source._is_temp_table("db._inner_mv")

        # Tables that should NOT match
        assert not source._is_temp_table("normal_table")
        assert not source._is_temp_table("db.regular_table")
        assert not source._is_temp_table("my_db.production_table")


def test_is_temp_table_custom_patterns():
    """Test _is_temp_table with custom patterns."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "temporary_tables_pattern": [
                r".*\.staging_.*",  # Any table with staging_ prefix
                r"^test_.*",  # Tables starting with test_
            ],
        }
    )

    with patch.object(ClickHouseSource, "__init__", lambda x, y, z: None):
        source = ClickHouseSource.__new__(ClickHouseSource)
        source.config = config

        assert source._is_temp_table("db.staging_data")
        assert source._is_temp_table("test_table")
        # Default patterns no longer match with custom patterns
        assert not source._is_temp_table("_temp_table")


def test_build_query_log_query_basic():
    """Test _build_query_log_query generates valid SQL."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "include_query_log_lineage": True,
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-08T00:00:00Z",
        }
    )

    with patch.object(ClickHouseSource, "__init__", lambda x, y, z: None):
        source = ClickHouseSource.__new__(ClickHouseSource)
        source.config = config

        query = source._build_query_log_query()

        # Verify basic query structure
        assert "SELECT" in query
        assert "query_id" in query
        assert "query" in query
        assert "FROM system.query_log" in query
        assert "type = 'QueryFinish'" in query
        assert "is_initial_query = 1" in query
        assert "2024-01-01" in query
        assert "2024-01-08" in query


def test_build_query_log_query_with_deny_usernames():
    """Test _build_query_log_query includes user filter clause."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "include_query_log_lineage": True,
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-08T00:00:00Z",
            "query_log_deny_usernames": ["system", "default"],
        }
    )

    with patch.object(ClickHouseSource, "__init__", lambda x, y, z: None):
        source = ClickHouseSource.__new__(ClickHouseSource)
        source.config = config

        query = source._build_query_log_query()

        # Verify user filters are included
        assert "user != 'system'" in query
        assert "user != 'default'" in query


def test_build_query_log_query_custom_table():
    """Test _build_query_log_query uses custom query_log_table."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "include_query_log_lineage": True,
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-08T00:00:00Z",
            "query_log_table": "monitoring.query_log_view",
        }
    )

    with patch.object(ClickHouseSource, "__init__", lambda x, y, z: None):
        source = ClickHouseSource.__new__(ClickHouseSource)
        source.config = config

        query = source._build_query_log_query()

        # Verify custom table is used
        assert "FROM monitoring.query_log_view" in query
        assert "FROM system.query_log" not in query


def test_parse_query_log_row():
    """Test _parse_query_log_row correctly parses a query log entry."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
        }
    )

    with patch.object(ClickHouseSource, "__init__", lambda x, y, z: None):
        source = ClickHouseSource.__new__(ClickHouseSource)
        source.config = config
        source.report = MagicMock()

        row = {
            "query_id": "abc-123",
            "query": "INSERT INTO target SELECT * FROM source",
            "query_kind": "Insert",
            "user": "analyst",
            "event_time": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            "query_duration_ms": 150,
            "read_rows": 1000,
            "written_rows": 1000,
            "current_database": "production",
            "normalized_query_hash": 12345678,
        }

        result = source._parse_query_log_row(row)

        assert result is not None
        assert result.query == "INSERT INTO target SELECT * FROM source"
        assert result.session_id == "abc-123"
        assert result.default_db == "production"
        assert result.query_hash == "12345678"
        assert str(result.user) == "urn:li:corpuser:analyst"


def test_parse_query_log_row_minimal():
    """Test _parse_query_log_row with minimal required fields."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
        }
    )

    with patch.object(ClickHouseSource, "__init__", lambda x, y, z: None):
        source = ClickHouseSource.__new__(ClickHouseSource)
        source.config = config
        source.report = MagicMock()

        row = {
            "query": "SELECT 1",
            "event_time": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
        }

        result = source._parse_query_log_row(row)

        assert result is not None
        assert result.query == "SELECT 1"
        assert result.user is None
        assert result.session_id is None


def test_parse_query_log_row_handles_naive_datetime():
    """Test _parse_query_log_row handles naive datetime objects."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
        }
    )

    with patch.object(ClickHouseSource, "__init__", lambda x, y, z: None):
        source = ClickHouseSource.__new__(ClickHouseSource)
        source.config = config
        source.report = MagicMock()

        # Naive datetime (no timezone)
        row = {
            "query": "SELECT 1",
            "event_time": datetime(2024, 1, 15, 10, 30, 0),  # naive datetime
        }

        result = source._parse_query_log_row(row)

        # Should still work, timestamp may be treated as local time
        assert result is not None
        assert result.query == "SELECT 1"
