from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.sql.clickhouse import (
    ClickHouseConfig,
    ClickHouseSource,
    get_view_definition,
)


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


# Query log extraction tests


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
    assert set(config.query_log_deny_usernames) == {
        "system",
        "default",
        "admin-user",
        "test_user123",
    }


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
    """Test that is_temp_table correctly identifies temporary tables."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
        }
    )

    # Tables that should match temporary patterns
    assert config.is_temp_table("_temp_table")
    assert config.is_temp_table("db.tmp_staging")
    assert config.is_temp_table("db.temp_data")
    assert config.is_temp_table("db._inner_mv")

    # Tables that should NOT match
    assert not config.is_temp_table("normal_table")
    assert not config.is_temp_table("db.regular_table")
    assert not config.is_temp_table("my_db.production_table")


def test_is_temp_table_custom_patterns():
    """Test is_temp_table with custom patterns."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "temporary_tables_pattern": [
                r".*\.staging_.*",  # Any table with staging_ prefix
                r"^test_.*",  # Tables starting with test_
            ],
        }
    )

    assert config.is_temp_table("db.staging_data")
    assert config.is_temp_table("test_table")
    # Default patterns no longer match with custom patterns
    assert not config.is_temp_table("_temp_table")


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

        expected = """
SELECT
    query_id,
    query,
    query_kind,
    user,
    event_time,
    query_duration_ms,
    read_rows,
    written_rows,
    current_database,
    normalized_query_hash
FROM system.query_log
WHERE type = 'QueryFinish'
  AND is_initial_query = 1
  AND event_time >= '2024-01-01 00:00:00'
  AND event_time < '2024-01-08 00:00:00'
  AND query_kind IN ('Insert', 'Create', 'Select')
  AND 1=1
  AND query NOT LIKE '%system.%'
ORDER BY event_time ASC
"""
        assert query == expected


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

        expected = """
SELECT
    query_id,
    query,
    query_kind,
    user,
    event_time,
    query_duration_ms,
    read_rows,
    written_rows,
    current_database,
    normalized_query_hash
FROM system.query_log
WHERE type = 'QueryFinish'
  AND is_initial_query = 1
  AND event_time >= '2024-01-01 00:00:00'
  AND event_time < '2024-01-08 00:00:00'
  AND query_kind IN ('Insert', 'Create', 'Select')
  AND user != 'system' AND user != 'default'
  AND query NOT LIKE '%system.%'
ORDER BY event_time ASC
"""
        assert query == expected


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

        expected = """
SELECT
    query_id,
    query,
    query_kind,
    user,
    event_time,
    query_duration_ms,
    read_rows,
    written_rows,
    current_database,
    normalized_query_hash
FROM monitoring.query_log_view
WHERE type = 'QueryFinish'
  AND is_initial_query = 1
  AND event_time >= '2024-01-01 00:00:00'
  AND event_time < '2024-01-08 00:00:00'
  AND query_kind IN ('Insert', 'Create', 'Select')
  AND 1=1
  AND query NOT LIKE '%system.%'
ORDER BY event_time ASC
"""
        assert query == expected
