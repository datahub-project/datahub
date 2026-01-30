from unittest.mock import MagicMock

from datahub.ingestion.source.sql.clickhouse import (
    ClickHouseConfig,
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
