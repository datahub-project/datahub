import pytest
from sqlalchemy.engine.url import make_url

from datahub.ingestion.source.sql.clickhouse import ClickHouseConfig
from datahub.ingestion.source.sql.clickhouse_connection import CLICKHOUSE_CLIENT_NAME


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
    url = make_url(config.get_sql_alchemy_url())
    assert url.drivername == "clickhouse"
    assert url.username == "user"
    assert url.password == "password"
    assert url.host == "host"
    assert url.port == 1111
    assert url.database == "db"
    assert url.query.get("protocol") == "https"
    assert url.query.get("header__User-Agent") == CLICKHOUSE_CLIENT_NAME


def test_clickhouse_uri_native():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "scheme": "clickhouse+native",
        }
    )
    url = make_url(config.get_sql_alchemy_url())
    assert url.drivername == "clickhouse+native"
    assert url.query.get("client_name") == CLICKHOUSE_CLIENT_NAME
    assert "header__User-Agent" not in url.query


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
    url = make_url(config.get_sql_alchemy_url())
    assert url.query.get("secure") == "True"
    assert url.query.get("client_name") == CLICKHOUSE_CLIENT_NAME


def test_clickhouse_uri_default_password():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "host_port": "host:1111",
            "database": "db",
            "scheme": "clickhouse+native",
        }
    )
    url = make_url(config.get_sql_alchemy_url())
    assert url.password is None
    assert url.query.get("client_name") == CLICKHOUSE_CLIENT_NAME


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
    url = make_url(config.get_sql_alchemy_url())
    assert url.query.get("secure") == "True"
    assert url.query.get("client_name") == CLICKHOUSE_CLIENT_NAME


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
    url = make_url(config.get_sql_alchemy_url())
    assert url.query.get("protocol") == "https"
    assert url.query.get("header__User-Agent") == CLICKHOUSE_CLIENT_NAME


def test_clickhouse_uri_preserves_user_supplied_ua():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "database": "db",
            "uri_opts": {"header__User-Agent": "mycorp"},
        }
    )
    url = make_url(config.get_sql_alchemy_url())
    assert url.query.get("header__User-Agent") == "mycorp"


def test_clickhouse_sqlalchemy_uri_gets_client_identity():
    # A hand-written sqlalchemy_uri (the docs-preferred form) is still tagged.
    config = ClickHouseConfig.model_validate(
        {"sqlalchemy_uri": "clickhouse://user:password@host:1111/db"}
    )
    url = make_url(config.get_sql_alchemy_url())
    assert url.query.get("header__User-Agent") == CLICKHOUSE_CLIENT_NAME


# Query log extraction tests


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
