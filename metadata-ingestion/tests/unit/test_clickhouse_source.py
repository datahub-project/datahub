from unittest.mock import MagicMock

from datahub.ingestion.source.sql.clickhouse import (
    ClickHouseConfig,
    _get_column_info,
)


def test_clickhouse_uri_https():
    config = ClickHouseConfig.parse_obj(
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
    config = ClickHouseConfig.parse_obj(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "scheme": "clickhouse+native",
        }
    )
    assert config.get_sql_alchemy_url() == "clickhouse+native://user:password@host:1111"


def test_clickhouse_uri_native_secure():
    config = ClickHouseConfig.parse_obj(
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
    config = ClickHouseConfig.parse_obj(
        {
            "username": "user",
            "host_port": "host:1111",
            "database": "db",
            "scheme": "clickhouse+native",
        }
    )
    assert config.get_sql_alchemy_url() == "clickhouse+native://user@host:1111/db"


def test_clickhouse_uri_native_secure_backward_compatibility():
    config = ClickHouseConfig.parse_obj(
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
    config = ClickHouseConfig.parse_obj(
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


def test_column_info_strips_nul_bytes():
    """Column comments from ClickHouse may contain NUL bytes due to encoding
    issues when reading multibyte characters via the HTTP interface."""
    mock_dialect = MagicMock()
    mock_dialect._get_column_type.return_value = "String"

    result = _get_column_info(
        mock_dialect,
        name="vat_ratio",
        format_type="String",
        comment="부가\ufffd\x00치세비율",
    )
    assert result["comment"] == "부가\ufffd치세비율"
    assert "\x00" not in result["comment"]


def test_column_info_preserves_normal_comment():
    """Normal column comments should be preserved as-is."""
    mock_dialect = MagicMock()
    mock_dialect._get_column_type.return_value = "String"

    result = _get_column_info(
        mock_dialect,
        name="vat_ratio",
        format_type="String",
        comment="부가가치세비율",
    )
    assert result["comment"] == "부가가치세비율"
