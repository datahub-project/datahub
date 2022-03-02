import pytest


@pytest.mark.integration
def test_clickhouse_uri_https():
    from datahub.ingestion.source.sql.clickhouse import ClickHouseConfig

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


@pytest.mark.integration
def test_clickhouse_uri_native():
    from datahub.ingestion.source.sql.clickhouse import ClickHouseConfig

    config = ClickHouseConfig.parse_obj(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "scheme": "clickhouse+native",
        }
    )
    assert config.get_sql_alchemy_url() == "clickhouse+native://user:password@host:1111"


@pytest.mark.integration
def test_clickhouse_uri_native_secure():
    from datahub.ingestion.source.sql.clickhouse import ClickHouseConfig

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
        == "clickhouse+native://user:password@host:1111/db?secure=true"
    )
