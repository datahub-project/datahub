import pytest


@pytest.mark.integration
def test_snowflake_uri():
    from datahub.ingestion.source.sql.snowflake import SnowflakeConfig

    config = SnowflakeConfig.parse_obj(
        {
            "username": "user",
            "password": "password",
            "host_port": "acctname",
            "database": "demo",
            "warehouse": "COMPUTE_WH",
            "role": "sysadmin",
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "snowflake://user:password@acctname/?warehouse=COMPUTE_WH&role=sysadmin&application=acryl_datahub"
    )
