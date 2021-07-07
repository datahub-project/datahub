from datahub.ingestion.source.snowflake import SnowflakeConfig


def test_snowflake_uri():
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
        == "snowflake://user:password@acctname/?warehouse=COMPUTE_WH&role=sysadmin"
    )
