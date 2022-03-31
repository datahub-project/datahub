def test_snowflake_uri_default_authentication():
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
        == "snowflake://user:password@acctname/?authenticator=SNOWFLAKE&warehouse=COMPUTE_WH&role"
        "=sysadmin&application=acryl_datahub"
    )


def test_snowflake_uri_external_browser_authentication():
    from datahub.ingestion.source.sql.snowflake import SnowflakeConfig

    config = SnowflakeConfig.parse_obj(
        {
            "username": "user",
            "host_port": "acctname",
            "database": "demo",
            "warehouse": "COMPUTE_WH",
            "role": "sysadmin",
            "authentication_type": "EXTERNAL_BROWSER_AUTHENTICATOR",
        }
    )

    assert (
        config.get_sql_alchemy_url()
        == "snowflake://user@acctname/?authenticator=EXTERNALBROWSER&warehouse=COMPUTE_WH&role"
        "=sysadmin&application=acryl_datahub"
    )


def test_snowflake_uri_key_pair_authentication():
    from datahub.ingestion.source.sql.snowflake import SnowflakeConfig

    config = SnowflakeConfig.parse_obj(
        {
            "username": "user",
            "host_port": "acctname",
            "database": "demo",
            "warehouse": "COMPUTE_WH",
            "role": "sysadmin",
            "authentication_type": "KEY_PAIR_AUTHENTICATOR",
            "private_key_path": "/a/random/path",
            "private_key_password": "a_random_password",
        }
    )

    assert (
        config.get_sql_alchemy_url()
        == "snowflake://user@acctname/?authenticator=SNOWFLAKE_JWT&warehouse=COMPUTE_WH&role"
        "=sysadmin&application=acryl_datahub"
    )
