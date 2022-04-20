import pytest

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.sql.snowflake import SnowflakeConfig


def test_snowflake_source_throws_error_on_account_id_missing():
    with pytest.raises(ConfigurationError):
        SnowflakeConfig.parse_obj(
            {
                "username": "user",
                "password": "password",
            }
        )


def test_account_id_is_added_when_host_port_is_present():
    config = SnowflakeConfig.parse_obj(
        {
            "username": "user",
            "password": "password",
            "host_port": "acctname",
            "database_pattern": {"allow": {"^demo$"}},
            "warehouse": "COMPUTE_WH",
            "role": "sysadmin",
        }
    )
    assert config.account_id == "acctname"


def test_snowflake_uri_default_authentication():

    config = SnowflakeConfig.parse_obj(
        {
            "username": "user",
            "password": "password",
            "account_id": "acctname",
            "database_pattern": {"allow": {"^demo$"}},
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

    config = SnowflakeConfig.parse_obj(
        {
            "username": "user",
            "account_id": "acctname",
            "database_pattern": {"allow": {"^demo$"}},
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

    config = SnowflakeConfig.parse_obj(
        {
            "username": "user",
            "account_id": "acctname",
            "database_pattern": {"allow": {"^demo$"}},
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
