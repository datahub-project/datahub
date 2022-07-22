from unittest.mock import patch

import pytest

from datahub.configuration.common import ConfigurationError, OauthConfiguration
from datahub.ingestion.source.sql.snowflake import SnowflakeConfig, SnowflakeSource


def test_snowflake_source_throws_error_on_account_id_missing():
    with pytest.raises(ConfigurationError):
        SnowflakeConfig.parse_obj(
            {
                "username": "user",
                "password": "password",
            }
        )


def test_snowflake_throws_error_on_client_id_missing_if_using_oauth():
    oauth_dict = {
        "provider": "microsoft",
        "scopes": ["https://microsoft.com/f4b353d5-ef8d/.default"],
        "client_secret": "6Hb9apkbc6HD7",
        "authority_url": "https://login.microsoftonline.com/yourorganisation.com",
    }
    # assert that this is a valid oauth config on its own
    OauthConfiguration.parse_obj(oauth_dict)
    with pytest.raises(ValueError):
        SnowflakeConfig.parse_obj(
            {
                "account_id": "test",
                "authentication_type": "OAUTH_AUTHENTICATOR",
                "oauth_config": oauth_dict,
            }
        )


def test_snowflake_throws_error_on_client_secret_missing_if_use_certificate_is_false():
    oauth_dict = {
        "client_id": "882e9831-7ea51cb2b954",
        "provider": "microsoft",
        "scopes": ["https://microsoft.com/f4b353d5-ef8d/.default"],
        "use_certificate": False,
        "authority_url": "https://login.microsoftonline.com/yourorganisation.com",
    }
    OauthConfiguration.parse_obj(oauth_dict)

    with pytest.raises(ValueError):
        SnowflakeConfig.parse_obj(
            {
                "account_id": "test",
                "authentication_type": "OAUTH_AUTHENTICATOR",
                "oauth_config": oauth_dict,
            }
        )


def test_snowflake_throws_error_on_encoded_oauth_private_key_missing_if_use_certificate_is_true():
    oauth_dict = {
        "client_id": "882e9831-7ea51cb2b954",
        "provider": "microsoft",
        "scopes": ["https://microsoft.com/f4b353d5-ef8d/.default"],
        "use_certificate": True,
        "authority_url": "https://login.microsoftonline.com/yourorganisation.com",
        "encoded_oauth_public_key": "fkdsfhkshfkjsdfiuwrwfkjhsfskfhksjf==",
    }
    OauthConfiguration.parse_obj(oauth_dict)
    with pytest.raises(ValueError):
        SnowflakeConfig.parse_obj(
            {
                "account_id": "test",
                "authentication_type": "OAUTH_AUTHENTICATOR",
                "oauth_config": oauth_dict,
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


def test_options_contain_connect_args():
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
    connect_args = config.get_options().get("connect_args")
    assert connect_args is not None


@patch("snowflake.connector.connect")
def test_test_connection_failure(mock_connect):
    mock_connect.side_effect = Exception("Failed to connect to snowflake")
    config = {
        "username": "user",
        "password": "password",
        "account_id": "missing",
        "warehouse": "COMPUTE_WH",
        "role": "sysadmin",
    }
    report = SnowflakeSource.test_connection(config)
    assert report is not None
    assert report.basic_connectivity
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason
    assert "Failed to connect to snowflake" in report.basic_connectivity.failure_reason


@patch("snowflake.connector.connect")
def test_test_connection_basic_success(mock_connect):
    config = {
        "username": "user",
        "password": "password",
        "account_id": "missing",
        "warehouse": "COMPUTE_WH",
        "role": "sysadmin",
    }
    report = SnowflakeSource.test_connection(config)
    assert report is not None
    assert report.basic_connectivity
    assert report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason is None
