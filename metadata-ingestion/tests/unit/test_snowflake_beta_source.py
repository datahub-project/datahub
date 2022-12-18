from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from datahub.configuration.common import ConfigurationError, OauthConfiguration
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_utils import SnowflakeCloudProvider
from datahub.ingestion.source.snowflake.snowflake_v2 import SnowflakeV2Source


def test_snowflake_source_throws_error_on_account_id_missing():
    with pytest.raises(ConfigurationError):
        SnowflakeV2Config.parse_obj(
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
        SnowflakeV2Config.parse_obj(
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
        SnowflakeV2Config.parse_obj(
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
        SnowflakeV2Config.parse_obj(
            {
                "account_id": "test",
                "authentication_type": "OAUTH_AUTHENTICATOR",
                "oauth_config": oauth_dict,
            }
        )


def test_account_id_is_added_when_host_port_is_present():
    config = SnowflakeV2Config.parse_obj(
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


def test_account_id_with_snowflake_host_suffix():
    config = SnowflakeV2Config.parse_obj(
        {
            "username": "user",
            "password": "password",
            "account_id": "https://acctname.snowflakecomputing.com",
            "database_pattern": {"allow": {"^demo$"}},
            "warehouse": "COMPUTE_WH",
            "role": "sysadmin",
        }
    )
    config.account_id == "acctname"


def test_snowflake_uri_default_authentication():

    config = SnowflakeV2Config.parse_obj(
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

    config = SnowflakeV2Config.parse_obj(
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

    config = SnowflakeV2Config.parse_obj(
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
    config = SnowflakeV2Config.parse_obj(
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


def test_snowflake_config_with_view_lineage_no_table_lineage_throws_error():
    with pytest.raises(ValidationError):
        SnowflakeV2Config.parse_obj(
            {
                "username": "user",
                "password": "password",
                "host_port": "acctname",
                "database_pattern": {"allow": {"^demo$"}},
                "warehouse": "COMPUTE_WH",
                "role": "sysadmin",
                "include_view_lineage": True,
                "include_table_lineage": False,
            }
        )


def test_snowflake_config_with_column_lineage_no_table_lineage_throws_error():
    with pytest.raises(ValidationError):
        SnowflakeV2Config.parse_obj(
            {
                "username": "user",
                "password": "password",
                "host_port": "acctname",
                "database_pattern": {"allow": {"^demo$"}},
                "warehouse": "COMPUTE_WH",
                "role": "sysadmin",
                "include_column_lineage": True,
                "include_table_lineage": False,
            }
        )


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
    report = SnowflakeV2Source.test_connection(config)
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
    report = SnowflakeV2Source.test_connection(config)
    assert report is not None
    assert report.basic_connectivity
    assert report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason is None


def setup_mock_connect(mock_connect, query_results=None):
    def default_query_results(query):
        if query == "select current_role()":
            return [("TEST_ROLE",)]
        elif query == "select current_secondary_roles()":
            return [('{"roles":"","value":""}',)]
        elif query == "select current_warehouse()":
            return [("TEST_WAREHOUSE")]
        # Unreachable code
        raise Exception()

    connection_mock = MagicMock()
    cursor_mock = MagicMock()
    cursor_mock.execute.side_effect = (
        query_results if query_results is not None else default_query_results
    )
    connection_mock.cursor.return_value = cursor_mock
    mock_connect.return_value = connection_mock


@patch("snowflake.connector.connect")
def test_test_connection_no_warehouse(mock_connect):
    def query_results(query):
        if query == "select current_role()":
            return [("TEST_ROLE",)]
        elif query == "select current_secondary_roles()":
            return [('{"roles":"","value":""}',)]
        elif query == "select current_warehouse()":
            return [(None,)]
        elif query == 'show grants to role "TEST_ROLE"':
            return [
                ("", "USAGE", "DATABASE", "DB1"),
                ("", "USAGE", "SCHEMA", "DB1.SCHEMA1"),
                ("", "REFERENCES", "TABLE", "DB1.SCHEMA1.TABLE1"),
            ]
        elif query == 'show grants to role "PUBLIC"':
            return []
        # Unreachable code
        raise Exception()

    config = {
        "username": "user",
        "password": "password",
        "account_id": "missing",
        "warehouse": "COMPUTE_WH",
        "role": "sysadmin",
    }
    setup_mock_connect(mock_connect, query_results)
    report = SnowflakeV2Source.test_connection(config)
    assert report is not None
    assert report.basic_connectivity
    assert report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason is None

    assert report.capability_report
    assert report.capability_report[SourceCapability.CONTAINERS].capable
    assert not report.capability_report[SourceCapability.SCHEMA_METADATA].capable
    failure_reason = report.capability_report[
        SourceCapability.SCHEMA_METADATA
    ].failure_reason
    assert failure_reason

    assert "Current role does not have permissions to use warehouse" in failure_reason


@patch("snowflake.connector.connect")
def test_test_connection_capability_schema_failure(mock_connect):
    def query_results(query):
        if query == "select current_role()":
            return [("TEST_ROLE",)]
        elif query == "select current_secondary_roles()":
            return [('{"roles":"","value":""}',)]
        elif query == "select current_warehouse()":
            return [("TEST_WAREHOUSE",)]
        elif query == 'show grants to role "TEST_ROLE"':
            return [("", "USAGE", "DATABASE", "DB1")]
        elif query == 'show grants to role "PUBLIC"':
            return []
        # Unreachable code
        raise Exception()

    setup_mock_connect(mock_connect, query_results)

    config = {
        "username": "user",
        "password": "password",
        "account_id": "missing",
        "warehouse": "COMPUTE_WH",
        "role": "sysadmin",
    }
    report = SnowflakeV2Source.test_connection(config)
    assert report is not None
    assert report.basic_connectivity
    assert report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason is None
    assert report.capability_report

    assert report.capability_report[SourceCapability.CONTAINERS].capable
    assert not report.capability_report[SourceCapability.SCHEMA_METADATA].capable
    assert (
        report.capability_report[SourceCapability.SCHEMA_METADATA].failure_reason
        is not None
    )


@patch("snowflake.connector.connect")
def test_test_connection_capability_schema_success(mock_connect):
    def query_results(query):
        if query == "select current_role()":
            return [("TEST_ROLE",)]
        elif query == "select current_secondary_roles()":
            return [('{"roles":"","value":""}',)]
        elif query == "select current_warehouse()":
            return [("TEST_WAREHOUSE")]
        elif query == 'show grants to role "TEST_ROLE"':
            return [
                ["", "USAGE", "DATABASE", "DB1"],
                ["", "USAGE", "SCHEMA", "DB1.SCHEMA1"],
                ["", "REFERENCES", "TABLE", "DB1.SCHEMA1.TABLE1"],
            ]
        elif query == 'show grants to role "PUBLIC"':
            return []
        # Unreachable code
        raise Exception()

    setup_mock_connect(mock_connect, query_results)

    config = {
        "username": "user",
        "password": "password",
        "account_id": "missing",
        "warehouse": "COMPUTE_WH",
        "role": "sysadmin",
    }
    report = SnowflakeV2Source.test_connection(config)

    assert report is not None
    assert report.basic_connectivity
    assert report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason is None
    assert report.capability_report

    assert report.capability_report[SourceCapability.CONTAINERS].capable
    assert report.capability_report[SourceCapability.SCHEMA_METADATA].capable
    assert report.capability_report[SourceCapability.DESCRIPTIONS].capable


@patch("snowflake.connector.connect")
def test_test_connection_capability_all_success(mock_connect):
    def query_results(query):
        if query == "select current_role()":
            return [("TEST_ROLE",)]
        elif query == "select current_secondary_roles()":
            return [('{"roles":"","value":""}',)]
        elif query == "select current_warehouse()":
            return [("TEST_WAREHOUSE")]
        elif query == 'show grants to role "TEST_ROLE"':
            return [
                ("", "USAGE", "DATABASE", "DB1"),
                ("", "USAGE", "SCHEMA", "DB1.SCHEMA1"),
                ("", "SELECT", "TABLE", "DB1.SCHEMA1.TABLE1"),
                ("", "USAGE", "ROLE", "TEST_USAGE_ROLE"),
            ]
        elif query == 'show grants to role "PUBLIC"':
            return []
        elif query == 'show grants to role "TEST_USAGE_ROLE"':
            return [
                ["", "USAGE", "DATABASE", "SNOWFLAKE"],
                ["", "USAGE", "SCHEMA", "ACCOUNT_USAGE"],
                ["", "USAGE", "VIEW", "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"],
                ["", "USAGE", "VIEW", "SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY"],
                ["", "USAGE", "VIEW", "SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES"],
            ]
        # Unreachable code
        raise Exception()

    setup_mock_connect(mock_connect, query_results)

    config = {
        "username": "user",
        "password": "password",
        "account_id": "missing",
        "warehouse": "COMPUTE_WH",
        "role": "sysadmin",
    }
    report = SnowflakeV2Source.test_connection(config)
    assert report is not None
    assert report.basic_connectivity
    assert report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason is None
    assert report.capability_report

    assert report.capability_report[SourceCapability.CONTAINERS].capable
    assert report.capability_report[SourceCapability.SCHEMA_METADATA].capable
    assert report.capability_report[SourceCapability.DATA_PROFILING].capable
    assert report.capability_report[SourceCapability.DESCRIPTIONS].capable
    assert report.capability_report[SourceCapability.LINEAGE_COARSE].capable


def test_aws_cloud_region_from_snowflake_region_id():
    (
        cloud,
        cloud_region_id,
    ) = SnowflakeV2Source.get_cloud_region_from_snowflake_region_id("aws_ca_central_1")

    assert cloud == SnowflakeCloudProvider.AWS
    assert cloud_region_id == "ca-central-1"

    (
        cloud,
        cloud_region_id,
    ) = SnowflakeV2Source.get_cloud_region_from_snowflake_region_id("aws_us_east_1_gov")

    assert cloud == SnowflakeCloudProvider.AWS
    assert cloud_region_id == "us-east-1"


def test_google_cloud_region_from_snowflake_region_id():
    (
        cloud,
        cloud_region_id,
    ) = SnowflakeV2Source.get_cloud_region_from_snowflake_region_id("gcp_europe_west2")

    assert cloud == SnowflakeCloudProvider.GCP
    assert cloud_region_id == "europe-west2"


def test_azure_cloud_region_from_snowflake_region_id():
    (
        cloud,
        cloud_region_id,
    ) = SnowflakeV2Source.get_cloud_region_from_snowflake_region_id(
        "azure_switzerlandnorth"
    )

    assert cloud == SnowflakeCloudProvider.AZURE
    assert cloud_region_id == "switzerlandnorth"

    (
        cloud,
        cloud_region_id,
    ) = SnowflakeV2Source.get_cloud_region_from_snowflake_region_id(
        "azure_centralindia"
    )

    assert cloud == SnowflakeCloudProvider.AZURE
    assert cloud_region_id == "central-india.azure"


def test_unknown_cloud_region_from_snowflake_region_id():
    with pytest.raises(Exception) as e:
        SnowflakeV2Source.get_cloud_region_from_snowflake_region_id(
            "somecloud_someregion"
        )
    assert "Unknown snowflake region" in str(e)
