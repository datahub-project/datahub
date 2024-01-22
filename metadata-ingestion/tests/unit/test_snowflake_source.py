from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.oauth import OAuthConfiguration
from datahub.configuration.pattern_utils import UUID_REGEX
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.snowflake.constants import (
    CLIENT_PREFETCH_THREADS,
    CLIENT_SESSION_KEEP_ALIVE,
    SnowflakeCloudProvider,
)
from datahub.ingestion.source.snowflake.snowflake_config import (
    DEFAULT_TABLES_DENY_LIST,
    SnowflakeV2Config,
)
from datahub.ingestion.source.snowflake.snowflake_query import (
    SnowflakeQuery,
    create_deny_regex_sql_filter,
)
from datahub.ingestion.source.snowflake.snowflake_usage_v2 import (
    SnowflakeObjectAccessEntry,
)
from datahub.ingestion.source.snowflake.snowflake_v2 import SnowflakeV2Source


def test_snowflake_source_throws_error_on_account_id_missing():
    with pytest.raises(ValidationError):
        SnowflakeV2Config.parse_obj(
            {
                "username": "user",
                "password": "password",
            }
        )


def test_no_client_id_invalid_oauth_config():
    oauth_dict = {
        "provider": "microsoft",
        "scopes": ["https://microsoft.com/f4b353d5-ef8d/.default"],
        "client_secret": "6Hb9apkbc6HD7",
        "authority_url": "https://login.microsoftonline.com/yourorganisation.com",
    }
    with pytest.raises(ValueError):
        OAuthConfiguration.parse_obj(oauth_dict)


def test_snowflake_throws_error_on_client_secret_missing_if_use_certificate_is_false():
    oauth_dict = {
        "client_id": "882e9831-7ea51cb2b954",
        "provider": "microsoft",
        "scopes": ["https://microsoft.com/f4b353d5-ef8d/.default"],
        "use_certificate": False,
        "authority_url": "https://login.microsoftonline.com/yourorganisation.com",
    }
    OAuthConfiguration.parse_obj(oauth_dict)

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
    OAuthConfiguration.parse_obj(oauth_dict)
    with pytest.raises(ValueError):
        SnowflakeV2Config.parse_obj(
            {
                "account_id": "test",
                "authentication_type": "OAUTH_AUTHENTICATOR",
                "oauth_config": oauth_dict,
            }
        )


def test_snowflake_oauth_okta_does_not_support_certificate():
    oauth_dict = {
        "client_id": "882e9831-7ea51cb2b954",
        "provider": "okta",
        "scopes": ["https://microsoft.com/f4b353d5-ef8d/.default"],
        "use_certificate": True,
        "authority_url": "https://login.microsoftonline.com/yourorganisation.com",
        "encoded_oauth_public_key": "fkdsfhkshfkjsdfiuwrwfkjhsfskfhksjf==",
    }
    OAuthConfiguration.parse_obj(oauth_dict)
    with pytest.raises(ValueError):
        SnowflakeV2Config.parse_obj(
            {
                "account_id": "test",
                "authentication_type": "OAUTH_AUTHENTICATOR",
                "oauth_config": oauth_dict,
            }
        )


def test_snowflake_oauth_happy_paths():
    okta_dict = {
        "client_id": "client_id",
        "client_secret": "secret",
        "provider": "okta",
        "scopes": ["datahub_role"],
        "authority_url": "https://dev-abc.okta.com/oauth2/def/v1/token",
    }
    assert SnowflakeV2Config.parse_obj(
        {
            "account_id": "test",
            "authentication_type": "OAUTH_AUTHENTICATOR",
            "oauth_config": okta_dict,
        }
    )

    microsoft_dict = {
        "client_id": "client_id",
        "provider": "microsoft",
        "scopes": ["https://microsoft.com/f4b353d5-ef8d/.default"],
        "use_certificate": True,
        "authority_url": "https://login.microsoftonline.com/yourorganisation.com",
        "encoded_oauth_public_key": "publickey",
        "encoded_oauth_private_key": "privatekey",
    }
    assert SnowflakeV2Config.parse_obj(
        {
            "account_id": "test",
            "authentication_type": "OAUTH_AUTHENTICATOR",
            "oauth_config": microsoft_dict,
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
    assert config.account_id == "acctname"


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

    assert config.get_sql_alchemy_url() == (
        "snowflake://user:password@acctname"
        "?application=acryl_datahub"
        "&authenticator=SNOWFLAKE"
        "&role=sysadmin"
        "&warehouse=COMPUTE_WH"
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

    assert config.get_sql_alchemy_url() == (
        "snowflake://user@acctname"
        "?application=acryl_datahub"
        "&authenticator=EXTERNALBROWSER"
        "&role=sysadmin"
        "&warehouse=COMPUTE_WH"
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

    assert config.get_sql_alchemy_url() == (
        "snowflake://user@acctname"
        "?application=acryl_datahub"
        "&authenticator=SNOWFLAKE_JWT"
        "&role=sysadmin"
        "&warehouse=COMPUTE_WH"
    )


def test_options_contain_connect_args():
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
    connect_args = config.get_options().get("connect_args")
    assert connect_args is not None


def test_snowflake_config_with_view_lineage_no_table_lineage_throws_error():
    with pytest.raises(ValidationError):
        SnowflakeV2Config.parse_obj(
            {
                "username": "user",
                "password": "password",
                "account_id": "acctname",
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
                "account_id": "acctname",
                "database_pattern": {"allow": {"^demo$"}},
                "warehouse": "COMPUTE_WH",
                "role": "sysadmin",
                "include_column_lineage": True,
                "include_table_lineage": False,
            }
        )


def test_snowflake_config_with_no_connect_args_returns_base_connect_args():
    config: SnowflakeV2Config = SnowflakeV2Config.parse_obj(
        {
            "username": "user",
            "password": "password",
            "account_id": "acctname",
            "database_pattern": {"allow": {"^demo$"}},
            "warehouse": "COMPUTE_WH",
            "role": "sysadmin",
        }
    )
    assert config.get_options()["connect_args"] is not None
    assert config.get_options()["connect_args"] == {
        CLIENT_PREFETCH_THREADS: 10,
        CLIENT_SESSION_KEEP_ALIVE: True,
    }


def test_private_key_set_but_auth_not_changed():
    with pytest.raises(ValidationError):
        SnowflakeV2Config.parse_obj(
            {
                "account_id": "acctname",
                "private_key_path": "/a/random/path",
            }
        )


def test_snowflake_config_with_connect_args_overrides_base_connect_args():
    config: SnowflakeV2Config = SnowflakeV2Config.parse_obj(
        {
            "username": "user",
            "password": "password",
            "account_id": "acctname",
            "database_pattern": {"allow": {"^demo$"}},
            "warehouse": "COMPUTE_WH",
            "role": "sysadmin",
            "connect_args": {
                CLIENT_PREFETCH_THREADS: 5,
            },
        }
    )
    assert config.get_options()["connect_args"] is not None
    assert config.get_options()["connect_args"][CLIENT_PREFETCH_THREADS] == 5
    assert config.get_options()["connect_args"][CLIENT_SESSION_KEEP_ALIVE] is True


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
        raise ValueError(f"Unexpected query: {query}")

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
        raise ValueError(f"Unexpected query: {query}")

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

    assert (
        "Current role TEST_ROLE does not have permissions to use warehouse"
        in failure_reason
    )


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
        raise ValueError(f"Unexpected query: {query}")

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
        raise ValueError(f"Unexpected query: {query}")

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
        raise ValueError(f"Unexpected query: {query}")

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
    assert cloud_region_id == "switzerland-north"

    (
        cloud,
        cloud_region_id,
    ) = SnowflakeV2Source.get_cloud_region_from_snowflake_region_id(
        "azure_centralindia"
    )

    assert cloud == SnowflakeCloudProvider.AZURE
    assert cloud_region_id == "central-india"


def test_unknown_cloud_region_from_snowflake_region_id():
    with pytest.raises(Exception) as e:
        SnowflakeV2Source.get_cloud_region_from_snowflake_region_id(
            "somecloud_someregion"
        )
    assert "Unknown snowflake region" in str(e)


def test_snowflake_object_access_entry_missing_object_id():
    SnowflakeObjectAccessEntry(
        **{
            "columns": [
                {"columnName": "A"},
                {"columnName": "B"},
            ],
            "objectDomain": "View",
            "objectName": "SOME.OBJECT.NAME",
        }
    )


def test_snowflake_query_create_deny_regex_sql():
    assert create_deny_regex_sql_filter([], ["col"]) == ""
    assert (
        create_deny_regex_sql_filter([".*tmp.*"], ["col"])
        == "NOT RLIKE(col,'.*tmp.*','i')"
    )

    assert (
        create_deny_regex_sql_filter([".*tmp.*", UUID_REGEX], ["col"])
        == "NOT RLIKE(col,'.*tmp.*','i') AND NOT RLIKE(col,'[a-f0-9]{8}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{12}','i')"
    )

    assert (
        create_deny_regex_sql_filter([".*tmp.*", UUID_REGEX], ["col1", "col2"])
        == "NOT RLIKE(col1,'.*tmp.*','i') AND NOT RLIKE(col1,'[a-f0-9]{8}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{12}','i') AND NOT RLIKE(col2,'.*tmp.*','i') AND NOT RLIKE(col2,'[a-f0-9]{8}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{12}','i')"
    )

    assert (
        create_deny_regex_sql_filter(DEFAULT_TABLES_DENY_LIST, ["upstream_table_name"])
        == r"NOT RLIKE(upstream_table_name,'.*\.FIVETRAN_.*_STAGING\..*','i') AND NOT RLIKE(upstream_table_name,'.*__DBT_TMP$','i') AND NOT RLIKE(upstream_table_name,'.*\.SEGMENT_[a-f0-9]{8}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{12}','i') AND NOT RLIKE(upstream_table_name,'.*\.STAGING_.*_[a-f0-9]{8}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{12}','i')"
    )


def test_snowflake_temporary_patterns_config_rename():
    conf = SnowflakeV2Config.parse_obj(
        {
            "account_id": "test",
            "username": "user",
            "password": "password",
            "upstreams_deny_pattern": [".*tmp.*"],
        }
    )
    assert conf.temporary_tables_pattern == [".*tmp.*"]


def test_email_filter_query_generation_with_one_deny():
    email_filter = AllowDenyPattern(deny=[".*@example.com"])
    filter_query = SnowflakeQuery.gen_email_filter_query(email_filter)
    assert filter_query == " AND NOT (rlike(user_name, '.*@example.com','i'))"


def test_email_filter_query_generation_without_any_filter():
    email_filter = AllowDenyPattern()
    filter_query = SnowflakeQuery.gen_email_filter_query(email_filter)
    assert filter_query == ""


def test_email_filter_query_generation_one_allow():
    email_filter = AllowDenyPattern(allow=[".*@example.com"])
    filter_query = SnowflakeQuery.gen_email_filter_query(email_filter)
    assert filter_query == "AND (rlike(user_name, '.*@example.com','i'))"


def test_email_filter_query_generation_one_allow_and_deny():
    email_filter = AllowDenyPattern(
        allow=[".*@example.com", ".*@example2.com"],
        deny=[".*@example2.com", ".*@example4.com"],
    )
    filter_query = SnowflakeQuery.gen_email_filter_query(email_filter)
    assert (
        filter_query
        == "AND (rlike(user_name, '.*@example.com','i') OR rlike(user_name, '.*@example2.com','i')) AND NOT (rlike(user_name, '.*@example2.com','i') OR rlike(user_name, '.*@example4.com','i'))"
    )


def test_email_filter_query_generation_with_case_insensitive_filter():
    email_filter = AllowDenyPattern(
        allow=[".*@example.com"], deny=[".*@example2.com"], ignoreCase=False
    )
    filter_query = SnowflakeQuery.gen_email_filter_query(email_filter)
    assert (
        filter_query
        == "AND (rlike(user_name, '.*@example.com','c')) AND NOT (rlike(user_name, '.*@example2.com','c'))"
    )
