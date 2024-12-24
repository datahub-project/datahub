from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

import datahub.ingestion.source.snowflake.snowflake_utils
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.pattern_utils import UUID_REGEX
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.snowflake.constants import (
    CLIENT_PREFETCH_THREADS,
    CLIENT_SESSION_KEEP_ALIVE,
    SnowflakeCloudProvider,
)
from datahub.ingestion.source.snowflake.oauth_config import OAuthConfiguration
from datahub.ingestion.source.snowflake.snowflake_config import (
    DEFAULT_TEMP_TABLES_PATTERNS,
    SnowflakeV2Config,
)
from datahub.ingestion.source.snowflake.snowflake_lineage_v2 import UpstreamLineageEdge
from datahub.ingestion.source.snowflake.snowflake_query import (
    SnowflakeQuery,
    create_deny_regex_sql_filter,
)
from datahub.ingestion.source.snowflake.snowflake_usage_v2 import (
    SnowflakeObjectAccessEntry,
)
from datahub.ingestion.source.snowflake.snowflake_utils import SnowsightUrlBuilder
from datahub.ingestion.source.snowflake.snowflake_v2 import SnowflakeV2Source
from datahub.testing.doctest import assert_doctest
from tests.test_helpers import test_connection_helpers

default_oauth_dict: Dict[str, Any] = {
    "client_id": "client_id",
    "client_secret": "secret",
    "use_certificate": False,
    "provider": "microsoft",
    "scopes": ["datahub_role"],
    "authority_url": "https://dev-abc.okta.com/oauth2/def/v1/token",
}


def test_snowflake_source_throws_error_on_account_id_missing():
    with pytest.raises(ValidationError, match="account_id\n  field required"):
        SnowflakeV2Config.parse_obj(
            {
                "username": "user",
                "password": "password",
            }
        )


def test_no_client_id_invalid_oauth_config():
    oauth_dict = default_oauth_dict.copy()
    del oauth_dict["client_id"]
    with pytest.raises(ValueError, match="client_id\n  field required"):
        OAuthConfiguration.parse_obj(oauth_dict)


def test_snowflake_throws_error_on_client_secret_missing_if_use_certificate_is_false():
    oauth_dict = default_oauth_dict.copy()
    del oauth_dict["client_secret"]
    OAuthConfiguration.parse_obj(oauth_dict)

    with pytest.raises(
        ValueError,
        match="'oauth_config.client_secret' was none but should be set when using use_certificate false for oauth_config",
    ):
        SnowflakeV2Config.parse_obj(
            {
                "account_id": "test",
                "authentication_type": "OAUTH_AUTHENTICATOR",
                "oauth_config": oauth_dict,
            }
        )


def test_snowflake_throws_error_on_encoded_oauth_private_key_missing_if_use_certificate_is_true():
    oauth_dict = default_oauth_dict.copy()
    oauth_dict["use_certificate"] = True
    OAuthConfiguration.parse_obj(oauth_dict)
    with pytest.raises(
        ValueError,
        match="'base64_encoded_oauth_private_key' was none but should be set when using certificate for oauth_config",
    ):
        SnowflakeV2Config.parse_obj(
            {
                "account_id": "test",
                "authentication_type": "OAUTH_AUTHENTICATOR",
                "oauth_config": oauth_dict,
            }
        )


def test_snowflake_oauth_okta_does_not_support_certificate():
    oauth_dict = default_oauth_dict.copy()
    oauth_dict["use_certificate"] = True
    oauth_dict["provider"] = "okta"
    OAuthConfiguration.parse_obj(oauth_dict)
    with pytest.raises(
        ValueError, match="Certificate authentication is not supported for Okta."
    ):
        SnowflakeV2Config.parse_obj(
            {
                "account_id": "test",
                "authentication_type": "OAUTH_AUTHENTICATOR",
                "oauth_config": oauth_dict,
            }
        )


def test_snowflake_oauth_happy_paths():
    oauth_dict = default_oauth_dict.copy()
    oauth_dict["provider"] = "okta"
    assert SnowflakeV2Config.parse_obj(
        {
            "account_id": "test",
            "authentication_type": "OAUTH_AUTHENTICATOR",
            "oauth_config": oauth_dict,
        }
    )
    oauth_dict["use_certificate"] = True
    oauth_dict["provider"] = "microsoft"
    oauth_dict["encoded_oauth_public_key"] = "publickey"
    oauth_dict["encoded_oauth_private_key"] = "privatekey"
    assert SnowflakeV2Config.parse_obj(
        {
            "account_id": "test",
            "authentication_type": "OAUTH_AUTHENTICATOR",
            "oauth_config": oauth_dict,
        }
    )


def test_snowflake_oauth_token_happy_path():
    assert SnowflakeV2Config.parse_obj(
        {
            "account_id": "test",
            "authentication_type": "OAUTH_AUTHENTICATOR_TOKEN",
            "token": "valid-token",
            "username": "test-user",
            "oauth_config": None,
        }
    )


def test_snowflake_oauth_token_without_token():
    with pytest.raises(
        ValidationError, match="Token required for OAUTH_AUTHENTICATOR_TOKEN."
    ):
        SnowflakeV2Config.parse_obj(
            {
                "account_id": "test",
                "authentication_type": "OAUTH_AUTHENTICATOR_TOKEN",
                "username": "test-user",
            }
        )


def test_snowflake_oauth_token_with_wrong_auth_type():
    with pytest.raises(
        ValueError,
        match="Token can only be provided when using OAUTH_AUTHENTICATOR_TOKEN.",
    ):
        SnowflakeV2Config.parse_obj(
            {
                "account_id": "test",
                "authentication_type": "OAUTH_AUTHENTICATOR",
                "token": "some-token",
                "username": "test-user",
            }
        )


def test_snowflake_oauth_token_with_empty_token():
    with pytest.raises(
        ValidationError, match="Token required for OAUTH_AUTHENTICATOR_TOKEN."
    ):
        SnowflakeV2Config.parse_obj(
            {
                "account_id": "test",
                "authentication_type": "OAUTH_AUTHENTICATOR_TOKEN",
                "token": "",
                "username": "test-user",
            }
        )


default_config_dict: Dict[str, Any] = {
    "username": "user",
    "password": "password",
    "account_id": "https://acctname.snowflakecomputing.com",
    "warehouse": "COMPUTE_WH",
    "role": "sysadmin",
}


def test_account_id_is_added_when_host_port_is_present():
    config_dict = default_config_dict.copy()
    del config_dict["account_id"]
    config_dict["host_port"] = "acctname"
    config = SnowflakeV2Config.parse_obj(config_dict)
    assert config.account_id == "acctname"


def test_account_id_with_snowflake_host_suffix():
    config = SnowflakeV2Config.parse_obj(default_config_dict)
    assert config.account_id == "acctname"


def test_snowflake_uri_default_authentication():
    config = SnowflakeV2Config.parse_obj(default_config_dict)
    assert config.get_sql_alchemy_url() == (
        "snowflake://user:password@acctname"
        "?application=acryl_datahub"
        "&authenticator=SNOWFLAKE"
        "&role=sysadmin"
        "&warehouse=COMPUTE_WH"
    )


def test_snowflake_uri_external_browser_authentication():
    config_dict = default_config_dict.copy()
    del config_dict["password"]
    config_dict["authentication_type"] = "EXTERNAL_BROWSER_AUTHENTICATOR"
    config = SnowflakeV2Config.parse_obj(config_dict)
    assert config.get_sql_alchemy_url() == (
        "snowflake://user@acctname"
        "?application=acryl_datahub"
        "&authenticator=EXTERNALBROWSER"
        "&role=sysadmin"
        "&warehouse=COMPUTE_WH"
    )


def test_snowflake_uri_key_pair_authentication():
    config_dict = default_config_dict.copy()
    del config_dict["password"]
    config_dict["authentication_type"] = "KEY_PAIR_AUTHENTICATOR"
    config_dict["private_key_path"] = "/a/random/path"
    config_dict["private_key_password"] = "a_random_password"
    config = SnowflakeV2Config.parse_obj(config_dict)

    assert config.get_sql_alchemy_url() == (
        "snowflake://user@acctname"
        "?application=acryl_datahub"
        "&authenticator=SNOWFLAKE_JWT"
        "&role=sysadmin"
        "&warehouse=COMPUTE_WH"
    )


def test_options_contain_connect_args():
    config = SnowflakeV2Config.parse_obj(default_config_dict)
    connect_args = config.get_options().get("connect_args")
    assert connect_args is not None


def test_snowflake_config_with_column_lineage_no_table_lineage_throws_error():
    config_dict = default_config_dict.copy()
    config_dict["include_column_lineage"] = True
    config_dict["include_table_lineage"] = False
    with pytest.raises(
        ValidationError,
        match="include_table_lineage must be True for include_column_lineage to be set",
    ):
        SnowflakeV2Config.parse_obj(config_dict)


def test_snowflake_config_with_no_connect_args_returns_base_connect_args():
    config: SnowflakeV2Config = SnowflakeV2Config.parse_obj(default_config_dict)
    assert config.get_options()["connect_args"] is not None
    assert config.get_options()["connect_args"] == {
        CLIENT_PREFETCH_THREADS: 10,
        CLIENT_SESSION_KEEP_ALIVE: True,
    }


def test_private_key_set_but_auth_not_changed():
    with pytest.raises(
        ValidationError,
        match="Either `private_key` and `private_key_path` is set but `authentication_type` is DEFAULT_AUTHENTICATOR. Should be set to 'KEY_PAIR_AUTHENTICATOR' when using key pair authentication",
    ):
        SnowflakeV2Config.parse_obj(
            {
                "account_id": "acctname",
                "private_key_path": "/a/random/path",
            }
        )


def test_snowflake_config_with_connect_args_overrides_base_connect_args():
    config_dict = default_config_dict.copy()
    config_dict["connect_args"] = {
        CLIENT_PREFETCH_THREADS: 5,
    }
    config: SnowflakeV2Config = SnowflakeV2Config.parse_obj(config_dict)
    assert config.get_options()["connect_args"] is not None
    assert config.get_options()["connect_args"][CLIENT_PREFETCH_THREADS] == 5
    assert config.get_options()["connect_args"][CLIENT_SESSION_KEEP_ALIVE] is True


@patch("snowflake.connector.connect")
def test_test_connection_failure(mock_connect):
    mock_connect.side_effect = Exception("Failed to connect to snowflake")
    report = test_connection_helpers.run_test_connection(
        SnowflakeV2Source, default_config_dict
    )
    test_connection_helpers.assert_basic_connectivity_failure(
        report, "Failed to connect to snowflake"
    )


@patch("snowflake.connector.connect")
def test_test_connection_basic_success(mock_connect):
    report = test_connection_helpers.run_test_connection(
        SnowflakeV2Source, default_config_dict
    )
    test_connection_helpers.assert_basic_connectivity_success(report)


class MissingQueryMock(Exception):
    pass


def setup_mock_connect(mock_connect, extra_query_results=None):
    def query_results(query):
        if extra_query_results is not None:
            try:
                return extra_query_results(query)
            except MissingQueryMock:
                pass

        if query == "select current_role()":
            return [{"CURRENT_ROLE()": "TEST_ROLE"}]
        elif query == "select current_secondary_roles()":
            return [{"CURRENT_SECONDARY_ROLES()": '{"roles":"","value":""}'}]
        elif query == "select current_warehouse()":
            return [{"CURRENT_WAREHOUSE()": "TEST_WAREHOUSE"}]
        elif query == 'show grants to role "PUBLIC"':
            return []
        raise MissingQueryMock(f"Unexpected query: {query}")

    connection_mock = MagicMock()
    cursor_mock = MagicMock()
    cursor_mock.execute.side_effect = query_results
    connection_mock.cursor.return_value = cursor_mock
    mock_connect.return_value = connection_mock


@patch("snowflake.connector.connect")
def test_test_connection_no_warehouse(mock_connect):
    def query_results(query):
        if query == "select current_warehouse()":
            return [{"CURRENT_WAREHOUSE()": None}]
        elif query == 'show grants to role "TEST_ROLE"':
            return [{"privilege": "USAGE", "granted_on": "DATABASE", "name": "DB1"}]
        raise MissingQueryMock(f"Unexpected query: {query}")

    setup_mock_connect(mock_connect, query_results)
    report = test_connection_helpers.run_test_connection(
        SnowflakeV2Source, default_config_dict
    )
    test_connection_helpers.assert_basic_connectivity_success(report)

    test_connection_helpers.assert_capability_report(
        capability_report=report.capability_report,
        success_capabilities=[SourceCapability.CONTAINERS],
        failure_capabilities={
            SourceCapability.SCHEMA_METADATA: "Current role TEST_ROLE does not have permissions to use warehouse"
        },
    )


@patch("snowflake.connector.connect")
def test_test_connection_capability_schema_failure(mock_connect):
    def query_results(query):
        if query == 'show grants to role "TEST_ROLE"':
            return [{"privilege": "USAGE", "granted_on": "DATABASE", "name": "DB1"}]
        raise MissingQueryMock(f"Unexpected query: {query}")

    setup_mock_connect(mock_connect, query_results)

    report = test_connection_helpers.run_test_connection(
        SnowflakeV2Source, default_config_dict
    )
    test_connection_helpers.assert_basic_connectivity_success(report)

    test_connection_helpers.assert_capability_report(
        capability_report=report.capability_report,
        success_capabilities=[SourceCapability.CONTAINERS],
        failure_capabilities={
            SourceCapability.SCHEMA_METADATA: "Either no tables exist or current role does not have permissions to access them"
        },
    )


@patch("snowflake.connector.connect")
def test_test_connection_capability_schema_success(mock_connect):
    def query_results(query):
        if query == 'show grants to role "TEST_ROLE"':
            return [
                {"privilege": "USAGE", "granted_on": "DATABASE", "name": "DB1"},
                {"privilege": "USAGE", "granted_on": "SCHEMA", "name": "DB1.SCHEMA1"},
                {
                    "privilege": "REFERENCES",
                    "granted_on": "TABLE",
                    "name": "DB1.SCHEMA1.TABLE1",
                },
            ]
        raise MissingQueryMock(f"Unexpected query: {query}")

    setup_mock_connect(mock_connect, query_results)

    report = test_connection_helpers.run_test_connection(
        SnowflakeV2Source, default_config_dict
    )
    test_connection_helpers.assert_basic_connectivity_success(report)

    test_connection_helpers.assert_capability_report(
        capability_report=report.capability_report,
        success_capabilities=[
            SourceCapability.CONTAINERS,
            SourceCapability.SCHEMA_METADATA,
            SourceCapability.DESCRIPTIONS,
        ],
    )


@patch("snowflake.connector.connect")
def test_test_connection_capability_all_success(mock_connect):
    def query_results(query):
        if query == 'show grants to role "TEST_ROLE"':
            return [
                {"privilege": "USAGE", "granted_on": "DATABASE", "name": "DB1"},
                {"privilege": "USAGE", "granted_on": "SCHEMA", "name": "DB1.SCHEMA1"},
                {
                    "privilege": "SELECT",
                    "granted_on": "TABLE",
                    "name": "DB1.SCHEMA1.TABLE1",
                },
                {"privilege": "USAGE", "granted_on": "ROLE", "name": "TEST_USAGE_ROLE"},
            ]
        elif query == 'show grants to role "TEST_USAGE_ROLE"':
            return [
                {"privilege": "USAGE", "granted_on": "DATABASE", "name": "SNOWFLAKE"},
                {"privilege": "USAGE", "granted_on": "SCHEMA", "name": "ACCOUNT_USAGE"},
                {
                    "privilege": "USAGE",
                    "granted_on": "VIEW",
                    "name": "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY",
                },
                {
                    "privilege": "USAGE",
                    "granted_on": "VIEW",
                    "name": "SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY",
                },
                {
                    "privilege": "USAGE",
                    "granted_on": "VIEW",
                    "name": "SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES",
                },
            ]
        raise MissingQueryMock(f"Unexpected query: {query}")

    setup_mock_connect(mock_connect, query_results)

    report = test_connection_helpers.run_test_connection(
        SnowflakeV2Source, default_config_dict
    )
    test_connection_helpers.assert_basic_connectivity_success(report)

    test_connection_helpers.assert_capability_report(
        capability_report=report.capability_report,
        success_capabilities=[
            SourceCapability.CONTAINERS,
            SourceCapability.SCHEMA_METADATA,
            SourceCapability.DATA_PROFILING,
            SourceCapability.DESCRIPTIONS,
            SourceCapability.LINEAGE_COARSE,
        ],
    )


def test_aws_cloud_region_from_snowflake_region_id():
    (
        cloud,
        cloud_region_id,
    ) = SnowsightUrlBuilder.get_cloud_region_from_snowflake_region_id(
        "aws_ca_central_1"
    )

    assert cloud == SnowflakeCloudProvider.AWS
    assert cloud_region_id == "ca-central-1"

    (
        cloud,
        cloud_region_id,
    ) = SnowsightUrlBuilder.get_cloud_region_from_snowflake_region_id(
        "aws_us_east_1_gov"
    )

    assert cloud == SnowflakeCloudProvider.AWS
    assert cloud_region_id == "us-east-1"


def test_google_cloud_region_from_snowflake_region_id():
    (
        cloud,
        cloud_region_id,
    ) = SnowsightUrlBuilder.get_cloud_region_from_snowflake_region_id(
        "gcp_europe_west2"
    )

    assert cloud == SnowflakeCloudProvider.GCP
    assert cloud_region_id == "europe-west2"


def test_azure_cloud_region_from_snowflake_region_id():
    (
        cloud,
        cloud_region_id,
    ) = SnowsightUrlBuilder.get_cloud_region_from_snowflake_region_id(
        "azure_switzerlandnorth"
    )

    assert cloud == SnowflakeCloudProvider.AZURE
    assert cloud_region_id == "switzerland-north"

    (
        cloud,
        cloud_region_id,
    ) = SnowsightUrlBuilder.get_cloud_region_from_snowflake_region_id(
        "azure_centralindia"
    )

    assert cloud == SnowflakeCloudProvider.AZURE
    assert cloud_region_id == "central-india"


def test_unknown_cloud_region_from_snowflake_region_id():
    with pytest.raises(Exception, match="Unknown snowflake region"):
        SnowsightUrlBuilder.get_cloud_region_from_snowflake_region_id(
            "somecloud_someregion"
        )


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
        create_deny_regex_sql_filter(
            DEFAULT_TEMP_TABLES_PATTERNS, ["upstream_table_name"]
        )
        == r"NOT RLIKE(upstream_table_name,'.*\.FIVETRAN_.*_STAGING\..*','i') AND NOT RLIKE(upstream_table_name,'.*__DBT_TMP$','i') AND NOT RLIKE(upstream_table_name,'.*\.SEGMENT_[a-f0-9]{8}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{12}','i') AND NOT RLIKE(upstream_table_name,'.*\.STAGING_.*_[a-f0-9]{8}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{12}','i') AND NOT RLIKE(upstream_table_name,'.*\.(GE_TMP_|GE_TEMP_|GX_TEMP_)[0-9A-F]{8}','i')"
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


def test_create_snowsight_base_url_us_west():
    result = SnowsightUrlBuilder(
        "account_locator", "aws_us_west_2", privatelink=False
    ).snowsight_base_url
    assert result == "https://app.snowflake.com/us-west-2/account_locator/"


def test_create_snowsight_base_url_ap_northeast_1():
    result = SnowsightUrlBuilder(
        "account_locator", "aws_ap_northeast_1", privatelink=False
    ).snowsight_base_url

    assert result == "https://app.snowflake.com/ap-northeast-1.aws/account_locator/"


def test_snowflake_utils() -> None:
    assert_doctest(datahub.ingestion.source.snowflake.snowflake_utils)


def test_using_removed_fields_causes_no_error() -> None:
    assert SnowflakeV2Config.parse_obj(
        {
            "account_id": "test",
            "username": "snowflake",
            "password": "snowflake",
            "include_view_lineage": "true",
            "include_view_column_lineage": "true",
        }
    )


def test_snowflake_query_result_parsing():
    db_row = {
        "DOWNSTREAM_TABLE_NAME": "db.schema.downstream_table",
        "DOWNSTREAM_TABLE_DOMAIN": "Table",
        "UPSTREAM_TABLES": [
            {
                "query_id": "01b92f61-0611-c826-000d-0103cf9b5db7",
                "upstream_object_domain": "Table",
                "upstream_object_name": "db.schema.upstream_table",
            }
        ],
        "UPSTREAM_COLUMNS": [{}],
        "QUERIES": [
            {
                "query_id": "01b92f61-0611-c826-000d-0103cf9b5db7",
                "query_text": "Query test",
                "start_time": "2022-12-01 19:56:34",
            }
        ],
    }
    assert UpstreamLineageEdge.parse_obj(db_row)
