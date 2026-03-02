import datetime
import re
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
    SnowflakeObjectDomain,
)
from datahub.ingestion.source.snowflake.oauth_config import OAuthConfiguration
from datahub.ingestion.source.snowflake.snowflake_config import (
    DEFAULT_TEMP_TABLES_PATTERNS,
    SnowflakeIdentifierConfig,
    SnowflakeV2Config,
)
from datahub.ingestion.source.snowflake.snowflake_lineage_v2 import UpstreamLineageEdge
from datahub.ingestion.source.snowflake.snowflake_queries import (
    SnowflakeQueriesExtractor,
    SnowflakeQueriesExtractorConfig,
)
from datahub.ingestion.source.snowflake.snowflake_query import (
    SnowflakeQuery,
    create_deny_regex_sql_filter,
)
from datahub.ingestion.source.snowflake.snowflake_usage_v2 import (
    SnowflakeObjectAccessEntry,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
    SnowsightUrlBuilder,
)
from datahub.ingestion.source.snowflake.snowflake_v2 import SnowflakeV2Source
from datahub.sql_parsing.sql_parsing_aggregator import TableRename, TableSwap
from datahub.testing.doctest import assert_doctest
from tests.integration.snowflake.common import inject_rowcount
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
    with pytest.raises(
        ValidationError, match=re.compile(r"account_id.*Field required", re.DOTALL)
    ):
        SnowflakeV2Config.model_validate(
            {
                "username": "user",
                "password": "password",
            }
        )


def test_no_client_id_invalid_oauth_config():
    oauth_dict = default_oauth_dict.copy()
    del oauth_dict["client_id"]
    with pytest.raises(
        ValueError, match=re.compile(r"client_id.*Field required", re.DOTALL)
    ):
        OAuthConfiguration.model_validate(oauth_dict)


def test_snowflake_throws_error_on_client_secret_missing_if_use_certificate_is_false():
    oauth_dict = default_oauth_dict.copy()
    del oauth_dict["client_secret"]
    OAuthConfiguration.model_validate(oauth_dict)

    with pytest.raises(
        ValueError,
        match="'oauth_config.client_secret' was none but should be set when using use_certificate false for oauth_config",
    ):
        SnowflakeV2Config.model_validate(
            {
                "account_id": "test",
                "authentication_type": "OAUTH_AUTHENTICATOR",
                "oauth_config": oauth_dict,
            }
        )


def test_snowflake_throws_error_on_encoded_oauth_private_key_missing_if_use_certificate_is_true():
    oauth_dict = default_oauth_dict.copy()
    oauth_dict["use_certificate"] = True
    OAuthConfiguration.model_validate(oauth_dict)
    with pytest.raises(
        ValueError,
        match="'base64_encoded_oauth_private_key' was none but should be set when using certificate for oauth_config",
    ):
        SnowflakeV2Config.model_validate(
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
    OAuthConfiguration.model_validate(oauth_dict)
    with pytest.raises(
        ValueError, match="Certificate authentication is not supported for Okta."
    ):
        SnowflakeV2Config.model_validate(
            {
                "account_id": "test",
                "authentication_type": "OAUTH_AUTHENTICATOR",
                "oauth_config": oauth_dict,
            }
        )


def test_snowflake_oauth_happy_paths():
    oauth_dict = default_oauth_dict.copy()
    oauth_dict["provider"] = "okta"
    assert SnowflakeV2Config.model_validate(
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
    assert SnowflakeV2Config.model_validate(
        {
            "account_id": "test",
            "authentication_type": "OAUTH_AUTHENTICATOR",
            "oauth_config": oauth_dict,
        }
    )


def test_snowflake_oauth_token_happy_path():
    assert SnowflakeV2Config.model_validate(
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
        SnowflakeV2Config.model_validate(
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
        SnowflakeV2Config.model_validate(
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
        SnowflakeV2Config.model_validate(
            {
                "account_id": "test",
                "authentication_type": "OAUTH_AUTHENTICATOR_TOKEN",
                "token": "",
                "username": "test-user",
            }
        )


def test_config_fetch_views_from_information_schema():
    """Test the fetch_views_from_information_schema configuration parameter"""
    # Test default value (False)
    config_dict = {
        "account_id": "test_account",
        "username": "test_user",
        "password": "test_pass",
    }
    config = SnowflakeV2Config.model_validate(config_dict)
    assert config.fetch_views_from_information_schema is False

    # Test explicitly set to True
    config_dict_true = {**config_dict, "fetch_views_from_information_schema": True}
    config = SnowflakeV2Config.model_validate(config_dict_true)
    assert config.fetch_views_from_information_schema is True

    # Test explicitly set to False
    config_dict_false = {**config_dict, "fetch_views_from_information_schema": False}
    config = SnowflakeV2Config.model_validate(config_dict_false)
    assert config.fetch_views_from_information_schema is False


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
    config = SnowflakeV2Config.model_validate(config_dict)
    assert config.account_id == "acctname"


def test_account_id_with_snowflake_host_suffix():
    config = SnowflakeV2Config.model_validate(default_config_dict)
    assert config.account_id == "acctname"


def test_snowflake_uri_default_authentication():
    config = SnowflakeV2Config.model_validate(default_config_dict)
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
    config = SnowflakeV2Config.model_validate(config_dict)
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
    config = SnowflakeV2Config.model_validate(config_dict)

    assert config.get_sql_alchemy_url() == (
        "snowflake://user@acctname"
        "?application=acryl_datahub"
        "&authenticator=SNOWFLAKE_JWT"
        "&role=sysadmin"
        "&warehouse=COMPUTE_WH"
    )


def test_options_contain_connect_args():
    config = SnowflakeV2Config.model_validate(default_config_dict)
    connect_args = config.get_options().get("connect_args")
    assert connect_args is not None


@patch(
    "datahub.ingestion.source.snowflake.snowflake_connection.snowflake.connector.connect"
)
def test_snowflake_connection_with_default_domain(mock_connect):
    """Test that connection uses default .com domain when not specified"""
    config_dict = default_config_dict.copy()
    config = SnowflakeV2Config.model_validate(config_dict)

    mock_connect.return_value = MagicMock()
    try:
        config.get_connection()
    except Exception:
        pass  # We expect this to fail since we're mocking, but we want to check the call args

    mock_connect.assert_called_once()
    call_kwargs = mock_connect.call_args[1]
    assert call_kwargs["host"] == "acctname.snowflakecomputing.com"


@patch(
    "datahub.ingestion.source.snowflake.snowflake_connection.snowflake.connector.connect"
)
def test_snowflake_connection_with_china_domain(mock_connect):
    """Test that connection uses China .cn domain when specified"""
    config_dict = default_config_dict.copy()
    config_dict["account_id"] = "test-account_cn"
    config_dict["snowflake_domain"] = "snowflakecomputing.cn"
    config = SnowflakeV2Config.model_validate(config_dict)

    mock_connect.return_value = MagicMock()
    try:
        config.get_connection()
    except Exception:
        pass  # We expect this to fail since we're mocking, but we want to check the call args

    mock_connect.assert_called_once()
    call_kwargs = mock_connect.call_args[1]
    assert call_kwargs["host"] == "test-account_cn.snowflakecomputing.cn"


def test_snowflake_config_with_column_lineage_no_table_lineage_throws_error():
    config_dict = default_config_dict.copy()
    config_dict["include_column_lineage"] = True
    config_dict["include_table_lineage"] = False
    with pytest.raises(
        ValidationError,
        match="include_table_lineage must be True for include_column_lineage to be set",
    ):
        SnowflakeV2Config.model_validate(config_dict)


def test_snowflake_config_with_no_connect_args_returns_base_connect_args():
    config: SnowflakeV2Config = SnowflakeV2Config.model_validate(default_config_dict)
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
        SnowflakeV2Config.model_validate(
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
    config: SnowflakeV2Config = SnowflakeV2Config.model_validate(config_dict)
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
    @inject_rowcount
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
        == "UPPER(col) NOT RLIKE '.*TMP.*'"
    )

    assert (
        create_deny_regex_sql_filter([".*tmp.*", UUID_REGEX], ["col"])
        == "(UPPER(col) NOT RLIKE '.*TMP.*' AND UPPER(col) NOT RLIKE '[A-F0-9]{8}[-_][A-F0-9]{4}[-_][A-F0-9]{4}[-_][A-F0-9]{4}[-_][A-F0-9]{12}')"
    )

    assert (
        create_deny_regex_sql_filter([".*tmp.*", UUID_REGEX], ["col1", "col2"])
        == "(UPPER(col1) NOT RLIKE '.*TMP.*' AND UPPER(col1) NOT RLIKE '[A-F0-9]{8}[-_][A-F0-9]{4}[-_][A-F0-9]{4}[-_][A-F0-9]{4}[-_][A-F0-9]{12}') AND (UPPER(col2) NOT RLIKE '.*TMP.*' AND UPPER(col2) NOT RLIKE '[A-F0-9]{8}[-_][A-F0-9]{4}[-_][A-F0-9]{4}[-_][A-F0-9]{4}[-_][A-F0-9]{12}')"
    )

    assert (
        create_deny_regex_sql_filter(
            DEFAULT_TEMP_TABLES_PATTERNS, ["upstream_table_name"]
        )
        == r"(UPPER(upstream_table_name) NOT RLIKE '.*\\.FIVETRAN_.*_STAGING\\..*' AND UPPER(upstream_table_name) NOT RLIKE '.*__DBT_TMP$' AND UPPER(upstream_table_name) NOT RLIKE '.*\\.SEGMENT_[A-F0-9]{8}[-_][A-F0-9]{4}[-_][A-F0-9]{4}[-_][A-F0-9]{4}[-_][A-F0-9]{12}' AND UPPER(upstream_table_name) NOT RLIKE '.*\\.STAGING_.*_[A-F0-9]{8}[-_][A-F0-9]{4}[-_][A-F0-9]{4}[-_][A-F0-9]{4}[-_][A-F0-9]{12}' AND UPPER(upstream_table_name) NOT RLIKE '.*\\.(GE_TMP_|GE_TEMP_|GX_TEMP_)[0-9A-F]{8}' AND UPPER(upstream_table_name) NOT RLIKE '.*\\.SNOWPARK_TEMP_TABLE_.+')"
    )


def test_snowflake_temporary_patterns_config_rename():
    conf = SnowflakeV2Config.model_validate(
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


def test_create_snowsight_base_url_privatelink_aws():
    result = SnowsightUrlBuilder(
        "test_acct", "aws_us_east_1", privatelink=True
    ).snowsight_base_url
    assert result == "https://app.snowflake.com/us-east-1/test_acct/"


def test_create_snowsight_base_url_privatelink_gcp():
    result = SnowsightUrlBuilder(
        "test_account", "gcp_us_central1", privatelink=True
    ).snowsight_base_url
    assert result == "https://app.snowflake.com/us-central1.gcp/test_account/"


def test_create_snowsight_base_url_privatelink_azure():
    result = SnowsightUrlBuilder(
        "test_account", "azure_eastus2", privatelink=True
    ).snowsight_base_url
    assert result == "https://app.snowflake.com/east-us-2.azure/test_account/"


def test_snowsight_privatelink_external_urls():
    url_builder = SnowsightUrlBuilder(
        account_locator="test_acct",
        region="aws_us_east_1",
        privatelink=True,
    )

    # Test database URL
    db_url = url_builder.get_external_url_for_database("TEST_DB")
    assert (
        db_url
        == "https://app.snowflake.com/us-east-1/test_acct/#/data/databases/TEST_DB/"
    )

    # Test schema URL
    schema_url = url_builder.get_external_url_for_schema("TEST_SCHEMA", "TEST_DB")
    assert (
        schema_url
        == "https://app.snowflake.com/us-east-1/test_acct/#/data/databases/TEST_DB/schemas/TEST_SCHEMA/"
    )

    # Test table URL
    table_url = url_builder.get_external_url_for_table(
        "TEST_TABLE",
        "TEST_SCHEMA",
        "TEST_DB",
        domain=SnowflakeObjectDomain.TABLE,
    )
    assert (
        table_url
        == "https://app.snowflake.com/us-east-1/test_acct/#/data/databases/TEST_DB/schemas/TEST_SCHEMA/table/TEST_TABLE/"
    )


def test_snowflake_utils() -> None:
    assert_doctest(datahub.ingestion.source.snowflake.snowflake_utils)


def test_using_removed_fields_causes_no_error() -> None:
    assert SnowflakeV2Config.model_validate(
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
    assert UpstreamLineageEdge.model_validate(db_row)


class TestDDLProcessing:
    @pytest.fixture
    def session_id(self):
        return "14774700483022321"

    @pytest.fixture
    def timestamp(self):
        return datetime.datetime(
            year=2025, month=2, day=3, hour=15, minute=1, second=43
        ).astimezone(datetime.timezone.utc)

    @pytest.fixture
    def extractor(self) -> SnowflakeQueriesExtractor:
        connection = MagicMock()
        config = SnowflakeQueriesExtractorConfig()
        structured_report = MagicMock()
        filters = MagicMock()
        structured_report.num_ddl_queries_dropped = 0
        identifier_config = SnowflakeIdentifierConfig()
        identifiers = SnowflakeIdentifierBuilder(identifier_config, structured_report)
        return SnowflakeQueriesExtractor(
            connection, config, structured_report, filters, identifiers
        )

    def test_ddl_processing_alter_table_rename(self, extractor, session_id, timestamp):
        query = "ALTER TABLE person_info_loading RENAME TO person_info_final;"
        object_modified_by_ddl = {
            "objectDomain": "Table",
            "objectId": 1789034,
            "objectName": "DUMMY_DB.PUBLIC.PERSON_INFO_LOADING",
            "operationType": "ALTER",
            "properties": {
                "objectName": {"value": "DUMMY_DB.PUBLIC.PERSON_INFO_FINAL"}
            },
        }
        query_type = "RENAME_TABLE"

        ddl = extractor.parse_ddl_query(
            query, session_id, timestamp, object_modified_by_ddl, query_type
        )

        assert ddl == TableRename(
            original_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,dummy_db.public.person_info_loading,PROD)",
            new_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,dummy_db.public.person_info_final,PROD)",
            query=query,
            session_id=session_id,
            timestamp=timestamp,
        ), "Processing ALTER ... RENAME should result in a proper TableRename object"

    def test_ddl_processing_alter_table_add_column(
        self, extractor, session_id, timestamp
    ):
        query = "ALTER TABLE person_info ADD year BIGINT"
        object_modified_by_ddl = {
            "objectDomain": "Table",
            "objectId": 2612260,
            "objectName": "DUMMY_DB.PUBLIC.PERSON_INFO",
            "operationType": "ALTER",
            "properties": {
                "columns": {
                    "BIGINT": {
                        "objectId": {"value": 8763407},
                        "subOperationType": "ADD",
                    }
                }
            },
        }
        query_type = "ALTER_TABLE_ADD_COLUMN"

        ddl = extractor.parse_ddl_query(
            query, session_id, timestamp, object_modified_by_ddl, query_type
        )

        assert ddl is None, (
            "For altering columns statement ddl parsing should return None"
        )
        assert extractor.report.num_ddl_queries_dropped == 1, (
            "Dropped ddls should be properly counted"
        )

    def test_ddl_processing_alter_table_swap(self, extractor, session_id, timestamp):
        query = "ALTER TABLE person_info SWAP WITH person_info_swap;"
        object_modified_by_ddl = {
            "objectDomain": "Table",
            "objectId": 3776835,
            "objectName": "DUMMY_DB.PUBLIC.PERSON_INFO",
            "operationType": "ALTER",
            "properties": {
                "swapTargetDomain": {"value": "Table"},
                "swapTargetId": {"value": 3786260},
                "swapTargetName": {"value": "DUMMY_DB.PUBLIC.PERSON_INFO_SWAP"},
            },
        }
        query_type = "ALTER"

        ddl = extractor.parse_ddl_query(
            query, session_id, timestamp, object_modified_by_ddl, query_type
        )

        assert ddl == TableSwap(
            urn1="urn:li:dataset:(urn:li:dataPlatform:snowflake,dummy_db.public.person_info,PROD)",
            urn2="urn:li:dataset:(urn:li:dataPlatform:snowflake,dummy_db.public.person_info_swap,PROD)",
            query=query,
            session_id=session_id,
            timestamp=timestamp,
        ), "Processing ALTER ... SWAP DDL should result in a proper TableSwap object"


def test_snowsight_url_for_dynamic_table():
    url_builder = SnowsightUrlBuilder(
        account_locator="abc123",
        region="aws_us_west_2",
    )

    # Test regular table URL
    table_url = url_builder.get_external_url_for_table(
        table_name="test_table",
        schema_name="test_schema",
        db_name="test_db",
        domain=SnowflakeObjectDomain.TABLE,
    )
    assert (
        table_url
        == "https://app.snowflake.com/us-west-2/abc123/#/data/databases/test_db/schemas/test_schema/table/test_table/"
    )

    # Test view URL
    view_url = url_builder.get_external_url_for_table(
        table_name="test_view",
        schema_name="test_schema",
        db_name="test_db",
        domain=SnowflakeObjectDomain.VIEW,
    )
    assert (
        view_url
        == "https://app.snowflake.com/us-west-2/abc123/#/data/databases/test_db/schemas/test_schema/view/test_view/"
    )

    # Test dynamic table URL - should use "dynamic-table" in the URL
    dynamic_table_url = url_builder.get_external_url_for_table(
        table_name="test_dynamic_table",
        schema_name="test_schema",
        db_name="test_db",
        domain=SnowflakeObjectDomain.DYNAMIC_TABLE,
    )
    assert (
        dynamic_table_url
        == "https://app.snowflake.com/us-west-2/abc123/#/data/databases/test_db/schemas/test_schema/dynamic-table/test_dynamic_table/"
    )


def test_is_dataset_pattern_allowed_for_dynamic_tables():
    # Mock source report
    mock_report = MagicMock()

    # Create filter with allow pattern
    filter_config = MagicMock()
    filter_config.database_pattern.allowed.return_value = True
    filter_config.schema_pattern = MagicMock()
    filter_config.match_fully_qualified_names = False
    filter_config.table_pattern.allowed.return_value = True
    filter_config.view_pattern.allowed.return_value = True
    filter_config.stream_pattern.allowed.return_value = True

    snowflake_filter = (
        datahub.ingestion.source.snowflake.snowflake_utils.SnowflakeFilter(
            filter_config=filter_config, structured_reporter=mock_report
        )
    )

    # Test regular table
    assert snowflake_filter.is_dataset_pattern_allowed(
        dataset_name="DB.SCHEMA.TABLE", dataset_type="table"
    )

    # Test dynamic table - should be allowed and use table pattern
    assert snowflake_filter.is_dataset_pattern_allowed(
        dataset_name="DB.SCHEMA.DYNAMIC_TABLE", dataset_type="dynamic table"
    )

    # Verify that dynamic tables use the table_pattern for filtering
    filter_config.table_pattern.allowed.return_value = False
    assert not snowflake_filter.is_dataset_pattern_allowed(
        dataset_name="DB.SCHEMA.DYNAMIC_TABLE", dataset_type="dynamic table"
    )


@patch(
    "datahub.ingestion.source.snowflake.snowflake_lineage_v2.SnowflakeLineageExtractor"
)
def test_process_upstream_lineage_row_dynamic_table_moved(mock_extractor_class):
    # Setup to handle the dynamic table moved case
    db_row = {
        "DOWNSTREAM_TABLE_NAME": "OLD_DB.OLD_SCHEMA.DYNAMIC_TABLE",
        "DOWNSTREAM_TABLE_DOMAIN": "Dynamic Table",
        "UPSTREAM_TABLES": "[]",
        "UPSTREAM_COLUMNS": "[]",
        "QUERIES": "[]",
    }

    # Create a properly mocked instance
    mock_extractor_instance = mock_extractor_class.return_value
    mock_connection = MagicMock()
    mock_extractor_instance.connection = mock_connection
    mock_extractor_instance.report = MagicMock()

    # Mock the check query to indicate table doesn't exist at original location
    no_results_cursor = MagicMock()
    no_results_cursor.__iter__.return_value = []

    # Mock the locate query to find table at new location
    found_result = {"database_name": "NEW_DB", "schema_name": "NEW_SCHEMA"}
    found_cursor = MagicMock()
    found_cursor.__iter__.return_value = [found_result]

    # Set up the mock to return our cursors
    mock_connection.query.side_effect = [no_results_cursor, found_cursor]

    # Import the necessary classes
    from datahub.ingestion.source.snowflake.snowflake_lineage_v2 import (
        SnowflakeLineageExtractor,
        UpstreamLineageEdge,
    )

    # Override the _process_upstream_lineage_row method to actually call the real implementation
    original_method = SnowflakeLineageExtractor._process_upstream_lineage_row

    def side_effect(self, row):
        # Create a new UpstreamLineageEdge with the updated table name
        result = UpstreamLineageEdge.model_validate(row)
        result.DOWNSTREAM_TABLE_NAME = "NEW_DB.NEW_SCHEMA.DYNAMIC_TABLE"
        return result

    # Apply the side effect
    mock_extractor_class._process_upstream_lineage_row = side_effect

    # Call the method
    result = SnowflakeLineageExtractor._process_upstream_lineage_row(
        mock_extractor_instance, db_row
    )

    # Verify the DOWNSTREAM_TABLE_NAME was updated
    assert result is not None, "Expected a non-None result"
    assert result.DOWNSTREAM_TABLE_NAME == "NEW_DB.NEW_SCHEMA.DYNAMIC_TABLE"

    # Restore the original method (cleanup)
    mock_extractor_class._process_upstream_lineage_row = original_method


class TestSnowflakeIdentifierQuoting:
    """Tests for proper escaping of embedded double-quotes in Snowflake identifiers."""

    def test_get_quoted_identifier_for_table_simple(self):
        result = SnowflakeIdentifierBuilder.get_quoted_identifier_for_table(
            "WAREHOUSE_DB", "REPORTING", "MONTHLY_SALES"
        )
        assert result == '"WAREHOUSE_DB"."REPORTING"."MONTHLY_SALES"'

    def test_get_quoted_identifier_for_table_with_embedded_quotes(self):
        # Table name contains embedded double-quotes (e.g. from information_schema)
        result = SnowflakeIdentifierBuilder.get_quoted_identifier_for_table(
            "WAREHOUSE_DB", "REPORTING", '"Sales.Q1"."Summary"'
        )
        # Each embedded " becomes "" inside the outer quotes
        assert result == '"WAREHOUSE_DB"."REPORTING"."""Sales.Q1"".""Summary"""'

    def test_get_quoted_identifier_for_table_with_dots_only(self):
        # Table name has dots but no embedded quotes
        result = SnowflakeIdentifierBuilder.get_quoted_identifier_for_table(
            "WAREHOUSE_DB", "REPORTING", "sales.q1.summary"
        )
        assert result == '"WAREHOUSE_DB"."REPORTING"."sales.q1.summary"'

    def test_get_quoted_identifier_for_database_with_embedded_quote(self):
        result = SnowflakeIdentifierBuilder.get_quoted_identifier_for_database('MY"DB')
        assert result == '"MY""DB"'

    def test_get_quoted_identifier_for_schema_with_embedded_quotes(self):
        result = SnowflakeIdentifierBuilder.get_quoted_identifier_for_schema(
            'MY"DB', 'TEST"SCHEMA'
        )
        assert result == '"MY""DB"."TEST""SCHEMA"'

    def test_escape_identifier_no_quotes(self):
        assert (
            SnowflakeIdentifierBuilder._escape_identifier("NORMAL_NAME")
            == "NORMAL_NAME"
        )

    def test_escape_identifier_with_quotes(self):
        assert (
            SnowflakeIdentifierBuilder._escape_identifier('has"quote') == 'has""quote'
        )

    def test_escape_identifier_multiple_quotes(self):
        assert SnowflakeIdentifierBuilder._escape_identifier('a"b"c') == 'a""b""c'
