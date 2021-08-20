import pathlib
from unittest.mock import Mock, patch

import jsonpickle
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.identity.azure import AzureConfig
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2021-08-20 11:00:00"


def test_azure_config():
    config = AzureConfig.parse_obj(
        dict(
            client_id="00000000-0000-0000-0000-000000000000",
            tenant_id="00000000-0000-0000-0000-000000000000",
            client_secret="client_secret",
            redirect="https://login.microsoftonline.com/common/oauth2/nativeclient",
            authority="https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000",
            token_url="https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000/oauth2/token",
            graph_url="https://graph.microsoft.com/v1.0",
            ingest_users=True,
            ingest_groups=True,
            ingest_group_membership=True,
        )
    )

    # Sanity on required configurations
    assert config.client_id == "00000000-0000-0000-0000-000000000000"
    assert config.tenant_id == "00000000-0000-0000-0000-000000000000"
    assert config.client_secret == "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    assert (
        config.redirect
        == "https://login.microsoftonline.com/common/oauth2/nativeclient"
    )
    assert (
        config.authority
        == "https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000"
    )
    assert (
        config.token_url
        == "https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000/oauth2/token"
    )
    assert config.graph_url == "https://graph.microsoft.com/v1.0"

    # assert on defaults
    assert config.ingest_users
    assert config.ingest_groups
    assert config.ingest_group_membership


@freeze_time(FROZEN_TIME)
def test_azure_source_default_configs(pytestconfig, tmp_path):

    test_resources_dir: pathlib.Path = pytestconfig.rootpath / "tests/integration/azure"

    with patch("datahub.ingestion.source.identity.azure") as MockClient:

        _init_mock_azure_client(test_resources_dir, MockClient)

        # Run an Okta usage ingestion run.
        pipeline = Pipeline.create(
            {
                "run_id": "test-azure",
                "source": {
                    "type": "azure",
                    "config": {
                        "client_id": "00000000-0000-0000-0000-000000000000",
                        "tenant_id": "00000000-0000-0000-0000-000000000000",
                        "client_secret": "client_secret",
                        "redirect": "https://login.microsoftonline.com/common/oauth2/nativeclient",
                        "authority": "https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000",
                        "token_url": "https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000/oauth2/token",
                        "graph_url": "https://graph.microsoft.com/v1.0",
                        "ingest_group_membership": "True",
                        "ingest_groups": "True",
                        "ingest_users": "True",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/azure_mces_default_config.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "azure_mces_default_config.json",
        golden_path=test_resources_dir / "azure_mces_golden_default_config.json",
    )


@freeze_time(FROZEN_TIME)
def test_azure_source_ingestion_disabled(pytestconfig, tmp_path):

    test_resources_dir: pathlib.Path = pytestconfig.rootpath / "tests/integration/azure"

    with patch("datahub.ingestion.source.identity.azure") as MockClient:

        _init_mock_azure_client(test_resources_dir, MockClient)

        # Run an Okta usage ingestion run.
        pipeline = Pipeline.create(
            {
                "run_id": "test-okta-usage",
                "source": {
                    "type": "okta",
                    "config": {
                        "okta_domain": "mock-domain.okta.com",
                        "okta_api_token": "mock-okta-token",
                        "ingest_users": "False",
                        "ingest_groups": "False",
                        "ingest_group_membership": "False",
                        "okta_profile_to_username_attr": "login",
                        "okta_profile_to_username_regex": "([^@]+)",
                        "okta_profile_to_group_name_attr": "name",
                        "okta_profile_to_group_name_regex": "(.*)",
                        "include_deprovisioned_users": "False",
                        "include_suspended_users": "False",
                        "page_size": "2",
                        "delay_seconds": "0.00",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/okta_mces_ingestion_disabled.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "okta_mces_ingestion_disabled.json",
        golden_path=test_resources_dir / "okta_mces_golden_ingestion_disabled.json",
    )


# Initializes a Mock Azure to return users from azure_users.json and groups from azure_groups.json.
def _init_mock_azure_client(test_resources_dir, MockClient):

    azure_users_json_file = test_resources_dir / "azure_users.json"
    azure_groups_json_file = test_resources_dir / "azure_groups.json"

    # Add mock Azure API responses.
    with azure_users_json_file.open() as azure_users_json:
        reference_users = jsonpickle.decode(azure_users_json.read())
        print(reference_users)
        # Create users from JSON dicts
        users = [user for user in reference_users]

    with azure_groups_json_file.open() as azure_groups_json:
        reference_groups = jsonpickle.decode(azure_groups_json.read())
        print(reference_groups)
        # Create groups from JSON dicts
        groups = [group for group in reference_groups]

    # For simplicity, each user is placed in ALL groups.

    # Mock Client List response.
    users_resp_mock = Mock()
    users_resp_mock.side_effect = [True, False]
    users_resp_mock.next.return_value = ""

    MockClient().list_users.return_value = ""

    # Mock Client Init
    groups_resp_mock = Mock()
    groups_resp_mock.has_next.side_effect = [True, False]
    groups_resp_mock.next.return_value = ""

    # groups, resp, err
    list_groups_future = ""
    list_groups_future.set_result((groups[0:-1], groups_resp_mock, None))
    MockClient().list_groups.return_value = list_groups_future

    # Create a separate response mock for each group in our sample data.
    list_group_users_result_values = []
    for group in groups:
        # Mock Get Group Membership
        group_users_resp_mock = Mock()
        group_users_resp_mock.has_next.side_effect = [True, False]
        group_users_resp_mock.next.return_value = group_users_next_future

    MockClient().list_group_users.side_effect = list_group_users_result_values
