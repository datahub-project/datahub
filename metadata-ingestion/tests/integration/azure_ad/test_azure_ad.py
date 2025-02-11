import json
import pathlib
from functools import partial
from typing import List
from unittest.mock import patch

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.identity.azure_ad import AzureADConfig
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2021-08-24 09:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


def default_recipe(tmp_path, output_file_name="azure_ad_mces_default_config.json"):
    return {
        "run_id": "test-azure-ad",
        "source": {
            "type": "azure-ad",
            "config": {
                "client_id": "00000000-0000-0000-0000-000000000000",
                "tenant_id": "00000000-0000-0000-0000-000000000000",
                "client_secret": "client_secret",
                "redirect": "https://login.microsoftonline.com/common/oauth2/nativeclient",
                "authority": "https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000",
                "token_url": "https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000/oauth2/token",
                "graph_url": "https://graph.microsoft.com/v1.0",
                "ingest_group_membership": True,
                "ingest_groups": True,
                "ingest_users": True,
            },
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": f"{tmp_path}/{output_file_name}",
            },
        },
    }


def run_ingest(
    pytestconfig,
    mock_datahub_graph,
    mocked_functions_reference,
    recipe,
):
    test_resources_dir: pathlib.Path = (
        pytestconfig.rootpath / "tests/integration/azure_ad"
    )

    with patch(
        "datahub.ingestion.source.identity.azure_ad.AzureADSource.get_token"
    ) as mock_token, patch(
        "datahub.ingestion.source.identity.azure_ad.AzureADSource._get_azure_ad_users"
    ) as mock_users, patch(
        "datahub.ingestion.source.identity.azure_ad.AzureADSource._get_azure_ad_groups"
    ) as mock_groups, patch(
        "datahub.ingestion.source.identity.azure_ad.AzureADSource._get_azure_ad_group_members"
    ) as mock_group_users, patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph

        mocked_functions_reference(
            test_resources_dir, mock_token, mock_users, mock_groups, mock_group_users
        )

        # Run an azure usage ingestion run.
        pipeline = Pipeline.create(recipe)
        pipeline.run()
        pipeline.raise_from_status()
        return pipeline


def load_test_resources(test_resources_dir):
    azure_ad_users_json_file = test_resources_dir / "azure_ad_users.json"
    azure_ad_groups_json_file = test_resources_dir / "azure_ad_groups.json"
    azure_ad_nested_group_json_file = test_resources_dir / "azure_ad_nested_group.json"
    azure_ad_nested_groups_members_json_file = (
        test_resources_dir / "azure_ad_nested_groups_members.json"
    )

    with azure_ad_users_json_file.open() as azure_ad_users_json:
        reference_users = json.loads(azure_ad_users_json.read())

    with azure_ad_groups_json_file.open() as azure_ad_groups_json:
        reference_groups = json.loads(azure_ad_groups_json.read())

    with azure_ad_nested_group_json_file.open() as azure_ad_nested_group_json:
        reference_nested_group = json.loads(azure_ad_nested_group_json.read())

    with (
        azure_ad_nested_groups_members_json_file.open()
    ) as azure_ad_nested_groups_users_json:
        reference_nested_groups_users = json.loads(
            azure_ad_nested_groups_users_json.read()
        )

    return (
        reference_users,
        reference_groups,
        reference_nested_group,
        reference_nested_groups_users,
    )


#
# Azure offers a tool called 'graph-explorer' that you can use to generate fake data,
# or use your tenant's Azure AD to generate real data:
# https://developer.microsoft.com/graph/graph-explorer
#
def mocked_functions(
    test_resources_dir,
    mock_token,
    mock_users,
    mock_groups,
    mock_groups_users,
    return_nested_group=False,
):
    # mock token response
    mock_token.return_value = "xxxxxxxx"

    # mock users and groups response
    users, groups, nested_group, nested_group_members = load_test_resources(
        test_resources_dir
    )
    mock_users.return_value = iter(list([users]))
    mock_groups.return_value = (
        iter(list([nested_group])) if return_nested_group else iter(list([groups]))
    )

    # For simplicity, each user is placed in ALL groups.
    # Create a separate response mock for each group in our sample data.
    # mock_groups_users.return_value = [users]
    def mocked_group_members(azure_ad_group: dict) -> List:
        group_id = azure_ad_group.get("id")
        if group_id == "00000000-0000-0000-0000-000000000000":
            return [users]
        if group_id == "00000000-0000-0000-0000-0000000000001":
            return [users]
        if group_id == "00000000-0000-0000-0000-0000000000002":
            return [users[0:1]]
        if group_id == "99999999-9999-9999-9999-999999999999":
            return [nested_group_members]
        raise ValueError(f"Unexpected Azure AD group ID {group_id}")

    mock_groups_users.side_effect = mocked_group_members


def overwrite_group_in_mocked_data(
    test_resources_dir,
    mock_token,
    mock_users,
    mock_groups,
    mock_groups_users,
    return_nested_group=False,
):
    """
    This function will work similar to mocked_functions except it will overwrite mock_groups to test azure-ad stateful
    ingestion
    """
    mocked_functions(
        test_resources_dir=test_resources_dir,
        mock_token=mock_token,
        mock_users=mock_users,
        mock_groups=mock_groups,
        mock_groups_users=mock_groups_users,
        return_nested_group=return_nested_group,
    )
    # overwrite groups
    azure_ad_groups_json_file = (
        test_resources_dir / "azure_ad_groups_deleted_groupDisplayName3.json"
    )

    with azure_ad_groups_json_file.open() as azure_ad_groups_json:
        reference_groups = json.loads(azure_ad_groups_json.read())

    mock_groups.return_value = iter(list([reference_groups]))


def test_azure_ad_config():
    config = AzureADConfig.parse_obj(
        dict(
            client_id="00000000-0000-0000-0000-000000000000",
            tenant_id="00000000-0000-0000-0000-000000000000",
            client_secret="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
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
def test_azure_ad_source_default_configs(pytestconfig, mock_datahub_graph, tmp_path):
    test_resources_dir: pathlib.Path = (
        pytestconfig.rootpath / "tests/integration/azure_ad"
    )

    run_ingest(
        pytestconfig=pytestconfig,
        mock_datahub_graph=mock_datahub_graph,
        recipe=default_recipe(tmp_path),
        mocked_functions_reference=mocked_functions,
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "azure_ad_mces_default_config.json",
        golden_path=test_resources_dir / "azure_ad_mces_golden_default_config.json",
    )


@freeze_time(FROZEN_TIME)
def test_azure_ad_source_empty_group_membership(
    pytestconfig, mock_datahub_graph, tmp_path
):
    test_resources_dir: pathlib.Path = (
        pytestconfig.rootpath / "tests/integration/azure_ad"
    )

    output_file_name = "azure_ad_mces_no_groups_mcp.json"
    new_recipe = default_recipe(tmp_path, output_file_name)

    new_recipe["source"]["config"]["ingest_group_membership"] = False

    run_ingest(
        pytestconfig=pytestconfig,
        mock_datahub_graph=mock_datahub_graph,
        recipe=new_recipe,
        mocked_functions_reference=mocked_functions,
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/{output_file_name}",
        golden_path=test_resources_dir / "azure_ad_mces_no_groups_golden_mcp.json",
    )


@freeze_time(FROZEN_TIME)
def test_azure_ad_source_nested_groups(pytestconfig, mock_datahub_graph, tmp_path):
    test_resources_dir: pathlib.Path = (
        pytestconfig.rootpath / "tests/integration/azure_ad"
    )

    output_file_name = "azure_ad_mces_nested_groups.json"
    new_recipe = default_recipe(tmp_path, output_file_name)

    new_recipe["source"]["config"]["ingest_users"] = False

    include_nested_group_in_mock = partial(mocked_functions, return_nested_group=True)

    run_ingest(
        pytestconfig=pytestconfig,
        mock_datahub_graph=mock_datahub_graph,
        recipe=new_recipe,
        mocked_functions_reference=include_nested_group_in_mock,
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/{output_file_name}",
        golden_path=test_resources_dir / "azure_ad_mces_golden_nested_groups.json",
    )


@freeze_time(FROZEN_TIME)
def test_azure_source_ingestion_disabled(pytestconfig, mock_datahub_graph, tmp_path):
    test_resources_dir: pathlib.Path = (
        pytestconfig.rootpath / "tests/integration/azure_ad"
    )

    output_file_name = "azure_ad_mces_ingestion_disabled.json"
    new_recipe = default_recipe(tmp_path, output_file_name)

    new_recipe["source"]["config"]["ingest_group_membership"] = False
    new_recipe["source"]["config"]["ingest_groups"] = False
    new_recipe["source"]["config"]["ingest_users"] = False

    run_ingest(
        pytestconfig=pytestconfig,
        mock_datahub_graph=mock_datahub_graph,
        recipe=new_recipe,
        mocked_functions_reference=mocked_functions,
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/{output_file_name}",
        golden_path=test_resources_dir / "azure_ad_mces_golden_ingestion_disabled.json",
    )


@freeze_time(FROZEN_TIME)
def test_azure_ad_stateful_ingestion(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    new_recipe = default_recipe(tmp_path)

    new_recipe["pipeline_name"] = "azure_ad_execution"
    new_recipe["source"]["config"]["stateful_ingestion"] = {
        "enabled": True,
        "state_provider": {
            "type": "datahub",
            "config": {"datahub_api": {"server": GMS_SERVER}},
        },
    }

    pipeline1 = run_ingest(
        pytestconfig=pytestconfig,
        mock_datahub_graph=mock_datahub_graph,
        recipe=new_recipe,
        mocked_functions_reference=mocked_functions,
    )

    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline1)
    assert checkpoint1
    assert checkpoint1.state

    pipeline2 = run_ingest(
        pytestconfig=pytestconfig,
        mock_datahub_graph=mock_datahub_graph,
        recipe=new_recipe,
        mocked_functions_reference=overwrite_group_in_mocked_data,
    )
    checkpoint2 = get_current_checkpoint_from_pipeline(pipeline2)
    assert checkpoint2
    assert checkpoint2.state

    # Validate that all providers have committed successfully.
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline1, expected_providers=1
    )
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline2, expected_providers=1
    )

    # Perform all assertions on the states. The deleted Dashboard should not be
    # part of the second state
    state1 = checkpoint1.state
    state2 = checkpoint2.state

    difference_dashboard_urns = list(
        state1.get_urns_not_in(type="corpGroup", other_checkpoint_state=state2)
    )

    assert len(difference_dashboard_urns) == 1
    assert difference_dashboard_urns == ["urn:li:corpGroup:groupDisplayName3"]
