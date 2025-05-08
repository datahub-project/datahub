import asyncio
import pathlib
from functools import partial
from unittest.mock import Mock, patch

import jsonpickle
import pytest
from freezegun import freeze_time
from okta.models import Group, User

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.identity.okta import OktaConfig
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2020-04-14 07:00:00"
USER_ID_NOT_IN_GROUPS = "5"

GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


def default_recipe(output_file_path):
    return {
        "run_id": "test-okta-usage",
        "source": {
            "type": "okta",
            "config": {
                "okta_domain": "mock-domain.okta.com",
                "okta_api_token": "mock-okta-token",
                "ingest_users": "True",
                "ingest_groups": "True",
                "ingest_group_membership": "True",
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
                "filename": f"{output_file_path}",
            },
        },
    }


def run_ingest(
    mock_datahub_graph,
    mocked_functions_reference,
    recipe,
):
    with patch(
        "datahub.ingestion.source.identity.okta.OktaClient"
    ) as MockClient, patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph

        mocked_functions_reference(MockClient=MockClient)

        # Run an Okta usage ingestion run.
        pipeline = Pipeline.create(recipe)
        pipeline.run()
        pipeline.raise_from_status()

        return pipeline


def test_okta_config():
    config = OktaConfig.parse_obj(
        dict(okta_domain="test.okta.com", okta_api_token="test-token")
    )

    # Sanity on required configurations
    assert config.okta_domain == "test.okta.com"
    assert config.okta_api_token == "test-token"

    # Assert on default configurations
    assert config.ingest_users is True
    assert config.ingest_groups is True
    assert config.ingest_group_membership is True
    assert config.okta_profile_to_username_attr == "email"
    assert config.okta_profile_to_username_regex == "(.*)"
    assert config.okta_profile_to_group_name_attr == "name"
    assert config.okta_profile_to_group_name_regex == "(.*)"
    assert config.include_deprovisioned_users is False
    assert config.include_suspended_users is False
    assert config.page_size == 100
    assert config.delay_seconds == 0.01


@freeze_time(FROZEN_TIME)
def test_okta_source_default_configs(pytestconfig, mock_datahub_graph, tmp_path):
    test_resources_dir: pathlib.Path = pytestconfig.rootpath / "tests/integration/okta"

    output_file_path = f"{tmp_path}/okta_mces_default_config.json"

    run_ingest(
        mock_datahub_graph=mock_datahub_graph,
        mocked_functions_reference=partial(
            _init_mock_okta_client, test_resources_dir=test_resources_dir
        ),
        recipe=default_recipe(output_file_path),
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file_path,
        golden_path=f"{test_resources_dir}/okta_mces_golden_default_config.json",
    )


@freeze_time(FROZEN_TIME)
def test_okta_source_ingest_groups_users(pytestconfig, mock_datahub_graph, tmp_path):
    test_resources_dir: pathlib.Path = pytestconfig.rootpath / "tests/integration/okta"

    output_file_path = f"{tmp_path}/okta_mces_ingest_groups_users.json"

    new_recipe = default_recipe(output_file_path)
    new_recipe["source"]["config"]["ingest_users"] = False
    new_recipe["source"]["config"]["ingest_groups"] = True
    new_recipe["source"]["config"]["ingest_groups_users"] = True

    run_ingest(
        mock_datahub_graph=mock_datahub_graph,
        mocked_functions_reference=partial(
            _init_mock_okta_client, test_resources_dir=test_resources_dir
        ),
        recipe=new_recipe,
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file_path,
        golden_path=f"{test_resources_dir}/okta_mces_golden_ingest_groups_users.json",
    )


@freeze_time(FROZEN_TIME)
def test_okta_source_ingestion_disabled(pytestconfig, mock_datahub_graph, tmp_path):
    test_resources_dir: pathlib.Path = pytestconfig.rootpath / "tests/integration/okta"

    output_file_path = f"{tmp_path}/okta_mces_ingestion_disabled.json"
    new_recipe = default_recipe(output_file_path)
    new_recipe["source"]["config"]["ingest_users"] = False
    new_recipe["source"]["config"]["ingest_groups"] = False
    new_recipe["source"]["config"]["ingest_group_membership"] = False

    run_ingest(
        mock_datahub_graph=mock_datahub_graph,
        mocked_functions_reference=partial(
            _init_mock_okta_client, test_resources_dir=test_resources_dir
        ),
        recipe=new_recipe,
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file_path,
        golden_path=f"{test_resources_dir}/okta_mces_golden_ingestion_disabled.json",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.asyncio
def test_okta_source_include_deprovisioned_suspended_users(
    pytestconfig, mock_datahub_graph, tmp_path
):
    test_resources_dir: pathlib.Path = pytestconfig.rootpath / "tests/integration/okta"

    output_file_path = (
        f"{tmp_path}/okta_mces_include_deprovisioned_suspended_users.json"
    )
    new_recipe = default_recipe(output_file_path)
    new_recipe["source"]["config"]["include_deprovisioned_users"] = True
    new_recipe["source"]["config"]["include_suspended_users"] = True

    run_ingest(
        mock_datahub_graph=mock_datahub_graph,
        mocked_functions_reference=partial(
            _init_mock_okta_client, test_resources_dir=test_resources_dir
        ),
        recipe=new_recipe,
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file_path,
        golden_path=f"{test_resources_dir}/okta_mces_golden_include_deprovisioned_suspended_users.json",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.asyncio
def test_okta_source_custom_user_name_regex(pytestconfig, mock_datahub_graph, tmp_path):
    test_resources_dir: pathlib.Path = pytestconfig.rootpath / "tests/integration/okta"

    output_file_path = f"{tmp_path}/okta_mces_custom_user_name_regex.json"
    new_recipe = default_recipe(output_file_path)
    new_recipe["source"]["config"]["okta_profile_to_username_regex"] = "(.*)"
    new_recipe["source"]["config"]["okta_profile_to_group_name_regex"] = "(.*)"

    run_ingest(
        mock_datahub_graph=mock_datahub_graph,
        mocked_functions_reference=partial(
            _init_mock_okta_client, test_resources_dir=test_resources_dir
        ),
        recipe=new_recipe,
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file_path,
        golden_path=f"{test_resources_dir}/okta_mces_golden_custom_user_name_regex.json",
    )


@freeze_time(FROZEN_TIME)
def test_okta_stateful_ingestion(pytestconfig, tmp_path, mock_time, mock_datahub_graph):
    test_resources_dir: pathlib.Path = pytestconfig.rootpath / "tests/integration/okta"

    output_file_path = f"{tmp_path}/temporary_mces.json"
    new_recipe = default_recipe(output_file_path)

    new_recipe["pipeline_name"] = "okta_execution"
    new_recipe["source"]["config"]["stateful_ingestion"] = {
        "enabled": True,
        "state_provider": {
            "type": "datahub",
            "config": {"datahub_api": {"server": GMS_SERVER}},
        },
    }

    pipeline1 = run_ingest(
        mock_datahub_graph=mock_datahub_graph,
        mocked_functions_reference=partial(
            _init_mock_okta_client, test_resources_dir=test_resources_dir
        ),
        recipe=new_recipe,
    )

    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline1)
    assert checkpoint1
    assert checkpoint1.state

    # Create new event loop as last one is closed because of previous ingestion run
    event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(event_loop)

    pipeline2 = run_ingest(
        mock_datahub_graph=mock_datahub_graph,
        mocked_functions_reference=partial(
            overwrite_group_in_mocked_data, test_resources_dir=test_resources_dir
        ),
        recipe=new_recipe,
    )
    checkpoint2 = get_current_checkpoint_from_pipeline(pipeline2)
    assert checkpoint2
    assert checkpoint2.state
    #
    # Validate that all providers have committed successfully.
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline1, expected_providers=1
    )
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline2, expected_providers=1
    )

    # Perform all assertions on the states. The deleted group should not be
    # part of the second state
    state1 = checkpoint1.state
    state2 = checkpoint2.state

    difference_group_urns = list(
        state1.get_urns_not_in(type="corpGroup", other_checkpoint_state=state2)
    )

    assert len(difference_group_urns) == 1
    assert difference_group_urns == ["urn:li:corpGroup:Engineering"]


def overwrite_group_in_mocked_data(test_resources_dir, MockClient):
    _init_mock_okta_client(
        test_resources_dir,
        MockClient,
        mock_groups_json=test_resources_dir / "okta_deleted_groups.json",
    )


# Initializes a Mock Okta Client to return users from okta_users.json and groups from okta_groups.json.
def _init_mock_okta_client(
    test_resources_dir, MockClient, mock_users_json=None, mock_groups_json=None
):
    okta_users_json_file = (
        test_resources_dir / "okta_users.json"
        if mock_users_json is None
        else mock_users_json
    )
    okta_groups_json_file = (
        test_resources_dir / "okta_groups.json"
        if mock_groups_json is None
        else mock_groups_json
    )

    # Add mock Okta API responses.
    with okta_users_json_file.open() as okta_users_json:
        reference_users = jsonpickle.decode(okta_users_json.read())
        # Create users from JSON dicts
        users = list(map(lambda userJson: User(userJson), reference_users))

    with okta_groups_json_file.open() as okta_groups_json:
        reference_groups = jsonpickle.decode(okta_groups_json.read())
        # Create groups from JSON dicts
        groups = list(map(lambda groupJson: Group(groupJson), reference_groups))

    # Mock Client List response.
    users_resp_mock = Mock()
    users_resp_mock.has_next.side_effect = [True, False]
    users_next_future = asyncio.Future()  # type: asyncio.Future
    users_next_future.set_result(
        # users, err
        ([users[-1]], None)
    )
    users_resp_mock.next.return_value = users_next_future

    # users, resp, err
    list_users_future = asyncio.Future()  # type: asyncio.Future
    list_users_future.set_result(
        # users, resp, err
        (users[0:-1], users_resp_mock, None)
    )
    MockClient().list_users.return_value = list_users_future

    # Mock Client Init
    groups_resp_mock = Mock()
    groups_resp_mock.has_next.side_effect = [True, False]
    groups_next_future = asyncio.Future()  # type: asyncio.Future
    groups_next_future.set_result(
        # groups, err
        ([groups[-1]], None)
    )
    groups_resp_mock.next.return_value = groups_next_future

    # groups, resp, err
    list_groups_future = asyncio.Future()  # type: asyncio.Future
    list_groups_future.set_result((groups[0:-1], groups_resp_mock, None))
    MockClient().list_groups.return_value = list_groups_future

    # Create a separate response mock for each group in our sample data.
    list_group_users_result_values = []
    for _ in groups:
        # Mock Get Group Membership
        group_users_resp_mock = Mock()
        group_users_resp_mock.has_next.side_effect = [True, False]
        group_users_next_future = asyncio.Future()  # type: asyncio.Future
        group_users_next_future.set_result(
            # users, err
            ([users[-1]], None)
        )
        group_users_resp_mock.next.return_value = group_users_next_future
        # users, resp, err
        list_group_users_future = asyncio.Future()  # type: asyncio.Future
        # Exclude last user from being in any groups
        filtered_users = [user for user in users if user.id != USER_ID_NOT_IN_GROUPS]
        list_group_users_future.set_result(
            (filtered_users, group_users_resp_mock, None)
        )
        list_group_users_result_values.append(list_group_users_future)

    MockClient().list_group_users.side_effect = list_group_users_result_values
