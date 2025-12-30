import json
from functools import partial
from unittest.mock import patch

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.identity.keycloak import KeycloakConfig
from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2020-04-14 07:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


def test_keycloak_config():
    config = KeycloakConfig.model_validate(
        dict(
            server_url="https://keycloak.example.com",
            realm="myrealm",
            client_id="myclient",
            client_secret="mysecret",
        )
    )

    # Sanity on required configurations
    assert config.server_url == "https://keycloak.example.com"
    assert config.realm == "myrealm"
    assert config.client_id == "myclient"
    assert config.client_secret == "mysecret"

    # Assert on default configurations
    assert config.ingest_users is True
    assert config.ingest_groups is True
    assert config.ingest_group_membership is True
    assert config.verify_ssl is True
    assert config.mask_group_id is True
    assert config.mask_user_id is True


@freeze_time(FROZEN_TIME)
def test_keycloak_ingestion(pytestconfig, mock_datahub_graph, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/keycloak"
    output_file_path = f"{tmp_path}/keycloak_mces.json"

    run_ingest(
        mock_datahub_graph=mock_datahub_graph,
        mocked_functions_reference=partial(
            _init_mock_keycloak_admin, test_resources_dir=test_resources_dir
        ),
        recipe=default_recipe(output_file_path),
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file_path,
        golden_path=f"{test_resources_dir}/keycloak_mces_golden_ingest_groups_users.json",
    )


@freeze_time(FROZEN_TIME)
def test_keycloak_nested_group(pytestconfig, mock_datahub_graph, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/keycloak"
    output_file_path = f"{tmp_path}/keycloak_mces_nested.json"

    run_ingest(
        mock_datahub_graph=mock_datahub_graph,
        mocked_functions_reference=partial(
            _init_mock_keycloak_admin,
            test_resources_dir=test_resources_dir,
            mock_groups_json="keycloak_nested_groups.json",
        ),
        recipe=default_recipe(output_file_path),
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file_path,
        golden_path=f"{test_resources_dir}/keycloak_mces_golden_nested_group.json",
    )


@freeze_time(FROZEN_TIME)
def test_keycloak_nested_membership(pytestconfig, mock_datahub_graph, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/keycloak"
    output_file_path = f"{tmp_path}/keycloak_mces_nested_membership.json"

    run_ingest(
        mock_datahub_graph=mock_datahub_graph,
        mocked_functions_reference=partial(
            _init_mock_keycloak_admin_nested_membership,
            test_resources_dir=test_resources_dir,
        ),
        recipe=default_recipe(output_file_path),
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file_path,
        golden_path=f"{test_resources_dir}/keycloak_mces_golden_nested_membership.json",
    )


@freeze_time(FROZEN_TIME)
def test_keycloak_ingestion_default_config(pytestconfig, mock_datahub_graph, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/keycloak"
    output_file_path = f"{tmp_path}/keycloak_mces_default.json"

    run_ingest(
        mock_datahub_graph=mock_datahub_graph,
        mocked_functions_reference=partial(
            _init_mock_keycloak_admin, test_resources_dir=test_resources_dir
        ),
        recipe=default_recipe(output_file_path),
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file_path,
        golden_path=f"{test_resources_dir}/keycloak_mces_golden_default_config.json",
    )


@freeze_time(FROZEN_TIME)
def test_keycloak_ingestion_disabled(pytestconfig, mock_datahub_graph, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/keycloak"
    output_file_path = f"{tmp_path}/keycloak_mces_disabled.json"

    recipe = default_recipe(output_file_path)
    # Disable all ingestion
    recipe["source"]["config"]["ingest_users"] = False
    recipe["source"]["config"]["ingest_groups"] = False
    recipe["source"]["config"]["ingest_group_membership"] = False

    run_ingest(
        mock_datahub_graph=mock_datahub_graph,
        mocked_functions_reference=partial(
            _init_mock_keycloak_admin, test_resources_dir=test_resources_dir
        ),
        recipe=recipe,
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file_path,
        golden_path=f"{test_resources_dir}/keycloak_mces_golden_ingestion_disabled.json",
    )


@freeze_time(FROZEN_TIME)
def test_keycloak_stateful_ingestion(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/keycloak"
    output_file_path = f"{tmp_path}/keycloak_mces_stateful.json"

    recipe = default_recipe(output_file_path)
    recipe["pipeline_name"] = "keycloak_execution"
    recipe["source"]["config"]["stateful_ingestion"] = {
        "enabled": True,
        "state_provider": {
            "type": "datahub",
            "config": {"datahub_api": {"server": GMS_SERVER}},
        },
    }

    pipeline1 = run_ingest(
        mock_datahub_graph=mock_datahub_graph,
        mocked_functions_reference=partial(
            _init_mock_keycloak_admin, test_resources_dir=test_resources_dir
        ),
        recipe=recipe,
    )

    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline1)
    assert checkpoint1
    assert checkpoint1.state

    pipeline2 = run_ingest(
        mock_datahub_graph=mock_datahub_graph,
        mocked_functions_reference=partial(
            _init_mock_keycloak_admin_deleted_group,
            test_resources_dir=test_resources_dir,
        ),
        recipe=recipe,
    )

    checkpoint2 = get_current_checkpoint_from_pipeline(pipeline2)
    assert checkpoint2
    assert checkpoint2.state

    validate_all_providers_have_committed_successfully(
        pipeline=pipeline1, expected_providers=1
    )
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline2, expected_providers=1
    )

    state1 = checkpoint1.state
    state2 = checkpoint2.state

    difference_group_urns = list(
        state1.get_urns_not_in(type="corpGroup", other_checkpoint_state=state2)
    )

    assert len(difference_group_urns) == 1
    assert difference_group_urns == ["urn:li:corpGroup:random_group_2"]


def default_recipe(output_file_path):
    return {
        "run_id": "test-keycloak-ingestion",
        "source": {
            "type": "keycloak",
            "config": {
                "server_url": "https://keycloak.example.com",
                "realm": "myrealm",
                "client_id": "myclient",
                "client_secret": "mysecret",
                "ingest_users": True,
                "ingest_groups": True,
                "ingest_group_membership": True,
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
    with (
        patch("datahub.ingestion.source.identity.keycloak.KeycloakAdmin") as MockAdmin,
        patch(
            "datahub.ingestion.source.identity.keycloak.KeycloakOpenIDConnection"
        ) as MockConnection,
        patch(
            "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
            mock_datahub_graph,
        ),
    ):
        mocked_functions_reference(MockAdmin=MockAdmin, MockConnection=MockConnection)

        pipeline = Pipeline.create(recipe)
        pipeline.run()
        pipeline.raise_from_status()

        return pipeline


# Initializes a mock Keycloak Admin client to return users and groups from JSON fixtures.
def _init_mock_keycloak_admin(
    test_resources_dir, MockAdmin, MockConnection, mock_groups_json=None
):
    mock_instance = MockAdmin.return_value

    groups_json_file = mock_groups_json if mock_groups_json else "keycloak_groups.json"
    with open(test_resources_dir / groups_json_file) as f:
        groups_data = json.load(f)

    with open(test_resources_dir / "keycloak_users.json") as f:
        users_data = json.load(f)

    mock_instance.get_groups.return_value = groups_data

    side_effect = []
    if groups_json_file == "keycloak_groups.json":
        # random_group_1: first user, then empty (pagination)
        # random_group_2: no members
        side_effect.append([users_data[0]])
        side_effect.append([])
        side_effect.append([])
    elif groups_json_file == "keycloak_nested_groups.json":
        mock_instance.get_group_members.return_value = []
        side_effect.clear()
    else:
        mock_instance.get_group_members.return_value = []
        side_effect.clear()

    if side_effect:
        mock_instance.get_group_members.side_effect = side_effect

    mock_instance.get_users_count.return_value = len(users_data)
    mock_instance.get_users.return_value = users_data


# Initializes mock Keycloak client with nested groups and specific user-group membership.
def _init_mock_keycloak_admin_nested_membership(
    test_resources_dir, MockAdmin, MockConnection
):
    mock_instance = MockAdmin.return_value

    with open(test_resources_dir / "keycloak_nested_groups.json") as f:
        groups_data = json.load(f)

    with open(test_resources_dir / "keycloak_users.json") as f:
        users_data = json.load(f)

    mock_instance.get_groups.return_value = groups_data

    # sequence of calls: Group 1 -> Group 2 -> Group 3
    side_effect = [
        [users_data[0]],  # Call 1: Group 1 -> John Doe is a member
        [],  # Call 2: Group 2 -> Empty (John Doe NOT a member)
        [users_data[0]],  # Call 3: Group 3 -> John Doe is a member
    ]

    mock_instance.get_group_members.side_effect = side_effect

    mock_instance.get_users_count.return_value = len(users_data)
    mock_instance.get_users.return_value = users_data


# Initializes mock with deleted groups fixture for stateful ingestion test.
def _init_mock_keycloak_admin_deleted_group(
    test_resources_dir, MockAdmin, MockConnection
):
    _init_mock_keycloak_admin(
        test_resources_dir,
        MockAdmin,
        MockConnection,
        mock_groups_json="keycloak_deleted_groups.json",
    )
