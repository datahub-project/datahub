import asyncio
import pathlib
from unittest.mock import Mock, patch

import jsonpickle
import pytest
from freezegun import freeze_time
from okta.models import Group, User

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.identity.okta import OktaConfig
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2020-04-14 07:00:00"


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
    assert config.okta_profile_to_username_attr == "login"
    assert config.okta_profile_to_username_regex == "([^@]+)"
    assert config.okta_profile_to_group_name_attr == "name"
    assert config.okta_profile_to_group_name_regex == "(.*)"
    assert config.include_deprovisioned_users is False
    assert config.include_suspended_users is False
    assert config.page_size == 100
    assert config.delay_seconds == 0.01


@freeze_time(FROZEN_TIME)
def test_okta_source_default_configs(pytestconfig, tmp_path):

    test_resources_dir: pathlib.Path = pytestconfig.rootpath / "tests/integration/okta"

    with patch("datahub.ingestion.source.identity.okta.OktaClient") as MockClient:

        _init_mock_okta_client(test_resources_dir, MockClient)

        # Run an Okta usage ingestion run.
        pipeline = Pipeline.create(
            {
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
                        "filename": f"{tmp_path}/okta_mces_default_config.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "okta_mces_default_config.json",
        golden_path=test_resources_dir / "okta_mces_golden_default_config.json",
    )


@freeze_time(FROZEN_TIME)
def test_okta_source_ingestion_disabled(pytestconfig, tmp_path):

    test_resources_dir: pathlib.Path = pytestconfig.rootpath / "tests/integration/okta"

    with patch("datahub.ingestion.source.identity.okta.OktaClient") as MockClient:

        _init_mock_okta_client(test_resources_dir, MockClient)

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


@freeze_time(FROZEN_TIME)
@pytest.mark.asyncio
def test_okta_source_include_deprovisioned_suspended_users(pytestconfig, tmp_path):

    test_resources_dir: pathlib.Path = pytestconfig.rootpath / "tests/integration/okta"

    with patch("datahub.ingestion.source.identity.okta.OktaClient") as MockClient:

        _init_mock_okta_client(test_resources_dir, MockClient)

        # Run an Okta usage ingestion run.
        pipeline = Pipeline.create(
            {
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
                        "include_deprovisioned_users": "True",
                        "include_suspended_users": "True",
                        "page_size": "2",
                        "delay_seconds": "0.00",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/okta_mces_include_deprovisioned_suspended_users.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "okta_mces_include_deprovisioned_suspended_users.json",
        golden_path=test_resources_dir
        / "okta_mces_golden_include_deprovisioned_suspended_users.json",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.asyncio
def test_okta_source_custom_user_name_regex(pytestconfig, tmp_path):

    test_resources_dir: pathlib.Path = pytestconfig.rootpath / "tests/integration/okta"

    with patch("datahub.ingestion.source.identity.okta.OktaClient") as MockClient:

        _init_mock_okta_client(test_resources_dir, MockClient)

        # Run an Okta usage ingestion run.
        pipeline = Pipeline.create(
            {
                "run_id": "test-okta-usage",
                "source": {
                    "type": "okta",
                    "config": {
                        "okta_domain": "mock-domain.okta.com",
                        "okta_api_token": "mock-okta-token",
                        "ingest_users": "True",
                        "ingest_groups": "True",
                        "ingest_group_membership": "True",
                        "okta_profile_to_username_attr": "email",
                        "okta_profile_to_username_regex": "(.*)",
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
                        "filename": f"{tmp_path}/okta_mces_custom_user_name_regex.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "okta_mces_custom_user_name_regex.json",
        golden_path=test_resources_dir / "okta_mces_golden_custom_user_name_regex.json",
    )


# Initializes a Mock Okta Client to return users from okta_users.json and groups from okta_groups.json.
def _init_mock_okta_client(test_resources_dir, MockClient):

    okta_users_json_file = test_resources_dir / "okta_users.json"
    okta_groups_json_file = test_resources_dir / "okta_groups.json"

    # Add mock Okta API responses.
    with okta_users_json_file.open() as okta_users_json:
        reference_users = jsonpickle.decode(okta_users_json.read())
        # Create users from JSON dicts
        users = list(map(lambda userJson: User(userJson), reference_users))

    with okta_groups_json_file.open() as okta_groups_json:
        reference_groups = jsonpickle.decode(okta_groups_json.read())
        # Create groups from JSON dicts
        groups = list(map(lambda groupJson: Group(groupJson), reference_groups))

    # For simplicity, each user is placed in ALL groups.

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
    for group in groups:

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
        list_group_users_future.set_result((users[0:-1], group_users_resp_mock, None))
        list_group_users_result_values.append(list_group_users_future)

    MockClient().list_group_users.side_effect = list_group_users_result_values
