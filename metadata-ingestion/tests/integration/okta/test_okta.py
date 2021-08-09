import pathlib
import asyncio
from unittest.mock import Mock, patch
from datetime import datetime, timedelta, timezone

import jsonpickle
import pydantic
import pytest

from okta.models import Group, GroupProfile, User, UserProfile, UserStatus

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.identity.okta import (
    OktaConfig,
    OktaSource,
)
from tests.test_helpers import mce_helpers

def test_okta_config():
    config = OktaConfig.parse_obj(
        dict(
            okta_domain="test.okta.com",
            okta_api_token="test-token"
        )
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

def test_okta_source(pytestconfig, tmp_path):
    # from google.cloud.logging_v2 import ProtobufEntry

    test_resources_dir: pathlib.Path = (
        pytestconfig.rootpath / "tests/integration/okta"
    )
    okta_users_json = test_resources_dir / "okta_users.json"
    okta_groups_json = test_resources_dir / "okta_groups.json"

    with patch(
        "datahub.ingestion.source.identity.okta.OktaClient", autospec=True
    ) as MockClient:
        # Add mock Okta API responses.
        with okta_users_json.open() as users:
            reference_users = jsonpickle.decode(users.read())
            # Create users from JSON dicts
            users = list(map(lambda userJson: User(userJson), reference_users))

        with okta_groups_json.open() as groups:
            reference_groups = jsonpickle.decode(groups.read())
            # Create groups from JSON dicts 
            groups = list(map(lambda groupJson: Group(groupJson), reference_groups))


        # For simplicity, each user is placed in ALL groups. 

        # Mock Client List response.
        users_resp_mock = Mock()
        users_resp_mock.has_next.side_effect = [ True, False ] 
        users_next_future = asyncio.Future()
        users_next_future.set_result(
            # users, err            
            ( [ users[-1] ], None )
        )
        users_resp_mock.next.return_value = users_next_future

        MockClient().list_users.return_value = ( users[0:-1], users_resp_mock, None )

        # Mock Client Init 
        groups_resp_mock = Mock()
        groups_resp_mock.has_next.side_effect = [ True, False ] 
        groups_next_future = asyncio.Future()
        groups_next_future.set_result(
            # groups, err
            ( [ groups[-1] ], None )
        )
        groups_resp_mock.next.return_value = groups_next_future

        MockClient().list_groups.return_value = ( groups[0:-1], groups_resp_mock, None )


        # Create a separate response mock for each group in our sample data. 
        list_group_users_result_values = []
        for group in groups:
                
            # Mock Get Group Membership
            group_users_resp_mock = Mock()
            group_users_resp_mock.has_next.side_effect = [ True, False ] 
            group_users_next_future = asyncio.Future()
            group_users_next_future.set_result(
                # users, err 
                ( [ users[-1] ], None )
            )
            group_users_resp_mock.next.return_value = group_users_next_future
            # users, resp, err
            list_group_users_result_values.append(( users[0:-1], group_users_resp_mock, None ))

        MockClient().list_group_users.side_effect = list_group_users_result_values

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
                        "delay_seconds": "0.00"
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/okta_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "okta_mces.json",
        golden_path=test_resources_dir / "okta_mces_golden.json",
    )
