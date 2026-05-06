from pathlib import Path
from typing import Any
from unittest import mock

import time_machine

from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import (
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2022-02-03 07:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"

test_resources_dir = Path(__file__).parent


def mock_msal_cca(*args, **kwargs):
    class MsalClient:
        def acquire_token_for_client(self, *args, **kwargs):
            return {
                "access_token": "dummy",
            }

    return MsalClient()


def register_mock_api_state1(request_mock):
    api_vs_response = {
        "https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo": {
            "method": "POST",
            "status_code": 403,
            "json": {},
        },
        "https://api.powerbi.com/v1.0/myorg/groups?%24skip=0&%24top=1000": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "id": "64ED5CAD-7C10-4684-8180-826122881108",
                        "isReadOnly": True,
                        "name": "Workspace 1",
                        "type": "Workspace",
                        "state": "Active",
                    },
                ],
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups?%24skip=1000&%24top=1000": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [],
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/dashboards": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "id": "7D668CAD-7FFC-4505-9215-655BCA5BEBAE",
                        "isReadOnly": True,
                        "displayName": "marketing",
                        "embedUrl": "https://localhost/dashboards/embed/1",
                        "webUrl": "https://localhost/dashboards/web/1",
                    },
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/dashboards/7D668CAD-7FFC-4505-9215-655BCA5BEBAE/tiles": {
            "method": "GET",
            "status_code": 200,
            "json": {"value": []},
        },
        "https://api.powerbi.com/v1.0/myorg/admin/dashboards/7D668CAD-7FFC-4505-9215-655BCA5BEBAE/users": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "identifier": "user1@foo.com",
                        "displayName": "user1",
                        "emailAddress": "user1@foo.com",
                        "datasetUserAccessRight": "ReadWrite",
                        "graphId": "C9EE53F2-88EA-4711-A173-AF0515A3CD46",
                        "principalType": "User",
                    },
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/reports": {
            "method": "GET",
            "status_code": 200,
            "json": {"value": []},
        },
    }

    for url in api_vs_response:
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url]["json"],
            status_code=api_vs_response[url]["status_code"],
        )


def register_mock_api_state2(request_mock):
    api_vs_response = {
        "https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo": {
            "method": "POST",
            "status_code": 403,
            "json": {},
        },
        "https://api.powerbi.com/v1.0/myorg/groups?%24skip=0&%24top=1000": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "id": "64ED5CAD-7C10-4684-8180-826122881108",
                        "isReadOnly": True,
                        "name": "Workspace 1",
                        "type": "Workspace",
                        "state": "Active",
                    },
                ],
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups?%24skip=1000&%24top=1000": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [],
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/dashboards": {
            "method": "GET",
            "status_code": 200,
            "json": {"value": []},
        },
    }

    for url in api_vs_response:
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url]["json"],
            status_code=api_vs_response[url]["status_code"],
        )


@time_machine.travel(FROZEN_TIME, tick=False)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_create_corp_user(
    pytestconfig,
    tmp_path,
    requests_mock,
    mock_datahub_graph,
):
    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph

        source_config_dict = {
            "client_id": "foo",
            "client_secret": "bar",
            "tenant_id": "0B0C960B-FCDF-4D0F-8C45-2E03BB59DDEB",
            "extract_ownership": True,
            "ownership": {
                "create_corp_user": True,
            },
            "stateful_ingestion": {
                "enabled": True,
                "fail_safe_threshold": 100.0,
                "state_provider": {
                    "type": "datahub",
                    "config": {"datahub_api": {"server": GMS_SERVER}},
                },
            },
        }

        pipeline_config_dict: dict[str, Any] = {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": source_config_dict,
            },
            "sink": {
                "type": "file",
                "config": {},
            },
            "pipeline_name": "powerbi-pipeline",
        }

        # Do the first run of the pipeline
        register_mock_api_state1(request_mock=requests_mock)
        mces_path = "{}/{}".format(tmp_path, "powerbi_create_corp_user.json")
        pipeline_config_dict["sink"]["config"] = {"filename": mces_path}
        pipeline_run1 = run_and_get_pipeline(pipeline_config_dict)

        # Do the second run of the pipeline
        register_mock_api_state2(request_mock=requests_mock)
        deleted_mces_path = "{}/{}".format(
            tmp_path, "powerbi_create_corp_user_deleted.json"
        )
        pipeline_config_dict["sink"]["config"] = {"filename": deleted_mces_path}
        pipeline_run2 = run_and_get_pipeline(pipeline_config_dict)

        # Validate that all providers have committed successfully.
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run1, expected_providers=1
        )
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run2, expected_providers=1
        )

        # Validate against golden MCEs
        # CorpUser should show up in the first set
        # CorpUser should _NOT_ be removed in the second set
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=mces_path,
            golden_path=test_resources_dir / "golden_test_create_corp_user.json",
        )
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=deleted_mces_path,
            golden_path=test_resources_dir
            / "golden_test_create_corp_user_deleted.json",
        )
