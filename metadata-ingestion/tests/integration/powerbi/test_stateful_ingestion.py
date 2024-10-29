from typing import Any, Dict, Optional, cast
from unittest import mock

from freezegun import freeze_time

from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.powerbi.powerbi import PowerBiDashboardSource
from datahub.ingestion.source.state.checkpoint import Checkpoint
from tests.test_helpers.state_helpers import (
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2022-02-03 07:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


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
                    {
                        "id": "44444444-7C10-4684-8180-826122881108",
                        "isReadOnly": True,
                        "name": "Multi Workspace",
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
                    {
                        "id": "e41cbfe7-9f54-40ad-8d6a-043ab97cf303",
                        "isReadOnly": True,
                        "displayName": "sales",
                        "embedUrl": "https://localhost/dashboards/embed/1",
                        "webUrl": "https://localhost/dashboards/web/1",
                    },
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/44444444-7C10-4684-8180-826122881108/dashboards": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "id": "7D668CAD-4444-4505-9215-655BCA5BEBAE",
                        "isReadOnly": True,
                        "displayName": "marketing",
                        "embedUrl": "https://localhost/dashboards/embed/multi_workspace",
                        "webUrl": "https://localhost/dashboards/web/multi_workspace",
                    },
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/dashboards/7D668CAD-7FFC-4505-9215-655BCA5BEBAE/tiles": {
            "method": "GET",
            "status_code": 200,
            "json": {"value": []},
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/dashboards/e41cbfe7-9f54-40ad-8d6a-043ab97cf303/tiles": {
            "method": "GET",
            "status_code": 200,
            "json": {"value": []},
        },
        "https://api.powerbi.com/v1.0/myorg/groups/44444444-7C10-4684-8180-826122881108/dashboards/7D668CAD-4444-4505-9215-655BCA5BEBAE/tiles": {
            "method": "GET",
            "status_code": 200,
            "json": {"value": []},
        },
    }

    for url in api_vs_response.keys():
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
                "@odata.count": 1,
                "value": [
                    {
                        "id": "64ED5CAD-7C10-4684-8180-826122881108",
                        "isReadOnly": True,
                        "name": "Workspace 1",
                        "type": "Workspace",
                    },
                    {
                        "id": "44444444-7C10-4684-8180-826122881108",
                        "isReadOnly": True,
                        "name": "Multi Workspace",
                        "type": "Workspace",
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
                    }
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/44444444-7C10-4684-8180-826122881108/dashboards": {
            "method": "GET",
            "status_code": 200,
            "json": {"value": []},
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/dashboards/7D668CAD-7FFC-4505-9215-655BCA5BEBAE/tiles": {
            "method": "GET",
            "status_code": 200,
            "json": {"value": []},
        },
    }

    for url in api_vs_response.keys():
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url]["json"],
            status_code=api_vs_response[url]["status_code"],
        )


def default_source_config():
    return {
        "client_id": "foo",
        "client_secret": "bar",
        "tenant_id": "0B0C960B-FCDF-4D0F-8C45-2E03BB59DDEB",
        "extract_lineage": False,
        "extract_reports": False,
        "extract_ownership": False,
        "stateful_ingestion": {
            "enabled": True,
            "state_provider": {
                "type": "datahub",
                "config": {"datahub_api": {"server": GMS_SERVER}},
            },
        },
        "convert_lineage_urns_to_lowercase": False,
        "workspace_id_pattern": {
            "allow": [
                "64ED5CAD-7C10-4684-8180-826122881108",
                "44444444-7C10-4684-8180-826122881108",
            ]
        },
        "dataset_type_mapping": {
            "PostgreSql": "postgres",
            "Oracle": "oracle",
        },
        "env": "DEV",
    }


def mock_msal_cca(*args, **kwargs):
    class MsalClient:
        def acquire_token_for_client(self, *args, **kwargs):
            return {
                "access_token": "dummy",
            }

    return MsalClient()


def get_current_checkpoint_from_pipeline(
    pipeline: Pipeline,
) -> Dict[JobId, Optional[Checkpoint[Any]]]:
    powerbi_source = cast(PowerBiDashboardSource, pipeline.source)
    checkpoints = {}
    for job_id in powerbi_source.state_provider._usecase_handlers.keys():
        # for multi-workspace checkpoint, every good checkpoint will have an unique workspaceid suffix
        checkpoints[job_id] = powerbi_source.state_provider.get_current_checkpoint(
            job_id
        )
    return checkpoints


def ingest(pipeline_name, tmp_path, mock_datahub_graph):
    config_dict = {
        "pipeline_name": pipeline_name,
        "source": {
            "type": "powerbi",
            "config": {
                **default_source_config(),
            },
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": f"{tmp_path}/powerbi_mces.json",
            },
        },
    }
    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph

        pipeline = Pipeline.create(config_dict)
        pipeline.run()
        pipeline.raise_from_status()

        return pipeline


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_powerbi_stateful_ingestion(
    mock_msal, pytestconfig, tmp_path, mock_time, requests_mock, mock_datahub_graph
):
    register_mock_api_state1(request_mock=requests_mock)
    pipeline1 = ingest("run1", tmp_path, mock_datahub_graph)
    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline1)
    for checkpoint in checkpoint1.values():
        assert checkpoint
        assert checkpoint.state

    register_mock_api_state2(request_mock=requests_mock)
    pipeline2 = ingest("run2", tmp_path, mock_datahub_graph)
    checkpoint2 = get_current_checkpoint_from_pipeline(pipeline2)
    for checkpoint in checkpoint2.values():
        assert checkpoint
        assert checkpoint.state

    # Validate that all providers have committed successfully.
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline1, expected_providers=1
    )
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline2, expected_providers=1
    )

    # Perform all assertions on the states. The deleted Dashboard should not be
    # part of the second state
    for job_id in checkpoint1.keys():
        if isinstance(checkpoint1[job_id], Checkpoint) and isinstance(
            checkpoint2[job_id], Checkpoint
        ):
            state1 = checkpoint1[job_id].state  # type:ignore
            state2 = checkpoint2[job_id].state  # type:ignore

        if (
            job_id
            == "powerbi_stale_entity_removal_64ED5CAD-7C10-4684-8180-826122881108"
        ):
            difference_dashboard_urns = list(
                state1.get_urns_not_in(type="dashboard", other_checkpoint_state=state2)
            )

            assert len(difference_dashboard_urns) == 1
            assert difference_dashboard_urns == [
                "urn:li:dashboard:(powerbi,dashboards.e41cbfe7-9f54-40ad-8d6a-043ab97cf303)"
            ]
        elif (
            job_id
            == "powerbi_stale_entity_removal_44444444-7C10-4684-8180-826122881108"
        ):
            difference_dashboard_urns = list(
                state1.get_urns_not_in(type="dashboard", other_checkpoint_state=state2)
            )

            assert len(difference_dashboard_urns) == 1
            assert difference_dashboard_urns == [
                "urn:li:dashboard:(powerbi,dashboards.7D668CAD-4444-4505-9215-655BCA5BEBAE)"
            ]
