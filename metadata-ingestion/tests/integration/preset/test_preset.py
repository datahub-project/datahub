from typing import Any, Dict, Optional
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2024-07-10 07:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


def register_mock_api(request_mock: Any, override_data: Optional[dict] = None) -> None:
    if override_data is None:
        override_data = {}

    api_vs_response = {
        "mock://mock-domain.preset.io/v1/auth/": {
            "method": "POST",
            "status_code": 200,
            "json": {
                "payload": {
                    "access_token": "test_token",
                }
            },
        },
        "mock://mock-domain.preset.io/version": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "ci": {
                    "built_at": "Tue Jul  10 00:00:00 UTC 2024",
                    "build_num": "1",
                    "triggered_by": "Not triggered by a user",
                },
                "git": {
                    "branch": "4.0.1.6",
                    "sha": "test_sha",
                    "sha_superset": "test_sha_superset",
                    "release_name": "test_release_name",
                },
                "chart_version": "1.16.1",
                "start_time": "2024-07-10 00:00:00",
                "mt_deployment": True,
            },
        },
        "mock://mock-domain.preset.io/api/v1/dashboard/": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "count": 2,
                "result": [
                    {
                        "id": "1",
                        "changed_by": {
                            "username": "test_username_1",
                        },
                        "changed_on_utc": "2024-07-10T07:00:00.000000+0000",
                        "dashboard_title": "test_dashboard_title_1",
                        "url": "/dashboard/test_dashboard_url_1",
                        "position_json": '{"CHART-test-1": {"meta": { "chartId": "10" }}, "CHART-test-2": {"meta": { "chartId": "11" }}}',
                        "status": "published",
                        "published": True,
                        "owners": [
                            {
                                "username": "test_username_1",
                            },
                            {
                                "username": "test_username_2",
                            },
                        ],
                        "certified_by": "Certification team",
                        "certification_details": "Approved",
                    },
                    {
                        "id": "2",
                        "changed_by": {
                            "username": "test_username_2",
                        },
                        "changed_on_utc": "2024-07-10T07:00:00.000000+0000",
                        "dashboard_title": "test_dashboard_title_2",
                        "url": "/dashboard/test_dashboard_url_2",
                        "position_json": '{"CHART-test-3": {"meta": { "chartId": "12" }}, "CHART-test-4": {"meta": { "chartId": "13" }}}',
                        "status": "draft",
                        "published": False,
                        "owners": [
                            {
                                "first_name": "name",
                            },
                        ],
                        "certified_by": "",
                        "certification_details": "",
                    },
                ],
            },
        },
        "mock://mock-domain.preset.io/api/v1/chart/": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "count": 4,
                "result": [
                    {
                        "id": "10",
                        "changed_by": {
                            "username": "test_username_1",
                        },
                        "changed_on_utc": "2024-07-10T07:00:00.000000+0000",
                        "slice_name": "test_chart_title_1",
                        "viz_type": "box_plot",
                        "url": "/explore/test_chart_url_10",
                        "datasource_id": "20",
                        "params": '{"metrics": [], "adhoc_filters": []}',
                    },
                    {
                        "id": "11",
                        "changed_by": {
                            "username": "test_username_1",
                        },
                        "changed_on_utc": "2024-07-10T07:00:00.000000+0000",
                        "slice_name": "test_chart_title_2",
                        "viz_type": "pie",
                        "url": "/explore/test_chart_url_11",
                        "datasource_id": "20",
                        "params": '{"metrics": [], "adhoc_filters": []}',
                    },
                    {
                        "id": "12",
                        "changed_by": {
                            "username": "test_username_2",
                        },
                        "changed_on_utc": "2024-07-10T07:00:00.000000+0000",
                        "slice_name": "test_chart_title_3",
                        "viz_type": "treemap",
                        "url": "/explore/test_chart_url_12",
                        "datasource_id": "20",
                        "params": '{"metrics": [], "adhoc_filters": []}',
                    },
                    {
                        "id": "13",
                        "changed_by": {
                            "username": "test_username_2",
                        },
                        "changed_on_utc": "2024-07-10T07:00:00.000000+0000",
                        "slice_name": "test_chart_title_4",
                        "viz_type": "histogram",
                        "url": "/explore/test_chart_url_13",
                        "datasource_id": "20",
                        "params": '{"metrics": [], "adhoc_filters": []}',
                    },
                ],
            },
        },
        "mock://mock-domain.preset.io/api/v1/dataset/20": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "result": {
                    "schema": "test_schema_name",
                    "table_name": "test_table_name",
                    "database": {
                        "id": "30",
                        "database_name": "test_database_name",
                    },
                },
            },
        },
        "mock://mock-domain.preset.io/api/v1/database/30": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "result": {
                    "sqlalchemy_uri": "test_sqlalchemy_uri",
                },
            },
        },
    }

    api_vs_response.update(override_data)

    for url in api_vs_response:
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url]["json"],
            status_code=api_vs_response[url]["status_code"],
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_preset_ingest(pytestconfig, tmp_path, mock_time, requests_mock):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/preset"

    register_mock_api(request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "preset-test",
            "source": {
                "type": "preset",
                "config": {
                    "connect_uri": "mock://mock-domain.preset.io/",
                    "manager_uri": "mock://mock-domain.preset.io",
                    "api_key": "test_key",
                    "api_secret": "test_secret",
                    "provider": "db",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/preset_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_ingest.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "preset_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_preset_stateful_ingest(
    pytestconfig, tmp_path, mock_time, requests_mock, mock_datahub_graph
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/preset"

    register_mock_api(request_mock=requests_mock)

    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "preset",
            "config": {
                "connect_uri": "mock://mock-domain.preset.io/",
                "manager_uri": "mock://mock-domain.preset.io",
                "api_key": "test_key",
                "api_secret": "test_secret",
                "provider": "db",
                # enable stateful ingestion
                "stateful_ingestion": {
                    "enabled": True,
                    "remove_stale_metadata": True,
                    "fail_safe_threshold": 100.0,
                    "state_provider": {
                        "type": "datahub",
                        "config": {"datahub_api": {"server": GMS_SERVER}},
                    },
                },
            },
        },
        "sink": {
            # we are not really interested in the resulting events for this test
            "type": "console"
        },
        "pipeline_name": "test_pipeline",
    }

    dashboard_endpoint_override = {
        "mock://mock-domain.preset.io/api/v1/dashboard/": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "count": 1,
                "result": [
                    {
                        "id": "1",
                        "changed_by": {
                            "username": "test_username_1",
                        },
                        "changed_on_utc": "2024-07-10T07:00:00.000000+0000",
                        "dashboard_title": "test_dashboard_title_1",
                        "url": "/dashboard/test_dashboard_url_1",
                        "position_json": '{"CHART-test-1": {"meta": { "chartId": "10" }}, "CHART-test-2": {"meta": { "chartId": "11" }}}',
                        "status": "published",
                        "published": True,
                        "owners": [
                            {
                                "username": "test_username_1",
                            },
                            {
                                "username": "test_username_2",
                            },
                        ],
                        "certified_by": "Certification team",
                        "certification_details": "Approved",
                    },
                ],
            },
        },
    }

    with patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        # Both checkpoint and reporting will use the same mocked graph instance.
        mock_checkpoint.return_value = mock_datahub_graph

        # Do the first run of the pipeline and get the default job's checkpoint.
        pipeline_run1 = run_and_get_pipeline(pipeline_config_dict)
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)

        assert checkpoint1
        assert checkpoint1.state

        # Remove one dashboard from the preset config.
        register_mock_api(
            request_mock=requests_mock, override_data=dashboard_endpoint_override
        )

        # Capture MCEs of second run to validate Status(removed=true)
        deleted_mces_path = f"{tmp_path}/preset_deleted_mces.json"
        pipeline_config_dict["sink"]["type"] = "file"
        pipeline_config_dict["sink"]["config"] = {"filename": deleted_mces_path}

        # Do the second run of the pipeline.
        pipeline_run2 = run_and_get_pipeline(pipeline_config_dict)
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

        assert checkpoint2
        assert checkpoint2.state

        # Perform all assertions on the states. The deleted dashboard should not be
        # part of the second state
        state1 = checkpoint1.state
        state2 = checkpoint2.state
        difference_urns = list(
            state1.get_urns_not_in(type="dashboard", other_checkpoint_state=state2)
        )

        assert len(difference_urns) == 1

        urn1 = "urn:li:dashboard:(preset,2)"

        assert urn1 in difference_urns

        # Validate that all providers have committed successfully.
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run1, expected_providers=1
        )
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run2, expected_providers=1
        )

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=deleted_mces_path,
            golden_path=test_resources_dir / "golden_test_stateful_ingest.json",
        )
