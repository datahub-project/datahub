import json
import pathlib
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from requests.models import HTTPError

from datahub.configuration.common import PipelineExecutionError
from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2021-11-11 07:00:00"

GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"

JSON_RESPONSE_MAP = {
    "http://localhost:3000/api/session": "session.json",
    "http://localhost:3000/api/user/current": "user.json",
    "http://localhost:3000/api/collection/?exclude-other-user-collections=false": "collections.json",
    "http://localhost:3000/api/collection/root/items?models=dashboard": "collection_dashboards.json",
    "http://localhost:3000/api/collection/150/items?models=dashboard": "collection_dashboards.json",
    "http://localhost:3000/api/dashboard/10": "dashboard_1.json",
    "http://localhost:3000/api/dashboard/20": "dashboard_2.json",
    "http://localhost:3000/api/user/1": "user.json",
    "http://localhost:3000/api/card": "card.json",
    "http://localhost:3000/api/database/1": "bigquery_database.json",
    "http://localhost:3000/api/database/2": "postgres_database.json",
    "http://localhost:3000/api/card/1": "card_1.json",
    "http://localhost:3000/api/card/2": "card_2.json",
    "http://localhost:3000/api/table/21": "table_21.json",
    "http://localhost:3000/api/card/3": "card_3.json",
}

RESPONSE_ERROR_LIST = ["http://localhost:3000/api/dashboard/public"]

test_resources_dir = pathlib.Path(__file__).parent


class MockResponse:
    def __init__(self, url, data=None, jsond=None, error_list=None):
        self.json_data = data
        self.url = url
        self.jsond = jsond
        self.error_list = error_list
        self.headers = {}
        self.auth = None
        self.status_code = 200

    def json(self):
        response_json_path = (
            f"{test_resources_dir}/setup/{JSON_RESPONSE_MAP.get(self.url)}"
        )
        with open(response_json_path) as file:
            data = json.loads(file.read())
            self.json_data = data
        return self.json_data

    def get(self, url):
        self.url = url
        return self

    def raise_for_status(self):
        if self.error_list is not None and self.url in self.error_list:
            http_error_msg = "%s Client Error: %s for url: %s" % (
                400,
                "Simulate error",
                self.url,
            )
            raise HTTPError(http_error_msg, response=self)


def mocked_requests_sucess(*args, **kwargs):
    return MockResponse(None)


def mocked_requests_failure(*args, **kwargs):
    return MockResponse(None, error_list=RESPONSE_ERROR_LIST)


def mocked_requests_session_post(url, data, json):
    return MockResponse(url, data, json)


def mocked_requests_session_delete(url, headers):
    return MockResponse(url, data=None, jsond=headers)


@pytest.fixture
def test_pipeline(pytestconfig, tmp_path):
    return {
        "run_id": "metabase-test",
        "source": {
            "type": "metabase",
            "config": {
                "username": "xxxx",
                "password": "xxxx",
                "connect_uri": "http://localhost:3000/",
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
        "pipeline_name": "test_pipeline",
        "sink": {
            "type": "file",
            "config": {
                "filename": f"{tmp_path}/metabase_mces.json",
            },
        },
    }


@freeze_time(FROZEN_TIME)
def test_mode_ingest_success(pytestconfig, tmp_path, test_pipeline, mock_datahub_graph):
    with patch(
        "datahub.ingestion.source.metabase.requests.session",
        side_effect=mocked_requests_sucess,
    ), patch(
        "datahub.ingestion.source.metabase.requests.post",
        side_effect=mocked_requests_session_post,
    ), patch(
        "datahub.ingestion.source.metabase.requests.delete",
        side_effect=mocked_requests_session_delete,
    ), patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph

        pipeline = Pipeline.create(test_pipeline)
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/metabase_mces.json",
            golden_path=test_resources_dir / "metabase_mces_golden.json",
            ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
        )


@freeze_time(FROZEN_TIME)
def test_stateful_ingestion(test_pipeline, mock_datahub_graph):
    with patch(
        "datahub.ingestion.source.metabase.requests.session",
        side_effect=mocked_requests_sucess,
    ), patch(
        "datahub.ingestion.source.metabase.requests.post",
        side_effect=mocked_requests_session_post,
    ), patch(
        "datahub.ingestion.source.metabase.requests.delete",
        side_effect=mocked_requests_session_delete,
    ), patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph

        pipeline_run1 = run_and_get_pipeline(test_pipeline)
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)

        assert checkpoint1
        assert checkpoint1.state

        # Mock the removal of one of the dashboards
        JSON_RESPONSE_MAP[
            "http://localhost:3000/api/collection/root/items?models=dashboard"
        ] = "collection_dashboards_deleted_item.json"
        JSON_RESPONSE_MAP[
            "http://localhost:3000/api/collection/150/items?models=dashboard"
        ] = "collection_dashboards_deleted_item.json"

        pipeline_run2 = run_and_get_pipeline(test_pipeline)
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

        assert checkpoint2
        assert checkpoint2.state

        state1 = checkpoint1.state
        state2 = checkpoint2.state

        difference_urns = list(
            state1.get_urns_not_in(type="dashboard", other_checkpoint_state=state2)
        )

        assert len(difference_urns) == 1
        assert difference_urns[0] == "urn:li:dashboard:(metabase,20)"

        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run1, expected_providers=1
        )
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run2, expected_providers=1
        )


@freeze_time(FROZEN_TIME)
def test_mode_ingest_failure(pytestconfig, tmp_path):
    with patch(
        "datahub.ingestion.source.metabase.requests.session",
        side_effect=mocked_requests_failure,
    ), patch(
        "datahub.ingestion.source.metabase.requests.post",
        side_effect=mocked_requests_session_post,
    ), patch(
        "datahub.ingestion.source.metabase.requests.delete",
        side_effect=mocked_requests_session_delete,
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "metabase-test",
                "source": {
                    "type": "metabase",
                    "config": {
                        "username": "xxxx",
                        "password": "xxxx",
                        "connect_uri": "http://localhost:3000/",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/metabase_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        try:
            pipeline.raise_from_status()
        except PipelineExecutionError as exec_error:
            assert exec_error.args[0] == "Source reported errors"
            assert len(exec_error.args[1].failures) == 1
            assert list(exec_error.args[1].failures.keys())[0] == "metabase-dashboard"
