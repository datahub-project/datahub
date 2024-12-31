import json
import pathlib
from typing import Sequence
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from requests.models import HTTPError

from datahub.configuration.common import PipelineExecutionError
from datahub.ingestion.api.source import StructuredLogEntry
from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2021-12-07 07:00:00"

JSON_RESPONSE_MAP = {
    "https://app.mode.com/api/verify": "verify.json",
    "https://app.mode.com/api/account": "user.json",
    "https://app.mode.com/api/acryl/spaces?filter=all": "spaces.json",
    "https://app.mode.com/api/acryl/spaces/157933cc1168/reports": "reports_157933cc1168.json",
    "https://app.mode.com/api/acryl/spaces/75737b70402e/reports": "reports_75737b70402e.json",
    "https://app.mode.com/api/modeuser": "user.json",
    "https://app.mode.com/api/acryl/reports/9d2da37fa91e/queries": "queries.json",
    "https://app.mode.com/api/acryl/reports/9d2da37fa91e/queries/6e26a9f3d4e2/charts": "charts.json",
    "https://app.mode.com/api/acryl/data_sources": "data_sources.json",
    "https://app.mode.com/api/acryl/definitions": "definitions.json",
    "https://app.mode.com/api/acryl/spaces/157933cc1168/datasets": "datasets_157933cc1168.json",
    "https://app.mode.com/api/acryl/spaces/75737b70402e/datasets": "datasets_75737b70402e.json",
    "https://app.mode.com/api/acryl/reports/24f66e1701b6": "dataset_24f66e1701b6.json",
    "https://app.mode.com/api/acryl/reports/24f66e1701b6/queries": "dataset_queries_24f66e1701b6.json",
}

ERROR_URL = "https://app.mode.com/api/acryl/spaces/75737b70402e/reports"

test_resources_dir = pathlib.Path(__file__).parent


class MockResponse:
    def __init__(self, error_list, status_code):
        self.json_data = None
        self.error_list = error_list
        self.status_code = status_code
        self.auth = None
        self.headers = {}
        self.url = None

    def json(self):
        return self.json_data

    def mount(self, prefix, adaptor):
        return self

    def get(self, url, timeout=40):
        if self.error_list is not None and self.url in self.error_list:
            http_error_msg = "{} Client Error: {} for url: {}".format(
                400,
                "Simulate error",
                self.url,
            )
            raise HTTPError(http_error_msg, response=self)

        self.url = url
        self.timeout = timeout
        response_json_path = f"{test_resources_dir}/setup/{JSON_RESPONSE_MAP.get(url)}"
        with open(response_json_path) as file:
            data = json.loads(file.read())
            self.json_data = data
        return self


class MockResponseJson(MockResponse):
    def __init__(
        self,
        status_code: int = 200,
        *,
        json_empty_list: Sequence[str] = (),
        json_error_list: Sequence[str] = (),
    ):
        super().__init__(None, status_code)
        self.json_empty_list = json_empty_list
        self.json_error_list = json_error_list

    def json(self):
        if self.url in self.json_empty_list:
            return json.loads("")  # Shouldn't be called
        if self.url in self.json_error_list:
            return json.loads("{")
        return super().json()

    def get(self, url, timeout=40):
        response = super().get(url, timeout)
        if self.url in self.json_empty_list:
            response.status_code = 204
        return response


def mocked_requests_success(*args, **kwargs):
    return MockResponse(None, 200)


def mocked_requests_failure(*args, **kwargs):
    return MockResponse([ERROR_URL], 200)


@freeze_time(FROZEN_TIME)
def test_mode_ingest_success(pytestconfig, tmp_path):
    with patch(
        "datahub.ingestion.source.mode.requests.Session",
        side_effect=mocked_requests_success,
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "mode-test",
                "source": {
                    "type": "mode",
                    "config": {
                        "token": "xxxx",
                        "password": "xxxx",
                        "connect_uri": "https://app.mode.com/",
                        "workspace": "acryl",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/mode_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/mode_mces.json",
            golden_path=test_resources_dir / "mode_mces_golden.json",
            ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
        )


@freeze_time(FROZEN_TIME)
def test_mode_ingest_failure(pytestconfig, tmp_path):
    with patch(
        "datahub.ingestion.source.mode.requests.Session",
        side_effect=mocked_requests_failure,
    ):
        global test_resources_dir
        test_resources_dir = pytestconfig.rootpath / "tests/integration/mode"

        pipeline = Pipeline.create(
            {
                "run_id": "mode-test",
                "source": {
                    "type": "mode",
                    "config": {
                        "token": "xxxx",
                        "password": "xxxx",
                        "connect_uri": "https://app.mode.com/",
                        "workspace": "acryl",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/mode_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        with pytest.raises(PipelineExecutionError) as exec_error:
            pipeline.raise_from_status()
        assert exec_error.value.args[0] == "Source reported errors"
        assert len(exec_error.value.args[1].failures) == 1
        error_dict: StructuredLogEntry
        _level, error_dict = exec_error.value.args[1].failures[0]
        error = next(iter(error_dict.context))
        assert "Simulate error" in error
        assert ERROR_URL in error


@freeze_time(FROZEN_TIME)
def test_mode_ingest_json_empty(pytestconfig, tmp_path):
    with patch(
        "datahub.ingestion.source.mode.requests.Session",
        side_effect=lambda *args, **kwargs: MockResponseJson(
            json_empty_list=["https://app.mode.com/api/modeuser"]
        ),
    ):
        global test_resources_dir
        test_resources_dir = pytestconfig.rootpath / "tests/integration/mode"

        pipeline = Pipeline.create(
            {
                "run_id": "mode-test",
                "source": {
                    "type": "mode",
                    "config": {
                        "token": "xxxx",
                        "password": "xxxx",
                        "connect_uri": "https://app.mode.com/",
                        "workspace": "acryl",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/mode_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status(raise_warnings=True)


@freeze_time(FROZEN_TIME)
def test_mode_ingest_json_failure(pytestconfig, tmp_path):
    with patch(
        "datahub.ingestion.source.mode.requests.Session",
        side_effect=lambda *args, **kwargs: MockResponseJson(
            json_error_list=["https://app.mode.com/api/modeuser"]
        ),
    ):
        global test_resources_dir
        test_resources_dir = pytestconfig.rootpath / "tests/integration/mode"

        pipeline = Pipeline.create(
            {
                "run_id": "mode-test",
                "source": {
                    "type": "mode",
                    "config": {
                        "token": "xxxx",
                        "password": "xxxx",
                        "connect_uri": "https://app.mode.com/",
                        "workspace": "acryl",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/mode_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status(raise_warnings=False)
        with pytest.raises(PipelineExecutionError) as exec_error:
            pipeline.raise_from_status(raise_warnings=True)
        assert len(exec_error.value.args[1].warnings) > 0
        error_dict: StructuredLogEntry
        _level, error_dict = exec_error.value.args[1].warnings[0]
        error = next(iter(error_dict.context))
        assert "Expecting property name enclosed in double quotes" in error
