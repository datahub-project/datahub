import json
from unittest.mock import patch

from freezegun import freeze_time
from requests.models import HTTPError

from datahub.configuration.common import PipelineExecutionError
from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2021-12-07 07:00:00"

JSON_RESPONSE_MAP = {
    "https://app.mode.com/api/account": "user.json",
    "https://app.mode.com/api/acryl/spaces": "spaces.json",
    "https://app.mode.com/api/acryl/spaces/9026edbd5a3c/reports": "reports.json",
    "https://app.mode.com/api/modeuser": "user.json",
    "https://app.mode.com/api/acryl/reports/72f2ef8fb3a8/queries": "queries.json",
    "https://app.mode.com/api/acryl/reports/72f2ef8fb3a8/queries/bc5f397e4b77/charts": "charts.json",
    "https://app.mode.com/api/acryl/data_sources": "data_sources.json",
}

RESPONSE_ERROR_LIST = ["https://app.mode.com/api/acryl/spaces/9026edbd5a3c/reports"]

test_resources_dir = None


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

    def get(self, url):
        self.url = url
        response_json_path = f"{test_resources_dir}/setup/{JSON_RESPONSE_MAP.get(url)}"
        with open(response_json_path) as file:
            data = json.loads(file.read())
            self.json_data = data
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
    return MockResponse(None, 200)


def mocked_requests_failure(*args, **kwargs):
    return MockResponse(RESPONSE_ERROR_LIST, 200)


@freeze_time(FROZEN_TIME)
def test_mode_ingest_success(pytestconfig, tmp_path):
    with patch(
        "datahub.ingestion.source.mode.requests.session",
        side_effect=mocked_requests_sucess,
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
        "datahub.ingestion.source.mode.requests.session",
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
        try:
            pipeline.raise_from_status()
        except PipelineExecutionError as exec_error:
            assert exec_error.args[0] == "Source reported errors"
            assert len(exec_error.args[1].failures) == 1
            assert (
                list(exec_error.args[1].failures.keys())[0]
                == "mode-report-9026edbd5a3c"
            )
