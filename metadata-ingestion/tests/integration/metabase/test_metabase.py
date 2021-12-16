import json
from unittest.mock import patch

from freezegun import freeze_time
from requests.models import HTTPError

from datahub.configuration.common import PipelineExecutionError
from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2021-11-11 07:00:00"

JSON_RESPONSE_MAP = {
    "http://localhost:3000/api/session": "session.json",
    "http://localhost:3000/api/user/current": "user.json",
    "http://localhost:3000/api/dashboard": "dashboard.json",
    "http://localhost:3000/api/dashboard/1": "dashboard_1.json",
    "http://localhost:3000/api/user/1": "user.json",
    "http://localhost:3000/api/card": "card.json",
    "http://localhost:3000/api/database/2": "database.json",
    "http://localhost:3000/api/card/1": "card_1.json",
    "http://localhost:3000/api/card/2": "card_2.json",
    "http://localhost:3000/api/table/21": "table_21.json",
}

RESPONSE_ERROR_LIST = ["http://localhost:3000/api/dashboard"]

test_resources_dir = None


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


@freeze_time(FROZEN_TIME)
def test_mode_ingest_success(pytestconfig, tmp_path):
    with patch(
        "datahub.ingestion.source.metabase.requests.session",
        side_effect=mocked_requests_sucess,
    ), patch(
        "datahub.ingestion.source.metabase.requests.post",
        side_effect=mocked_requests_session_post,
    ), patch(
        "datahub.ingestion.source.metabase.requests.delete",
        side_effect=mocked_requests_session_delete,
    ):
        global test_resources_dir
        test_resources_dir = pytestconfig.rootpath / "tests/integration/metabase"

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
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/metabase_mces.json",
            golden_path=test_resources_dir / "metabase_mces_golden.json",
            ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
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
        global test_resources_dir
        test_resources_dir = pytestconfig.rootpath / "tests/integration/metabase"

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
