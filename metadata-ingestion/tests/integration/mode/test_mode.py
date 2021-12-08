import json
from unittest import mock

from freezegun import freeze_time

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

test_resources_dir = None


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code
            self.auth = None
            self.headers = {}

        def json(self):
            return self.json_data

        def get(self, url):
            response_json_path = (
                f"{test_resources_dir}/setup/{JSON_RESPONSE_MAP.get(url)}"
            )
            with open(response_json_path) as file:
                data = json.loads(file.read())
                self.json_data = data
            return self

        def raise_for_status(self):
            pass

    return MockResponse(None, 200)


@freeze_time(FROZEN_TIME)
@mock.patch(
    "datahub.ingestion.source.mode.requests.session", side_effect=mocked_requests_get
)
def test_mode_ingest(mock_session, pytestconfig, tmp_path):
    global test_resources_dir
    test_resources_dir = pytestconfig.rootpath / "tests/integration/mode"

    pipeline = Pipeline.create(
        {
            "run_id": "mode-test",
            "source": {
                "type": "mode-analytics",
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
