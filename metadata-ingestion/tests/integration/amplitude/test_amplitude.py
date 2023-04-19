import json
from unittest.mock import patch

from freezegun import freeze_time

from datahub.configuration.common import PipelineExecutionError
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.amplitude.source import AmplitudeTaxonomyEndpoints
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2019-03-29 05:29:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"
BASE_URL = "https://amplitude.com/api/2"

test_resources_dir = None

JSON_RESPONSE_MAP = {
    f"{BASE_URL}" f"{AmplitudeTaxonomyEndpoints.EVENT_TYPE}": "event_type.json",
    f"{BASE_URL}"
    f"{AmplitudeTaxonomyEndpoints.EVENT_PROPERTIES}": "event_properties.json",
    f"{BASE_URL}"
    f"{AmplitudeTaxonomyEndpoints.USER_PROPERTIES}": "user_properties.json",
}


class MockResponse:
    def __int__(self, url, auth, data=None):
        self.url = url
        self.data = data
        self.json_data = None
        self.auth = auth

    def json(self):
        return self.json_data

    def get(self, url, auth, data=None):
        response_json_path = f"{test_resources_dir}/setup/{JSON_RESPONSE_MAP.get(url)}"
        with open(response_json_path) as file:
            j_data = json.loads(file.read())
            self.json_data = j_data
        return self

    @staticmethod
    def raise_for_status():
        return 200


def mock_request(*args, **kwargs):
    return [MockResponse()]


@freeze_time(FROZEN_TIME)
def test_amplitude_ingestion_success(pytestconfig, tmp_path):
    with patch(
        "datahub.ingestion.source.amplitude.source.requests.session",
        side_effect=mock_request(),
    ):
        global test_resources_dir
        test_resources_dir = pytestconfig.rootpath / "tests/integration/amplitude"

        pipeline = Pipeline.create(
            {
                "run_id": "amplitude-test",
                "source": {
                    "type": "amplitude",
                    "config": {
                        "connect_uri": f"{BASE_URL}",
                        "projects": [
                            {
                                "name": "Amplitude Test Project",
                                "description": "This is a test project",
                                "api_key": "xxxxxx",
                                "secret_key": "xxxxxx",
                            }
                        ],
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": f"{tmp_path}/amplitude_mces.json"},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/amplitude_mces.json",
            golden_path=test_resources_dir / "amplitude_mces_golden.json",
            ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
        )


@freeze_time(FROZEN_TIME)
def test_amplitude_ingestion_failure(pytestconfig, tmp_path):
    with patch(
        "datahub.ingestion.source.amplitude.source.requests.session",
        side_effect=mock_request(),
    ):
        global test_resources_dir
        test_resources_dir = pytestconfig.rootpath / "tests/integration/amplitude"

        pipeline = Pipeline.create(
            {
                "run_id": "amplitude-test",
                "source": {
                    "type": "amplitude",
                    "config": {
                        "connect_uri": f"{BASE_URL}",
                        "projects": [
                            {
                                "name": "Amplitude Test Project",
                                "description": "This is a test project",
                                "api_key": "",
                                "secret_key": "",
                            }
                        ],
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": f"{tmp_path}/amplitude_mces.json"},
                },
            }
        )
        pipeline.run()
        try:
            pipeline.raise_from_status()
        except PipelineExecutionError as execution_error:
            assert execution_error.args[0] == "Source reported errors"
            assert execution_error.args[1].events_produced == 6
            assert (
                execution_error.args[1].failures["/taxonomy/event"][0][1]
                == "Error: Project api_key is required"
            )
            assert (
                execution_error.args[1].failures["/taxonomy/user-property"][0][1]
                == "Error: Project api_key is required"
            )
            assert (
                execution_error.args[1].failures["Add user properties to project"][0][1]
                == "No user properties for project Amplitude Test Project"
            )
