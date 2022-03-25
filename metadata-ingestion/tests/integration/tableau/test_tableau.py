import json
import pathlib
from unittest import mock

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2021-12-07 07:00:00"

test_resources_dir = None


def _read_response(file_name):
    response_json_path = f"{test_resources_dir}/setup/{file_name}"
    with open(response_json_path) as file:
        data = json.loads(file.read())
        return data


def side_effect_query_metadata(query):
    if "workbooksConnection (first:0" in query:
        return _read_response("workbooksConnection_0.json")

    if "workbooksConnection (first:3" in query:
        return _read_response("workbooksConnection_all.json")

    if "publishedDatasourcesConnection (first:0" in query:
        return _read_response("publishedDatasourcesConnection_0.json")

    if "publishedDatasourcesConnection (first:2" in query:
        return _read_response("publishedDatasourcesConnection_all.json")

    if "customSQLTablesConnection (first:0" in query:
        return _read_response("customSQLTablesConnection_0.json")

    if "customSQLTablesConnection (first:2" in query:
        return _read_response("customSQLTablesConnection_all.json")


@freeze_time(FROZEN_TIME)
def test_tableau_ingest(pytestconfig, tmp_path):

    global test_resources_dir
    test_resources_dir = pathlib.Path(
        pytestconfig.rootpath / "tests/integration/tableau"
    )

    with mock.patch("tableauserverclient.Server") as mock_sdk:
        mock_client = mock.Mock()
        mocked_metadata = mock.Mock()
        mocked_metadata.query.side_effect = side_effect_query_metadata
        mock_client.metadata = mocked_metadata
        mock_client.auth = mock.Mock()
        mock_client.auth.sign_in.return_value = None
        mock_client.auth.sign_out.return_value = None
        mock_sdk.return_value = mock_client
        mock_sdk._auth_token = "ABC"

        pipeline = Pipeline.create(
            {
                "run_id": "tableau-test",
                "source": {
                    "type": "tableau",
                    "config": {
                        "username": "username",
                        "password": "pass`",
                        "connect_uri": "https://do-not-connect",
                        "site": "acryl",
                        "projects": ["default", "Project 2"],
                        "ingest_tags": True,
                        "ingest_owner": True,
                        "default_schema_map": {
                            "dvdrental": "public",
                            "someotherdb": "schema",
                        },
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/tableau_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/tableau_mces.json",
            golden_path=test_resources_dir / "tableau_mces_golden.json",
            ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
        )
