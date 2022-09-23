import json
import pathlib
from unittest import mock

from tableauserverclient.models import ViewItem

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2021-12-07 07:00:00"

test_resources_dir = None


def _read_response(file_name):
    response_json_path = f"{test_resources_dir}/setup/{file_name}"
    with open(response_json_path) as file:
        data = json.loads(file.read())
        return data


def define_query_metadata_func(workbook_0: str, workbook_all: str):  # type: ignore
    def side_effect_query_metadata(query):

        if "workbooksConnection (first:0" in query:
            return _read_response(workbook_0)

        if "workbooksConnection (first:3" in query:
            return _read_response(workbook_all)

        if "embeddedDatasourcesConnection (first:0" in query:
            return _read_response("embeddedDatasourcesConnection_0.json")

        if "embeddedDatasourcesConnection (first:8" in query:
            return _read_response("embeddedDatasourcesConnection_all.json")

        if "publishedDatasourcesConnection (first:0" in query:
            return _read_response("publishedDatasourcesConnection_0.json")

        if "publishedDatasourcesConnection (first:2" in query:
            return _read_response("publishedDatasourcesConnection_all.json")

        if "customSQLTablesConnection (first:0" in query:
            return _read_response("customSQLTablesConnection_0.json")

        if "customSQLTablesConnection (first:2" in query:
            return _read_response("customSQLTablesConnection_all.json")

    return side_effect_query_metadata


def side_effect_usage_stat(*arg, **kwargs):
    mock_pagination = mock.MagicMock()
    mock_pagination.total_available = None

    dashboard_stat: ViewItem = ViewItem()

    # Added as luid of Dashboard in workbooksConnection_state_all.json
    dashboard_stat._id = "fc9ea488-f810-4fa8-ac19-aa96018b5d66"
    dashboard_stat._total_views = 3

    # Added as luid of Sheet in workbooksConnection_state_all.json
    sheet_stat: ViewItem = ViewItem()
    sheet_stat._id = "f0779f9d-6765-47a9-a8f6-c740cfd27783"
    sheet_stat._total_views = 5

    return [dashboard_stat, sheet_stat], mock_pagination


def tableau_ingest_common(
    pytestconfig,
    tmp_path,
    side_effect_query_metadata_func,
    golden_file_name,
    output_file_name,
):
    global test_resources_dir
    test_resources_dir = pathlib.Path(
        pytestconfig.rootpath / "tests/integration/tableau"
    )

    with mock.patch("tableauserverclient.Server") as mock_sdk:
        mock_client = mock.Mock()
        mocked_metadata = mock.Mock()
        mocked_metadata.query.side_effect = side_effect_query_metadata_func
        mock_client.metadata = mocked_metadata
        mock_client.auth = mock.Mock()
        mock_client.views = mock.Mock()
        mock_client.views.get.side_effect = side_effect_usage_stat
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
                        "page_size": 10,
                        "ingest_tags": True,
                        "ingest_owner": True,
                        "ingest_tables_external": True,
                        "default_schema_map": {
                            "dvdrental": "public",
                            "someotherdb": "schema",
                        },
                        "platform_instance_map": {"postgres": "demo_postgres_instance"},
                        "extract_usage_stats": True,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/{output_file_name}",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/{output_file_name}",
            golden_path=test_resources_dir / golden_file_name,
            ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
        )
