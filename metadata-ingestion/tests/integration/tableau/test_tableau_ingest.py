import json
import pathlib
from unittest import mock

import pytest
from freezegun import freeze_time
from tableauserverclient.models import ViewItem

from datahub.configuration.source_common import DEFAULT_ENV
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.tableau_common import (
    TableauLineageOverrides,
    make_table_urn,
)
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2021-12-07 07:00:00"

test_resources_dir = None


def _read_response(file_name):
    response_json_path = f"{test_resources_dir}/setup/{file_name}"
    with open(response_json_path) as file:
        data = json.loads(file.read())
        return data


def define_query_metadata_func(workbook_all: str):  # type: ignore
    def side_effect_query_metadata(query):
        if "workbooksConnection (first:10," in query:
            return _read_response(workbook_all)

        if "embeddedDatasourcesConnection (first:10," in query:
            return _read_response("embeddedDatasourcesConnection_all.json")

        if "publishedDatasourcesConnection (first:10," in query:
            return _read_response("publishedDatasourcesConnection_all.json")

        if "customSQLTablesConnection (first:10," in query:
            return _read_response("customSQLTablesConnection_all.json")

        raise Exception(f"Missing mock for query: {query}")

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

    with mock.patch("datahub.ingestion.source.tableau.Server") as mock_sdk:
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


@freeze_time(FROZEN_TIME)
@pytest.mark.slow_unit
def test_tableau_ingest(pytestconfig, tmp_path):
    output_file_name: str = "tableau_mces.json"
    golden_file_name: str = "tableau_mces_golden.json"
    side_effect_query_metadata = define_query_metadata_func(
        "workbooksConnection_all.json"
    )
    tableau_ingest_common(
        pytestconfig,
        tmp_path,
        side_effect_query_metadata,
        golden_file_name,
        output_file_name,
    )


def test_lineage_overrides():
    # Simple - specify platform instance to presto table
    assert (
        make_table_urn(
            DEFAULT_ENV,
            "presto_catalog",
            "presto",
            "test-schema",
            "presto_catalog.test-schema.test-table",
            platform_instance_map={"presto": "my_presto_instance"},
        )
        == "urn:li:dataset:(urn:li:dataPlatform:presto,my_presto_instance.presto_catalog.test-schema.test-table,PROD)"
    )

    # Transform presto urn to hive urn
    # resulting platform instance for hive = mapped platform instance + presto_catalog
    assert (
        make_table_urn(
            DEFAULT_ENV,
            "presto_catalog",
            "presto",
            "test-schema",
            "presto_catalog.test-schema.test-table",
            platform_instance_map={"presto": "my_instance"},
            lineage_overrides=TableauLineageOverrides(
                platform_override_map={"presto": "hive"},
            ),
        )
        == "urn:li:dataset:(urn:li:dataPlatform:hive,my_instance.presto_catalog.test-schema.test-table,PROD)"
    )

    # tranform hive urn to presto urn
    assert (
        make_table_urn(
            DEFAULT_ENV,
            "",
            "hive",
            "test-schema",
            "test-schema.test-table",
            platform_instance_map={"hive": "my_presto_instance.presto_catalog"},
            lineage_overrides=TableauLineageOverrides(
                platform_override_map={"hive": "presto"},
            ),
        )
        == "urn:li:dataset:(urn:li:dataPlatform:presto,my_presto_instance.presto_catalog.test-schema.test-table,PROD)"
    )
