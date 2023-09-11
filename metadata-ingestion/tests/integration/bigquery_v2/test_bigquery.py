from typing import Any, Dict
from unittest.mock import patch

from freezegun import freeze_time
from google.cloud.bigquery.table import TableListItem

from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryDataset,
    BigqueryTable,
)
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import run_and_get_pipeline

FROZEN_TIME = "2022-02-03 07:00:00"


@freeze_time(FROZEN_TIME)
@patch(
    "datahub.ingestion.source.bigquery_v2.bigquery_schema.BigQueryDataDictionary.get_tables_for_dataset"
)
@patch(
    "datahub.ingestion.source.bigquery_v2.bigquery.BigqueryV2Source.get_core_table_details"
)
@patch(
    "datahub.ingestion.source.bigquery_v2.bigquery_schema.BigQueryDataDictionary.get_datasets_for_project_id"
)
@patch("google.cloud.bigquery.Client")
def test_bigquery_v2_ingest(
    client,
    get_datasets_for_project_id,
    get_core_table_details,
    get_tables_for_dataset,
    pytestconfig,
    tmp_path,
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/bigquery_v2"
    mcp_golden_path = "{}/bigquery_mcp_golden.json".format(test_resources_dir)
    mcp_output_path = "{}/{}".format(tmp_path, "bigquery_mcp_output.json")

    get_datasets_for_project_id.return_value = [
        BigqueryDataset(name="bigquery-dataset-1")
    ]

    table_list_item = TableListItem(
        {"tableReference": {"projectId": "", "datasetId": "", "tableId": ""}}
    )
    table_name = "table-1"
    get_core_table_details.return_value = {table_name: table_list_item}

    bigquery_table = BigqueryTable(
        name=table_name,
        comment=None,
        created=None,
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
    )
    get_tables_for_dataset.return_value = iter([bigquery_table])

    source_config_dict: Dict[str, Any] = {
        "project_ids": ["project-id-1"],
        "include_usage_statistics": False,
        "include_table_lineage": False,
        "include_data_platform_instance": True,
    }

    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "bigquery",
            "config": source_config_dict,
        },
        "sink": {"type": "file", "config": {"filename": mcp_output_path}},
    }

    run_and_get_pipeline(pipeline_config_dict)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=mcp_output_path,
        golden_path=mcp_golden_path,
    )
