from dataclasses import dataclass
from unittest.mock import patch
from freezegun import freeze_time

from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryDataset, BigqueryTable, BigqueryView
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import run_and_get_pipeline


@dataclass
class BigQueryConfig:
    def do_something(self):
        pass

FROZEN_TIME = "2022-02-03 07:00:00"

@freeze_time(FROZEN_TIME)
@patch("datahub.ingestion.source.bigquery_v2.bigquery_schema.BigQueryDataDictionary.get_datasets_for_project_id")
@patch("google.cloud.bigquery.Client")
def test_bigquery_v2_ingest(client, list_datasets, pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/bigquery_v2"
    mcp_golden_path = "{}/bigquery_mcp_golden.json".format(
        test_resources_dir
    )
    mcp_output_path = "{}/{}".format(tmp_path, "bigquery_mcp_output.json")
    list_datasets.return_value = [
        BigqueryDataset(name="bigquery-dataset-1",
                        tables=[BigqueryTable(name="table-1",
                                              comment=None,
                                              created=None,
                                              last_altered=None,
                                              size_in_bytes=None,
                                              rows_count=None),
                                BigqueryView(name="view-1",
                                             comment=None,
                                             created=None,
                                             last_altered=None,
                                             view_definition="view")])]

    source_config_dict: Dict[str, Any] = {
        "project_ids": ["project-id-1"],
    }

    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "bigquery",
            "config": source_config_dict,
        },
        "sink": {
            "type": "file",
            "config" : {
                "filename": mcp_output_path
            }
        },
        "pipeline_name": "pipeline",
    }
    pipeline_run = run_and_get_pipeline(pipeline_config_dict )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=mcp_output_path,
        golden_path=mcp_golden_path,
    )
