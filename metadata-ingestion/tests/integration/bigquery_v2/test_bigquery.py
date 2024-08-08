import random
import string
from typing import Any, Dict
from unittest.mock import patch

from freezegun import freeze_time
from google.cloud.bigquery.table import TableListItem

from datahub.ingestion.glossary.classifier import (
    ClassificationConfig,
    DynamicTypedClassifierConfig,
)
from datahub.ingestion.glossary.datahub_classifier import DataHubClassifierConfig
from datahub.ingestion.source.bigquery_v2.bigquery_data_reader import BigQueryDataReader
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryDataset,
    BigQuerySchemaApi,
    BigqueryTable,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema_gen import (
    BigQuerySchemaGenerator,
)
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import run_and_get_pipeline

FROZEN_TIME = "2022-02-03 07:00:00"


def random_email():
    return (
        "".join(
            [
                random.choice(string.ascii_lowercase)
                for i in range(random.randint(10, 15))
            ]
        )
        + "@xyz.com"
    )


@freeze_time(FROZEN_TIME)
@patch.object(BigQuerySchemaApi, "get_tables_for_dataset")
@patch.object(BigQuerySchemaGenerator, "get_core_table_details")
@patch.object(BigQuerySchemaApi, "get_datasets_for_project_id")
@patch.object(BigQuerySchemaApi, "get_columns_for_dataset")
@patch.object(BigQueryDataReader, "get_sample_data_for_table")
@patch("google.cloud.bigquery.Client")
@patch("google.cloud.datacatalog_v1.PolicyTagManagerClient")
def test_bigquery_v2_ingest(
    client,
    policy_tag_manager_client,
    get_sample_data_for_table,
    get_columns_for_dataset,
    get_datasets_for_project_id,
    get_core_table_details,
    get_tables_for_dataset,
    pytestconfig,
    tmp_path,
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/bigquery_v2"
    mcp_golden_path = f"{test_resources_dir}/bigquery_mcp_golden.json"
    mcp_output_path = "{}/{}".format(tmp_path, "bigquery_mcp_output.json")

    get_datasets_for_project_id.return_value = [
        BigqueryDataset(name="bigquery-dataset-1")
    ]

    table_list_item = TableListItem(
        {"tableReference": {"projectId": "", "datasetId": "", "tableId": ""}}
    )
    table_name = "table-1"
    get_core_table_details.return_value = {table_name: table_list_item}
    get_columns_for_dataset.return_value = {
        table_name: [
            BigqueryColumn(
                name="age",
                ordinal_position=1,
                is_nullable=False,
                field_path="col_1",
                data_type="INT",
                comment="comment",
                is_partition_column=False,
                cluster_column_position=None,
                policy_tags=["Test Policy Tag"],
            ),
            BigqueryColumn(
                name="email",
                ordinal_position=1,
                is_nullable=False,
                field_path="col_2",
                data_type="STRING",
                comment="comment",
                is_partition_column=False,
                cluster_column_position=None,
            ),
        ]
    }
    get_sample_data_for_table.return_value = {
        "age": [random.randint(1, 80) for i in range(20)],
        "email": [random_email() for i in range(20)],
    }

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
        "classification": ClassificationConfig(
            enabled=True,
            classifiers=[
                DynamicTypedClassifierConfig(
                    type="datahub",
                    config=DataHubClassifierConfig(
                        minimum_values_threshold=1,
                    ),
                )
            ],
            max_workers=1,
        ).dict(),
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
