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
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_data_reader import BigQueryDataReader
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryDataset,
    BigqueryProject,
    BigQuerySchemaApi,
    BigqueryTable,
    BigqueryTableSnapshot,
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


def recipe(mcp_output_path: str, override: dict = {}) -> dict:
    return {
        "source": {
            "type": "bigquery",
            "config": {
                "project_ids": ["project-id-1"],
                "include_usage_statistics": False,
                "include_table_lineage": True,
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
            },
        },
        "sink": {"type": "file", "config": {"filename": mcp_output_path}},
    }


@freeze_time(FROZEN_TIME)
@patch.object(BigQuerySchemaApi, "get_snapshots_for_dataset")
@patch.object(BigQuerySchemaApi, "get_tables_for_dataset")
@patch.object(BigQuerySchemaGenerator, "get_core_table_details")
@patch.object(BigQuerySchemaApi, "get_datasets_for_project_id")
@patch.object(BigQuerySchemaApi, "get_columns_for_dataset")
@patch.object(BigQueryDataReader, "get_sample_data_for_table")
@patch("google.cloud.bigquery.Client")
@patch("google.cloud.datacatalog_v1.PolicyTagManagerClient")
@patch("google.cloud.resourcemanager_v3.ProjectsClient")
def test_bigquery_v2_ingest(
    client,
    policy_tag_manager_client,
    projects_client,
    get_sample_data_for_table,
    get_columns_for_dataset,
    get_datasets_for_project_id,
    get_core_table_details,
    get_tables_for_dataset,
    get_snapshots_for_dataset,
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
    snapshot_table_name = "snapshot-table-1"
    get_core_table_details.return_value = {table_name: table_list_item}
    columns = [
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

    get_columns_for_dataset.return_value = {
        table_name: columns,
        snapshot_table_name: columns,
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
    snapshot_table = BigqueryTableSnapshot(
        name=snapshot_table_name,
        comment=None,
        created=None,
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
        base_table_identifier=BigqueryTableIdentifier(
            project_id="project-id-1",
            dataset="bigquery-dataset-1",
            table="table-1",
        ),
    )
    get_snapshots_for_dataset.return_value = iter([snapshot_table])

    pipeline_config_dict: Dict[str, Any] = recipe(mcp_output_path=mcp_output_path)

    run_and_get_pipeline(pipeline_config_dict)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=mcp_output_path,
        golden_path=mcp_golden_path,
    )


@freeze_time(FROZEN_TIME)
@patch.object(BigQuerySchemaApi, attribute="get_projects_with_labels")
@patch.object(BigQuerySchemaApi, "get_tables_for_dataset")
@patch.object(BigQuerySchemaGenerator, "get_core_table_details")
@patch.object(BigQuerySchemaApi, "get_datasets_for_project_id")
@patch.object(BigQuerySchemaApi, "get_columns_for_dataset")
@patch.object(BigQueryDataReader, "get_sample_data_for_table")
@patch("google.cloud.bigquery.Client")
@patch("google.cloud.datacatalog_v1.PolicyTagManagerClient")
@patch("google.cloud.resourcemanager_v3.ProjectsClient")
def test_bigquery_v2_project_labels_ingest(
    client,
    policy_tag_manager_client,
    projects_client,
    get_sample_data_for_table,
    get_columns_for_dataset,
    get_datasets_for_project_id,
    get_core_table_details,
    get_tables_for_dataset,
    get_projects_with_labels,
    pytestconfig,
    tmp_path,
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/bigquery_v2"
    mcp_golden_path = f"{test_resources_dir}/bigquery_project_label_mcp_golden.json"
    mcp_output_path = "{}/{}".format(tmp_path, "bigquery_project_label_mcp_output.json")

    get_datasets_for_project_id.return_value = [
        BigqueryDataset(name="bigquery-dataset-1")
    ]

    get_projects_with_labels.return_value = [
        BigqueryProject(id="dev", name="development")
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

    pipeline_config_dict: Dict[str, Any] = recipe(mcp_output_path=mcp_output_path)

    del pipeline_config_dict["source"]["config"]["project_ids"]

    pipeline_config_dict["source"]["config"]["project_labels"] = [
        "environment:development"
    ]

    run_and_get_pipeline(pipeline_config_dict)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=mcp_output_path,
        golden_path=mcp_golden_path,
    )
