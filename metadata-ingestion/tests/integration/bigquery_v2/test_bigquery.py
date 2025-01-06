import random
import string
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time
from google.cloud.bigquery.table import TableListItem

from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
)
from datahub.ingestion.glossary.classifier import (
    ClassificationConfig,
    DynamicTypedClassifierConfig,
)
from datahub.ingestion.glossary.datahub_classifier import DataHubClassifierConfig
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_data_reader import BigQueryDataReader
from datahub.ingestion.source.bigquery_v2.bigquery_platform_resource_helper import (
    BigQueryLabelInfo,
    BigQueryPlatformResourceHelper,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryDataset,
    BigqueryProject,
    BigQuerySchemaApi,
    BigqueryTable,
    BigqueryTableSnapshot,
    BigqueryView,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema_gen import (
    BigQuerySchemaGenerator,
    BigQueryV2Config,
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


def recipe(mcp_output_path: str, source_config_override: dict = {}) -> dict:
    return {
        "source": {
            "type": "bigquery",
            "config": {
                "project_ids": ["project-id-1"],
                "credential": {
                    "project_id": "project-id-1",
                    "private_key_id": "private_key_id",
                    "private_key": "private_key",
                    "client_email": "client_email",
                    "client_id": "client_id",
                },
                "include_usage_statistics": False,
                "include_table_lineage": True,
                "include_data_platform_instance": True,
                "capture_table_label_as_tag": True,
                "capture_dataset_label_as_tag": True,
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
                **source_config_override,
            },
        },
        "sink": {"type": "file", "config": {"filename": mcp_output_path}},
    }


@freeze_time(FROZEN_TIME)
@patch.object(BigQuerySchemaApi, "get_snapshots_for_dataset")
@patch.object(BigQuerySchemaApi, "get_views_for_dataset")
@patch.object(BigQuerySchemaApi, "get_tables_for_dataset")
@patch.object(BigQuerySchemaGenerator, "get_core_table_details")
@patch.object(BigQuerySchemaApi, "get_datasets_for_project_id")
@patch.object(BigQuerySchemaApi, "get_columns_for_dataset")
@patch.object(BigQueryDataReader, "get_sample_data_for_table")
@patch.object(BigQueryPlatformResourceHelper, "get_platform_resource")
@patch("google.cloud.bigquery.Client")
@patch("google.cloud.datacatalog_v1.PolicyTagManagerClient")
@patch("google.cloud.resourcemanager_v3.ProjectsClient")
def test_bigquery_v2_ingest(
    client,
    policy_tag_manager_client,
    projects_client,
    get_platform_resource,
    get_sample_data_for_table,
    get_columns_for_dataset,
    get_datasets_for_project_id,
    get_core_table_details,
    get_tables_for_dataset,
    get_views_for_dataset,
    get_snapshots_for_dataset,
    pytestconfig,
    tmp_path,
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/bigquery_v2"
    mcp_golden_path = f"{test_resources_dir}/bigquery_mcp_golden.json"
    mcp_output_path = "{}/{}".format(tmp_path, "bigquery_mcp_output.json")

    dataset_name = "bigquery-dataset-1"

    def side_effect(*args: Any) -> Optional[PlatformResource]:
        if args[0].primary_key == "mixedcasetag":
            return PlatformResource.create(
                key=PlatformResourceKey(
                    primary_key="mixedcasetag",
                    resource_type="BigQueryLabelInfo",
                    platform="bigquery",
                ),
                value=BigQueryLabelInfo(
                    datahub_urn="urn:li:tag:MixedCaseTag",
                    managed_by_datahub=True,
                    key="mixedcasetag",
                    value="",
                ),
            )
        return None

    get_platform_resource.side_effect = side_effect
    get_datasets_for_project_id.return_value = [
        # BigqueryDataset(name=dataset_name, location="US")
        BigqueryDataset(
            name=dataset_name, location="US", labels={"priority": "medium:test"}
        )
    ]

    table_list_item = TableListItem(
        {"tableReference": {"projectId": "", "datasetId": "", "tableId": ""}}
    )
    table_name = "table-1"
    snapshot_table_name = "snapshot-table-1"
    view_name = "view-1"
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
        view_name: columns,
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
        labels={
            "priority": "high",
            "purchase": "",
            "mixedcasetag": "",
        },
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

    bigquery_view = BigqueryView(
        name=view_name,
        comment=None,
        created=None,
        view_definition=f"create view `{dataset_name}.view-1` as select email from `{dataset_name}.table-1`",
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
        materialized=False,
    )

    get_views_for_dataset.return_value = iter([bigquery_view])

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


@freeze_time(FROZEN_TIME)
@patch.object(BigQuerySchemaApi, "get_snapshots_for_dataset")
@patch.object(BigQuerySchemaApi, "get_views_for_dataset")
@patch.object(BigQuerySchemaApi, "get_tables_for_dataset")
@patch.object(BigQuerySchemaGenerator, "get_core_table_details")
@patch.object(BigQuerySchemaApi, "get_datasets_for_project_id")
@patch.object(BigQuerySchemaApi, "get_columns_for_dataset")
@patch.object(BigQueryDataReader, "get_sample_data_for_table")
@patch("google.cloud.bigquery.Client")
@patch("google.cloud.datacatalog_v1.PolicyTagManagerClient")
@patch("google.cloud.resourcemanager_v3.ProjectsClient")
def test_bigquery_queries_v2_ingest(
    client,
    policy_tag_manager_client,
    projects_client,
    get_sample_data_for_table,
    get_columns_for_dataset,
    get_datasets_for_project_id,
    get_core_table_details,
    get_tables_for_dataset,
    get_views_for_dataset,
    get_snapshots_for_dataset,
    pytestconfig,
    tmp_path,
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/bigquery_v2"
    mcp_golden_path = f"{test_resources_dir}/bigquery_mcp_queries_golden.json"
    mcp_output_path = "{}/{}".format(tmp_path, "bigquery_mcp_queries_output.json")

    dataset_name = "bigquery-dataset-1"
    get_datasets_for_project_id.return_value = [
        BigqueryDataset(name=dataset_name, location="US")
    ]

    table_list_item = TableListItem(
        {"tableReference": {"projectId": "", "datasetId": "", "tableId": ""}}
    )
    table_name = "table-1"
    snapshot_table_name = "snapshot-table-1"
    view_name = "view-1"
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
        view_name: columns,
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

    bigquery_view = BigqueryView(
        name=view_name,
        comment=None,
        created=None,
        view_definition=f"create view `{dataset_name}.view-1` as select email from `{dataset_name}.table-1`",
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
        materialized=False,
    )

    get_views_for_dataset.return_value = iter([bigquery_view])
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

    # Even if `include_table_lineage` is disabled, we still ingest view and snapshot lineage
    # if use_queries_v2 is set.
    pipeline_config_dict: Dict[str, Any] = recipe(
        mcp_output_path=mcp_output_path,
        source_config_override={"use_queries_v2": True, "include_table_lineage": False},
    )

    run_and_get_pipeline(pipeline_config_dict)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=mcp_output_path,
        golden_path=mcp_golden_path,
    )


@freeze_time(FROZEN_TIME)
@patch.object(BigQuerySchemaApi, "get_datasets_for_project_id")
@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch("google.cloud.datacatalog_v1.PolicyTagManagerClient")
@patch("google.cloud.resourcemanager_v3.ProjectsClient")
def test_bigquery_queries_v2_lineage_usage_ingest(
    projects_client,
    policy_tag_manager_client,
    get_bigquery_client,
    get_datasets_for_project_id,
    pytestconfig,
    tmp_path,
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/bigquery_v2"
    mcp_golden_path = f"{test_resources_dir}/bigquery_lineage_usage_golden.json"
    mcp_output_path = "{}/{}".format(tmp_path, "bigquery_lineage_usage_output.json")

    dataset_name = "bigquery-dataset-1"
    get_datasets_for_project_id.return_value = [BigqueryDataset(name=dataset_name)]

    client = MagicMock()
    get_bigquery_client.return_value = client
    client.list_tables.return_value = [
        TableListItem(
            {"tableReference": {"projectId": "", "datasetId": "", "tableId": "table-1"}}
        ),
        TableListItem(
            {"tableReference": {"projectId": "", "datasetId": "", "tableId": "view-1"}}
        ),
    ]

    # mocking the query results for fetching audit log
    # note that this is called twice, once for each region
    client.query.return_value = [
        {
            "job_id": "1",
            "project_id": "project-id-1",
            "creation_time": datetime.now(timezone.utc),
            "user_email": "foo@xyz.com",
            "query": "select * from `bigquery-dataset-1`.`table-1`",
            "session_id": None,
            "query_hash": None,
            "statement_type": "SELECT",
            "destination_table": None,
            "referenced_tables": None,
        },
        {
            "job_id": "2",
            "project_id": "project-id-1",
            "creation_time": datetime.now(timezone.utc),
            "user_email": "foo@xyz.com",
            "query": "create view `bigquery-dataset-1`.`view-1` as select * from `bigquery-dataset-1`.`table-1`",
            "session_id": None,
            "query_hash": None,
            "statement_type": "CREATE",
            "destination_table": None,
            "referenced_tables": None,
        },
        {
            "job_id": "3",
            "project_id": "project-id-1",
            "creation_time": datetime.now(timezone.utc),
            "user_email": "service_account@xyz.com",
            "query": """\
select * from `bigquery-dataset-1`.`view-1`
LIMIT 100
-- {"user":"@bar","email":"bar@xyz.com","url":"https://modeanalytics.com/acryltest/reports/6234ff78bc7d/runs/662b21949629/queries/f0aad24d5b37","scheduled":false}
""",
            "session_id": None,
            "query_hash": None,
            "statement_type": "SELECT",
            "destination_table": None,
            "referenced_tables": None,
        },
        {
            "job_id": "4",
            "project_id": "project-id-1",
            "creation_time": datetime.now(timezone.utc),
            "user_email": "service_account@xyz.com",
            "query": """\
select * from `bigquery-dataset-1`.`view-1`
LIMIT 100
-- {"user":"@foo","email":"foo@xyz.com","url":"https://modeanalytics.com/acryltest/reports/6234ff78bc7d/runs/662b21949629/queries/f0aad24d5b37","scheduled":false}
""",
            "session_id": None,
            "query_hash": None,
            "statement_type": "SELECT",
            "destination_table": None,
            "referenced_tables": None,
        },
    ]

    pipeline_config_dict: Dict[str, Any] = recipe(
        mcp_output_path=mcp_output_path,
        source_config_override={
            "use_queries_v2": True,
            "include_schema_metadata": False,
            "include_table_lineage": True,
            "include_usage_statistics": True,
            "classification": {"enabled": False},
        },
    )

    run_and_get_pipeline(pipeline_config_dict)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=mcp_output_path,
        golden_path=mcp_golden_path,
    )


@freeze_time(FROZEN_TIME)
@patch.object(BigQuerySchemaApi, "get_snapshots_for_dataset")
@patch.object(BigQuerySchemaApi, "get_views_for_dataset")
@patch.object(BigQuerySchemaApi, "get_tables_for_dataset")
@patch.object(BigQuerySchemaGenerator, "get_core_table_details")
@patch.object(BigQuerySchemaApi, "get_datasets_for_project_id")
@patch.object(BigQuerySchemaApi, "get_columns_for_dataset")
@patch.object(BigQueryDataReader, "get_sample_data_for_table")
@patch("google.cloud.bigquery.Client")
@patch("google.cloud.datacatalog_v1.PolicyTagManagerClient")
@patch("google.cloud.resourcemanager_v3.ProjectsClient")
@pytest.mark.parametrize(
    "use_queries_v2, include_table_lineage, include_usage_statistics, golden_file",
    [
        (True, False, False, "bigquery_mcp_lineage_golden_1.json"),
        (True, True, False, "bigquery_mcp_lineage_golden_1.json"),
        (False, False, True, "bigquery_mcp_lineage_golden_2.json"),
        (False, True, True, "bigquery_mcp_lineage_golden_2.json"),
    ],
)
def test_bigquery_lineage_v2_ingest_view_snapshots(
    client,
    policy_tag_manager_client,
    projects_client,
    get_sample_data_for_table,
    get_columns_for_dataset,
    get_datasets_for_project_id,
    get_core_table_details,
    get_tables_for_dataset,
    get_views_for_dataset,
    get_snapshots_for_dataset,
    pytestconfig,
    tmp_path,
    use_queries_v2,
    include_table_lineage,
    include_usage_statistics,
    golden_file,
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/bigquery_v2"
    mcp_golden_path = f"{test_resources_dir}/{golden_file}"
    mcp_output_path = "{}/{}_output.json".format(tmp_path, golden_file)

    dataset_name = "bigquery-dataset-1"
    get_datasets_for_project_id.return_value = [
        BigqueryDataset(name=dataset_name, location="US")
    ]

    table_list_item = TableListItem(
        {"tableReference": {"projectId": "", "datasetId": "", "tableId": ""}}
    )
    table_name = "table-1"
    snapshot_table_name = "snapshot-table-1"
    view_name = "view-1"
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
        view_name: columns,
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

    bigquery_view = BigqueryView(
        name=view_name,
        comment=None,
        created=None,
        view_definition=f"create view `{dataset_name}.view-1` as select email from `{dataset_name}.table-1`",
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
        materialized=False,
    )

    get_views_for_dataset.return_value = iter([bigquery_view])
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

    pipeline_config_dict: Dict[str, Any] = recipe(
        mcp_output_path=mcp_output_path,
        source_config_override={
            "use_queries_v2": use_queries_v2,
            "include_table_lineage": include_table_lineage,
            "include_usage_statistics": include_usage_statistics,
            "classification": {"enabled": False},
        },
    )

    run_and_get_pipeline(pipeline_config_dict)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=mcp_output_path,
        golden_path=mcp_golden_path,
    )
