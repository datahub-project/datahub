"""Regenerate the golden file with:

    pytest tests/integration/bigquery_v2/test_bigquery_linked_datasets_integration.py \
        --update-golden-files -v
"""

from types import SimpleNamespace
from typing import Any, Dict, Iterator, Optional
from unittest.mock import MagicMock, patch

import pytest
import time_machine
from google.cloud import bigquery_analyticshub_v1
from google.cloud.bigquery.table import TableListItem

from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryDataset,
    BigQuerySchemaApi,
    BigqueryTable,
    BigqueryView,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema_gen import (
    BigQuerySchemaGenerator,
)
from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import run_and_get_pipeline

FROZEN_TIME = "2022-02-03 07:00:00"


@pytest.fixture(autouse=True)
def mock_service_account_credentials() -> Iterator[None]:
    """Stop the BigQuery connection from validating the dummy private key."""
    with patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.service_account.Credentials.from_service_account_info"
    ):
        yield


def _recipe(
    mcp_output_path: str, source_config_override: Optional[dict] = None
) -> dict:
    source_config_override = source_config_override or {}
    return {
        "source": {
            "type": "bigquery",
            "config": {
                "project_ids": ["consumer-project"],
                "credential": {
                    "project_id": "consumer-project",
                    "private_key_id": "private_key_id",
                    "private_key": "private_key",
                    "client_email": "client_email",
                    "client_id": "client_id",
                },
                "include_usage_statistics": False,
                "include_table_lineage": False,
                "include_data_platform_instance": True,
                "include_linked_datasets": True,
                "include_linked_dataset_lineage": True,
                **source_config_override,
            },
        },
        "sink": {"type": "file", "config": {"filename": mcp_output_path}},
    }


def _make_subscription() -> SimpleNamespace:
    """Stand-in for `bigquery_analyticshub_v1.Subscription`.

    SimpleNamespace avoids the real proto's field typing; the handler reads
    the same attribute names regardless.
    """
    destination = SimpleNamespace(
        dataset_reference=SimpleNamespace(
            project_id="consumer-project", dataset_id="shared_dataset"
        )
    )
    return SimpleNamespace(
        name="projects/consumer-project/locations/us/subscriptions/sub_1",
        listing=(
            "projects/111222333/locations/us/dataExchanges/exch_a/listings/listing_a"
        ),
        data_exchange="",
        state=int(bigquery_analyticshub_v1.Subscription.State.STATE_ACTIVE),
        organization_id="987654321",
        organization_display_name="Publisher Inc",
        subscriber_contact="ops@example.com",
        creation_time=None,
        last_modify_time=None,
        log_linked_dataset_query_user_email=False,
        resource_type=int(bigquery_analyticshub_v1.SharedResourceType.BIGQUERY_DATASET),
        destination_dataset=destination,
    )


def _make_linked_dataset() -> SimpleNamespace:
    """Stand-in for `bq_client.get_dataset` on the linked (consumer) dataset.

    The handler reads `_properties` for linked-dataset fields not exposed as
    typed attributes.
    """
    return SimpleNamespace(
        _properties={
            "linkedDatasetSource": {
                "sourceDataset": {
                    "projectId": "111222333",
                    "datasetId": "publisher_dataset",
                }
            },
            "linkedDatasetMetadata": {"linkState": "LINKED"},
        }
    )


def _make_columns() -> list:
    """Five-column fixture so per-column FineGrainedLineage is exercised."""
    return [
        BigqueryColumn(
            name="user_id",
            ordinal_position=1,
            is_nullable=False,
            field_path="user_id",
            data_type="INT64",
            comment="Primary key",
            is_partition_column=False,
            cluster_column_position=None,
        ),
        BigqueryColumn(
            name="email",
            ordinal_position=2,
            is_nullable=False,
            field_path="email",
            data_type="STRING",
            comment="User email address",
            is_partition_column=False,
            cluster_column_position=None,
        ),
        BigqueryColumn(
            name="created_at",
            ordinal_position=3,
            is_nullable=False,
            field_path="created_at",
            data_type="TIMESTAMP",
            comment="Account creation time",
            is_partition_column=False,
            cluster_column_position=None,
        ),
        BigqueryColumn(
            name="country",
            ordinal_position=4,
            is_nullable=True,
            field_path="country",
            data_type="STRING",
            comment="ISO country code",
            is_partition_column=False,
            cluster_column_position=None,
        ),
        BigqueryColumn(
            name="active",
            ordinal_position=5,
            is_nullable=False,
            field_path="active",
            data_type="BOOL",
            comment="Active flag",
            is_partition_column=False,
            cluster_column_position=None,
        ),
    ]


@time_machine.travel(FROZEN_TIME, tick=False)
@patch.object(BigQuerySchemaApi, "get_snapshots_for_dataset")
@patch.object(BigQuerySchemaApi, "get_views_for_dataset")
@patch.object(BigQuerySchemaApi, "get_tables_for_dataset")
@patch.object(BigQuerySchemaGenerator, "get_core_table_details")
@patch.object(BigQuerySchemaApi, "get_datasets_for_project_id")
@patch.object(BigQuerySchemaApi, "get_columns_for_dataset")
@patch(
    "datahub.ingestion.source.bigquery_v2.bigquery_linked_datasets."
    "BigQueryLinkedDatasetsHandler._get_ah_client"
)
@patch(
    "datahub.ingestion.source.bigquery_v2.bigquery_linked_datasets."
    "BigQueryLinkedDatasetsHandler._get_bq_client"
)
@patch(
    "datahub.ingestion.source.bigquery_v2.bigquery_linked_datasets."
    "BigQueryLinkedDatasetsHandler._get_rm_client"
)
@patch("google.cloud.bigquery.Client")
@patch("google.cloud.datacatalog_v1.PolicyTagManagerClient")
@patch("google.cloud.resourcemanager_v3.ProjectsClient")
def test_bigquery_linked_datasets_ingest(
    projects_client,
    policy_tag_manager_client,
    bq_client,
    handler_get_rm_client,
    handler_get_bq_client,
    handler_get_ah_client,
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
    mcp_golden_path = f"{test_resources_dir}/bigquery_linked_datasets_mces_golden.json"
    mcp_output_path = "{}/{}".format(
        tmp_path, "bigquery_linked_datasets_mces_output.json"
    )

    # Datasets in the consumer project: one linked, one regular.
    linked_dataset_name = "shared_dataset"
    regular_dataset_name = "regular_dataset"
    get_datasets_for_project_id.return_value = [
        BigqueryDataset(name=linked_dataset_name, location="US"),
        BigqueryDataset(name=regular_dataset_name, location="US"),
    ]

    table_list_item = TableListItem(
        {"tableReference": {"projectId": "", "datasetId": "", "tableId": ""}}
    )

    # Tables and views inside the linked dataset.
    users_table_name = "users"
    active_users_view_name = "active_users"
    regular_table_name = "regular_table"

    get_core_table_details.return_value = {
        users_table_name: table_list_item,
        regular_table_name: table_list_item,
    }
    columns = _make_columns()
    get_columns_for_dataset.return_value = {
        users_table_name: columns,
        active_users_view_name: columns,
        regular_table_name: columns[:2],
    }

    def _tables_side_effect(project_id, dataset_name, *args, **kwargs):
        if dataset_name == linked_dataset_name:
            return iter(
                [
                    BigqueryTable(
                        name=users_table_name,
                        comment="Users mirrored from publisher",
                        created=None,
                        last_altered=None,
                        size_in_bytes=None,
                        rows_count=None,
                    )
                ]
            )
        return iter(
            [
                BigqueryTable(
                    name=regular_table_name,
                    comment="A regular consumer-side table",
                    created=None,
                    last_altered=None,
                    size_in_bytes=None,
                    rows_count=None,
                )
            ]
        )

    get_tables_for_dataset.side_effect = _tables_side_effect

    def _views_side_effect(project_id, dataset_name, *args, **kwargs):
        if dataset_name == linked_dataset_name:
            return iter(
                [
                    BigqueryView(
                        name=active_users_view_name,
                        comment="Active users view mirrored from publisher",
                        created=None,
                        view_definition="SELECT * FROM users WHERE active",
                        last_altered=None,
                        size_in_bytes=None,
                        rows_count=None,
                        materialized=False,
                    )
                ]
            )
        return iter([])

    get_views_for_dataset.side_effect = _views_side_effect
    get_snapshots_for_dataset.return_value = iter([])

    # Analytics Hub mock — return the subscription only for `us` location and
    # only against the consumer-project parent.
    ah_mock = MagicMock()
    subscription = _make_subscription()

    def _list_subscriptions(parent: str) -> list:
        if parent == "projects/consumer-project/locations/us":
            return [subscription]
        return []

    ah_mock.list_subscriptions.side_effect = _list_subscriptions
    handler_get_ah_client.return_value = ah_mock

    # BigQuery `get_dataset` mock — return the linked-dataset shape only
    # for the linked dataset, raise for the regular dataset (handler should
    # not call get_dataset on it because no subscription references it).
    bq_mock = MagicMock()
    linked_dataset_obj = _make_linked_dataset()

    def _get_dataset(fqn: str) -> Any:
        if fqn == "consumer-project.shared_dataset":
            return linked_dataset_obj
        raise AssertionError(f"unexpected get_dataset call for {fqn}")

    bq_mock.get_dataset.side_effect = _get_dataset
    handler_get_bq_client.return_value = bq_mock

    # Cloud Resource Manager mock — resolve publisher project number.
    rm_mock = MagicMock()
    rm_mock.get_project.return_value = SimpleNamespace(project_id="publisher-project")
    handler_get_rm_client.return_value = rm_mock

    pipeline_config_dict: Dict[str, Any] = _recipe(mcp_output_path=mcp_output_path)
    run_and_get_pipeline(pipeline_config_dict)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=mcp_output_path,
        golden_path=mcp_golden_path,
    )
