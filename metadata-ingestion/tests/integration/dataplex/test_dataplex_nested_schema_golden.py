"""Golden test for nested, REPEATED and hive-style complex schema columns."""

from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import Mock, patch

import time_machine
from google.cloud import dataplex_v1

from datahub.testing import mce_helpers
from tests.integration.dataplex.test_dataplex_integration import (
    FROZEN_TIME,
    create_mock_entry,
    create_mock_entry_group,
    dataplex_entries_recipe,
)
from tests.test_helpers.state_helpers import run_and_get_pipeline

BIGQUERY_TABLE_ENTRY_TYPE = (
    "projects/test-project/locations/us/entryTypes/bigquery-table"
)
DPMS_TABLE_ENTRY_TYPE = (
    "projects/test-project/locations/us/entryTypes/dataproc-metastore-table"
)


def _with_schema(entry: Mock, fields: List[Dict[str, Any]]) -> Mock:
    # A real Aspect, so .data is marshalled exactly as the client returns it.
    entry.aspects = {
        "655216118709.global.schema": dataplex_v1.Aspect(data={"fields": fields})
    }
    return entry


@time_machine.travel(FROZEN_TIME, tick=False)
@patch("google.auth.default")
@patch("google.cloud.dataplex_v1.CatalogServiceClient")
@patch("google.cloud.datacatalog_lineage.LineageClient")
def test_dataplex_nested_schema_golden(
    mock_lineage_client_class: Mock,
    mock_catalog_client_class: Mock,
    mock_google_auth: Mock,
    pytestconfig: Any,
    tmp_path: Path,
) -> None:
    mock_google_auth.return_value = (Mock(), "test-project")
    mock_catalog_client = Mock()
    mock_catalog_client_class.return_value = mock_catalog_client
    mock_lineage_client_class.return_value = Mock()

    orders = _with_schema(
        create_mock_entry(
            project_id="test-project",
            location="us",
            entry_group_id="@bigquery",
            entry_id="orders",
            fqn="bigquery:test-project.sales.orders",
            entry_type=BIGQUERY_TABLE_ENTRY_TYPE,
        ),
        [
            {
                "name": "order_id",
                "dataType": "INTEGER",
                "metadataType": "NUMBER",
                "mode": "REQUIRED",
            },
            {
                "name": "customer",
                "dataType": "RECORD",
                "metadataType": "STRUCT",
                "mode": "NULLABLE",
                "description": "Who placed the order",
                "fields": [
                    {
                        "name": "customer_id",
                        "dataType": "STRING",
                        "metadataType": "STRING",
                        "description": "Customer key",
                    },
                    {
                        "name": "signed_up_at",
                        "dataType": "TIMESTAMP",
                        "metadataType": "TIMESTAMP",
                    },
                    {
                        "name": "is_member",
                        "dataType": "BOOLEAN",
                        "metadataType": "BOOLEAN",
                    },
                    {
                        "name": "address",
                        "dataType": "RECORD",
                        "metadataType": "STRUCT",
                        "fields": [
                            {
                                "name": "city",
                                "dataType": "STRING",
                                "metadataType": "STRING",
                            }
                        ],
                    },
                ],
            },
            {
                "name": "items",
                "dataType": "RECORD",
                "metadataType": "STRUCT",
                "mode": "REPEATED",
                "fields": [
                    {
                        "name": "sku",
                        "dataType": "STRING",
                        "metadataType": "STRING",
                        "mode": "REQUIRED",
                    },
                    {
                        "name": "quantity",
                        "dataType": "INTEGER",
                        "metadataType": "NUMBER",
                    },
                    {
                        "name": "unit_price",
                        "dataType": "NUMERIC",
                        "metadataType": "NUMBER",
                    },
                    {
                        "name": "discount",
                        "dataType": "FLOAT",
                        "metadataType": "NUMBER",
                    },
                ],
            },
            {
                "name": "labels",
                "dataType": "STRING",
                "metadataType": "STRING",
                "mode": "REPEATED",
            },
        ],
    )
    events = _with_schema(
        create_mock_entry(
            project_id="test-project",
            location="us",
            entry_group_id="@dataproc_metastore",
            entry_id="events",
            fqn="dataproc_metastore:test-project.us.my-service.my_db.events",
            entry_type=DPMS_TABLE_ENTRY_TYPE,
        ),
        [
            {"name": "event_id", "dataType": "string", "metadataType": "STRING"},
            {"name": "attributes", "dataType": "map<string,string>"},
            {
                "name": "properties",
                "dataType": "map",
                "metadataType": "STRUCT",
                "mode": "REPEATED",
                "fields": [
                    {
                        "name": "key",
                        "dataType": "string",
                        "metadataType": "STRING",
                        "mode": "REQUIRED",
                    },
                    {
                        "name": "value",
                        "dataType": "struct<label:string,weight:double>",
                        "metadataType": "OTHER",
                        "mode": "NULLABLE",
                    },
                ],
            },
            {
                "name": "aliases",
                "dataType": "array<string>",
                "metadataType": "OTHER",
                "mode": "REPEATED",
            },
            {
                "name": "tags",
                "dataType": "array<struct<key:string,value:string>>",
                "metadataType": "OTHER",
                "mode": "REPEATED",
            },
        ],
    )
    entries = [orders, events]
    entries_by_name = {entry.name: entry for entry in entries}
    mock_catalog_client.list_entry_groups.return_value = [
        create_mock_entry_group("test-project", "us", "@bigquery"),
        create_mock_entry_group("test-project", "us", "@dataproc_metastore"),
    ]
    mock_catalog_client.list_entries.side_effect = lambda request: [
        entry for entry in entries if entry.name.startswith(request.parent + "/")
    ]
    mock_catalog_client.get_entry.side_effect = lambda request: entries_by_name[
        request.name
    ]

    output_path = tmp_path / "dataplex_nested_schema_mces.json"
    run_and_get_pipeline(dataplex_entries_recipe(str(output_path)))

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(
            Path(__file__).parent / "golden" / "dataplex_nested_schema_golden.json"
        ),
    )
