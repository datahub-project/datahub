"""Golden test for column-level lineage (fineGrainedLineages)."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import Mock, patch

import time_machine
from google.cloud import dataplex_v1

from datahub.testing import mce_helpers
from tests.integration.dataplex.test_dataplex_integration import (
    FROZEN_TIME,
    create_mock_entry,
    create_mock_entry_group,
)
from tests.test_helpers.state_helpers import run_and_get_pipeline

BIGQUERY_TABLE_ENTRY_TYPE = (
    "projects/test-project/locations/us/entryTypes/bigquery-table"
)
UPSTREAM_FQN = "bigquery:test-project.raw.orders"
DOWNSTREAM_FQN = "bigquery:test-project.analytics.daily_orders"
# Only referenced by a column link, so it must also become a table-level upstream.
COLUMN_ONLY_UPSTREAM_FQN = "bigquery:test-project.raw.fx_rates"
COLUMN_LINKS = [
    (UPSTREAM_FQN, ["id"], ["order_id"]),
    (UPSTREAM_FQN, ["cust_id"], ["customer", "id"]),
    (UPSTREAM_FQN, ["amount"], ["total"]),
    (COLUMN_ONLY_UPSTREAM_FQN, ["rate"], ["total"]),
]


def _recipe(output_path: str) -> Dict[str, Any]:
    return {
        "source": {
            "type": "dataplex",
            "config": {
                "project_ids": ["test-project"],
                "entries_locations": ["us"],
                "lineage_locations": ["us"],
                "include_lineage": True,
                "include_schema": True,
                "include_column_lineage": True,
            },
        },
        "sink": {"type": "file", "config": {"filename": output_path}},
    }


def _ref(fqn: str, field: List[str]) -> SimpleNamespace:
    return SimpleNamespace(fully_qualified_name=fqn, field=field)


def _search_links(request: Any) -> List[SimpleNamespace]:
    if request.targets.entities:
        requested = {tuple(entity.field) for entity in request.targets.entities}
        return [
            SimpleNamespace(
                source=_ref(source_fqn, source_field),
                target=_ref(DOWNSTREAM_FQN, target_field),
            )
            for source_fqn, source_field, target_field in COLUMN_LINKS
            if tuple(target_field) in requested
        ]
    if request.target.fully_qualified_name == DOWNSTREAM_FQN:
        return [
            SimpleNamespace(
                source=_ref(UPSTREAM_FQN, []), target=_ref(DOWNSTREAM_FQN, [])
            )
        ]
    return []


def _table(entry_id: str, fqn: str, fields: List[Dict[str, Any]]) -> Mock:
    entry = create_mock_entry(
        project_id="test-project",
        location="us",
        entry_group_id="@bigquery",
        entry_id=entry_id,
        fqn=fqn,
        entry_type=BIGQUERY_TABLE_ENTRY_TYPE,
    )
    entry.aspects = {
        "655216118709.global.schema": dataplex_v1.Aspect(data={"fields": fields})
    }
    return entry


@time_machine.travel(FROZEN_TIME, tick=False)
@patch("google.auth.default")
@patch("google.cloud.dataplex_v1.CatalogServiceClient")
@patch("datahub.ingestion.source.dataplex.dataplex.LineageClient")
def test_dataplex_column_lineage_golden(
    mock_lineage_client_class: Mock,
    mock_catalog_client_class: Mock,
    mock_google_auth: Mock,
    pytestconfig: Any,
    tmp_path: Path,
) -> None:
    mock_google_auth.return_value = (Mock(), "test-project")
    mock_catalog_client = Mock()
    mock_catalog_client_class.return_value = mock_catalog_client
    mock_lineage_client = Mock()
    mock_lineage_client.search_links.side_effect = lambda request: _search_links(
        request
    )
    mock_lineage_client_class.return_value = mock_lineage_client

    entries = [
        _table(
            "orders",
            UPSTREAM_FQN,
            [
                {"name": "id", "dataType": "STRING"},
                {"name": "cust_id", "dataType": "STRING"},
                {"name": "amount", "dataType": "FLOAT64"},
            ],
        ),
        _table(
            "daily_orders",
            DOWNSTREAM_FQN,
            [
                {"name": "order_id", "dataType": "STRING"},
                {
                    "name": "customer",
                    "dataType": "RECORD",
                    "fields": [{"name": "id", "dataType": "STRING"}],
                },
                {"name": "total", "dataType": "FLOAT64"},
            ],
        ),
    ]
    entries_by_name = {entry.name: entry for entry in entries}
    mock_catalog_client.list_entry_groups.return_value = [
        create_mock_entry_group("test-project", "us", "@bigquery")
    ]
    mock_catalog_client.list_entries.return_value = entries
    mock_catalog_client.get_entry.side_effect = lambda request: entries_by_name[
        request.name
    ]

    output_path = tmp_path / "dataplex_column_lineage_mces.json"
    run_and_get_pipeline(_recipe(str(output_path)))

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(
            Path(__file__).parent / "golden" / "dataplex_column_lineage_golden.json"
        ),
    )
