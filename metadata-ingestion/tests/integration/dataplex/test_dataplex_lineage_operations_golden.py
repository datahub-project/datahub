"""Golden tests for lineage operation nodes (Query entities on lineage edges)."""

import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Sequence
from unittest.mock import Mock, patch

import pytest
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
DOWNSTREAM_FQN = "bigquery:test-project.analytics.daily_orders"
SQL_UPSTREAM_FQN = "bigquery:test-project.raw.orders"
# No producing process is known for this link, so it stays a plain edge.
PLAIN_UPSTREAM_FQN = "bigquery:test-project.raw.fx_rates"
PROCESS_NAME = "projects/test-project/locations/us/processes/daily-orders-load"
SQL = "INSERT INTO analytics.daily_orders SELECT * FROM raw.orders"
BIGQUERY_JOB_ID = "job_daily_orders"
# order_id is covered by the query alone; total spans the query and a plain edge.
COLUMN_LINKS = [
    (SQL_UPSTREAM_FQN, ["id"], ["order_id"]),
    (SQL_UPSTREAM_FQN, ["amount"], ["total"]),
    (PLAIN_UPSTREAM_FQN, ["rate"], ["total"]),
]
COMPOSER_PROCESS = SimpleNamespace(
    name=PROCESS_NAME,
    origin=SimpleNamespace(source_type=SimpleNamespace(name="COMPOSER"), name=""),
    attributes={"sql": SQL, "dag_id": "daily_orders"},
)
BIGQUERY_PROCESS = SimpleNamespace(
    name=PROCESS_NAME,
    origin=SimpleNamespace(source_type=SimpleNamespace(name="BIGQUERY"), name=""),
    attributes={"bigquery_job_id": BIGQUERY_JOB_ID},
)
BIGQUERY_JOB = SimpleNamespace(
    query=SQL, job_type="query", total_bytes_processed=1024, labels={"team": "data"}
)
COLUMN_OVERRIDES: Dict[str, Any] = {
    "include_schema": True,
    "include_column_lineage": True,
    "include_lineage_operation_sql": True,
}


def _recipe(output_path: str, **overrides: Any) -> Dict[str, Any]:
    return {
        "source": {
            "type": "dataplex",
            "config": {
                "project_ids": ["test-project"],
                "entries_locations": ["us"],
                "lineage_locations": ["us"],
                "include_lineage": True,
                "include_schema": False,
                "include_glossaries": False,
                "include_lineage_operations": True,
                **overrides,
            },
        },
        "sink": {"type": "file", "config": {"filename": output_path}},
    }


def _ref(fqn: str, field: Sequence[str] = ()) -> SimpleNamespace:
    return SimpleNamespace(fully_qualified_name=fqn, field=list(field))


def _search_links(request: Any) -> List[SimpleNamespace]:
    if request.targets.entities:
        requested = {tuple(entity.field) for entity in request.targets.entities}
        return [
            SimpleNamespace(
                source=_ref(source_fqn, source_field),
                target=_ref(DOWNSTREAM_FQN, target_field),
                name="",
            )
            for source_fqn, source_field, target_field in COLUMN_LINKS
            if tuple(target_field) in requested
        ]
    if request.target.fully_qualified_name != DOWNSTREAM_FQN:
        return []
    return [
        SimpleNamespace(
            source=_ref(SQL_UPSTREAM_FQN), target=_ref(DOWNSTREAM_FQN), name="links/a"
        ),
        SimpleNamespace(
            source=_ref(PLAIN_UPSTREAM_FQN), target=_ref(DOWNSTREAM_FQN), name="links/b"
        ),
    ]


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.parametrize(
    "golden_name,process,overrides",
    [
        ("dataplex_lineage_operations_golden", COMPOSER_PROCESS, {}),
        (
            "dataplex_lineage_operations_columns_golden",
            BIGQUERY_PROCESS,
            COLUMN_OVERRIDES,
        ),
        (
            "dataplex_lineage_operations_incremental_golden",
            BIGQUERY_PROCESS,
            {**COLUMN_OVERRIDES, "incremental_lineage": True},
        ),
    ],
)
@patch("google.cloud.bigquery.Client")
@patch("google.auth.default")
@patch("google.cloud.dataplex_v1.CatalogServiceClient")
@patch("datahub.ingestion.source.dataplex.dataplex.LineageClient")
def test_dataplex_lineage_operations_golden(
    mock_lineage_client_class: Mock,
    mock_catalog_client_class: Mock,
    mock_google_auth: Mock,
    mock_bigquery_client_class: Mock,
    golden_name: str,
    process: SimpleNamespace,
    overrides: Dict[str, Any],
    pytestconfig: Any,
    tmp_path: Path,
) -> None:
    mock_google_auth.return_value = (Mock(), "test-project")
    mock_bigquery_client_class.return_value.get_job.return_value = BIGQUERY_JOB
    lineage_client = mock_lineage_client_class.return_value
    lineage_client.search_links.side_effect = _search_links
    lineage_client.batch_search_link_processes.return_value = [
        SimpleNamespace(
            process=PROCESS_NAME,
            links=[
                SimpleNamespace(
                    link="links/a",
                    start_time=datetime.datetime(
                        2024, 1, 15, 11, 0, tzinfo=datetime.timezone.utc
                    ),
                    end_time=datetime.datetime(
                        2024, 1, 15, 11, 5, tzinfo=datetime.timezone.utc
                    ),
                )
            ],
        )
    ]
    lineage_client.get_process.return_value = process

    entry = create_mock_entry(
        project_id="test-project",
        location="us",
        entry_group_id="@bigquery",
        entry_id="daily_orders",
        fqn=DOWNSTREAM_FQN,
        entry_type=BIGQUERY_TABLE_ENTRY_TYPE,
    )
    if overrides.get("include_schema"):
        entry.aspects = {
            "655216118709.global.schema": dataplex_v1.Aspect(
                data={
                    "fields": [
                        {"name": "order_id", "dataType": "STRING"},
                        {"name": "total", "dataType": "FLOAT64"},
                    ]
                }
            )
        }
    catalog = mock_catalog_client_class.return_value
    catalog.list_entry_groups.return_value = [
        create_mock_entry_group("test-project", "us", "@bigquery")
    ]
    catalog.list_entries.return_value = [entry]
    catalog.get_entry.return_value = entry

    output_path = tmp_path / f"{golden_name}.json"
    run_and_get_pipeline(_recipe(str(output_path), **overrides))

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(Path(__file__).parent / "golden" / f"{golden_name}.json"),
    )
