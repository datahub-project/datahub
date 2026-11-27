"""Golden test for lineage upstreams that are not Dataplex catalog entries."""

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
    create_mock_lineage_link,
)
from tests.test_helpers.state_helpers import run_and_get_pipeline

ENTRY_TYPE_PREFIX = "projects/test-project/locations/us/entryTypes/"
DOWNSTREAM_FQN = "bigquery:test-project.analytics.daily_orders"
UPSTREAM_FQNS = [
    "gcs:raw-bucket.`orders/2024/01/15.json`",
    # Matches the Dataproc Metastore table ingested below.
    "hive_metastore:`localhost:9083`.my_db.events",
    # Matches nothing in this run, so it becomes a hive node.
    "hive_metastore:thrift-host.other_db.clicks",
    "pubsub:subscription:test-project.orders-sub",
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
                "include_schema": False,
                "include_storage_lineage": True,
                "resolve_pubsub_subscriptions": True,
            },
        },
        "sink": {"type": "file", "config": {"filename": output_path}},
    }


def _search_links(request: Any) -> List[Mock]:
    if request.target.fully_qualified_name != DOWNSTREAM_FQN:
        return []
    return [create_mock_lineage_link(fqn, DOWNSTREAM_FQN) for fqn in UPSTREAM_FQNS]


@time_machine.travel(FROZEN_TIME, tick=False)
@patch("google.auth.default")
@patch("google.cloud.dataplex_v1.CatalogServiceClient")
@patch("datahub.ingestion.source.dataplex.dataplex.LineageClient")
@patch("google.cloud.pubsub_v1.SubscriberClient")
def test_dataplex_upstream_resolution_golden(
    mock_subscriber_client_class: Mock,
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
    mock_subscriber_client_class.return_value.get_subscription.return_value = Mock(
        topic="projects/test-project/topics/orders"
    )

    daily_orders = create_mock_entry(
        project_id="test-project",
        location="us",
        entry_group_id="@bigquery",
        entry_id="daily_orders",
        fqn=DOWNSTREAM_FQN,
        entry_type=ENTRY_TYPE_PREFIX + "bigquery-table",
    )
    events = create_mock_entry(
        project_id="test-project",
        location="us",
        entry_group_id="@dataproc_metastore",
        entry_id="events",
        fqn="dataproc_metastore:test-project.us.my-service.my_db.events",
        entry_type=ENTRY_TYPE_PREFIX + "dataproc-metastore-table",
    )
    events.aspects = {
        "655216118709.global.storage": dataplex_v1.Aspect(
            data={
                "service": "CLOUD_STORAGE",
                "resourceName": "gs://warehouse-bucket/my_db.db/events",
            }
        )
    }
    entries = [daily_orders, events]
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

    output_path = tmp_path / "dataplex_upstream_resolution_mces.json"
    run_and_get_pipeline(_recipe(str(output_path)))

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(
            Path(__file__).parent
            / "golden"
            / "dataplex_upstream_resolution_golden.json"
        ),
    )
