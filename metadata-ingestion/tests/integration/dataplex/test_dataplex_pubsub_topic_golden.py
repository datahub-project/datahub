"""Golden test for Pub/Sub topic entries, including topic ids with periods."""

from pathlib import Path
from typing import Any, Dict
from unittest.mock import Mock, patch

import time_machine

from datahub.testing import mce_helpers
from tests.integration.dataplex.test_dataplex_integration import (
    FROZEN_TIME,
    create_mock_entry,
    create_mock_entry_group,
)
from tests.test_helpers.state_helpers import run_and_get_pipeline

PUBSUB_TOPIC_ENTRY_TYPE = (
    "projects/test-project/locations/global/entryTypes/pubsub-topic"
)


def _recipe(output_path: str) -> Dict[str, Any]:
    return {
        "source": {
            "type": "dataplex",
            "config": {
                "project_ids": ["test-project"],
                "entries_locations": ["global"],
                "include_lineage": False,
            },
        },
        "sink": {"type": "file", "config": {"filename": output_path}},
    }


@time_machine.travel(FROZEN_TIME, tick=False)
@patch("google.auth.default")
@patch("google.cloud.dataplex_v1.CatalogServiceClient")
@patch("google.cloud.datacatalog_lineage.LineageClient")
def test_dataplex_pubsub_topics_golden(
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

    entries = [
        create_mock_entry(
            project_id="test-project",
            location="global",
            entry_group_id="@pubsub",
            entry_id="events",
            fqn="pubsub:topic:test-project.events",
            entry_type=PUBSUB_TOPIC_ENTRY_TYPE,
        ),
        # A segment with a period is backtick-quoted in the FQN.
        create_mock_entry(
            project_id="test-project",
            location="global",
            entry_group_id="@pubsub",
            entry_id="orders.v2",
            fqn="pubsub:topic:test-project.`orders.v2`",
            entry_type=PUBSUB_TOPIC_ENTRY_TYPE,
        ),
    ]
    entries_by_name = {entry.name: entry for entry in entries}
    mock_catalog_client.list_entry_groups.return_value = [
        create_mock_entry_group("test-project", "global", "@pubsub")
    ]
    mock_catalog_client.list_entries.return_value = entries
    mock_catalog_client.get_entry.side_effect = lambda request: entries_by_name[
        request.name
    ]

    output_path = tmp_path / "dataplex_pubsub_topics_mces.json"
    run_and_get_pipeline(_recipe(str(output_path)))

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(
            Path(__file__).parent / "golden" / "dataplex_pubsub_topics_golden.json"
        ),
    )
