"""Golden test for Vertex AI feature groups, feature online stores and model versions."""

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

ENTRY_TYPE_PREFIX = "projects/test-project/locations/global/entryTypes/"


def _recipe(output_path: str) -> Dict[str, Any]:
    return {
        "source": {
            "type": "dataplex",
            "config": {
                "project_ids": ["test-project"],
                "entries_locations": ["us-central1"],
                "include_lineage": False,
                # Two entry types share the project container; keep one worker
                # so its properties do not depend on thread order.
                "max_workers_entries": 1,
            },
        },
        "sink": {"type": "file", "config": {"filename": output_path}},
    }


@time_machine.travel(FROZEN_TIME, tick=False)
@patch("google.auth.default")
@patch("google.cloud.dataplex_v1.CatalogServiceClient")
@patch("google.cloud.datacatalog_lineage.LineageClient")
def test_dataplex_vertex_ai_golden(
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
            location="us-central1",
            entry_group_id="@vertexai",
            entry_id="my_feature_group",
            fqn="vertex_ai:featuregroup:test-project.us-central1.my_feature_group",
            entry_type=ENTRY_TYPE_PREFIX + "vertexai-feature-group",
            description="Features for the churn model",
        ),
        create_mock_entry(
            project_id="test-project",
            location="us-central1",
            entry_group_id="@vertexai",
            entry_id="my_online_store",
            fqn="vertex_ai:featureonlinestore:test-project.us-central1.my_online_store",
            entry_type=ENTRY_TYPE_PREFIX + "vertexai-feature-online-store",
        ),
        create_mock_entry(
            project_id="test-project",
            location="us-central1",
            entry_group_id="@vertexai",
            entry_id="123456@1",
            fqn="vertex_ai:model:test-project.us-central1.123456.1",
            entry_type=ENTRY_TYPE_PREFIX + "vertexai-model-version",
            description="Churn model, version 1",
        ),
    ]
    entries_by_name = {entry.name: entry for entry in entries}
    mock_catalog_client.list_entry_groups.return_value = [
        create_mock_entry_group("test-project", "us-central1", "@vertexai")
    ]
    mock_catalog_client.list_entries.return_value = entries
    mock_catalog_client.get_entry.side_effect = lambda request: entries_by_name[
        request.name
    ]

    output_path = tmp_path / "dataplex_vertex_ai_mces.json"
    run_and_get_pipeline(_recipe(str(output_path)))

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(
            Path(__file__).parent / "golden" / "dataplex_vertex_ai_golden.json"
        ),
    )
