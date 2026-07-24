"""Integration test for the Dataplex export extraction method.

Runs the full pipeline with ``extraction_method: export``: a mocked
metadataJobs REST session (submit + poll), a mocked GCS client streaming canned
export JSONL, and golden file validation. Proves that exported entries flow
through the same mapper pipeline as the API path.
"""

import json
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, Mock, patch

import time_machine

from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport
from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import run_and_get_pipeline

FROZEN_TIME = "2024-01-15 12:00:00"
# uuid.uuid4 is patched so the export job id (which embeds uuid4().hex[:8]) is
# predictable and the canned blob names can carry the matching job= marker.
FIXED_UUID = uuid.UUID("00000000-0000-0000-0000-000000000000")
JOB_ID = "datahub-export-us-00000000"


def dataplex_export_recipe(mcp_output_path: str) -> Dict[str, Any]:
    """Create a test recipe for Dataplex ingestion using the export method."""
    return {
        "source": {
            "type": "dataplex",
            "config": {
                "project_ids": ["test-project"],
                "entries_locations": ["us"],
                "extraction_method": "export",
                "export_config": {
                    "export_job_runner_project": "runner-project",
                    "bucket_base_name": "export-bucket",
                    "export_poll_seconds": 1,
                },
                "include_lineage": False,
                "include_schema": True,
                "include_glossaries": False,
            },
        },
        "sink": {"type": "file", "config": {"filename": mcp_output_path}},
    }


def make_export_entry_line(
    entry_id: str,
    fqn: str,
    entry_type_short_name: str,
    description: str = "",
    parent_entry: str = "",
    columns: Optional[List[Dict[str, str]]] = None,
) -> str:
    """Build one export JSONL line (the JSON form of a dataplex_v1.Entry)."""
    entry: Dict[str, Any] = {
        "name": (
            "projects/test-project/locations/us/entryGroups/@bigquery"
            f"/entries/{entry_id}"
        ),
        "entryType": (
            f"projects/655216118709/locations/global/entryTypes/{entry_type_short_name}"
        ),
        "fullyQualifiedName": fqn,
        "entrySource": {
            "description": description,
            "createTime": "2024-01-04T00:00:00Z",
            "updateTime": "2024-01-13T00:00:00Z",
        },
    }
    if parent_entry:
        entry["parentEntry"] = parent_entry
    if columns:
        entry["aspects"] = {
            "655216118709.global.schema": {
                "aspectType": (
                    "projects/655216118709/locations/global/aspectTypes/schema"
                ),
                "data": {"columns": columns},
            }
        }
    return json.dumps({"entry": entry})


def make_blob(name: str, lines: List[str]) -> Mock:
    """Mock a GCS blob whose open() streams the given JSONL lines."""
    blob = Mock()
    blob.name = name
    text = "\n".join(lines) + "\n"
    handle = MagicMock()
    handle.__enter__.return_value = iter(text.splitlines(keepends=True))
    blob.open.return_value = handle
    return blob


def make_export_session() -> MagicMock:
    """Mock AuthorizedSession: metadataJobs.create accepted, job SUCCEEDED."""
    session = MagicMock()
    post_resp = Mock(status_code=200)
    post_resp.json.return_value = {
        "name": f"projects/runner-project/locations/us/metadataJobs/{JOB_ID}"
    }
    session.post.return_value = post_resp

    get_resp = Mock(status_code=200)
    get_resp.json.return_value = {"status": {"state": "SUCCEEDED"}}
    session.get.return_value = get_resp
    return session


@time_machine.travel(FROZEN_TIME, tick=False)
@patch("datahub.ingestion.source.dataplex.dataplex_export.uuid.uuid4")
@patch("google.cloud.storage.Client")
@patch("datahub.ingestion.source.dataplex.dataplex.build_authed_session")
@patch("google.auth.default")
def test_dataplex_export_integration(
    mock_google_auth,
    mock_build_authed_session,
    mock_storage_client_class,
    mock_uuid4,
    pytestconfig,
    tmp_path,
):
    """Export-mode pipeline run against canned export JSONL, with golden validation."""
    mock_credentials = Mock()
    mock_google_auth.return_value = (mock_credentials, "test-project")
    mock_uuid4.return_value = FIXED_UUID
    mock_build_authed_session.return_value = make_export_session()

    dataset_parent_entry = (
        "projects/test-project/locations/us/entryGroups/@bigquery/entries/"
        "bigquery.googleapis.com/projects/test-project/datasets/analytics"
    )

    export_lines = [
        make_export_entry_line(
            entry_id=(
                "bigquery.googleapis.com/projects/test-project/datasets/analytics"
            ),
            fqn="bigquery:test-project.analytics",
            entry_type_short_name="bigquery-dataset",
            description="Analytics dataset",
        ),
        make_export_entry_line(
            entry_id=(
                "bigquery.googleapis.com/projects/test-project/datasets/analytics"
                "/tables/customers"
            ),
            fqn="bigquery:test-project.analytics.customers",
            entry_type_short_name="bigquery-table",
            description="Customer master data table",
            parent_entry=dataset_parent_entry,
            columns=[
                {"column": "id", "dataType": "INT64", "description": "Customer ID"},
                {
                    "column": "name",
                    "dataType": "STRING",
                    "description": "Customer name",
                },
                {
                    "column": "email",
                    "dataType": "STRING",
                    "description": "Customer email address",
                },
            ],
        ),
        make_export_entry_line(
            entry_id=(
                "bigquery.googleapis.com/projects/test-project/datasets/analytics"
                "/tables/orders"
            ),
            fqn="bigquery:test-project.analytics.orders",
            entry_type_short_name="bigquery-table",
            description="Orders transaction table",
            parent_entry=dataset_parent_entry,
            columns=[
                {"column": "order_id", "dataType": "INT64", "description": "Order ID"},
                {
                    "column": "total_amount",
                    "dataType": "FLOAT64",
                    "description": "Order total amount",
                },
            ],
        ),
    ]

    export_blob = make_blob(
        f"metadata/job={JOB_ID}/entry_group=@bigquery/part-0.jsonl",
        export_lines,
    )
    # An older run's output in the same bucket must be ignored.
    stale_blob = make_blob(
        "metadata/job=datahub-export-us-99999999/entry_group=@bigquery/part-0.jsonl",
        [
            make_export_entry_line(
                entry_id="stale",
                fqn="bigquery:test-project.stale.table",
                entry_type_short_name="bigquery-table",
            )
        ],
    )

    mock_storage_client = Mock()
    mock_storage_client.list_blobs.return_value = [export_blob, stale_blob]
    mock_storage_client_class.return_value = mock_storage_client

    mcp_output_path = tmp_path / "dataplex_export_mces.json"
    mcp_golden_path = (
        Path(__file__).parent / "golden" / "dataplex_export_entries_golden.json"
    )

    pipeline_config = dataplex_export_recipe(str(mcp_output_path))
    pipeline = run_and_get_pipeline(pipeline_config)

    report = pipeline.source.get_report()
    assert isinstance(report, DataplexReport)
    assert report.failures == []
    assert report.export_jobs_submitted == 1
    assert report.export_jobs_succeeded == 1
    assert report.export_blobs_read == 1
    assert report.export_entries_read == 3

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(mcp_output_path),
        golden_path=str(mcp_golden_path),
    )
