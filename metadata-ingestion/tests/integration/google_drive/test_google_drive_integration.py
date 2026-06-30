"""Integration tests for the Google Drive connector.

These tests validate the connector against a real Google Drive folder shared
with a service account (or accessible via domain-wide delegation).

Requirements:
    GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_FILE: Path to a service account JSON key
        file. The service account must have the Google Drive API enabled and
        read access to GOOGLE_DRIVE_TEST_FOLDER_ID (required).
    GOOGLE_DRIVE_TEST_FOLDER_ID: Google Drive folder ID containing at least
        one Google Doc to ingest (required).
    GOOGLE_DRIVE_DELEGATED_USER_EMAIL: Email of a Workspace user to
        impersonate via domain-wide delegation (optional).

Setup:
    1. Create a service account at
       https://console.cloud.google.com/iam-admin/serviceaccounts and enable
       the Google Drive API for its project.
    2. Either:
       a. Share a test folder (containing at least one Google Doc) with the
          service account's email address, OR
       b. Configure domain-wide delegation for the service account and set
          GOOGLE_DRIVE_DELEGATED_USER_EMAIL to a user whose Drive contains
          the test folder.
    3. Download the service account JSON key file.
    4. Export GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_FILE=<path-to-key.json>
    5. Export GOOGLE_DRIVE_TEST_FOLDER_ID=<folder-id>
    6. (Optional) Export GOOGLE_DRIVE_DELEGATED_USER_EMAIL=<user@example.com>

Run with:
    pytest -m integration tests/integration/google_drive/
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List

import pytest

from datahub.ingestion.run.pipeline import Pipeline

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


def _build_recipe(tmp_path: Path, output_filename: str) -> Dict[str, Any]:
    service_account_key_file = os.environ["GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_FILE"]
    folder_id = os.environ["GOOGLE_DRIVE_TEST_FOLDER_ID"]
    delegated_user_email = os.environ.get("GOOGLE_DRIVE_DELEGATED_USER_EMAIL")

    source_config: Dict[str, Any] = {
        "credentials": {
            "service_account_key_file": service_account_key_file,
        },
        "folder_ids": [folder_id],
        "max_documents": 10,
        "advanced": {
            "continue_on_failure": False,
            "raise_on_error": True,
        },
    }
    if delegated_user_email:
        source_config["delegated_user_email"] = delegated_user_email

    return {
        "run_id": "test-google-drive-integration",
        "source": {
            "type": "google-drive",
            "config": source_config,
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": str(tmp_path / output_filename),
            },
        },
    }


def test_google_drive_ingestion(tmp_path: Path) -> None:
    """Test that a real Google Drive folder can be ingested end-to-end.

    Expected behavior:
        - Ingestion completes without failures.
        - At least one ``document`` entity MCP is emitted to the sink.
    """
    recipe = _build_recipe(tmp_path, "google_drive_ingestion.json")

    pipeline = Pipeline.create(recipe)
    pipeline.run()
    pipeline.raise_from_status()

    output_file = tmp_path / "google_drive_ingestion.json"
    assert output_file.exists(), "Output file was not created"
    assert output_file.stat().st_size > 0, "Output file is empty"

    with open(output_file) as f:
        output_data: List[Dict[str, Any]] = json.load(f)

    assert isinstance(output_data, list), "Output should be a JSON array"
    assert len(output_data) > 0, "Output should contain at least one record"

    document_mcps = [
        record
        for record in output_data
        if isinstance(record, dict) and record.get("entityType") == "document"
    ]
    entity_types = sorted(
        str(r.get("entityType")) for r in output_data if isinstance(r, dict)
    )
    assert document_mcps, (
        "Expected at least one 'document' entity MCP in the ingestion output, "
        f"got entity types: {entity_types}"
    )

    report = pipeline.source.get_report()
    assert not report.failures, f"Ingestion reported failures: {report.failures}"


def test_google_drive_test_connection() -> None:
    """Test the test_connection capability of the Google Drive source.

    Expected behavior:
        - test_connection succeeds with valid service-account credentials.
        - Basic connectivity to the Google Drive API is reported as capable.
    """
    from datahub.ingestion.source.google_drive.google_drive_source import (
        GoogleDriveSource,
    )

    service_account_key_file = os.environ["GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_FILE"]
    folder_id = os.environ["GOOGLE_DRIVE_TEST_FOLDER_ID"]
    delegated_user_email = os.environ.get("GOOGLE_DRIVE_DELEGATED_USER_EMAIL")

    config: Dict[str, Any] = {
        "credentials": {
            "service_account_key_file": service_account_key_file,
        },
        "folder_ids": [folder_id],
    }
    if delegated_user_email:
        config["delegated_user_email"] = delegated_user_email

    report = GoogleDriveSource.test_connection(config)

    assert report.basic_connectivity, "Basic connectivity should be reported"
    assert report.basic_connectivity.capable, (
        f"Should be able to connect to the Google Drive API: "
        f"{report.basic_connectivity.failure_reason}"
    )
