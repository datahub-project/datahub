# ABOUTME: Smoke tests for Slack notifications when dataset schemas are changed.
# ABOUTME: Tests schema metadata updates and verifies Slack messages are sent for DATASET_SCHEMA_CHANGE notifications.

import json
import logging
import os
import tempfile
import time
import uuid
from contextlib import contextmanager
from typing import Any, Dict, List

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities.env_vars import (
    get_release_test_notification_channel,
    get_release_test_notification_token,
)
from tests.utilities.slack_helpers import (
    check_slack_notification,
    get_channel_id_by_name,
)
from tests.utils import get_gms_url, ingest_file_via_rest

TEST_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"
BASELINE_DATA_FILE = "tests/release_tests/schema_notification_data.json"

logger = logging.getLogger(__name__)


def get_dataset_schema(auth_session, dataset_urn: str):
    """Get schema metadata for a dataset."""
    from typing import Any, Dict

    from tests.utils import execute_graphql

    query = """
        query getDatasetSchema($urn: String!) {
            dataset(urn: $urn) {
                urn
                schemaMetadata {
                    name
                    version
                    fields {
                        fieldPath
                        nullable
                        description
                        type
                        nativeDataType
                        recursive
                    }
                }
            }
        }
    """
    variables: Dict[str, Any] = {"urn": dataset_urn}

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["dataset"]["schemaMetadata"]


def get_dataset_schema_via_rest(auth_session, dataset_urn: str):
    """Get schema metadata via REST API for backup/restore."""
    import urllib.parse

    gms_url = get_gms_url()
    encoded_urn = urllib.parse.quote(dataset_urn, safe="")
    url = f"{gms_url}/entities/{encoded_urn}"

    response = auth_session.get(url)
    response.raise_for_status()

    entity_data = response.json()
    aspects = entity_data.get("aspects", {})
    return aspects.get("schemaMetadata")


def restore_dataset_schema_via_rest(auth_session, dataset_urn: str, schema_aspect):
    """Restore schema metadata via REST API."""
    if schema_aspect is None:
        logger.info("No schema to restore (was None)")
        return

    gms_url = get_gms_url()
    url = f"{gms_url}/aspects?action=ingestProposal"

    proposal = {
        "proposal": {
            "entityType": "dataset",
            "entityUrn": dataset_urn,
            "aspectName": "schemaMetadata",
            "changeType": "UPSERT",
            "aspect": {
                "contentType": "application/json",
                "value": schema_aspect,
            },
        }
    }

    response = auth_session.post(url, json=proposal)
    response.raise_for_status()
    logger.info(f"Restored original schema for {dataset_urn}")


@contextmanager
def backup_and_restore_schema(auth_session, dataset_urn: str):
    """Context manager to backup and restore dataset schema for test idempotency."""
    logger.info(f"Backing up current schema for {dataset_urn}")
    original_schema = None
    try:
        original_schema = get_dataset_schema_via_rest(auth_session, dataset_urn)
        logger.info("Original schema backed up successfully")
    except Exception as e:
        logger.warning(f"Could not backup original schema: {e}")

    try:
        yield original_schema
    finally:
        logger.info("Cleaning up - restoring original schema")
        try:
            if original_schema:
                restore_dataset_schema_via_rest(
                    auth_session, dataset_urn, original_schema
                )
                wait_for_writes_to_sync()
                logger.info("Cleanup successful")
            else:
                logger.warning("No original schema to restore")
        except Exception as e:
            logger.warning(f"Cleanup failed: {e}")


def load_baseline_schema() -> List[Dict[str, Any]]:
    """Load baseline schema data from JSON file."""
    with open(BASELINE_DATA_FILE, "r") as f:
        data = json.load(f)
    return data[0]["aspect"]["json"]["fields"]


def ingest_schema_with_fields(
    auth_session, dataset_urn: str, fields: List[Dict[str, Any]]
):
    """Ingest schema metadata with specified fields using baseline structure."""
    # Load baseline to preserve structure
    with open(BASELINE_DATA_FILE, "r") as f:
        schema_data = json.load(f)

    # Update the fields in the schema
    schema_data[0]["aspect"]["json"]["fields"] = fields

    # Write to temp file and ingest
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as temp_file:
        json.dump(schema_data, temp_file)
        temp_file_path = temp_file.name

    try:
        logger.info(f"Updating schema for {dataset_urn} with {len(fields)} fields")
        ingest_file_via_rest(auth_session, temp_file_path)
        logger.info("Schema update ingested successfully")
    finally:
        os.unlink(temp_file_path)


def verify_slack_notification(
    slack_token: str,
    slack_channel: str,
    expected_text: str,
    timestamp_seconds: str,
    dataset_name: str = "SampleHdfsDataset",
):
    """Verify that a Slack notification was sent with expected content."""
    channel_id = get_channel_id_by_name(slack_token, slack_channel)
    assert channel_id is not None, f"Failed to resolve channel '{slack_channel}' to ID"

    check_slack_notification(
        slack_token,
        channel_id,
        expected_text,
        timestamp_seconds,
        expected_text=dataset_name,
    )
    logger.info("Slack notification verified")


@pytest.fixture(scope="module", autouse=True)
def setup_baseline_schema(auth_session):
    """Ensure baseline schema exists before running tests."""
    logger.info("Setting up baseline schema from data file")
    ingest_file_via_rest(auth_session, BASELINE_DATA_FILE)
    wait_for_writes_to_sync()
    logger.info("Baseline schema setup complete")
    yield
    logger.info("Module cleanup complete")


@pytest.mark.release_tests
def test_add_columns_with_slack_notification(auth_session):
    """
    Test that adding columns to a dataset schema triggers a Slack notification.

    Steps:
    1. Backup current schema (for cleanup/idempotency)
    2. Get current schema and count fields
    3. Add one new field (increase count by 1)
    4. Verify Slack notification shows "has added column(s)"
    5. Cleanup: Restore original schema
    """
    slack_channel = get_release_test_notification_channel()
    slack_token = get_release_test_notification_token()
    if not slack_channel or not slack_token:
        raise ValueError(
            "RELEASE_TEST_NOTIFICATION_CHANNEL or RELEASE_TEST_NOTIFICATION_TOKEN not set"
        )

    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting add columns test at timestamp: {timestamp_seconds}")

    with backup_and_restore_schema(auth_session, TEST_DATASET_URN):
        # Load baseline fields from JSON file
        baseline_fields = load_baseline_schema()
        logger.info(f"Baseline schema has {len(baseline_fields)} fields")

        # Create new field to add
        random_suffix = uuid.uuid4().hex[:8]
        new_field_name = f"field_baz_{random_suffix}"
        new_field = {
            "fieldPath": new_field_name,
            "nullable": False,
            "description": "New field added for testing",
            "type": {"type": {"com.linkedin.schema.NumberType": {}}},
            "nativeDataType": "integer",
            "recursive": False,
        }

        # Add new field to baseline fields
        updated_fields = baseline_fields + [new_field]
        logger.info(
            f"Adding field {new_field_name} (total fields: {len(updated_fields)})"
        )

        # Ingest updated schema
        ingest_schema_with_fields(auth_session, TEST_DATASET_URN, updated_fields)
        wait_for_writes_to_sync()

        # Verify schema was updated
        updated_schema = get_dataset_schema(auth_session, TEST_DATASET_URN)
        assert updated_schema is not None, "Expected updated schema to be present"
        updated_field_paths = [f["fieldPath"] for f in updated_schema.get("fields", [])]
        assert new_field_name in updated_field_paths, (
            f"Expected {new_field_name} to be present"
        )
        logger.info(
            f"Schema updated successfully with {len(updated_field_paths)} fields"
        )

        # Verify Slack notification
        logger.info("Verifying Slack notification for added columns")
        verify_slack_notification(
            slack_token, slack_channel, "has added column(s)", timestamp_seconds
        )

    logger.info("Add columns test complete")


@pytest.mark.release_tests
def test_remove_columns_with_slack_notification(auth_session):
    """
    Test that removing columns from a dataset schema triggers a Slack notification.

    Steps:
    1. Backup current schema (for cleanup/idempotency)
    2. Get current schema and remove one or more fields
    3. Verify Slack notification shows "has removed column(s)"
    4. Cleanup: Restore original schema
    """
    slack_channel = get_release_test_notification_channel()
    slack_token = get_release_test_notification_token()
    if not slack_channel or not slack_token:
        raise ValueError(
            "RELEASE_TEST_NOTIFICATION_CHANNEL or RELEASE_TEST_NOTIFICATION_TOKEN not set"
        )

    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting remove columns test at timestamp: {timestamp_seconds}")

    with backup_and_restore_schema(auth_session, TEST_DATASET_URN):
        # Load baseline fields from JSON file
        baseline_fields = load_baseline_schema()
        logger.info(f"Baseline schema has {len(baseline_fields)} fields")

        assert len(baseline_fields) >= 2, "Need at least 2 fields to test removal"

        # Remove one field (keep all but the last one)
        removed_field_name = baseline_fields[-1]["fieldPath"]
        reduced_fields = baseline_fields[:-1]
        logger.info(
            f"Removing field {removed_field_name} (remaining fields: {len(reduced_fields)})"
        )

        # Ingest reduced schema
        ingest_schema_with_fields(auth_session, TEST_DATASET_URN, reduced_fields)
        wait_for_writes_to_sync()

        # Verify schema was updated
        updated_schema = get_dataset_schema(auth_session, TEST_DATASET_URN)
        assert updated_schema is not None, "Expected updated schema to be present"
        updated_field_paths = [f["fieldPath"] for f in updated_schema.get("fields", [])]
        assert removed_field_name not in updated_field_paths, (
            f"Expected {removed_field_name} to be removed"
        )
        logger.info(
            f"Schema updated successfully with {len(updated_field_paths)} fields"
        )

        # Verify Slack notification
        logger.info("Verifying Slack notification for removed columns")
        verify_slack_notification(
            slack_token, slack_channel, "has removed column(s)", timestamp_seconds
        )

    logger.info("Remove columns test complete")


@pytest.mark.release_tests
def test_rename_columns_with_slack_notification(auth_session):
    """
    Test that renaming columns in a dataset schema triggers a Slack notification.

    Steps:
    1. Backup current schema (for cleanup/idempotency)
    2. Get current schema and rename all fields to new random names
    3. Verify Slack notification shows "has renamed column(s)"
    4. Cleanup: Restore original schema
    """
    slack_channel = get_release_test_notification_channel()
    slack_token = get_release_test_notification_token()
    if not slack_channel or not slack_token:
        raise ValueError(
            "RELEASE_TEST_NOTIFICATION_CHANNEL or RELEASE_TEST_NOTIFICATION_TOKEN not set"
        )

    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting rename columns test at timestamp: {timestamp_seconds}")

    with backup_and_restore_schema(auth_session, TEST_DATASET_URN):
        # Load baseline fields from JSON file
        baseline_fields = load_baseline_schema()
        logger.info(f"Baseline schema has {len(baseline_fields)} fields")

        # Generate random field names to simulate rename
        random_suffix = uuid.uuid4().hex[:8]
        renamed_fields = []
        for i, field in enumerate(baseline_fields):
            renamed_field = field.copy()
            renamed_field["fieldPath"] = f"renamed_field_{i}_{random_suffix}"
            renamed_fields.append(renamed_field)
        logger.info(f"Renaming all {len(renamed_fields)} fields to random names")

        # Ingest renamed schema
        ingest_schema_with_fields(auth_session, TEST_DATASET_URN, renamed_fields)
        wait_for_writes_to_sync()

        # Verify schema was updated
        updated_schema = get_dataset_schema(auth_session, TEST_DATASET_URN)
        assert updated_schema is not None, "Expected updated schema to be present"
        updated_field_paths = [f["fieldPath"] for f in updated_schema.get("fields", [])]
        assert any("renamed_field_" in path for path in updated_field_paths), (
            "Expected renamed fields to be present"
        )
        logger.info(
            f"Schema updated successfully with {len(updated_field_paths)} renamed fields"
        )

        # Verify Slack notification
        logger.info("Verifying Slack notification for renamed columns")
        verify_slack_notification(
            slack_token, slack_channel, "has renamed column(s)", timestamp_seconds
        )

    logger.info("Rename columns test complete")
