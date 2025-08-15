import json

import pytest

from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    EditableDatasetPropertiesClass,
)
from tests.utils import ingest_file_via_rest, wait_for_writes_to_sync

ingested_dataset_run_id = ""
ingested_editable_run_id = ""


@pytest.fixture(autouse=True)
def test_setup(auth_session, graph_client):
    """Fixture to execute asserts before and after a test is run"""

    global ingested_dataset_run_id
    global ingested_editable_run_id

    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-rollback"

    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    gms_host = graph_client.config.server

    assert graph_client.get_aspect(dataset_urn, BrowsePathsV2Class) is None
    assert graph_client.get_aspect(dataset_urn, EditableDatasetPropertiesClass) is None

    ingested_dataset_run_id = ingest_file_via_rest(
        auth_session, "tests/cli/cli_test_data.json"
    ).config.run_id
    print("Setup ingestion id: " + ingested_dataset_run_id)

    assert graph_client.get_aspect(dataset_urn, BrowsePathsV2Class) is not None

    yield

    # Clean up
    rollback_url = f"{gms_host}/runs?action=rollback"

    auth_session.post(
        rollback_url,
        data=json.dumps(
            {"runId": ingested_editable_run_id, "dryRun": False, "hardDelete": True}
        ),
    )
    auth_session.post(
        rollback_url,
        data=json.dumps(
            {"runId": ingested_dataset_run_id, "dryRun": False, "hardDelete": True}
        ),
    )

    assert graph_client.get_aspect(dataset_urn, BrowsePathsV2Class) is None
    assert graph_client.get_aspect(dataset_urn, EditableDatasetPropertiesClass) is None


def test_rollback_editable(auth_session, graph_client):
    global ingested_dataset_run_id
    global ingested_editable_run_id
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-rollback"
    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    gms_host = graph_client.config.server

    print("Ingested dataset id:", ingested_dataset_run_id)
    # Assert that second data ingestion worked

    assert graph_client.get_aspect(dataset_urn, BrowsePathsV2Class) is not None

    # Make editable change
    ingested_editable_run_id = ingest_file_via_rest(
        auth_session, "tests/cli/cli_editable_test_data.json"
    ).config.run_id
    print("ingested editable id:", ingested_editable_run_id)
    # Assert that second data ingestion worked

    assert (
        graph_client.get_aspect(dataset_urn, EditableDatasetPropertiesClass) is not None
    )

    # rollback ingestion 1
    rollback_url = f"{gms_host}/runs?action=rollback"

    auth_session.post(
        rollback_url,
        data=json.dumps(
            {"runId": ingested_dataset_run_id, "dryRun": False, "hardDelete": False}
        ),
    )

    # Allow async MCP processor to handle ingestions & rollbacks
    wait_for_writes_to_sync()

    # EditableDatasetProperties should still be part of the entity that was soft deleted.
    assert (
        graph_client.get_aspect(dataset_urn, EditableDatasetPropertiesClass) is not None
    )

    # But first ingestion aspects should not be present
    assert graph_client.get_aspect(dataset_urn, BrowsePathsV2Class) is None
