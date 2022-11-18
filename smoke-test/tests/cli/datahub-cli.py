import json
import pytest
from time import sleep
from datahub.cli.cli_utils import guess_entity_type, post_entity, get_aspects_for_entity
from datahub.cli.ingest_cli import get_session_and_host, rollback
from tests.utils import ingest_file_via_rest

ingested_dataset_run_id = ""
ingested_editable_run_id = ""


@pytest.fixture(autouse=True)
def test_setup():
    """Fixture to execute asserts before and after a test is run"""

    global ingested_dataset_run_id
    global ingested_editable_run_id

    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-rollback"

    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    session, gms_host = get_session_and_host()

    assert "browsePaths" not in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["browsePaths"], typed=False)
    assert "editableDatasetProperties" not in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["editableDatasetProperties"], typed=False)

    ingested_dataset_run_id = ingest_file_via_rest("tests/cli/cli_test_data.json").config.run_id
    print("Setup ingestion id: " + ingested_dataset_run_id)

    assert "browsePaths" in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["browsePaths"], typed=False)

    yield

    # Clean up
    rollback_url = f"{gms_host}/runs?action=rollback"

    session.post(rollback_url, data=json.dumps({"runId": ingested_editable_run_id, "dryRun": False, "hardDelete": True}))
    session.post(rollback_url, data=json.dumps({"runId": ingested_dataset_run_id, "dryRun": False, "hardDelete": True}))

    assert "browsePaths" not in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["browsePaths"], typed=False)
    assert "editableDatasetProperties" not in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["editableDatasetProperties"], typed=False)


@pytest.mark.dependency()
def test_rollback_editable():
    global ingested_dataset_run_id
    global ingested_editable_run_id
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = (
        "test-rollback"
    )
    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    session, gms_host = get_session_and_host()

    print("Ingested dataset id:", ingested_dataset_run_id)
    # Assert that second data ingestion worked
    assert "browsePaths" in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["browsePaths"], typed=False)

    # Make editable change
    ingested_editable_run_id = ingest_file_via_rest("tests/cli/cli_editable_test_data.json").config.run_id
    print("ingested editable id:", ingested_editable_run_id)
    # Assert that second data ingestion worked
    assert "editableDatasetProperties" in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["editableDatasetProperties"], typed=False)

    # rollback ingestion 1
    rollback_url = f"{gms_host}/runs?action=rollback"

    session.post(rollback_url, data=json.dumps({"runId": ingested_dataset_run_id, "dryRun": False, "hardDelete": False}))

    # Allow async MCP processor to handle ingestions & rollbacks
    sleep(5)

    # EditableDatasetProperties should still be part of the entity that was soft deleted.
    assert "editableDatasetProperties" in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["editableDatasetProperties"], typed=False)
    # But first ingestion aspects should not be present
    assert "browsePaths" not in get_aspects_for_entity(entity_urn=dataset_urn, typed=False)
