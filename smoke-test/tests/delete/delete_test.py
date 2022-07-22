import os
import json
import pytest
from time import sleep
from datahub.cli.cli_utils import get_aspects_for_entity
from datahub.cli.ingest_cli import get_session_and_host
from datahub.cli.delete_cli import delete_references
from tests.utils import ingest_file_via_rest, wait_for_healthcheck_util

# Disable telemetry
os.putenv("DATAHUB_TELEMETRY_ENABLED", "false")


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    wait_for_healthcheck_util()
    yield


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


@pytest.fixture(autouse=True)
def test_setup():
    """Fixture to execute asserts before and after a test is run"""

    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-delete"

    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    session, gms_host = get_session_and_host()

    assert "browsePaths" not in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["browsePaths"], typed=False)
    assert "editableDatasetProperties" not in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["editableDatasetProperties"], typed=False)

    ingested_dataset_run_id = ingest_file_via_rest("tests/delete/cli_test_data.json").config.run_id

    sleep(3)

    assert "browsePaths" in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["browsePaths"], typed=False)

    yield
    rollback_url = f"{gms_host}/runs?action=rollback"
    session.post(rollback_url, data=json.dumps({"runId": ingested_dataset_run_id, "dryRun": False, "hardDelete": True, "safe": False}))

    sleep(3)

    assert "browsePaths" not in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["browsePaths"], typed=False)
    assert "editableDatasetProperties" not in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["editableDatasetProperties"], typed=False)


@pytest.mark.dependency()
def test_delete_reference(depends=["test_healthchecks"]):
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-delete"

    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"
    tag_urn = "urn:li:tag:NeedsDocs"

    session, gms_host = get_session_and_host()

    # Validate that the ingested tag is being referenced by the dataset
    references_count, related_aspects = delete_references(tag_urn, dry_run=True, cached_session_host=(session, gms_host))
    print("reference count: " + str(references_count))
    print(related_aspects)
    assert references_count == 1
    assert related_aspects[0]['entity'] == dataset_urn

    # Delete references to the tag
    delete_references(tag_urn, dry_run=False, cached_session_host=(session, gms_host))

    sleep(3)

    # Validate that references no longer exist
    references_count, related_aspects = delete_references(tag_urn, dry_run=True, cached_session_host=(session, gms_host))
    assert references_count == 0
