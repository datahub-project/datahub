import os
import json
import pytest
from time import sleep
from datahub.cli.cli_utils import get_aspects_for_entity
from datahub.cli.ingest_cli import get_session_and_host
from tests.utils import (
    ingest_file_via_rest,
    wait_for_healthcheck_util,
    delete_urns_from_file,
    wait_for_writes_to_sync,
    get_datahub_graph,
)

# Disable telemetry
os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    wait_for_healthcheck_util()
    yield


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


@pytest.fixture(autouse=False)
def test_setup():
    """Fixture to execute asserts before and after a test is run"""

    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-delete"

    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    session, gms_host = get_session_and_host()

    try:
        assert "browsePaths" not in get_aspects_for_entity(
            entity_urn=dataset_urn, aspects=["browsePaths"], typed=False
        )
        assert "editableDatasetProperties" not in get_aspects_for_entity(
            entity_urn=dataset_urn, aspects=["editableDatasetProperties"], typed=False
        )
    except Exception as e:
        delete_urns_from_file("tests/delete/cli_test_data.json")
        raise e

    ingested_dataset_run_id = ingest_file_via_rest(
        "tests/delete/cli_test_data.json"
    ).config.run_id

    assert "browsePaths" in get_aspects_for_entity(
        entity_urn=dataset_urn, aspects=["browsePaths"], typed=False
    )

    yield
    rollback_url = f"{gms_host}/runs?action=rollback"
    session.post(
        rollback_url,
        data=json.dumps(
            {"runId": ingested_dataset_run_id, "dryRun": False, "hardDelete": True}
        ),
    )

    wait_for_writes_to_sync()

    assert "browsePaths" not in get_aspects_for_entity(
        entity_urn=dataset_urn, aspects=["browsePaths"], typed=False
    )
    assert "editableDatasetProperties" not in get_aspects_for_entity(
        entity_urn=dataset_urn, aspects=["editableDatasetProperties"], typed=False
    )


@pytest.mark.dependency()
def test_delete_reference(test_setup, depends=["test_healthchecks"]):
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-delete"

    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"
    tag_urn = "urn:li:tag:NeedsDocs"

    graph = get_datahub_graph()

    # Validate that the ingested tag is being referenced by the dataset
    references_count, related_aspects = graph.delete_references_to_urn(
        tag_urn, dry_run=True
    )
    print("reference count: " + str(references_count))
    print(related_aspects)
    assert references_count == 1
    assert related_aspects[0]["entity"] == dataset_urn

    # Delete references to the tag
    graph.delete_references_to_urn(tag_urn, dry_run=False)

    wait_for_writes_to_sync()
    
    # Validate that references no longer exist
    references_count, related_aspects = graph.delete_references_to_urn(
        tag_urn, dry_run=True
    )
    assert references_count == 0
