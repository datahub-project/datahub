import json
import os

import pytest

from datahub.cli.cli_utils import get_aspects_for_entity
from tests.utils import (
    delete_urns_from_file,
    ingest_file_via_rest,
    wait_for_writes_to_sync,
)

# Disable telemetry
os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"


@pytest.fixture(autouse=False)
def test_setup(auth_session, graph_client):
    """Fixture to execute asserts before and after a test is run"""

    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-delete"

    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    session = graph_client._session
    gms_host = graph_client.config.server

    try:
        assert "institutionalMemory" not in get_aspects_for_entity(
            session,
            gms_host,
            entity_urn=dataset_urn,
            aspects=["institutionalMemory"],
            typed=False,
        )
        assert "editableDatasetProperties" not in get_aspects_for_entity(
            session,
            gms_host,
            entity_urn=dataset_urn,
            aspects=["editableDatasetProperties"],
            typed=False,
        )
    except Exception as e:
        delete_urns_from_file(graph_client, "tests/delete/cli_test_data.json")
        raise e

    ingested_dataset_run_id = ingest_file_via_rest(
        auth_session, "tests/delete/cli_test_data.json"
    ).config.run_id

    assert "institutionalMemory" in get_aspects_for_entity(
        session,
        gms_host,
        entity_urn=dataset_urn,
        aspects=["institutionalMemory"],
        typed=False,
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

    assert "institutionalMemory" not in get_aspects_for_entity(
        session,
        gms_host,
        entity_urn=dataset_urn,
        aspects=["institutionalMemory"],
        typed=False,
    )
    assert "editableDatasetProperties" not in get_aspects_for_entity(
        session,
        gms_host,
        entity_urn=dataset_urn,
        aspects=["editableDatasetProperties"],
        typed=False,
    )


@pytest.mark.dependency()
def test_delete_reference(graph_client, test_setup):
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-delete"

    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"
    tag_urn = "urn:li:tag:NeedsDocs"

    # Validate that the ingested tag is being referenced by the dataset
    references_count, related_aspects = graph_client.delete_references_to_urn(
        tag_urn, dry_run=True
    )
    print("reference count: " + str(references_count))
    print(related_aspects)
    assert references_count == 1
    assert related_aspects[0]["entity"] == dataset_urn

    # Delete references to the tag
    graph_client.delete_references_to_urn(tag_urn, dry_run=False)

    wait_for_writes_to_sync()

    # Validate that references no longer exist
    references_count, related_aspects = graph_client.delete_references_to_urn(
        tag_urn, dry_run=True
    )
    assert references_count == 0
