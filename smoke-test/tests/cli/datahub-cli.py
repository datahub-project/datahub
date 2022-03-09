import json
import pytest
from time import sleep
from datahub.cli import delete_cli, ingest_cli
from datahub.cli.cli_utils import guess_entity_type, post_entity, get_aspects_for_entity
from datahub.cli.ingest_cli import get_session_and_host
from datahub.cli.delete_cli import guess_entity_type, delete_one_urn_cmd
from tests.utils import ingest_file_via_rest, delete_urns_from_file

def delete_by_urn(urn, session, host):
    entity_type = guess_entity_type(urn=urn)
    delete_one_urn_cmd(
        urn,
        soft=False,
        dry_run=False,
        entity_type=entity_type,
        cached_session_host=(session, host),
    )
    sleep(5)
    print()

@pytest.mark.dependency()
def test_rollback_editable():
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = (
        "test-rollback"
    )
    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    session, gms_host = get_session_and_host()

    # Clean slate.
    delete_by_urn(dataset_urn, session, gms_host)

    assert "browsePaths" not in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["browsePaths"], typed=False)
    assert "editableDatasetProperties" not in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["editableDatasetProperties"], typed=False)

    # Ingest dataset
    ingested_dataset_run_id = ingest_file_via_rest("tests/cli/cli_test_data.json").config.run_id
    print("Ingested dataset id:", ingested_dataset_run_id)
    # Assert that second data ingestion worked
    assert "browsePaths" in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["browsePaths"], typed=False)

    # Sleep forces ingestion of files to have distinct run-ids.
    sleep(1)

    # Make editable change
    ingested_editable_run_id = ingest_file_via_rest("tests/cli/cli_editable_test_data.json").config.run_id
    print("ingested editable id:", ingested_editable_run_id)
    # Assert that second data ingestion worked
    assert "editableDatasetProperties" in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["editableDatasetProperties"], typed=False)

    # rollback ingestion 1
    rollback_url = f"{gms_host}/runs?action=rollback"

    session.post(rollback_url, data=json.dumps({"runId": ingested_dataset_run_id, "dryRun": False, "hardDelete": False}))

    # Allow async MCP processor to handle ingestions & rollbacks
    sleep(10)

    # EditableDatasetProperties should still be part of the entity that was soft deleted.
    assert "editableDatasetProperties" in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["editableDatasetProperties"], typed=False)
    # But first ingestion aspects should not be present
    assert "browsePaths" not in get_aspects_for_entity(entity_urn=dataset_urn, aspects=["browsePaths"], typed=False)
    pass