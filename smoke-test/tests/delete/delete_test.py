import json
import logging
import os

import pytest

from datahub.cli.cli_utils import get_aspects_for_entity
from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn
from tests.utilities.domains import Domain
from tests.utils import (
    delete_urns_from_file,
    ingest_file_via_rest,
    materialize_with_unique_name,
    wait_for_writes_to_sync,
)

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.PLATFORM)

# Disable telemetry
os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"


@pytest.fixture(autouse=False)
def test_setup(auth_session, graph_client, tmp_path):
    """Ingest sample data with run-unique tag + dataset URNs.

    Tag uniquification is required because ``delete_references_to_urn`` is
    non-local: it strips *all* graph references to that tag URN, not just
    associations created by this module. A shared ``NeedsDocs`` tag would
    delete other workers' tag links under xdist. Dataset uniquification
    similarly avoids colliding aspect assertions on ``test-delete``.
    """

    # Uniquify dataset first, then tag, chaining through the temp file so both
    # replacements land in the ingested payload.
    data_file, dataset_name = materialize_with_unique_name(
        "tests/delete/cli_test_data.json", "test-delete", tmp_path
    )
    data_file, tag_name = materialize_with_unique_name(data_file, "NeedsDocs", tmp_path)

    dataset_urn = make_dataset_urn(platform="kafka", name=dataset_name, env="PROD")
    tag_urn = make_tag_urn(tag_name)

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
        delete_urns_from_file(graph_client, data_file)
        raise e

    ingested_dataset_run_id = ingest_file_via_rest(
        auth_session, data_file
    ).config.run_id

    assert "institutionalMemory" in get_aspects_for_entity(
        session,
        gms_host,
        entity_urn=dataset_urn,
        aspects=["institutionalMemory"],
        typed=False,
    )

    yield tag_urn, dataset_urn
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
    tag_urn, dataset_urn = test_setup

    # Validate that the ingested tag is being referenced by the dataset
    references_count, related_aspects = graph_client.delete_references_to_urn(
        tag_urn, dry_run=True
    )
    logger.info("reference count: " + str(references_count))
    logger.info(related_aspects)
    assert references_count == 1
    assert related_aspects[0]["entity"] == dataset_urn

    # Delete references to the tag
    graph_client.delete_references_to_urn(tag_urn, dry_run=False)

    wait_for_writes_to_sync(mae_only=True)

    # Validate that references no longer exist
    references_count, related_aspects = graph_client.delete_references_to_urn(
        tag_urn, dry_run=True
    )
    assert references_count == 0
