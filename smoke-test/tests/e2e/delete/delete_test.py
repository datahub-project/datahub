import json
import logging
import os

import pytest

from datahub.cli.cli_utils import get_aspects_for_entity
from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AccessClass,
    RoleAssociationClass,
    RolePropertiesClass,
)
from tests.e2e.utils import (
    delete_urn,
    delete_urns_from_file,
    ingest_file_via_rest,
    materialize_with_unique_name,
    unique_dataset_urn,
    unique_suffix,
    wait_for_writes_to_sync,
    with_test_retry,
)
from utilities.domains import Domain

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
        "tests/e2e/delete/cli_test_data.json", "test-delete", tmp_path
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


@pytest.mark.p0
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


@with_test_retry()
def _assert_role_referenced(graph_client, role_urn: str) -> None:
    references_count, _ = graph_client.delete_references_to_urn(role_urn, dry_run=True)
    assert references_count > 0


@with_test_retry()
def _assert_role_detached(
    graph_client, dataset_urn: str, role_urn: str, kept_role_urn: str
) -> None:
    access = graph_client.get_aspect(dataset_urn, AccessClass)
    assert access is not None
    assert [role.urn for role in access.roles or []] == [kept_role_urn]
    references_count, _ = graph_client.delete_references_to_urn(role_urn, dry_run=True)
    assert references_count == 0


def test_hard_delete_role_detaches_access(graph_client):
    suffix = unique_suffix()
    role_urn = f"urn:li:role:deleted-role-{suffix}"
    kept_role_urn = f"urn:li:role:kept-role-{suffix}"
    dataset_urn = unique_dataset_urn("role-detach")
    try:
        graph_client.emit_mcps(
            [
                MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=RolePropertiesClass(name=urn, type="READ"),
                )
                for urn in (role_urn, kept_role_urn)
            ]
            + [
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=AccessClass(
                        roles=[
                            RoleAssociationClass(urn=role_urn),
                            RoleAssociationClass(urn=kept_role_urn),
                        ]
                    ),
                )
            ]
        )
        wait_for_writes_to_sync()
        _assert_role_referenced(graph_client, role_urn)

        graph_client.hard_delete_entity(role_urn)
        wait_for_writes_to_sync()

        _assert_role_detached(graph_client, dataset_urn, role_urn, kept_role_urn)
    finally:
        for urn in (dataset_urn, role_urn, kept_role_urn):
            try:
                delete_urn(graph_client, urn)
            except Exception:
                logger.warning("cleanup failed for %s", urn, exc_info=True)
