import os
import pytest
from time import sleep
from datahub.cli.cli_utils import get_aspects_for_entity
from datahub.cli.ingest_cli import get_session_and_host
from datahub.cli.delete_cli import delete_references, delete_with_filters
from tests.utils import (
    delete_urns_from_file,
    ingest_file_via_rest,
    wait_for_healthcheck_util,
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


@pytest.fixture(scope="module", autouse=False)
def test_setup():
    """Fixture to execute asserts before and after a test is run"""

    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-delete"

    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    assert "browsePaths" not in get_aspects_for_entity(
        entity_urn=dataset_urn, aspects=["browsePaths"], typed=False
    )
    assert "editableDatasetProperties" not in get_aspects_for_entity(
        entity_urn=dataset_urn, aspects=["editableDatasetProperties"], typed=False
    )

    ingest_file_via_rest("tests/delete/cli_test_data.json")
    ingest_file_via_rest("tests/containers/data.json")

    assert "browsePaths" in get_aspects_for_entity(
        entity_urn=dataset_urn, aspects=["browsePaths"], typed=False
    )

    yield
    print("removing delete test data")
    delete_urns_from_file("tests/delete/cli_test_data.json")
    delete_urns_from_file("tests/containers/data.json")

    sleep(3)

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

    session, gms_host = get_session_and_host()

    # Validate that the ingested tag is being referenced by the dataset
    references_count, related_aspects = delete_references(
        tag_urn, dry_run=True, cached_session_host=(session, gms_host)
    )
    print("reference count: " + str(references_count))
    print(related_aspects)
    assert references_count == 1
    assert related_aspects[0]["entity"] == dataset_urn

    # Delete references to the tag
    delete_references(tag_urn, dry_run=False, cached_session_host=(session, gms_host))

    sleep(3)

    # Validate that references no longer exist
    references_count, related_aspects = delete_references(
        tag_urn, dry_run=True, cached_session_host=(session, gms_host)
    )
    assert references_count == 0


@pytest.mark.dependency()
def test_delete_by_container(test_setup, depends=["test_healthchecks"]):
    container = "urn:li:container:SCHEMA"

    result = delete_with_filters(
        dry_run=True,
        soft=True,
        force=False,
        include_removed=False,
        container=container,
    )
    assert result.num_entities == 1


@pytest.mark.dependency()
def test_delete_tags_remove_references(test_setup, depends=["test_healthchecks"]):
    tag_to_delete = "urn:li:tag:NeedsOwners"
    session, gms_host = get_session_and_host()

    references_count, _ = delete_references(
        tag_to_delete, dry_run=True, cached_session_host=(session, gms_host)
    )
    assert references_count == 1

    # Hard delete tag and delete references to the tag
    result = delete_with_filters(
        dry_run=False,
        soft=True,
        force=True,
        include_removed=False,
        search_query="Owners",
        entity_type="tag",
        remove_references=True,
    )
    assert result.num_entities == 1

    sleep(3)

    # Validate that references no longer exist for soft deleted tags
    references_count, _ = delete_references(
        tag_to_delete, dry_run=True, cached_session_host=(session, gms_host)
    )
    assert references_count == 0


@pytest.mark.dependency()
def test_delete_tags_keep_references(test_setup, depends=["test_healthchecks"]):

    tag_to_delete = "urn:li:tag:NeedsReview"

    session, gms_host = get_session_and_host()

    references_count, _ = delete_references(
        tag_to_delete, dry_run=True, cached_session_host=(session, gms_host)
    )
    assert references_count == 1

    # Soft delete tag but keep references to the tag
    result = delete_with_filters(
        dry_run=False,
        soft=True,
        force=True,
        include_removed=False,
        entity_type="tag",
        search_query="Review",
        remove_references=False,
    )
    assert result.num_entities == 1

    sleep(3)

    # Validate that references still exist for soft deleted tags
    references_count, _ = delete_references(
        tag_to_delete, dry_run=True, cached_session_host=(session, gms_host)
    )
    assert references_count == 1
