import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DatasetKeyClass,
    VersionPropertiesClass,
    VersionSetPropertiesClass,
)
from datahub.metadata.urns import DatasetUrn, VersionSetUrn
from tests.consistency_utils import wait_for_writes_to_sync

VERSION_SET_URN = VersionSetUrn("12345678910", DatasetUrn.ENTITY_TYPE).urn()
ENTITY_URN_OBJS = [DatasetUrn("snowflake", f"versioning_{i}") for i in range(3)]
ENTITY_URNS = [urn.urn() for urn in ENTITY_URN_OBJS]


@pytest.fixture(scope="function", autouse=True)
def ingest_cleanup_data(graph_client: DataHubGraph):
    try:
        for urn in ENTITY_URN_OBJS:
            graph_client.emit_mcp(
                MetadataChangeProposalWrapper(
                    entityUrn=urn.urn(),
                    aspect=DatasetKeyClass(
                        platform=urn.platform, name=urn.name, origin=urn.env
                    ),
                )
            )
        for i in [2, 1, 0, 0, 1, 2]:
            graph_client.unlink_asset_from_version_set(ENTITY_URNS[i])
        yield
    finally:
        for i in [2, 1, 0, 0, 1, 2]:
            graph_client.unlink_asset_from_version_set(ENTITY_URNS[i])
        wait_for_writes_to_sync()


@pytest.mark.skip("Flaky: remove after unlink fixed")
def test_link_unlink_version(graph_client: DataHubGraph):
    assert graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass) is None
    version_set_urn = graph_client.link_asset_to_version_set(
        ENTITY_URNS[0], VERSION_SET_URN, "v0"
    )
    assert version_set_urn == VERSION_SET_URN
    # We should not need to wait here, but we do
    wait_for_writes_to_sync()
    unlinked_version_set = graph_client.unlink_asset_from_version_set(ENTITY_URNS[0])
    assert unlinked_version_set == VERSION_SET_URN
    assert graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass) is None


@pytest.mark.skip("Flaky: remove after unlink fixed")
def test_link_unlink_three_versions(graph_client):
    assert graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass) is None
    for i, entity_urn in enumerate(ENTITY_URNS):
        version_set_urn = graph_client.link_asset_to_version_set(
            entity_urn, VERSION_SET_URN, f"v{i}"
        )
        assert version_set_urn == VERSION_SET_URN
    # We should not need to wait here, but we do
    wait_for_writes_to_sync()
    assert (
        graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass).latest
        == ENTITY_URNS[2]
    )
    assert graph_client.get_aspect(ENTITY_URNS[2], VersionPropertiesClass).isLatest

    unlinked_version_set = graph_client.unlink_asset_from_version_set(ENTITY_URNS[2])
    assert unlinked_version_set == VERSION_SET_URN

    assert (
        graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass).latest
        == ENTITY_URNS[1]
    )
    assert graph_client.get_aspect(ENTITY_URNS[1], VersionPropertiesClass).isLatest


@pytest.mark.skip("Flaky: remove after unlink fixed")
def test_link_unlink_three_versions_unlink_all(graph_client):
    assert graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass) is None
    for i, entity_urn in enumerate(ENTITY_URNS):
        version_set_urn = graph_client.link_asset_to_version_set(
            entity_urn, VERSION_SET_URN, f"v{i}"
        )
        assert version_set_urn == VERSION_SET_URN

    # We should not need to wait here, but we do
    wait_for_writes_to_sync()
    assert (
        graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass).latest
        == ENTITY_URNS[2]
    )
    assert graph_client.get_aspect(ENTITY_URNS[2], VersionPropertiesClass).isLatest

    unlinked_version_set = graph_client.unlink_asset_from_version_set(ENTITY_URNS[2])
    # We should not need to wait here, but we do
    wait_for_writes_to_sync()
    assert unlinked_version_set == VERSION_SET_URN

    assert (
        graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass).latest
        == ENTITY_URNS[1]
    )
    assert graph_client.get_aspect(ENTITY_URNS[1], VersionPropertiesClass).isLatest

    unlinked_version_set = graph_client.unlink_asset_from_version_set(ENTITY_URNS[1])
    # We should not need to wait here, but we do
    wait_for_writes_to_sync()

    assert unlinked_version_set == VERSION_SET_URN

    assert (
        graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass).latest
        == ENTITY_URNS[0]
    )
    assert graph_client.get_aspect(ENTITY_URNS[0], VersionPropertiesClass).isLatest

    unlinked_version_set = graph_client.unlink_asset_from_version_set(ENTITY_URNS[0])

    assert unlinked_version_set == VERSION_SET_URN

    assert graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass) is None


@pytest.mark.skip("Flaky: remove after unlink fixed")
def test_link_unlink_three_versions_unlink_middle_and_latest(graph_client):
    assert graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass) is None
    for i, entity_urn in enumerate(ENTITY_URNS):
        version_set_urn = graph_client.link_asset_to_version_set(
            entity_urn, VERSION_SET_URN, f"v{i}"
        )
        assert version_set_urn == VERSION_SET_URN
    # We should not need to wait here, but we do
    wait_for_writes_to_sync()
    assert (
        graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass).latest
        == ENTITY_URNS[2]
    )
    assert graph_client.get_aspect(ENTITY_URNS[2], VersionPropertiesClass).isLatest

    unlinked_version_set = graph_client.unlink_asset_from_version_set(ENTITY_URNS[1])
    # We should not need to wait here, but we do
    wait_for_writes_to_sync()
    assert unlinked_version_set == VERSION_SET_URN

    assert (
        graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass).latest
        == ENTITY_URNS[2]
    )
    assert graph_client.get_aspect(ENTITY_URNS[2], VersionPropertiesClass).isLatest

    unlinked_version_set = graph_client.unlink_asset_from_version_set(ENTITY_URNS[2])

    assert unlinked_version_set == VERSION_SET_URN

    assert (
        graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass).latest
        == ENTITY_URNS[0]
    )
    assert graph_client.get_aspect(ENTITY_URNS[0], VersionPropertiesClass).isLatest


@pytest.mark.skip("Flaky: remove after unlink fixed")
def test_link_unlink_three_versions_unlink_and_relink(graph_client):
    assert graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass) is None
    for i, entity_urn in enumerate(ENTITY_URNS):
        version_set_urn = graph_client.link_asset_to_version_set(
            entity_urn, VERSION_SET_URN, f"v{i}"
        )
        assert version_set_urn == VERSION_SET_URN
    # We should not need to wait here, but we do
    wait_for_writes_to_sync()
    assert (
        graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass).latest
        == ENTITY_URNS[2]
    )
    assert graph_client.get_aspect(ENTITY_URNS[2], VersionPropertiesClass).isLatest

    unlinked_version_set = graph_client.unlink_asset_from_version_set(ENTITY_URNS[2])
    # We should not need to wait here, but we do
    wait_for_writes_to_sync()
    assert unlinked_version_set == VERSION_SET_URN

    assert (
        graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass).latest
        == ENTITY_URNS[1]
    )
    assert graph_client.get_aspect(ENTITY_URNS[1], VersionPropertiesClass).isLatest

    unlinked_version_set = graph_client.unlink_asset_from_version_set(ENTITY_URNS[1])
    # We should not need to wait here, but we do
    wait_for_writes_to_sync()

    assert unlinked_version_set == VERSION_SET_URN

    assert (
        graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass).latest
        == ENTITY_URNS[0]
    )
    assert graph_client.get_aspect(ENTITY_URNS[0], VersionPropertiesClass).isLatest

    unlinked_version_set = graph_client.unlink_asset_from_version_set(ENTITY_URNS[0])

    assert unlinked_version_set == VERSION_SET_URN

    assert graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass) is None

    for i, entity_urn in enumerate(ENTITY_URNS):
        version_set_urn = graph_client.link_asset_to_version_set(
            entity_urn, VERSION_SET_URN, f"v{i}"
        )
        assert version_set_urn == VERSION_SET_URN

    assert (
        graph_client.get_aspect(VERSION_SET_URN, VersionSetPropertiesClass).latest
        == ENTITY_URNS[2]
    )
    assert graph_client.get_aspect(ENTITY_URNS[2], VersionPropertiesClass).isLatest
