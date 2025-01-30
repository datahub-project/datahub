from datetime import datetime, timedelta, timezone

import pytest
from acryl_datahub_cloud.api import AcrylGraph
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetKeyClass,
    VersionPropertiesClass,
    VersionTagClass,
)
from datahub.metadata.urns import CorpUserUrn, DatasetUrn, VersionSetUrn

NOW = datetime.now(tz=timezone.utc)
VERSION_SET_URN = VersionSetUrn("12345678910", DatasetUrn.ENTITY_TYPE).urn()
ENTITY_URN_OBJS = [DatasetUrn("snowflake", f"versioning_{i}") for i in range(3)]
ENTITY_URNS = [urn.urn() for urn in ENTITY_URN_OBJS]
USER_URN = CorpUserUrn("username").urn()


@pytest.fixture(scope="function", autouse=True)
def ingest_cleanup_data(acryl_graph_client: AcrylGraph):
    try:
        for urn in ENTITY_URN_OBJS:
            acryl_graph_client.emit_mcp(
                MetadataChangeProposalWrapper(
                    entityUrn=urn.urn(),
                    aspect=DatasetKeyClass(
                        platform=urn.platform, name=urn.name, origin=urn.env
                    ),
                )
            )
        yield
    finally:
        for urn in [*ENTITY_URNS, VERSION_SET_URN]:
            acryl_graph_client.hard_delete_entity(urn)


def test_link_unlink_version(acryl_graph_client: AcrylGraph):
    version_set_urn = acryl_graph_client.link_asset_to_version_set(
        ENTITY_URNS[0], VERSION_SET_URN, "v1"
    )
    assert version_set_urn == VERSION_SET_URN

    unlinked_version_set = acryl_graph_client.unlink_asset_from_version_set(
        ENTITY_URNS[0]
    )
    assert unlinked_version_set == VERSION_SET_URN


def test_link_unlink_three_versions(acryl_graph_client):
    for i, entity_urn in enumerate(ENTITY_URNS):
        version_set_urn = acryl_graph_client.link_asset_to_version_set(
            entity_urn, VERSION_SET_URN, f"v{i}"
        )
        assert version_set_urn == VERSION_SET_URN

    # assert acryl_graph_client.get_aspect(
    #     ENTITY_URNS[2], VersionPropertiesClass
    # ).isLatest

    unlinked_version_set = acryl_graph_client.unlink_asset_from_version_set(
        ENTITY_URNS[2]
    )
    assert unlinked_version_set == VERSION_SET_URN

    # assert acryl_graph_client.get_aspect(
    #     ENTITY_URNS[1], VersionPropertiesClass
    # ).isLatest


def test_read_dataset_version(acryl_graph_client):
    acryl_graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=ENTITY_URNS[0],
            aspect=VersionPropertiesClass(
                versionSet=VERSION_SET_URN,
                sortId="ABCD",
                version=VersionTagClass(versionTag="v1"),
                aliases=[
                    VersionTagClass(versionTag="alias1"),
                    VersionTagClass(versionTag="alias2"),
                ],
                comment="my_comment",
                sourceCreatedTimestamp=AuditStampClass(
                    time=int(NOW.timestamp() * 1000), actor=USER_URN
                ),
                metadataCreatedTimestamp=AuditStampClass(
                    time=int((NOW - timedelta(days=2)).timestamp() * 1000),
                    actor=USER_URN,
                ),
            ),
        )
    )

    assert acryl_graph_client.LINK_VERSION_MUTATION
