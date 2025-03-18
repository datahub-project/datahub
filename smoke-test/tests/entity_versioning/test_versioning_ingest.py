import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    VersioningSchemeClass,
    VersionPropertiesClass,
    VersionSetPropertiesClass,
    VersionTagClass,
)
from datahub.metadata.urns import DatasetUrn, VersionSetUrn
from tests.consistency_utils import wait_for_writes_to_sync

OLD_LATEST_URN = DatasetUrn("v", "versioning_old_latest")
ENTITY_URN = DatasetUrn("v", "versioning_entity")
EXISTS_VERSION_SET_URN = VersionSetUrn("exists", DatasetUrn.ENTITY_TYPE)
NOT_EXISTS_VERSION_SET_URN = VersionSetUrn("not-exists", DatasetUrn.ENTITY_TYPE)
BULK_URNS = [DatasetUrn("v", f"versioning_entity_{i}").urn() for i in range(5, 15)]


@pytest.fixture(scope="function", autouse=True)
def ingest_cleanup_data(graph_client: DataHubGraph):
    try:
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=OLD_LATEST_URN.urn(),
                aspect=VersionPropertiesClass(
                    versionSet=EXISTS_VERSION_SET_URN.urn(),
                    version=VersionTagClass(versionTag="first"),
                    sortId="abc",
                    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
                ),
            )
        )
        yield
    finally:
        graph_client.hard_delete_entity(EXISTS_VERSION_SET_URN.urn())
        graph_client.hard_delete_entity(NOT_EXISTS_VERSION_SET_URN.urn())
        graph_client.hard_delete_entity(ENTITY_URN.urn())
        graph_client.hard_delete_entity(OLD_LATEST_URN.urn())


def test_ingest_version_properties(graph_client: DataHubGraph):
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=ENTITY_URN.urn(),
            aspect=VersionPropertiesClass(
                versionSet=NOT_EXISTS_VERSION_SET_URN.urn(),
                version=VersionTagClass(versionTag="first"),
                sortId="abc",
                versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
            ),
        )
    )
    version_set_properties = graph_client.get_aspect(
        NOT_EXISTS_VERSION_SET_URN.urn(), VersionSetPropertiesClass
    )
    assert version_set_properties
    assert version_set_properties.latest == ENTITY_URN.urn()
    assert (
        version_set_properties.versioningScheme
        == VersioningSchemeClass.LEXICOGRAPHIC_STRING
    )

    version_properties = graph_client.get_aspect(
        ENTITY_URN.urn(), VersionPropertiesClass
    )
    assert version_properties
    assert version_properties.isLatest


def test_ingest_version_properties_alphanumeric(graph_client: DataHubGraph):
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=ENTITY_URN.urn(),
            aspect=VersionPropertiesClass(
                versionSet=NOT_EXISTS_VERSION_SET_URN.urn(),
                version=VersionTagClass(versionTag="first"),
                sortId="abc",
                versioningScheme=VersioningSchemeClass.ALPHANUMERIC_GENERATED_BY_DATAHUB,
            ),
        )
    )
    version_properties = graph_client.get_aspect(
        ENTITY_URN.urn(), VersionPropertiesClass
    )
    assert version_properties
    assert version_properties.isLatest
    version_set_properties = graph_client.get_aspect(
        NOT_EXISTS_VERSION_SET_URN.urn(), VersionSetPropertiesClass
    )
    assert version_set_properties
    assert version_set_properties.latest == ENTITY_URN.urn()
    assert (
        version_set_properties.versioningScheme
        == VersioningSchemeClass.ALPHANUMERIC_GENERATED_BY_DATAHUB
    )


def test_ingest_version_properties_version_set_new_latest(graph_client: DataHubGraph):
    old_latest_version_properties = graph_client.get_aspect(
        OLD_LATEST_URN.urn(), VersionPropertiesClass
    )
    assert old_latest_version_properties
    assert old_latest_version_properties.isLatest

    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=ENTITY_URN.urn(),
            aspect=VersionPropertiesClass(
                versionSet=EXISTS_VERSION_SET_URN.urn(),
                version=VersionTagClass(versionTag="second"),
                sortId="bbb",
                versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
            ),
        )
    )
    old_latest_version_properties = graph_client.get_aspect(
        OLD_LATEST_URN.urn(), VersionPropertiesClass
    )
    assert old_latest_version_properties
    assert not old_latest_version_properties.isLatest
    version_set_properties = graph_client.get_aspect(
        EXISTS_VERSION_SET_URN.urn(), VersionSetPropertiesClass
    )
    assert version_set_properties
    assert version_set_properties.latest == ENTITY_URN.urn()

    new_latest_version_properties = graph_client.get_aspect(
        ENTITY_URN.urn(), VersionPropertiesClass
    )
    assert new_latest_version_properties
    assert new_latest_version_properties.isLatest


def test_ingest_version_properties_version_set_not_latest(graph_client: DataHubGraph):
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=ENTITY_URN.urn(),
            aspect=VersionPropertiesClass(
                versionSet=EXISTS_VERSION_SET_URN.urn(),
                version=VersionTagClass(versionTag="zero"),
                sortId="aaa",
                versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
            ),
        )
    )
    new_latest_version_properties = graph_client.get_aspect(
        ENTITY_URN.urn(), VersionPropertiesClass
    )
    assert new_latest_version_properties
    assert not new_latest_version_properties.isLatest
    old_latest_version_properties = graph_client.get_aspect(
        OLD_LATEST_URN.urn(), VersionPropertiesClass
    )
    assert old_latest_version_properties
    assert old_latest_version_properties.isLatest

    version_set_properties = graph_client.get_aspect(
        EXISTS_VERSION_SET_URN.urn(), VersionSetPropertiesClass
    )
    assert version_set_properties
    assert version_set_properties.latest == OLD_LATEST_URN.urn()


@pytest.fixture
def ingest_cleanup_data_bulk(graph_client: DataHubGraph):
    try:
        yield
    finally:
        for urn in BULK_URNS:
            graph_client.hard_delete_entity(urn)


def test_ingest_many_versions(graph_client: DataHubGraph, ingest_cleanup_data_bulk):
    for i in range(5, 15):
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=BULK_URNS[i - 5],
                aspect=VersionPropertiesClass(
                    versionSet=NOT_EXISTS_VERSION_SET_URN.urn(),
                    version=VersionTagClass(versionTag=f"v{i}"),
                    sortId=str(i).zfill(2),
                    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
                ),
            )
        )
    expected_latest_urn = BULK_URNS[-1]
    expected_latest_version_properties = graph_client.get_aspect(
        expected_latest_urn, VersionPropertiesClass
    )
    assert expected_latest_version_properties
    assert expected_latest_version_properties.isLatest

    version_set_properties = graph_client.get_aspect(
        NOT_EXISTS_VERSION_SET_URN.urn(), VersionSetPropertiesClass
    )
    assert version_set_properties
    assert version_set_properties.latest == expected_latest_urn
    assert (
        version_set_properties.versioningScheme
        == VersioningSchemeClass.LEXICOGRAPHIC_STRING
    )
    wait_for_writes_to_sync()
    result = graph_client.execute_graphql(
        """
            query getVersions($urn: String!) {
              versionSet(urn: $urn) {
                versionsSearch(input: {query: "*", count: 20, searchFlags: {skipCache: true}}) {
                  searchResults {
                    entity {
                      urn
                    }
                  }
                }
              }
            }
        """,
        variables={"urn": NOT_EXISTS_VERSION_SET_URN.urn()},
    )
    assert result["versionSet"]["versionsSearch"]["searchResults"] == [
        {"entity": {"urn": urn}} for urn in list(reversed(BULK_URNS))
    ]
