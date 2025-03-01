import time

import pytest
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import *
from datahub.metadata.urns import DatasetUrn, VersionSetUrn

OLD_LATEST_URN = DatasetUrn("v", f"vol")
ENTITY_URN = DatasetUrn("v", f"vi")
EXISTS_VERSION_SET_URN = VersionSetUrn("exists", DatasetUrn.ENTITY_TYPE)
NOT_EXISTS_VERSION_SET_URN = VersionSetUrn("not-exists", DatasetUrn.ENTITY_TYPE)


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
        pass
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
    assert version_set_properties.latest == ENTITY_URN.urn()
    assert (
        version_set_properties.versioningScheme
        == VersioningSchemeClass.LEXICOGRAPHIC_STRING
    )

    assert graph_client.get_aspect(ENTITY_URN.urn(), VersionPropertiesClass).isLatest


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
    assert graph_client.get_aspect(ENTITY_URN.urn(), VersionPropertiesClass).isLatest
    version_set_properties = graph_client.get_aspect(
        NOT_EXISTS_VERSION_SET_URN.urn(), VersionSetPropertiesClass
    )
    assert version_set_properties.latest == ENTITY_URN.urn()
    assert (
        version_set_properties.versioningScheme
        == VersioningSchemeClass.ALPHANUMERIC_GENERATED_BY_DATAHUB
    )


def test_ingest_version_properties_version_set_new_latest(graph_client: DataHubGraph):
    assert graph_client.get_aspect(
        OLD_LATEST_URN.urn(), VersionPropertiesClass
    ).isLatest
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
    assert not graph_client.get_aspect(
        OLD_LATEST_URN.urn(), VersionPropertiesClass
    ).isLatest
    version_set_properties = graph_client.get_aspect(
        EXISTS_VERSION_SET_URN.urn(), VersionSetPropertiesClass
    )
    assert version_set_properties.latest == ENTITY_URN.urn()
    assert graph_client.get_aspect(ENTITY_URN.urn(), VersionPropertiesClass).isLatest


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
    assert not graph_client.get_aspect(
        ENTITY_URN.urn(), VersionPropertiesClass
    ).isLatest
    assert graph_client.get_aspect(
        OLD_LATEST_URN.urn(), VersionPropertiesClass
    ).isLatest
    version_set_properties = graph_client.get_aspect(
        EXISTS_VERSION_SET_URN.urn(), VersionSetPropertiesClass
    )
    assert version_set_properties.latest == OLD_LATEST_URN.urn()


def test_ingest_many_versions(graph_client: DataHubGraph):
    for i in range(20):
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=DatasetUrn("v", f"entity_{i}").urn(),
                aspect=VersionPropertiesClass(
                    versionSet=NOT_EXISTS_VERSION_SET_URN.urn(),
                    version=VersionTagClass(versionTag=f"v{i}"),
                    sortId=str(i).zfill(2),
                    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
                ),
            )
        )
    expected_latest_urn = DatasetUrn("v", f"entity_{i}")
    assert graph_client.get_aspect(
        expected_latest_urn.urn(), VersionPropertiesClass
    ).isLatest
    version_set_properties = graph_client.get_aspect(
        NOT_EXISTS_VERSION_SET_URN.urn(), VersionSetPropertiesClass
    )
    assert version_set_properties.latest == expected_latest_urn.urn()
    assert (
        version_set_properties.versioningScheme
        == VersioningSchemeClass.LEXICOGRAPHIC_STRING
    )
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
        {"entity": {"urn": DatasetUrn("v", f"entity_{i}").urn()}}
        for i in reversed(range(20))
    ]
