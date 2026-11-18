"""
Smoke tests for GlossaryTerm versioning (KPI versioning scenario).

Demonstrates that GlossaryTerm entities support the same VersionSet / VersionProperties
mechanism as Datasets:
  - Two glossary terms represent v1 and v2 of a KPI definition
  - Ingesting VersionProperties links them to a shared VersionSet
  - The side-effect layer promotes the later version to isLatest=True
  - Both URNs remain independently resolvable (point-in-time retrieval)
"""

import logging
import os

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    GlossaryTermInfoClass,
    VersioningSchemeClass,
    VersionPropertiesClass,
    VersionSetPropertiesClass,
    VersionTagClass,
)
from datahub.metadata.urns import VersionSetUrn
from tests.e2e.utils import with_test_retry
from utilities.consistency_utils import wait_for_writes_to_sync
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

# URNs for the KPI versioning scenario
GLOSSARY_TERM_ENTITY_TYPE = "glossaryTerm"
VERSION_SET_URN = VersionSetUrn("kpi-revenue", GLOSSARY_TERM_ENTITY_TYPE).urn()
TERM_V1_URN = "urn:li:glossaryTerm:SmokeTestKpiRevenue"
TERM_V2_URN = "urn:li:glossaryTerm:SmokeTestKpiRevenue_v2"

_V1_DEFINITION = "Revenue = sum of all closed-won deals in the period"
_V2_DEFINITION = "Revenue = sum of all closed-won deals excluding internal accounts"


@pytest.fixture(scope="function", autouse=True)
def ingest_cleanup_data(graph_client: DataHubGraph):
    """Create v1 glossary term before each test and clean up all created entities after.

    Set DATAHUB_SKIP_CLEANUP=true to leave entities in place after the test run
    (useful for inspecting results in the UI).
    """
    skip_cleanup = os.environ.get("DATAHUB_SKIP_CLEANUP", "").lower() in (
        "1",
        "true",
        "yes",
    )
    try:
        # Seed v1 term with initial formula definition and version properties
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=TERM_V1_URN,
                aspect=GlossaryTermInfoClass(
                    definition=_V1_DEFINITION,
                    name="KpiRevenue",
                    termSource="INTERNAL",
                ),
            )
        )
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=TERM_V1_URN,
                aspect=VersionPropertiesClass(
                    versionSet=VERSION_SET_URN,
                    version=VersionTagClass(versionTag="v1"),
                    sortId="AAAAAAAA",
                    versioningScheme=VersioningSchemeClass.ALPHANUMERIC_GENERATED_BY_DATAHUB,
                ),
            )
        )
        wait_for_writes_to_sync()
        yield
    finally:
        if skip_cleanup:
            logger.info(
                "DATAHUB_SKIP_CLEANUP=true — leaving entities in place:\n"
                "  v1: %s\n  v2: %s\n  versionSet: %s",
                TERM_V1_URN,
                TERM_V2_URN,
                VERSION_SET_URN,
            )
        else:
            graph_client.hard_delete_entity(TERM_V1_URN)
            graph_client.hard_delete_entity(TERM_V2_URN)
            graph_client.hard_delete_entity(VERSION_SET_URN)


def _emit_v2(graph_client: DataHubGraph) -> None:
    """Helper: publish v2 term with updated formula and version properties."""
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=TERM_V2_URN,
            aspect=GlossaryTermInfoClass(
                definition=_V2_DEFINITION,
                name="KpiRevenue_v2",
                termSource="INTERNAL",
            ),
        )
    )
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=TERM_V2_URN,
            aspect=VersionPropertiesClass(
                versionSet=VERSION_SET_URN,
                version=VersionTagClass(versionTag="v2"),
                sortId="AAAAAAAB",
                versioningScheme=VersioningSchemeClass.ALPHANUMERIC_GENERATED_BY_DATAHUB,
            ),
        )
    )
    wait_for_writes_to_sync()


@pytest.mark.p0
def test_glossary_term_v1_is_initial_latest(graph_client: DataHubGraph):
    """After ingesting v1, it should be the latest in the version set."""
    version_props = graph_client.get_aspect(TERM_V1_URN, VersionPropertiesClass)
    assert version_props is not None, "v1 should have VersionProperties"
    assert version_props.isLatest, (
        "v1 should be isLatest=True when it is the only version"
    )
    assert version_props.versionSet == VERSION_SET_URN

    version_set_props = graph_client.get_aspect(
        VERSION_SET_URN, VersionSetPropertiesClass
    )
    assert version_set_props is not None, (
        "VersionSet should have been created automatically"
    )
    assert version_set_props.latest == TERM_V1_URN


def test_glossary_term_point_in_time_retrieval(graph_client: DataHubGraph):
    """v1 GlossaryTermInfo should remain independently accessible after adding v2."""
    _emit_v2(graph_client)

    # v1 definition is unchanged (point-in-time retrieval)
    v1_info = graph_client.get_aspect(TERM_V1_URN, GlossaryTermInfoClass)
    assert v1_info is not None
    assert "internal accounts" not in v1_info.definition, (
        "v1 definition should not include v2 changes"
    )
    assert "closed-won deals in the period" in v1_info.definition

    # v2 has the new formula
    v2_info = graph_client.get_aspect(TERM_V2_URN, GlossaryTermInfoClass)
    assert v2_info is not None
    assert "internal accounts" in v2_info.definition


def test_glossary_term_version_promotion(graph_client: DataHubGraph):
    """Adding v2 should make it isLatest and demote v1."""
    _emit_v2(graph_client)

    v1_props = graph_client.get_aspect(TERM_V1_URN, VersionPropertiesClass)
    assert v1_props is not None
    assert not v1_props.isLatest, (
        "v1 should be demoted to isLatest=False after v2 is added"
    )

    v2_props = graph_client.get_aspect(TERM_V2_URN, VersionPropertiesClass)
    assert v2_props is not None
    assert v2_props.isLatest, "v2 should be isLatest=True"
    assert v2_props.versionSet == VERSION_SET_URN

    version_set_props = graph_client.get_aspect(
        VERSION_SET_URN, VersionSetPropertiesClass
    )
    assert version_set_props is not None
    assert version_set_props.latest == TERM_V2_URN, (
        "VersionSet.latest should point to v2"
    )


def test_glossary_term_version_history_via_graphql(graph_client: DataHubGraph):
    """Both versions should be discoverable through the VersionSet versionsSearch query."""
    _emit_v2(graph_client)

    # Search indexing can lag the writes above, so poll rather than assert once.
    @with_test_retry(max_attempts=12)
    def _assert_both_versions_found():
        result = graph_client.execute_graphql(
            """
            query getVersionHistory($urn: String!) {
              versionSet(urn: $urn) {
                versionsSearch(input: {query: "*", count: 10, searchFlags: {skipCache: true}}) {
                  searchResults {
                    entity {
                      urn
                    }
                  }
                }
              }
            }
            """,
            variables={"urn": VERSION_SET_URN},
        )
        search_results = result["versionSet"]["versionsSearch"]["searchResults"]
        found_urns = {r["entity"]["urn"] for r in search_results}
        assert TERM_V1_URN in found_urns, "v1 should appear in version history"
        assert TERM_V2_URN in found_urns, "v2 should appear in version history"

    _assert_both_versions_found()
