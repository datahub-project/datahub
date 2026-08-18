"""
Smoke test for getTimeline with includeVersionSet=true.

Regression test for the bug where filterNonLatestVersions=true (the default) caused the
version-set sibling search inside GetTimelineResolver to return only the latest version,
so change history across all versions was collapsed to a single entity's events.
"""

import logging

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    VersioningSchemeClass,
    VersionPropertiesClass,
    VersionSetPropertiesClass,
    VersionTagClass,
)
from datahub.metadata.urns import DatasetUrn, VersionSetUrn
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

# Stable URNs for this test — deterministic so cleanup is reliable.
_PLATFORM = "timeline_smoke"
_VERSION_SET_URN = VersionSetUrn("timeline-test-vset-001", DatasetUrn.ENTITY_TYPE)
_V1 = DatasetUrn(_PLATFORM, "timeline_test_v1_0_0")
_V2 = DatasetUrn(_PLATFORM, "timeline_test_v2_0_0")
_V3 = DatasetUrn(_PLATFORM, "timeline_test_v3_0_0")
_VERSIONS = [(_V1, "1.0.0", "01"), (_V2, "2.0.0", "02"), (_V3, "3.0.0", "03")]

_GET_TIMELINE_QUERY = """
query getTimeline($input: GetTimelineInput!) {
  getTimeline(input: $input) {
    changeTransactions {
      timestampMillis
      changes {
        category
        operation
        description
      }
    }
    skippedVersionCount
  }
}
"""


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(graph_client: DataHubGraph):
    """Emit three versioned datasets then clean up after the module."""
    logger.info("Ingesting versioned test entities for timeline smoke test")
    for _, (urn_obj, tag, sort_id) in enumerate(_VERSIONS):
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=urn_obj.urn(),
                aspect=DatasetPropertiesClass(
                    name=f"Timeline Test {tag}",
                    description=f"Version {tag} of the timeline smoke-test entity.",
                ),
            )
        )
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=urn_obj.urn(),
                aspect=VersionPropertiesClass(
                    versionSet=_VERSION_SET_URN.urn(),
                    version=VersionTagClass(versionTag=tag),
                    sortId=sort_id,
                    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
                    comment=f"Release {tag}",
                ),
            )
        )

    # Point the version set at the latest
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=_VERSION_SET_URN.urn(),
            aspect=VersionSetPropertiesClass(
                latest=_V3.urn(),
                versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
            ),
        )
    )

    wait_for_writes_to_sync(mcp_only=True)
    yield

    logger.info("Cleaning up versioned test entities")
    for urn_obj, _, _ in _VERSIONS:
        graph_client.hard_delete_entity(urn_obj.urn())
    graph_client.hard_delete_entity(_VERSION_SET_URN.urn())


def test_get_timeline_without_version_set(graph_client: DataHubGraph):
    """Baseline: single-entity timeline returns only that entity's events."""
    result = graph_client.execute_graphql(
        _GET_TIMELINE_QUERY,
        variables={
            "input": {
                "urn": _V3.urn(),
                "changeCategories": ["DOCUMENTATION"],
                "includeVersionSet": False,
            }
        },
    )
    assert "errors" not in result, f"GraphQL errors: {result.get('errors')}"

    data = result["getTimeline"]
    txns = data["changeTransactions"]
    assert len(txns) >= 1, "Expected at least one transaction for the latest version"
    assert data["skippedVersionCount"] == 0


def test_get_timeline_with_version_set_returns_all_versions(graph_client: DataHubGraph):
    """
    Core regression test: with includeVersionSet=true, getTimeline must return
    change events from every version in the set, not just the latest one.

    Before the fix, filterNonLatestVersions=true was applied to the sibling
    search, so v1 and v2 were silently excluded and only v3's events appeared.
    """
    result = graph_client.execute_graphql(
        _GET_TIMELINE_QUERY,
        variables={
            "input": {
                "urn": _V3.urn(),
                "changeCategories": ["DOCUMENTATION"],
                "includeVersionSet": True,
            }
        },
    )
    assert "errors" not in result, f"GraphQL errors: {result.get('errors')}"

    data = result["getTimeline"]
    txns = data["changeTransactions"]

    # We emitted DatasetProperties for 3 distinct versions, so we must see at
    # least 3 change transactions when the full version set is included.
    assert len(txns) >= 3, (
        f"Expected change transactions from all 3 versions, got {len(txns)}. "
        "This is the filterNonLatestVersions regression — only the latest "
        "version's events appeared."
    )

    assert data["skippedVersionCount"] == 0


def test_get_timeline_version_set_superset_of_single_entity(graph_client: DataHubGraph):
    """
    The all-versions timeline must include at least everything the single-entity
    timeline shows, plus events from the other versions.
    """
    single_result = graph_client.execute_graphql(
        _GET_TIMELINE_QUERY,
        variables={
            "input": {
                "urn": _V3.urn(),
                "changeCategories": ["DOCUMENTATION"],
                "includeVersionSet": False,
            }
        },
    )
    all_result = graph_client.execute_graphql(
        _GET_TIMELINE_QUERY,
        variables={
            "input": {
                "urn": _V3.urn(),
                "changeCategories": ["DOCUMENTATION"],
                "includeVersionSet": True,
            }
        },
    )

    single_count = len(single_result["getTimeline"]["changeTransactions"])
    all_count = len(all_result["getTimeline"]["changeTransactions"])

    assert all_count >= single_count, (
        "All-versions timeline should have at least as many transactions as the single-entity view"
    )
    assert all_count > single_count, (
        "All-versions timeline should have MORE transactions than the single-entity view "
        f"(got {all_count} vs {single_count})"
    )
