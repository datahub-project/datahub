"""Smoke tests for the scroll_relationships DataHubGraph method."""

import logging

import pytest

from conftest import _ingest_cleanup_data_impl
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RawSearchFilter
from datahub.ingestion.graph.openapi import RelationshipDirection

logger = logging.getLogger(__name__)

ALPHA = "urn:li:dataset:(urn:li:dataPlatform:scrolltest,alpha,PROD)"
ZETA = "urn:li:dataset:(urn:li:dataPlatform:scrolltest,zeta,PROD)"
MODEL_1 = "urn:li:mlModel:(urn:li:dataPlatform:scrolltest,model1,PROD)"
# MLFeature has no platform field so it is not counted in SCROLLTEST_FILTER results.
FEATURE_1 = "urn:li:mlFeature:(scrolltest,feature1)"

# Relationships created by the test data:
#   ZETA      --DownstreamOf--> ALPHA      (dataset -> dataset, via UpstreamLineage)
#   FEATURE_1 --DerivedFrom-->  ALPHA      (mlFeature -> dataset, via MLFeatureProperties.sources)
#   MODEL_1   --Consumes-->     FEATURE_1  (mlModel -> mlFeature, via MLModelProperties.mlFeatures)

_DATA_FILE = "tests/openapi/v3/data/scroll_test_data.json"


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client, _DATA_FILE, "scroll"
    )


def test_scroll_relationships_basic(graph_client: DataHubGraph) -> None:
    result = graph_client.scroll_relationships(count=5)
    assert isinstance(result.relationships, list)
    for rel in result.relationships:
        assert rel.source_urn
        assert rel.destination_urn
        assert rel.relationship_type
    logger.info(
        f"scroll_relationships basic: {len(result.relationships)} relationships returned"
    )


def test_scroll_relationships_by_type_downstream(graph_client: DataHubGraph) -> None:
    """DownstreamOf relationships exist; filter by source URN to scope to our test data."""
    result = graph_client.scroll_relationships(
        relationship_types=["DownstreamOf"],
        source_urns=[ZETA],
        count=10,
    )
    assert len(result.relationships) >= 1
    for rel in result.relationships:
        assert rel.relationship_type == "DownstreamOf"
    edges = {(r.source_urn, r.destination_urn) for r in result.relationships}
    assert (ZETA, ALPHA) in edges, (
        f"Expected ZETA->ALPHA DownstreamOf edge not found in {edges}"
    )
    logger.info(
        f"scroll_relationships DownstreamOf: {len(result.relationships)} edges for ZETA"
    )


def test_scroll_relationships_by_type_derived_from(graph_client: DataHubGraph) -> None:
    """DerivedFrom relationship: feature1 is derived from alpha dataset."""
    result = graph_client.scroll_relationships(
        relationship_types=["DerivedFrom"],
        source_urns=[FEATURE_1],
        count=10,
    )
    assert len(result.relationships) >= 1
    for rel in result.relationships:
        assert rel.relationship_type == "DerivedFrom"
    edges = {(r.source_urn, r.destination_urn) for r in result.relationships}
    assert (FEATURE_1, ALPHA) in edges, (
        f"Expected FEATURE_1->ALPHA DerivedFrom edge not found in {edges}"
    )
    logger.info(
        f"scroll_relationships DerivedFrom: {len(result.relationships)} edges for FEATURE_1"
    )


def test_scroll_relationships_by_type_consumes(graph_client: DataHubGraph) -> None:
    """Consumes relationship: model1 consumes feature1."""
    result = graph_client.scroll_relationships(
        relationship_types=["Consumes"],
        source_urns=[MODEL_1],
        count=10,
    )
    assert len(result.relationships) >= 1
    for rel in result.relationships:
        assert rel.relationship_type == "Consumes"
    edges = {(r.source_urn, r.destination_urn) for r in result.relationships}
    assert (MODEL_1, FEATURE_1) in edges, (
        f"Expected MODEL_1->FEATURE_1 Consumes edge not found in {edges}"
    )
    logger.info(
        f"scroll_relationships Consumes: {len(result.relationships)} edges for MODEL_1"
    )


def test_scroll_relationships_with_source_urns(graph_client: DataHubGraph) -> None:
    result = graph_client.scroll_relationships(source_urns=[ZETA], count=10)
    assert len(result.relationships) >= 1
    found = [
        r
        for r in result.relationships
        if r.source_urn == ZETA
        and r.destination_urn == ALPHA
        and r.relationship_type == "DownstreamOf"
    ]
    assert len(found) == 1, (
        f"Expected exactly one ZETA->ALPHA DownstreamOf, got: {result.relationships}"
    )
    logger.info(
        f"scroll_relationships source_urns: {len(result.relationships)} relationships for ZETA"
    )


def test_scroll_relationships_with_destination_urns(graph_client: DataHubGraph) -> None:
    """destination_urns=[ALPHA] should return relationships from both ZETA (DownstreamOf)
    and FEATURE_1 (DerivedFrom) since both point to ALPHA as destination."""
    result = graph_client.scroll_relationships(destination_urns=[ALPHA], count=100)
    assert len(result.relationships) >= 2
    edges = {
        (r.source_urn, r.destination_urn, r.relationship_type)
        for r in result.relationships
    }
    assert (ZETA, ALPHA, "DownstreamOf") in edges, (
        "Expected ZETA->ALPHA DownstreamOf in destination_urns=[ALPHA] results"
    )
    assert (FEATURE_1, ALPHA, "DerivedFrom") in edges, (
        "Expected FEATURE_1->ALPHA DerivedFrom in destination_urns=[ALPHA] results"
    )
    logger.info(
        f"scroll_relationships destination_urns=[ALPHA]: {len(result.relationships)} relationships"
    )


def test_scroll_relationships_source_urns_and_filter_equivalent(
    graph_client: DataHubGraph,
) -> None:
    """source_urns=[X] and source_filter with urn=X are two Python code paths that should
    produce identical requests. Verify by comparing results."""
    via_urns = graph_client.scroll_relationships(source_urns=[ZETA], count=10)
    source_filter: RawSearchFilter = [
        {"and": [{"field": "urn", "values": [ZETA], "condition": "EQUAL"}]}
    ]
    via_filter = graph_client.scroll_relationships(
        source_filter=source_filter, count=10
    )
    urns_edges = {
        (r.source_urn, r.destination_urn, r.relationship_type)
        for r in via_urns.relationships
    }
    filter_edges = {
        (r.source_urn, r.destination_urn, r.relationship_type)
        for r in via_filter.relationships
    }
    assert urns_edges == filter_edges, (
        "source_urns and source_filter with the same URN should produce identical results"
    )
    assert len(urns_edges) > 0, (
        "source_urns=[ZETA] should return at least one relationship"
    )
    logger.info(
        f"scroll_relationships source equivalence: both paths returned {len(urns_edges)} edges"
    )


def test_scroll_relationships_destination_urns_and_filter_equivalent(
    graph_client: DataHubGraph,
) -> None:
    """destination_urns=[X] and destination_filter with urn=X should produce identical results."""
    via_urns = graph_client.scroll_relationships(destination_urns=[ALPHA], count=100)
    dest_filter: RawSearchFilter = [
        {"and": [{"field": "urn", "values": [ALPHA], "condition": "EQUAL"}]}
    ]
    via_filter = graph_client.scroll_relationships(
        destination_filter=dest_filter, count=100
    )
    urns_edges = {
        (r.source_urn, r.destination_urn, r.relationship_type)
        for r in via_urns.relationships
    }
    filter_edges = {
        (r.source_urn, r.destination_urn, r.relationship_type)
        for r in via_filter.relationships
    }
    assert urns_edges == filter_edges, (
        "destination_urns and destination_filter with the same URN should produce identical results"
    )
    assert len(urns_edges) > 0, (
        "destination_urns=[ALPHA] should return at least one relationship"
    )
    logger.info(
        f"scroll_relationships destination equivalence: both paths returned {len(urns_edges)} edges"
    )


def test_scroll_relationships_with_source_types(graph_client: DataHubGraph) -> None:
    """source_types=["mlFeature"] with source_urns=[FEATURE_1] should return DerivedFrom edges
    where the source entity type is mlFeature."""
    result = graph_client.scroll_relationships(
        source_urns=[FEATURE_1],
        source_types=["mlFeature"],
        count=10,
    )
    assert len(result.relationships) >= 1
    for rel in result.relationships:
        assert rel.source_entity_type == "mlFeature", (
            f"Expected source entity type 'mlFeature', got '{rel.source_entity_type}'"
        )
    logger.info(
        f"scroll_relationships source_types: {len(result.relationships)} relationships with mlFeature sources"
    )


def test_scroll_relationships_with_destination_types(
    graph_client: DataHubGraph,
) -> None:
    """destination_types=["dataset"] with destination_urns=[ALPHA] returns only edges
    pointing to dataset entities."""
    result = graph_client.scroll_relationships(
        destination_urns=[ALPHA],
        destination_types=["dataset"],
        count=100,
    )
    assert len(result.relationships) >= 1
    for rel in result.relationships:
        assert rel.destination_entity_type == "dataset", (
            f"Expected destination entity type 'dataset', got '{rel.destination_entity_type}'"
        )
    # Both ZETA->ALPHA (DownstreamOf) and FEATURE_1->ALPHA (DerivedFrom) should be present
    edges = {(r.source_urn, r.destination_urn) for r in result.relationships}
    assert (ZETA, ALPHA) in edges
    assert (FEATURE_1, ALPHA) in edges
    logger.info(
        f"scroll_relationships destination_types: {len(result.relationships)} relationships with dataset destinations"
    )


def test_scroll_relationships_with_edge_filter(graph_client: DataHubGraph) -> None:
    edge_filter: RawSearchFilter = [
        {
            "and": [
                {
                    "field": "relationshipType",
                    "values": ["DownstreamOf"],
                    "condition": "EQUAL",
                }
            ]
        }
    ]
    result = graph_client.scroll_relationships(
        source_urns=[ZETA], edge_filter=edge_filter, count=10
    )
    assert len(result.relationships) >= 1
    for rel in result.relationships:
        assert rel.relationship_type == "DownstreamOf"
    found = [
        r
        for r in result.relationships
        if r.source_urn == ZETA and r.destination_urn == ALPHA
    ]
    assert len(found) == 1, (
        f"Expected exactly one ZETA->ALPHA DownstreamOf edge, got: {result.relationships}"
    )
    logger.info(
        f"scroll_relationships edge_filter: {len(result.relationships)} DownstreamOf relationships for ZETA"
    )


def test_scroll_relationships_with_direction_outgoing(
    graph_client: DataHubGraph,
) -> None:
    """direction=OUTGOING with source_urns=[ZETA] should include the DownstreamOf edge
    since edges are stored as source→destination (outgoing)."""
    result = graph_client.scroll_relationships(
        relationship_types=["DownstreamOf"],
        source_urns=[ZETA],
        direction=RelationshipDirection.OUTGOING,
        count=10,
    )
    edges = {
        (r.source_urn, r.destination_urn, r.relationship_type)
        for r in result.relationships
    }
    assert (ZETA, ALPHA, "DownstreamOf") in edges, (
        f"Expected ZETA->ALPHA DownstreamOf with direction=OUTGOING, got: {edges}"
    )
    logger.info(
        f"scroll_relationships direction=OUTGOING: {len(result.relationships)} edges for ZETA"
    )


def test_scroll_relationships_with_direction_incoming(
    graph_client: DataHubGraph,
) -> None:
    result = graph_client.scroll_relationships(
        source_urns=[ALPHA],
        direction=RelationshipDirection.INCOMING,
        count=100,
    )
    assert len(result.relationships) >= 2
    edges = {
        (r.source_urn, r.destination_urn, r.relationship_type)
        for r in result.relationships
    }
    assert (ZETA, ALPHA, "DownstreamOf") in edges, (
        f"Expected ZETA->ALPHA DownstreamOf with direction=INCOMING, got: {edges}"
    )
    assert (FEATURE_1, ALPHA, "DerivedFrom") in edges, (
        f"Expected FEATURE_1->ALPHA DerivedFrom with direction=INCOMING, got: {edges}"
    )
    logger.info(
        f"scroll_relationships direction=INCOMING: {len(result.relationships)} edges into ALPHA"
    )


def test_scroll_relationships_with_entity_urn_incoming(
    graph_client: DataHubGraph,
) -> None:
    """entity_urn + INCOMING uses walker mode (pin destination literal edge end).

    Unlike direction remapping without entity_urn, destination is the focal URN —
    do not also pass destination_urns with INCOMING (that double-applies "incoming").
    """
    result = graph_client.scroll_relationships(
        entity_urn=ALPHA,
        direction=RelationshipDirection.INCOMING,
        count=100,
    )
    assert len(result.relationships) >= 2
    edges = {
        (r.source_urn, r.destination_urn, r.relationship_type)
        for r in result.relationships
    }
    assert (ZETA, ALPHA, "DownstreamOf") in edges, (
        f"Expected ZETA->ALPHA DownstreamOf with entity_urn walker INCOMING, got: {edges}"
    )
    assert (FEATURE_1, ALPHA, "DerivedFrom") in edges, (
        f"Expected FEATURE_1->ALPHA DerivedFrom with entity_urn walker INCOMING, got: {edges}"
    )
    logger.info(
        f"scroll_relationships entity_urn INCOMING: {len(result.relationships)} edges into ALPHA"
    )


def test_scroll_relationships_with_entity_urn_outgoing(
    graph_client: DataHubGraph,
) -> None:
    result = graph_client.scroll_relationships(
        entity_urn=ZETA,
        relationship_types=["DownstreamOf"],
        direction=RelationshipDirection.OUTGOING,
        count=10,
    )
    edges = {
        (r.source_urn, r.destination_urn, r.relationship_type)
        for r in result.relationships
    }
    assert (ZETA, ALPHA, "DownstreamOf") in edges, (
        f"Expected ZETA->ALPHA DownstreamOf with entity_urn walker OUTGOING, got: {edges}"
    )
    logger.info(
        f"scroll_relationships entity_urn OUTGOING: {len(result.relationships)} edges from ZETA"
    )


def test_scroll_relationships_pagination(graph_client: DataHubGraph) -> None:
    """Pagination via scroll_id should produce non-overlapping pages."""
    first = graph_client.scroll_relationships(destination_urns=[ALPHA], count=1)
    if not first.relationships or first.scroll_id is None:
        pytest.skip("Not enough relationships pointing to ALPHA to test pagination")

    second = graph_client.scroll_relationships(
        destination_urns=[ALPHA],
        count=1,
        scroll_id=first.scroll_id,
    )
    if not second.relationships:
        pytest.skip(
            "Second page returned no relationships; not enough data to test pagination"
        )

    first_edges = {
        (r.source_urn, r.destination_urn, r.relationship_type)
        for r in first.relationships
    }
    second_edges = {
        (r.source_urn, r.destination_urn, r.relationship_type)
        for r in second.relationships
    }
    assert first_edges.isdisjoint(second_edges), (
        "Consecutive scroll pages should return different relationships"
    )
    logger.info(
        f"scroll_relationships pagination: page1={len(first_edges)}, page2={len(second_edges)} distinct edges"
    )
