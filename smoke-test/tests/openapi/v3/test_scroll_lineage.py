"""Smoke tests for the scroll_lineage DataHubGraph method.

scroll_lineage answers "what are the upstreams / downstreams of a set of URNs?". This is
distinct from scanning OUTGOING or INCOMING edges of a set of source URNs: the graph index
only stores each edge as (source, destination), but whether the source or the destination is
the *upstream* depends on the ``isUpstream`` flag of the relationship's ``@Relationship``
annotation, which varies by relationship type. The endpoint resolves this for us and returns
each edge as an explicit (upstream, downstream) pair.

For most relationships the source is downstream and the destination upstream — e.g.
``DownstreamOf`` (dataset A DownstreamOf dataset B => B is upstream), ``DerivedFrom`` and
``Consumes``. But ``Produces`` is the opposite: a dataJob ``Produces`` a dataset, so the
*source* (dataJob) is upstream and the *destination* (dataset) is downstream.

The test data exercises both orientations (shown as stored source --rel--> destination):
    ZETA      --DownstreamOf--> ALPHA      => ALPHA upstream, ZETA downstream
    FEATURE_1 --DerivedFrom-->  ALPHA      => ALPHA upstream, FEATURE_1 downstream
    MODEL_1   --Consumes-->     FEATURE_1  => FEATURE_1 upstream, MODEL_1 downstream
    JOB_1     --Consumes-->     ALPHA      => ALPHA upstream, JOB_1 downstream
    JOB_1     --Produces-->     GAMMA      => JOB_1 upstream, GAMMA downstream (source is upstream!)
"""

import logging
from typing import List, Optional, Set, Tuple

import pytest

from conftest import _ingest_cleanup_data_impl
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.openapi import LineageDirection
from tests.utils import with_test_retry

logger = logging.getLogger(__name__)

ALPHA = "urn:li:dataset:(urn:li:dataPlatform:scrolltest,alpha,PROD)"
GAMMA = "urn:li:dataset:(urn:li:dataPlatform:scrolltest,gamma,PROD)"
ZETA = "urn:li:dataset:(urn:li:dataPlatform:scrolltest,zeta,PROD)"
MODEL_1 = "urn:li:mlModel:(urn:li:dataPlatform:scrolltest,model1,PROD)"
FEATURE_1 = "urn:li:mlFeature:(scrolltest,feature1)"
JOB_1 = "urn:li:dataJob:(urn:li:dataFlow:(scrolltest,flow1,PROD),job1)"

# Every lineage edge in the test data, as (upstream, downstream, relationship_type).
DOWNSTREAM_OF = (ALPHA, ZETA, "DownstreamOf")
DERIVED_FROM = (ALPHA, FEATURE_1, "DerivedFrom")
CONSUMES_FEATURE = (FEATURE_1, MODEL_1, "Consumes")
CONSUMES_INPUT = (ALPHA, JOB_1, "Consumes")
PRODUCES_OUTPUT = (JOB_1, GAMMA, "Produces")

Edge = Tuple[str, str, str]

_DATA_FILE = "tests/openapi/v3/data/scroll_test_data.json"


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client, _DATA_FILE, "scroll"
    )


def _scroll_all_lineage_edges(
    graph_client: DataHubGraph,
    *,
    urns: Optional[List[str]] = None,
    direction: Optional[LineageDirection] = None,
    relationship_types: Optional[List[str]] = None,
    count: int = 100,
) -> Set[Edge]:
    """Scroll through every page and collect the returned edges as (upstream, downstream, type).

    Direction filtering happens server-side after each page is fetched, so a page may hold
    fewer than ``count`` results; we must keep scrolling until scroll_id is None.
    """
    edges: Set[Edge] = set()
    scroll_id: Optional[str] = None
    while True:
        result = graph_client.scroll_lineage(
            urns=urns,
            direction=direction,
            relationship_types=relationship_types,
            count=count,
            scroll_id=scroll_id,
        )
        edges.update(
            (r.upstream_urn, r.downstream_urn, r.relationship_type)
            for r in result.relationships
        )
        scroll_id = result.scroll_id
        if scroll_id is None:
            break
    return edges


@with_test_retry(max_attempts=10)
def _assert_lineage_edges(
    graph_client: DataHubGraph,
    expected: Set[Edge],
    *,
    urns: Optional[List[str]] = None,
    direction: Optional[LineageDirection] = None,
    relationship_types: Optional[List[str]] = None,
) -> None:
    edges = _scroll_all_lineage_edges(
        graph_client,
        urns=urns,
        direction=direction,
        relationship_types=relationship_types,
    )
    assert edges == expected, (
        f"lineage edges for urns={urns} direction={direction} "
        f"relationship_types={relationship_types}\nexpected: {expected}\ngot:      {edges}"
    )


def test_scroll_lineage_all_edges_around_urn(graph_client: DataHubGraph) -> None:
    """With no direction, every lineage edge touching ALPHA is returned. ALPHA is the upstream
    endpoint of all three (the graph is queried undirected, so both sides are matched)."""
    _assert_lineage_edges(
        graph_client,
        {DOWNSTREAM_OF, DERIVED_FROM, CONSUMES_INPUT},
        urns=[ALPHA],
    )
    logger.info("scroll_lineage all edges around ALPHA: 3 edges")


def test_scroll_lineage_downstream_of_upstream_node(graph_client: DataHubGraph) -> None:
    """ALPHA is an upstream root: it is the upstream endpoint of all three edges touching it, so
    those are its *downstreams* (DownstreamOf/DerivedFrom/Consumes are isUpstream=true). It has
    no upstream. This is the case that plain INCOMING-edge scanning would get backwards."""
    _assert_lineage_edges(
        graph_client,
        {DOWNSTREAM_OF, DERIVED_FROM, CONSUMES_INPUT},
        urns=[ALPHA],
        direction=LineageDirection.DOWNSTREAM,
    )
    _assert_lineage_edges(
        graph_client,
        set(),
        urns=[ALPHA],
        direction=LineageDirection.UPSTREAM,
    )
    logger.info("scroll_lineage ALPHA: 3 downstream, 0 upstream")


def test_scroll_lineage_produces_source_is_upstream(graph_client: DataHubGraph) -> None:
    """GAMMA also has a single edge pointing into it (JOB_1 --Produces--> GAMMA), yet because
    Produces is isUpstream=false the source JOB_1 is GAMMA's *upstream*, not downstream. Same
    stored-edge topology as ALPHA's incoming edges, opposite lineage direction."""
    _assert_lineage_edges(
        graph_client,
        {PRODUCES_OUTPUT},
        urns=[GAMMA],
        direction=LineageDirection.UPSTREAM,
    )
    _assert_lineage_edges(
        graph_client,
        set(),
        urns=[GAMMA],
        direction=LineageDirection.DOWNSTREAM,
    )
    logger.info("scroll_lineage GAMMA: 1 upstream (Produces), 0 downstream")


def test_scroll_lineage_datajob_directions(graph_client: DataHubGraph) -> None:
    """JOB_1 is the source of both its edges (both stored as OUTGOING), yet one is its upstream
    (Consumes ALPHA) and the other its downstream (Produces GAMMA). Edge direction alone cannot
    distinguish them; only the per-relationship-type lineage classification can."""
    _assert_lineage_edges(
        graph_client,
        {CONSUMES_INPUT},
        urns=[JOB_1],
        direction=LineageDirection.UPSTREAM,
    )
    _assert_lineage_edges(
        graph_client,
        {PRODUCES_OUTPUT},
        urns=[JOB_1],
        direction=LineageDirection.DOWNSTREAM,
    )
    logger.info(
        "scroll_lineage JOB_1: upstream ALPHA (Consumes), downstream GAMMA (Produces)"
    )


def test_scroll_lineage_feature_directions(graph_client: DataHubGraph) -> None:
    """FEATURE_1 is the source of its upstream edge (DerivedFrom ALPHA) and the destination of
    its downstream edge (MODEL_1 Consumes FEATURE_1) — upstream/downstream does not line up with
    whether FEATURE_1 is the source or destination of the stored edge."""
    _assert_lineage_edges(
        graph_client,
        {DERIVED_FROM},
        urns=[FEATURE_1],
        direction=LineageDirection.UPSTREAM,
    )
    _assert_lineage_edges(
        graph_client,
        {CONSUMES_FEATURE},
        urns=[FEATURE_1],
        direction=LineageDirection.DOWNSTREAM,
    )
    logger.info("scroll_lineage FEATURE_1: upstream ALPHA, downstream MODEL_1")


def test_scroll_lineage_relationship_types_filter(graph_client: DataHubGraph) -> None:
    """relationship_types narrows the edges: DerivedFrom keeps only the FEATURE_1/ALPHA edge and
    drops the DownstreamOf and Consumes edges that also touch ALPHA."""
    _assert_lineage_edges(
        graph_client,
        {DERIVED_FROM},
        urns=[ALPHA],
        relationship_types=["DerivedFrom"],
    )
    logger.info("scroll_lineage relationship_types=[DerivedFrom]: 1 edge")


def test_scroll_lineage_pagination(graph_client: DataHubGraph) -> None:
    """Paginating one edge at a time should cover all three edges touching ALPHA without
    overlap between pages."""

    @with_test_retry(max_attempts=10)
    def _assert() -> None:
        seen: Set[Edge] = set()
        scroll_id: Optional[str] = None
        pages = 0
        while True:
            result = graph_client.scroll_lineage(
                urns=[ALPHA], count=1, scroll_id=scroll_id
            )
            page_edges = {
                (r.upstream_urn, r.downstream_urn, r.relationship_type)
                for r in result.relationships
            }
            assert seen.isdisjoint(page_edges), (
                f"page {pages} overlaps previous pages: {page_edges & seen}"
            )
            seen.update(page_edges)
            scroll_id = result.scroll_id
            pages += 1
            if scroll_id is None:
                break
        assert seen == {DOWNSTREAM_OF, DERIVED_FROM, CONSUMES_INPUT}, (
            f"pagination should cover all edges touching ALPHA, got: {seen}"
        )

    _assert()
    logger.info("scroll_lineage pagination: covered all 3 edges one page at a time")


def test_scroll_lineage_no_urns_returns_lineage_edges(
    graph_client: DataHubGraph,
) -> None:
    """With no urns, all lineage edges are scrolled. We can't assert an exact set against a
    shared instance, but every returned edge must have both endpoints resolved."""
    result = graph_client.scroll_lineage(count=10)
    assert isinstance(result.relationships, list)
    for rel in result.relationships:
        assert rel.upstream_urn
        assert rel.downstream_urn
        assert rel.relationship_type
    logger.info(
        f"scroll_lineage no urns: {len(result.relationships)} lineage edges returned"
    )
