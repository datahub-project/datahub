import logging
import os
import tempfile
import uuid
from typing import Any, Dict, List

import pytest

from conftest import _ingest_cleanup_data_impl
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SiblingsClass,
    StatusClass,
)
from tests.utilities.domains import Domain
from tests.utils import execute_graphql, with_test_retry

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

# Unique per run so sibling counts are deterministic regardless of what else is in the instance.
_RUN_ID = uuid.uuid4().hex[:8]

_DBT = "dbt"
_WAREHOUSE = "snowflake"


def _urn(platform: str, name: str) -> str:
    return make_dataset_urn(platform, f"siblings_batching_{_RUN_ID}.{name}")


# --- Group 1: asymmetric pairs -------------------------------------------------------------
# The siblings aspect is written ONLY on the warehouse side. Querying from the dbt side is the
# reverse direction, which the dbt entity's own aspect cannot answer -- this is precisely why
# siblingsSearch is a search rather than an aspect read, and why the batched loader has to
# attribute hits back by reading each hit's siblings aspect.
_PAIRED = ["orders", "users", "events"]

# --- Group 2: no siblings ------------------------------------------------------------------
# The zero path is where a batched loader most easily goes wrong: a missing facet bucket must
# read as total 0 AND count 0, not as the requested page size or a dropped key.
_ORPHAN = "orphan"

# --- Group 3: one dbt model with several warehouse siblings --------------------------------
# Separates "total" (from the facet, counts them all) from "count"/results (bounded by the
# requested page size).
_MULTI = "multi"
_MULTI_SIBLING_COUNT = 3

# --- Group 4: chunk boundary ---------------------------------------------------------------
# The loader chunks keys at MAX_URNS_PER_AGG (25) and issues one search per chunk. Asking for
# more than that in a single request is the only way to exercise the multi-chunk path, where the
# failure modes are chunk results applied to the wrong keys and later chunks silently reading 0.
_CHUNK_SPAN_COUNT = 30

# --- Group 5: symmetric pair ---------------------------------------------------------------
# The hook-created shape, where both sides carry the aspect.
_SYMMETRIC = "symmetric"


class _FileEmitter:
    def __init__(self, filename: str) -> None:
        self.sink: FileSink = FileSink(
            ctx=PipelineContext(run_id="siblings_search_batching"),
            config=FileSinkConfig(filename=filename),
        )

    def emit(self, event) -> None:
        self.sink.write_record_async(
            record_envelope=RecordEnvelope(record=event, metadata={}),
            write_callback=NoopWriteCallback(),
        )

    def close(self) -> None:
        self.sink.close()


def _dataset(urn: str, name: str) -> List[MetadataChangeProposalWrapper]:
    return [
        MetadataChangeProposalWrapper(
            entityUrn=urn, aspect=DatasetPropertiesClass(name=name)
        ),
        MetadataChangeProposalWrapper(entityUrn=urn, aspect=StatusClass(removed=False)),
    ]


def _build_test_data(filename: str) -> None:
    mcps: List[MetadataChangeProposalWrapper] = []

    for name in _PAIRED:
        dbt_urn, wh_urn = _urn(_DBT, name), _urn(_WAREHOUSE, name)
        mcps += _dataset(dbt_urn, name)
        mcps += _dataset(wh_urn, name)
        # Warehouse side only -- the dbt entity gets no siblings aspect of its own.
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=wh_urn, aspect=SiblingsClass(siblings=[dbt_urn], primary=True)
            )
        )

    mcps += _dataset(_urn(_DBT, _ORPHAN), _ORPHAN)

    multi_urn = _urn(_DBT, _MULTI)
    mcps += _dataset(multi_urn, _MULTI)
    for i in range(_MULTI_SIBLING_COUNT):
        wh_urn = _urn(_WAREHOUSE, f"{_MULTI}_{i}")
        mcps += _dataset(wh_urn, f"{_MULTI}_{i}")
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=wh_urn,
                aspect=SiblingsClass(siblings=[multi_urn], primary=True),
            )
        )

    # Every other span dataset gets a sibling, so expected values alternate. Any shift or
    # cross-chunk swap of key/result alignment changes an assertion rather than coincidentally
    # matching, and the zero-sibling keys land in both chunks.
    for i in range(_CHUNK_SPAN_COUNT):
        dbt_urn = _urn(_DBT, f"span{i:03d}")
        mcps += _dataset(dbt_urn, f"span{i:03d}")
        if i % 2 == 0:
            wh_urn = _urn(_WAREHOUSE, f"span{i:03d}")
            mcps += _dataset(wh_urn, f"span{i:03d}")
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=wh_urn,
                    aspect=SiblingsClass(siblings=[dbt_urn], primary=True),
                )
            )

    sym_dbt, sym_wh = _urn(_DBT, _SYMMETRIC), _urn(_WAREHOUSE, _SYMMETRIC)
    mcps += _dataset(sym_dbt, _SYMMETRIC)
    mcps += _dataset(sym_wh, _SYMMETRIC)
    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=sym_dbt, aspect=SiblingsClass(siblings=[sym_wh], primary=False)
        )
    )
    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=sym_wh, aspect=SiblingsClass(siblings=[sym_dbt], primary=True)
        )
    )

    emitter = _FileEmitter(filename)
    for mcp in mcps:
        emitter.emit(mcp)
    emitter.close()


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    _, filename = tempfile.mkstemp(suffix=".json")
    try:
        _build_test_data(filename)
        yield from _ingest_cleanup_data_impl(
            auth_session, graph_client, filename, "siblings_search_batching"
        )
    finally:
        os.remove(filename)


_SIBLINGS_SELECTION = """
    urn
    siblingsSearch(input: {query: "*", count: %d}) {
      total
      count
      searchResults { entity { urn } }
    }
"""


def _aliased_query(name: str, urns: List[str], count: int = 5) -> str:
    """Alias every urn into ONE request so the per-dataset .load() calls coalesce into a single
    SiblingsSearchBatchLoader.batchLoad. A single-dataset query would only prove a one-key batch."""
    fields = "\n  ".join(
        f'd{i}: dataset(urn: "{urn}") {{{_SIBLINGS_SELECTION % count}}}'
        for i, urn in enumerate(urns)
    )
    return f"query {name} {{\n  {fields}\n}}"


def _sibling_urns(node: Dict[str, Any]) -> List[str]:
    return sorted(r["entity"]["urn"] for r in node["siblingsSearch"]["searchResults"])


def test_siblings_search_batched_results_attributed_to_own_dataset(auth_session):
    """Each dbt model in one request must get its OWN warehouse sibling, not a neighbour's."""
    urns = [_urn(_DBT, name) for name in _PAIRED]

    @with_test_retry()
    def check() -> None:
        res = execute_graphql(auth_session, _aliased_query("siblingsBatched", urns), {})
        data = res["data"]
        actual = {
            data[f"d{i}"]["urn"]: _sibling_urns(data[f"d{i}"]) for i in range(len(urns))
        }
        logger.info(f"batched siblings: {actual}")

        expected = {_urn(_DBT, name): [_urn(_WAREHOUSE, name)] for name in _PAIRED}
        assert actual == expected

    check()


def test_siblings_search_reports_zero_for_dataset_without_siblings(auth_session):
    """A dataset with no siblings must report total 0 AND count 0.

    count is "entities included in the result set", derived from the response by the unbatched
    path -- not the requested page size. Returning the requested count here is a real regression
    the batched path is prone to.
    """
    urns = [_urn(_DBT, _ORPHAN), _urn(_DBT, _PAIRED[0])]

    @with_test_retry()
    def check() -> None:
        res = execute_graphql(auth_session, _aliased_query("siblingsZero", urns), {})
        orphan = res["data"]["d0"]["siblingsSearch"]
        paired = res["data"]["d1"]["siblingsSearch"]

        assert orphan["total"] == 0
        assert orphan["count"] == 0
        assert orphan["searchResults"] == []
        # The neighbour in the same batch still resolves, so a zero key is not a dropped key.
        assert paired["total"] == 1
        assert paired["count"] == 1

    check()


def test_siblings_search_correct_across_chunk_boundary(auth_session):
    """More keys than MAX_URNS_PER_AGG (25), so the loader must issue and stitch several chunks."""
    urns = [_urn(_DBT, f"span{i:03d}") for i in range(_CHUNK_SPAN_COUNT)]
    expected = {
        _urn(_DBT, f"span{i:03d}"): (
            [_urn(_WAREHOUSE, f"span{i:03d}")] if i % 2 == 0 else []
        )
        for i in range(_CHUNK_SPAN_COUNT)
    }

    @with_test_retry()
    def check() -> None:
        res = execute_graphql(auth_session, _aliased_query("siblingsChunked", urns), {})
        data = res["data"]
        actual = {
            data[f"d{i}"]["urn"]: _sibling_urns(data[f"d{i}"])
            for i in range(_CHUNK_SPAN_COUNT)
        }
        # Compare the whole mapping so a mis-attributed or zeroed chunk shows as a diff rather
        # than passing on the keys that happened to be right.
        assert actual == expected

    check()


def test_siblings_search_finds_asymmetric_siblings(auth_session):
    """The dbt side carries no siblings aspect, so only the reverse-direction search finds it."""
    dbt_urn = _urn(_DBT, _PAIRED[0])

    @with_test_retry()
    def check() -> None:
        res = execute_graphql(
            auth_session, _aliased_query("siblingsAsymmetric", [dbt_urn]), {}
        )
        node = res["data"]["d0"]
        assert _sibling_urns(node) == [_urn(_WAREHOUSE, _PAIRED[0])]

    check()


def test_siblings_search_symmetric_pair_resolves_from_both_sides(auth_session):
    """Hook-created pairs carry the aspect on both sides and must resolve in either direction."""
    urns = [_urn(_DBT, _SYMMETRIC), _urn(_WAREHOUSE, _SYMMETRIC)]

    @with_test_retry()
    def check() -> None:
        res = execute_graphql(
            auth_session, _aliased_query("siblingsSymmetric", urns), {}
        )
        assert _sibling_urns(res["data"]["d0"]) == [_urn(_WAREHOUSE, _SYMMETRIC)]
        assert _sibling_urns(res["data"]["d1"]) == [_urn(_DBT, _SYMMETRIC)]

    check()


def test_siblings_search_total_counts_all_siblings_while_results_respect_count(
    auth_session,
):
    """total counts every attributed sibling; results are bounded by the page size."""
    multi_urn = _urn(_DBT, _MULTI)

    @with_test_retry()
    def check() -> None:
        res = execute_graphql(
            auth_session, _aliased_query("siblingsMulti", [multi_urn], count=2), {}
        )
        node = res["data"]["d0"]["siblingsSearch"]
        assert node["total"] == _MULTI_SIBLING_COUNT
        assert node["count"] == 2
        assert len(node["searchResults"]) == 2

    check()


def test_siblings_search_zero_count_returns_total_only(auth_session):
    """`count: 0` is a legitimate totals-only request and must not fail.

    An empty page satisfies the requested size, so it reads as full while having no last hit to
    take a cursor from. Both arms must answer the same way.
    """
    multi_urn = _urn(_DBT, _MULTI)

    @with_test_retry()
    def check() -> None:
        res = execute_graphql(
            auth_session, _aliased_query("siblingsZero", [multi_urn], count=0), {}
        )
        assert not res.get("errors"), res.get("errors")
        node = res["data"]["d0"]["siblingsSearch"]
        assert node["total"] == _MULTI_SIBLING_COUNT
        assert node["count"] == 0
        assert node["searchResults"] == []

    check()


def test_siblings_search_same_urn_with_different_input_not_shared(auth_session):
    """The DataLoader key carries the query shape, not just the urn.

    Asking about the same dataset twice with different page sizes in one request must produce
    two independent answers. If the key ever collapsed to the bare urn, one alias would silently
    receive the other's result -- no error, just wrong data.
    """
    multi_urn = _urn(_DBT, _MULTI)
    query = (
        "query siblingsSameUrnTwoShapes {\n"
        f'  small: dataset(urn: "{multi_urn}") {{{_SIBLINGS_SELECTION % 1}}}\n'
        f'  large: dataset(urn: "{multi_urn}") {{{_SIBLINGS_SELECTION % 3}}}\n'
        "}"
    )

    @with_test_retry()
    def check() -> None:
        res = execute_graphql(auth_session, query, {})
        small = res["data"]["small"]["siblingsSearch"]
        large = res["data"]["large"]["siblingsSearch"]

        assert small["count"] == 1
        assert len(small["searchResults"]) == 1
        assert large["count"] == _MULTI_SIBLING_COUNT
        assert len(large["searchResults"]) == _MULTI_SIBLING_COUNT
        # total is a property of the dataset, not the page size, so it agrees across shapes.
        assert small["total"] == large["total"] == _MULTI_SIBLING_COUNT

    check()


# --- Ordering and scroll continuation -------------------------------------------------------
#
# Review feedback on the batching PR: the batched and unbatched paths originally used different
# search APIs (searchAcrossEntities from/size vs scrollAcrossEntities search_after), so first-page
# ranking could differ — and several UI paths read searchResults[0]. Both paths now use
# scrollAcrossEntities, and these tests assert the properties that were previously only compared
# as unordered sets.
#
# A request carrying a scrollId is deliberately routed to the unbatched path, so paging from a
# batched first page exercises BOTH paths in one run without touching the feature flag.

_SIBLINGS_PAGE = """
query siblingsPage($urn: String!, $count: Int!, $scrollId: String) {
  dataset(urn: $urn) {
    siblingsSearch(
      input: {
        query: "*"
        count: $count
        scrollId: $scrollId
      }
    ) {
      nextScrollId
      total
      count
      searchResults { entity { urn } }
    }
  }
}
"""


def _page(auth_session, urn: str, count: int, scroll_id=None) -> Dict[str, Any]:
    variables: Dict[str, Any] = {"urn": urn, "count": count, "scrollId": scroll_id}
    res = execute_graphql(auth_session, _SIBLINGS_PAGE, variables)
    return res["data"]["dataset"]["siblingsSearch"]


def _urns_of(page: Dict[str, Any]) -> List[str]:
    return [r["entity"]["urn"] for r in page["searchResults"]]


def test_sibling_hit_order_is_stable_across_identical_requests(auth_session):
    """Identical requests must return siblings in identical order.

    Ordering used to depend on an unordered map's iteration order, which made
    `searchResults[0]` vary between byte-identical requests — invisible to any comparison that
    sorts before asserting.
    """
    multi_urn = _urn(_DBT, _MULTI)

    @with_test_retry()
    def check() -> None:
        first = _urns_of(_page(auth_session, multi_urn, _MULTI_SIBLING_COUNT))
        assert len(first) == _MULTI_SIBLING_COUNT
        for _ in range(4):
            assert (
                _urns_of(_page(auth_session, multi_urn, _MULTI_SIBLING_COUNT)) == first
            )

    check()


def test_full_page_returns_a_cursor_and_short_page_does_not(auth_session):
    """A cursor is offered only when the page filled, matching the unbatched contract."""
    multi_urn = _urn(_DBT, _MULTI)

    @with_test_retry()
    def check() -> None:
        full = _page(auth_session, multi_urn, 1)
        assert full["count"] == 1
        assert full["nextScrollId"], "a full page must offer a resume point"

        short = _page(auth_session, multi_urn, _MULTI_SIBLING_COUNT + 5)
        assert short["count"] == _MULTI_SIBLING_COUNT
        assert short["nextScrollId"] is None, "a short page has nothing to resume from"

    check()


def test_cursor_from_first_page_continues_on_the_unbatched_path(auth_session):
    """Page 1 (no scrollId, batched) hands a cursor that page 2 (scrollId, unbatched) can use.

    This is the cross-path check: the cursor is built from the hit's own sort values, so it has to
    be valid for a query the batched path never issued.
    """
    multi_urn = _urn(_DBT, _MULTI)

    @with_test_retry()
    def check() -> None:
        first = _page(auth_session, multi_urn, 1)
        assert first["nextScrollId"]
        first_urns = _urns_of(first)

        second = _page(auth_session, multi_urn, 1, scroll_id=first["nextScrollId"])
        second_urns = _urns_of(second)

        assert second_urns, "continuing from the cursor returned nothing"
        assert not set(first_urns) & set(second_urns), (
            f"cursor replayed a row already returned: {first_urns} then {second_urns}"
        )

    check()


def test_paging_by_cursor_yields_every_sibling_exactly_once(auth_session):
    """Walking the cursor must enumerate the full sibling set with no gaps or repeats."""
    multi_urn = _urn(_DBT, _MULTI)

    @with_test_retry()
    def check() -> None:
        expected = _urns_of(_page(auth_session, multi_urn, _MULTI_SIBLING_COUNT))
        assert len(expected) == _MULTI_SIBLING_COUNT

        seen: List[str] = []
        scroll_id = None
        # Bounded so a cursor that fails to advance fails the test rather than hanging.
        for _ in range(_MULTI_SIBLING_COUNT + 2):
            page = _page(auth_session, multi_urn, 1, scroll_id=scroll_id)
            seen += _urns_of(page)
            scroll_id = page["nextScrollId"]
            if not scroll_id:
                break

        assert seen == expected, f"paged {seen}, single request gave {expected}"
        assert len(set(seen)) == len(seen), "a sibling was returned twice while paging"

    check()
