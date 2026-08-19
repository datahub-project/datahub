import logging
import os
import re
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, List

import pytest

from conftest import _ingest_cleanup_data_impl
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    DatasetPropertiesClass,
)
from tests.utilities.domains import Domain
from tests.utils import execute_graphql, with_test_retry

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

# Unique per run so the containers are freshly seeded and their counts are deterministic
# regardless of what else is in the instance.
_RUN_ID = uuid.uuid4().hex[:8]

_PLATFORM = "snowflake"


def _container_urn(label: str) -> str:
    return f"urn:li:container:batching_{label}_{_RUN_ID}"


# Ground truth. C is intentionally empty to cover the zero-count path, which is where a
# batched loader most easily goes wrong (a missing facet bucket must read as 0, not as a
# dropped key or a default).
_CONTAINER_A = _container_urn("a")
_CONTAINER_B = _container_urn("b")
_CONTAINER_C = _container_urn("c")

_EXPECTED = {_CONTAINER_A: 2, _CONTAINER_B: 3, _CONTAINER_C: 0}

# The loader chunks keys at MAX_CONTAINERS_PER_AGG (25), issuing one aggregation search per
# chunk. Requesting more than that in a single GraphQL request is the only way to exercise the
# multi-chunk path end to end, where the failure modes are chunk results being applied to the
# wrong keys, later chunks silently reading as 0, and off-by-one key/result alignment.
_CHUNK_SPAN_COUNT = 30

# Counts cycle 0,1,2 so the expected value differs at every adjacent position: any shift or
# cross-chunk swap of the key/result alignment changes the assertion, and the zero-count
# containers land in both chunks (a missing facet bucket must read as 0 in a later chunk too,
# not as a dropped key).
_CHUNK_SPAN_EXPECTED = {
    _container_urn(f"span{i}"): i % 3 for i in range(_CHUNK_SPAN_COUNT)
}


class _FileEmitter:
    def __init__(self, filename: str) -> None:
        self.sink: FileSink = FileSink(
            ctx=PipelineContext(run_id="container_entities_batching"),
            config=FileSinkConfig(filename=filename),
        )

    def emit(self, event) -> None:
        self.sink.write_record_async(
            record_envelope=RecordEnvelope(record=event, metadata={}),
            write_callback=NoopWriteCallback(),
        )

    def close(self) -> None:
        self.sink.close()


def _dataset_in_container(
    name: str, container_urn: str
) -> List[MetadataChangeProposalWrapper]:
    dataset_urn = make_dataset_urn(_PLATFORM, name)
    return [
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(name=name),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=ContainerClass(container=container_urn),
        ),
    ]


def _build_test_data(filename: str) -> None:
    mcps: List[MetadataChangeProposalWrapper] = []
    for urn, label in ((_CONTAINER_A, "A"), (_CONTAINER_B, "B"), (_CONTAINER_C, "C")):
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=ContainerPropertiesClass(
                    name=f"Batching Test {label} {_RUN_ID}"
                ),
            )
        )

    for i in range(_EXPECTED[_CONTAINER_A]):
        mcps += _dataset_in_container(f"batching_a_{_RUN_ID}_{i}", _CONTAINER_A)
    for i in range(_EXPECTED[_CONTAINER_B]):
        mcps += _dataset_in_container(f"batching_b_{_RUN_ID}_{i}", _CONTAINER_B)

    for span_index, (urn, expected) in enumerate(_CHUNK_SPAN_EXPECTED.items()):
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=ContainerPropertiesClass(
                    name=f"Batching Span {span_index} {_RUN_ID}"
                ),
            )
        )
        for i in range(expected):
            mcps += _dataset_in_container(
                f"batching_span{span_index}_{_RUN_ID}_{i}", urn
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
            auth_session, graph_client, filename, "container_entities_batching"
        )
    finally:
        os.remove(filename)


# Mirrors the count-only shape the search fragments issue for a page of containers. Fetching
# all three as aliased fields in a SINGLE request coalesces the per-container .load() calls
# into one ContainerEntityCountsBatchLoader.batchLoad, so this exercises the batched fast path
# and per-container attribution end to end — a single-container query would only prove a
# one-key batch.
_BATCHED_COUNTS_QUERY = """
query containerEntityCounts($a: String!, $b: String!, $c: String!) {
  a: container(urn: $a) { urn entities(input: {}) { total } }
  b: container(urn: $b) { urn entities(input: {}) { total } }
  c: container(urn: $c) { urn entities(input: {}) { total } }
}
"""

# Selecting hits disqualifies the fast path, so this must fall back to the direct search and
# still agree with the batched total.
_DIRECT_QUERY = """
query containerEntitiesDirect($urn: String!) {
  container(urn: $urn) {
    entities(input: {start: 0, count: 10}) {
      total
      searchResults { entity { urn } }
    }
  }
}
"""

# A facet filter also disqualifies the fast path, and must actually be applied: filtering to
# CHART excludes the container's datasets entirely.
_FILTERED_QUERY = """
query containerEntitiesFiltered($urn: String!) {
  datasets: container(urn: $urn) {
    entities(input: {filters: [{field: "_entityType", values: ["DATASET"]}]}) { total }
  }
  charts: container(urn: $urn) {
    entities(input: {filters: [{field: "_entityType", values: ["CHART"]}]}) { total }
  }
}
"""


def _chunk_span_query() -> str:
    """One request aliasing every span container, so all keys coalesce into one batchLoad."""
    fields = "\n  ".join(
        f'c{i}: container(urn: "{urn}") {{ urn entities(input: {{}}) {{ total }} }}'
        for i, urn in enumerate(_CHUNK_SPAN_EXPECTED)
    )
    return f"query containerEntityCountsAcrossChunks {{\n  {fields}\n}}"


def test_container_entities_counts_are_batched_and_correct(auth_session):
    variables: Dict[str, Any] = {
        "a": _CONTAINER_A,
        "b": _CONTAINER_B,
        "c": _CONTAINER_C,
    }

    @with_test_retry()
    def check() -> None:
        res = execute_graphql(auth_session, _BATCHED_COUNTS_QUERY, variables)
        data = res["data"]
        actual = {
            data[alias]["urn"]: data[alias]["entities"]["total"] for alias in "abc"
        }
        logger.info(f"batched container entity counts: {actual}")

        for alias, urn in (
            ("a", _CONTAINER_A),
            ("b", _CONTAINER_B),
            ("c", _CONTAINER_C),
        ):
            assert data[alias]["urn"] == urn
            expected = _EXPECTED[urn]
            assert data[alias]["entities"]["total"] == expected, (
                f"{urn} entities: expected {expected}, "
                f"got {data[alias]['entities']['total']}"
            )

    check()


def test_container_entities_counts_correct_across_chunk_boundary(auth_session):
    @with_test_retry()
    def check() -> None:
        res = execute_graphql(auth_session, _chunk_span_query(), {})
        data = res["data"]

        actual = {
            data[f"c{i}"]["urn"]: data[f"c{i}"]["entities"]["total"]
            for i in range(_CHUNK_SPAN_COUNT)
        }
        logger.info(f"chunk-spanning container entity counts: {actual}")

        # Compare the whole mapping at once so a mis-attributed or zeroed chunk shows up as a
        # diff rather than passing on the containers that happened to be right.
        assert actual == _CHUNK_SPAN_EXPECTED

    check()


def test_container_entities_direct_path_matches_batched_total(auth_session):
    @with_test_retry()
    def check() -> None:
        res = execute_graphql(auth_session, _DIRECT_QUERY, {"urn": _CONTAINER_B})
        entities = res["data"]["container"]["entities"]
        expected = _EXPECTED[_CONTAINER_B]
        assert entities["total"] == expected
        assert len(entities["searchResults"]) == expected

    check()


def test_container_entities_filters_are_applied(auth_session):
    @with_test_retry()
    def check() -> None:
        res = execute_graphql(auth_session, _FILTERED_QUERY, {"urn": _CONTAINER_B})
        data = res["data"]
        assert data["datasets"]["entities"]["total"] == _EXPECTED[_CONTAINER_B]
        assert data["charts"]["entities"]["total"] == 0

    check()


_FRONTEND_GRAPHQL_DIR = (
    Path(__file__).resolve().parents[3] / "datahub-web-react" / "src" / "graphql"
)
_PRODUCTION_SEARCH_OPERATION = "getSearchResultsForMultiple"

# The assembled operation is what makes this test meaningful: one copy normalizes to just
# under graphql-java's 100k field cap, so two copies cross it. If the frontend fragments
# shrink enough that the document no longer approaches the cap, the test would still pass
# while checking nothing -- these floors make that drift fail loudly instead.
_MIN_FRAGMENTS = 40
_MIN_DOCUMENT_CHARS = 30_000
_DEFINITION_RE = re.compile(
    r"^(fragment|query|mutation|subscription)\s+(\w+)[\s\S]*?"
    r"(?=^(?:fragment|query|mutation|subscription)\s|\Z)",
    re.MULTILINE,
)


def _assemble_production_search_document() -> str:
    """Rebuild the real search query, with its transitive fragments, from the frontend sources."""
    if not _FRONTEND_GRAPHQL_DIR.is_dir():
        pytest.skip(f"frontend graphql sources not found at {_FRONTEND_GRAPHQL_DIR}")

    fragments: Dict[str, str] = {}
    operation = None
    for path in sorted(_FRONTEND_GRAPHQL_DIR.glob("*.graphql")):
        for match in _DEFINITION_RE.finditer(path.read_text()):
            kind, name, body = match.group(1), match.group(2), match.group(0)
            if kind == "fragment":
                fragments[name] = body
            elif name == _PRODUCTION_SEARCH_OPERATION:
                operation = body

    assert operation is not None, (
        f"{_PRODUCTION_SEARCH_OPERATION} not found under {_FRONTEND_GRAPHQL_DIR}; "
        "the query this regression test depends on was renamed or removed"
    )

    needed: List[str] = []
    seen = set()
    pending = re.findall(r"\.\.\.(\w+)", operation)
    while pending:
        name = pending.pop(0)
        if name in seen or name not in fragments:
            continue
        seen.add(name)
        needed.append(name)
        pending.extend(re.findall(r"\.\.\.(\w+)", fragments[name]))

    document = (
        operation.rstrip()
        + "\n\n"
        + "\n\n".join(fragments[name].rstrip() for name in needed)
    )
    assert len(needed) >= _MIN_FRAGMENTS and len(document) >= _MIN_DOCUMENT_CHARS, (
        f"assembled {_PRODUCTION_SEARCH_OPERATION} is smaller than expected "
        f"({len(needed)} fragments, {len(document)} chars); this test only exercises the "
        "field-count regression while the document approaches graphql-java's 100k cap"
    )
    return document


def _with_aliased_copies(document: str, copies: int) -> str:
    """Alias the operation's root selection N times, multiplying the normalized field count."""
    split_at = document.index("\nfragment ")
    operation, fragments = document[:split_at], document[split_at:]
    match = re.search(r"\{([\s\S]*)\}\s*$", operation)
    assert match is not None, (
        f"could not locate the body of {_PRODUCTION_SEARCH_OPERATION}; "
        "the operation's shape in the frontend sources changed"
    )
    header, body = operation[: operation.index("{")], match.group(1)
    aliased = "\n".join(
        body.replace("searchAcrossEntities(", f"copy{i}: searchAcrossEntities(", 1)
        for i in range(copies)
    )
    return f"{header}{{\n{aliased}\n}}\n{fragments}"


def test_container_entities_survives_large_query_normalization(auth_session):
    """Regression test for "Maximum field count exceeded. 100001 > 100000".

    ContainerEntitiesResolver used to decide whether it could serve a count-only selection
    by calling environment.getSelectionSet(), which makes graphql-java normalize the whole
    operation. Normalization is capped at 100k fields, so once a Container hit reached this
    resolver from the production search query, the entire request was aborted -- not just
    this field. The resolver now reads its selection from the AST, which is local to the
    field and never normalizes.
    """
    query = _with_aliased_copies(_assemble_production_search_document(), copies=2)
    variables: Dict[str, Any] = {
        "input": {"types": ["CONTAINER"], "query": "*", "start": 0, "count": 5},
        "skipSiblingsSearch": False,
        "skipLineage": False,
    }

    res = execute_graphql(auth_session, query, variables, expect_errors=True)

    messages = [error.get("message", "") for error in res.get("errors") or []]
    assert not any("Maximum field count" in message for message in messages), (
        f"the field-count regression is back: {messages}"
    )
    assert not messages, f"unexpected GraphQL errors: {messages}"
    # The seeded containers are CONTAINER entities, so the search must find at least those.
    assert res["data"]["copy0"]["total"] >= len(_EXPECTED)
