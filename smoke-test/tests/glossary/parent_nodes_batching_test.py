import logging
import os
import tempfile
import uuid
from typing import Any, Dict, List

import pytest

from conftest import _ingest_cleanup_data_impl
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.metadata.schema_classes import GlossaryNodeInfoClass, GlossaryTermInfoClass
from tests.utils import execute_graphql, with_test_retry

logger = logging.getLogger(__name__)

# Unique per run so the hierarchy is deterministic regardless of instance history.
_RUN_ID = uuid.uuid4().hex[:8]

# `parentNodes` resolves through a request-scoped DataLoader: several terms in one request share a
# single ancestor fetch. The risks are all about that sharing — chains applied to the wrong term,
# hierarchy order lost in the shared response map, and one term's failure spreading to the others.

_DEPTH = 3
# Terms per leaf node. >1 is the point: siblings share an entire ancestor chain, so a correct
# implementation fetches those ancestors once for all of them.
_TERMS_PER_LEAF = 4


def _node(level: int, idx: int) -> str:
    return f"urn:li:glossaryNode:pnb_{_RUN_ID}_l{level}_{idx}"


def _term(leaf: int, i: int) -> str:
    return f"urn:li:glossaryTerm:pnb_{_RUN_ID}_leaf{leaf}_t{i}"


# Two independent chains, so a chain leaking across terms is visible.
_CHAINS = {
    0: [_node(2, 0), _node(1, 0), _node(0, 0)],
    1: [_node(2, 1), _node(1, 1), _node(0, 1)],
}


class _FileEmitter:
    def __init__(self, filename: str) -> None:
        self.sink: FileSink = FileSink(
            ctx=PipelineContext(run_id="parent_nodes_batching"),
            config=FileSinkConfig(filename=filename),
        )

    def emit(self, event) -> None:
        self.sink.write_record_async(
            record_envelope=RecordEnvelope(record=event, metadata={}),
            write_callback=NoopWriteCallback(),
        )

    def close(self) -> None:
        self.sink.close()


def _build_test_data(filename: str) -> None:
    mcps: List[MetadataChangeProposalWrapper] = []

    for branch in (0, 1):
        # Root has no parent; each deeper level points at the one above it.
        for level in range(_DEPTH):
            parent = _node(level - 1, branch) if level > 0 else None
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=_node(level, branch),
                    aspect=GlossaryNodeInfoClass(
                        name=f"pnb_{_RUN_ID}_l{level}_{branch}",
                        definition="parent nodes batching",
                        parentNode=parent,
                    ),
                )
            )
        for i in range(_TERMS_PER_LEAF):
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=_term(branch, i),
                    aspect=GlossaryTermInfoClass(
                        name=f"pnb_{_RUN_ID}_leaf{branch}_t{i}",
                        definition="parent nodes batching",
                        termSource="INTERNAL",
                        parentNode=_node(_DEPTH - 1, branch),
                    ),
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
            auth_session, graph_client, filename, "parent_nodes_batching"
        )
    finally:
        os.remove(filename)


def _parent_nodes_query(urns: List[str], operation: str) -> str:
    fields = "\n  ".join(
        f'''t{i}: glossaryTerm(urn: "{urn}") {{
      urn
      parentNodes {{ count nodes {{ urn }} }}
    }}'''
        for i, urn in enumerate(urns)
    )
    return f"query {operation} {{\n  {fields}\n}}"


def _fetch(auth_session, urns: List[str], operation: str) -> Dict[str, List[str]]:
    res = execute_graphql(auth_session, _parent_nodes_query(urns, operation), {})
    data = res["data"]
    out: Dict[str, List[str]] = {}
    for i in range(len(urns)):
        entry = data[f"t{i}"]
        assert entry is not None, f"glossaryTerm({urns[i]}) resolved to null"
        out[entry["urn"]] = [n["urn"] for n in entry["parentNodes"]["nodes"]]
    return out


def test_parent_nodes_batched_chains_stay_with_their_term(auth_session):
    """Terms from two branches in one request must each get their own chain, in order."""
    urns = [_term(b, i) for i in range(_TERMS_PER_LEAF) for b in (0, 1)]

    @with_test_retry()
    def check() -> None:
        actual = _fetch(auth_session, urns, "parentNodesInterleavedBranches")
        for branch in (0, 1):
            for i in range(_TERMS_PER_LEAF):
                got = actual[_term(branch, i)]
                assert got == _CHAINS[branch], (
                    f"{_term(branch, i)} expected {_CHAINS[branch]}, got {got}"
                )

    check()


def test_parent_nodes_batched_matches_one_per_request(auth_session):
    """The batched answer must equal resolving each term in its own request."""
    urns = [_term(b, i) for i in range(_TERMS_PER_LEAF) for b in (0, 1)]

    @with_test_retry()
    def check() -> None:
        batched = _fetch(auth_session, urns, "parentNodesBatchedForCompare")
        single: Dict[str, List[str]] = {}
        for i, urn in enumerate(urns):
            single.update(_fetch(auth_session, [urn], f"parentNodesSingle{i}"))
        assert batched == single

    check()


def test_parent_nodes_duplicate_terms_in_one_request(auth_session):
    """A term repeated in one request is deduplicated for the fetch but every alias still answers."""
    urns = [_term(0, 0), _term(1, 0), _term(0, 0)]

    @with_test_retry()
    def check() -> None:
        res = execute_graphql(
            auth_session, _parent_nodes_query(urns, "parentNodesDuplicates"), {}
        )
        data = res["data"]
        chains = [
            [n["urn"] for n in data[f"t{i}"]["parentNodes"]["nodes"]]
            for i in range(len(urns))
        ]
        assert chains == [_CHAINS[0], _CHAINS[1], _CHAINS[0]]

    check()


def test_parent_nodes_full_depth_resolved(auth_session):
    """Every level of the chain must come back, not just the immediate parent."""
    variables: Dict[str, Any] = {"urn": _term(0, 0)}
    query = """
        query parentNodesDepth($urn: String!) {
          glossaryTerm(urn: $urn) { parentNodes { count nodes { urn } } }
        }
    """

    @with_test_retry()
    def check() -> None:
        res = execute_graphql(auth_session, query, variables)
        parents = res["data"]["glossaryTerm"]["parentNodes"]
        assert parents["count"] == _DEPTH
        assert [n["urn"] for n in parents["nodes"]] == _CHAINS[0]

    check()
