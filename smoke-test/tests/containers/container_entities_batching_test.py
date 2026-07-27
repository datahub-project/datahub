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
    ContainerClass,
    ContainerPropertiesClass,
    DatasetPropertiesClass,
)
from tests.utils import execute_graphql, with_test_retry

logger = logging.getLogger(__name__)

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
