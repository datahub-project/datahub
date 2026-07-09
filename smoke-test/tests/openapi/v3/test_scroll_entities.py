"""Smoke tests for the scroll_entities DataHubGraph method."""

import logging
from typing import cast

import pytest

from conftest import _ingest_cleanup_data_impl
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RawSearchFilter
from datahub.ingestion.graph.openapi import SortCriterionDict
from datahub.metadata.schema_classes import DatasetKeyClass
from tests.utils import with_test_retry

logger = logging.getLogger(__name__)

PLATFORM = "urn:li:dataPlatform:scrolltest"
ALPHA = "urn:li:dataset:(urn:li:dataPlatform:scrolltest,alpha,PROD)"
BETA = "urn:li:dataset:(urn:li:dataPlatform:scrolltest,beta,PROD)"
GAMMA = "urn:li:dataset:(urn:li:dataPlatform:scrolltest,gamma,PROD)"
DELTA = "urn:li:dataset:(urn:li:dataPlatform:scrolltest,delta,PROD)"
EPSILON = "urn:li:dataset:(urn:li:dataPlatform:scrolltest,epsilon,PROD)"
ZETA = "urn:li:dataset:(urn:li:dataPlatform:scrolltest,zeta,PROD)"
MODEL_1 = "urn:li:mlModel:(urn:li:dataPlatform:scrolltest,model1,PROD)"
MODEL_2 = "urn:li:mlModel:(urn:li:dataPlatform:scrolltest,model2,PROD)"

ALL_DATASET_URNS = {ALPHA, BETA, GAMMA, DELTA, EPSILON, ZETA}
ALL_MODEL_URNS = {MODEL_1, MODEL_2}

SCROLLTEST_FILTER: RawSearchFilter = [
    {"and": [{"field": "platform", "values": [PLATFORM], "condition": "EQUAL"}]}
]

_DATA_FILE = "tests/openapi/v3/data/scroll_test_data.json"


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client, _DATA_FILE, "scroll"
    )


def test_scroll_entities_basic(graph_client: DataHubGraph) -> None:
    result = graph_client.scroll_entities(
        entity_names=["dataset"],
        filter=SCROLLTEST_FILTER,
        aspects=["datasetKey"],
        count=5,
    )
    assert isinstance(result.total_count, int)
    assert result.total_count == 6
    assert len(result.entities) == 5
    assert result.scroll_id is not None
    assert isinstance(result.entities, dict)
    logger.info(
        f"scroll_entities basic: total_count={result.total_count}, returned={len(result.entities)}"
    )


def test_scroll_entities_by_entity_type(graph_client: DataHubGraph) -> None:
    # Filter by "dataset" — should return only our 6 datasets, not the 2 ML models.
    datasets = graph_client.scroll_entities(
        entity_names=["dataset"],
        filter=SCROLLTEST_FILTER,
        aspects=["datasetKey"],
        count=10,
    )
    assert set(datasets.entities.keys()) == ALL_DATASET_URNS
    for urn, aspects in datasets.entities.items():
        assert urn.startswith("urn:li:dataset:")
        raw_aspect, _ = aspects["datasetKey"]
        dataset_key_aspect = cast(DatasetKeyClass, raw_aspect)
        assert dataset_key_aspect.platform == PLATFORM
        assert dataset_key_aspect.name

    # Filter by "mlModel" — should return only our 2 ML models, not the datasets.
    models = graph_client.scroll_entities(
        entity_names=["mlModel"],
        filter=SCROLLTEST_FILTER,
        aspects=["mlModelKey"],
        count=10,
    )
    assert set(models.entities.keys()) == ALL_MODEL_URNS
    for urn in models.entities:
        assert urn.startswith("urn:li:mlModel:")

    logger.info(
        f"scroll_entities by entity type: {len(datasets.entities)} datasets, "
        f"{len(models.entities)} ML models"
    )


def test_scroll_entities_by_multiple_entity_types(graph_client: DataHubGraph) -> None:
    """Requesting entity_names=["dataset","mlModel"] with the shared platform filter should
    return exactly our 6 datasets + 2 ML models."""
    result = graph_client.scroll_entities(
        entity_names=["dataset", "mlModel"],
        aspects=["datasetKey", "mlModelKey"],
        filter=SCROLLTEST_FILTER,
        count=20,
    )
    assert set(result.entities.keys()) == ALL_DATASET_URNS | ALL_MODEL_URNS
    for urn in result.entities:
        assert urn.startswith("urn:li:dataset:") or urn.startswith("urn:li:mlModel:")
    logger.info(
        f"scroll_entities multiple types: {len(result.entities)} scrolltest entities "
        f"({len(ALL_DATASET_URNS)} datasets + {len(ALL_MODEL_URNS)} ML models)"
    )


def test_scroll_entities_filter_is_applied(graph_client: DataHubGraph) -> None:
    """Verify the filter is actually sent in the correct shape and applied server-side.

    An impossible filter should return 0 results. If the filter shape is wrong and
    silently ignored by the server, total_count will be > 0 and the test will fail.
    """
    impossible_filter: RawSearchFilter = [
        {
            "and": [
                {
                    "field": "platform",
                    "values": ["urn:li:dataPlatform:__nonexistent__"],
                    "condition": "EQUAL",
                }
            ]
        }
    ]
    result = graph_client.scroll_entities(
        entity_names=["dataset"],
        aspects=["datasetKey"],
        count=10,
        filter=impossible_filter,
    )
    assert result.total_count == 0, (
        f"Impossible filter should return no results, got {result.total_count}"
    )
    logger.info(
        "scroll_entities filter is applied: impossible filter returned 0 results"
    )


def test_scroll_entities_pagination(graph_client: DataHubGraph) -> None:
    first = graph_client.scroll_entities(
        filter=SCROLLTEST_FILTER,
        entity_names=["dataset"],
        aspects=["datasetKey"],
        count=3,
    )
    assert len(first.entities) == 3
    assert first.scroll_id is not None

    second = graph_client.scroll_entities(
        filter=SCROLLTEST_FILTER,
        entity_names=["dataset"],
        aspects=["datasetKey"],
        count=3,
        scroll_id=first.scroll_id,
    )
    assert len(second.entities) == 3
    assert set(first.entities).isdisjoint(set(second.entities)), (
        "Second page should return different URNs than the first"
    )
    assert set(first.entities) | set(second.entities) == ALL_DATASET_URNS
    logger.info(
        "scroll_entities pagination: two pages of 3 cover all 6 scrolltest URNs"
    )


def test_scroll_entities_with_sort_criteria(graph_client: DataHubGraph) -> None:
    criteria: list[SortCriterionDict] = [{"field": "urn", "order": "ASCENDING"}]
    result = graph_client.scroll_entities(
        entity_names=["dataset"],
        filter=SCROLLTEST_FILTER,
        aspects=["datasetKey"],
        count=10,
        sort_criteria=criteria,
    )
    assert set(result.entities.keys()) == ALL_DATASET_URNS
    urns = list(result.entities)
    assert urns == sorted(urns), "Results should be sorted ascending by URN"
    logger.info(
        f"scroll_entities sort_criteria: {len(urns)} scrolltest datasets in URN order"
    )


def test_scroll_entities_with_query(graph_client: DataHubGraph) -> None:
    """query= narrows results by full-text search; searching for "alpha" within the
    scrolltest platform should include ALPHA and exclude unrelated datasets."""

    @with_test_retry(max_attempts=10)
    def _assert() -> None:
        result = graph_client.scroll_entities(
            entity_names=["dataset"],
            filter=SCROLLTEST_FILTER,
            aspects=["datasetKey"],
            query="alpha",
            count=10,
        )
        assert result.total_count > 0
        assert ALPHA in result.entities, (
            f"ALPHA should appear in query='alpha' results, got: {set(result.entities)}"
            f"\ntotal_count={result.total_count}"
        )
        logger.info(
            f"scroll_entities with_query: query='alpha' returned {result.total_count} result(s)"
        )

    _assert()


def test_scroll_entities_with_system_metadata(graph_client: DataHubGraph) -> None:
    """with_system_metadata=True should populate the second element of each aspect tuple."""
    result = graph_client.scroll_entities(
        entity_names=["dataset"],
        filter=SCROLLTEST_FILTER,
        aspects=["datasetKey"],
        count=10,
        with_system_metadata=True,
    )
    assert set(result.entities.keys()) == ALL_DATASET_URNS
    for urn, aspects in result.entities.items():
        aspect, sys_meta = aspects["datasetKey"]
        assert sys_meta is not None, (
            f"system_metadata should be populated for {urn} when with_system_metadata=True"
        )
        assert sys_meta.runId, f"system_metadata.runId should be set for {urn}"
    logger.info(
        "scroll_entities with_system_metadata: all aspects have system metadata"
    )


def test_scroll_entities_parallel_slicing(graph_client: DataHubGraph) -> None:
    """slice_id/slice_max splits the index into non-overlapping shards; the union
    of all slices should cover the full set of scrolltest datasets."""
    slice0 = graph_client.scroll_entities(
        entity_names=["dataset"],
        filter=SCROLLTEST_FILTER,
        aspects=["datasetKey"],
        count=10,
        slice_id=0,
        slice_max=2,
    )
    slice1 = graph_client.scroll_entities(
        entity_names=["dataset"],
        filter=SCROLLTEST_FILTER,
        aspects=["datasetKey"],
        count=10,
        slice_id=1,
        slice_max=2,
    )
    combined = set(slice0.entities) | set(slice1.entities)
    assert set(slice0.entities).isdisjoint(set(slice1.entities)), (
        "Slices should be non-overlapping"
    )
    assert combined == ALL_DATASET_URNS, (
        f"Union of all slices should equal ALL_DATASET_URNS, got: {combined}"
    )
    logger.info(
        f"scroll_entities parallel_slicing: slice0={len(slice0.entities)}, "
        f"slice1={len(slice1.entities)}, combined={len(combined)}"
    )
