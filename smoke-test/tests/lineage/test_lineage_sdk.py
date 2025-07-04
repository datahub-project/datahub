import logging
import time
from typing import Dict, Generator, Literal, Optional

import pytest

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sdk.dataset import Dataset
from datahub.sdk.lineage_client import LineageResult
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_filters import Filter, FilterDsl as F
from tests.utils import wait_for_writes_to_sync

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def test_client(graph_client: DataHubGraph) -> DataHubClient:
    return DataHubClient(graph=graph_client)


@pytest.fixture(scope="module")
def test_datasets(
    test_client: DataHubClient,
) -> Generator[Dict[str, Dataset], None, None]:
    datasets = {
        "upstream": Dataset(
            platform="snowflake",
            name="test_lineage_upstream_001",
            schema=[("name", "string"), ("id", "int")],
        ),
        "downstream1": Dataset(
            platform="snowflake",
            name="test_lineage_downstream_001",
            schema=[("name", "string"), ("id", "int")],
        ),
        "downstream2": Dataset(
            platform="snowflake",
            name="test_lineage_downstream_002",
            schema=[("name", "string"), ("id", "int")],
        ),
        "downstream3": Dataset(
            platform="mysql",
            name="test_lineage_downstream_003",
            schema=[("name", "string"), ("id", "int")],
        ),
    }
    try:
        for entity in datasets.values():
            test_client._graph.delete_entity(str(entity.urn), hard=True)
        for entity in datasets.values():
            test_client.entities.upsert(entity)
        test_client.lineage.add_lineage(
            upstream=str(datasets["upstream"].urn),
            downstream=str(datasets["downstream1"].urn),
            column_lineage=True,
        )
        test_client.lineage.add_lineage(
            upstream=str(datasets["downstream1"].urn),
            downstream=str(datasets["downstream2"].urn),
            column_lineage=True,
        )
        test_client.lineage.add_lineage(
            upstream=str(datasets["downstream2"].urn),
            downstream=str(datasets["downstream3"].urn),
            column_lineage=True,
        )
        wait_for_writes_to_sync()
        time.sleep(5)

        yield datasets

    finally:
        for entity in datasets.values():
            try:
                test_client._graph.delete_entity(str(entity.urn), hard=True)
            except Exception as e:
                logger.warning(f"Could not delete entity {entity.urn}: {e}")


def robust_lineage_retrieval(
    test_client: DataHubClient,
    source_urn: str,
    source_column: Optional[str],
    direction: Literal["downstream", "upstream"] = "downstream",
    max_hops: int = 3,
    filter: Optional[Filter] = None,
    max_retries: int = 5,
) -> list[LineageResult]:
    for attempt in range(max_retries):
        try:
            lineage_results = test_client.lineage.get_lineage(
                source_urn=source_urn,
                source_column=source_column,
                direction=direction,
                max_hops=max_hops,
                filter=filter,
            )

            logger.info(
                f"Attempt {attempt + 1}: Retrieved {len(lineage_results)} lineage results"
            )

            return lineage_results

        except Exception as e:
            logger.warning(f"Lineage retrieval attempt {attempt + 1} failed: {e}")
            time.sleep(10)  # Give time for lineage propagation

    raise AssertionError(
        f"Failed to retrieve lineage results after {max_retries} attempts. "
        "Possible synchronization or infrastructure issues."
    )


def validate_lineage_results(
    lineage_result: LineageResult,
    hops=None,
    direction=None,
    platform=None,
    urn=None,
    paths_len=None,
):
    if hops is not None:
        assert lineage_result.hops == hops
    if direction is not None:
        assert lineage_result.direction == direction
    if platform is not None:
        assert lineage_result.platform == platform
    if urn is not None:
        assert lineage_result.urn == urn
    if paths_len is not None and lineage_result.paths is not None:
        assert len(lineage_result.paths) == paths_len


def test_table_level_lineage(
    test_client: DataHubClient, test_datasets: Dict[str, Dataset]
):
    table_lineage_results = robust_lineage_retrieval(
        test_client, source_urn=str(test_datasets["upstream"].urn), source_column=None
    )
    assert len(table_lineage_results) == 3
    urns = {r.urn for r in table_lineage_results}
    expected = {
        str(test_datasets["downstream1"].urn),
        str(test_datasets["downstream2"].urn),
        str(test_datasets["downstream3"].urn),
    }
    assert urns == expected

    table_lineage_results = sorted(table_lineage_results, key=lambda x: x.hops)
    validate_lineage_results(
        table_lineage_results[0],
        hops=1,
        platform="snowflake",
        urn=str(test_datasets["downstream1"].urn),
        paths_len=0,
    )
    validate_lineage_results(
        table_lineage_results[1],
        hops=2,
        platform="snowflake",
        urn=str(test_datasets["downstream2"].urn),
        paths_len=0,
    )
    validate_lineage_results(
        table_lineage_results[2],
        hops=3,
        platform="mysql",
        urn=str(test_datasets["downstream3"].urn),
        paths_len=0,
    )


def test_column_level_lineage(
    test_client: DataHubClient, test_datasets: Dict[str, Dataset]
):
    column_lineage_results = robust_lineage_retrieval(
        test_client, source_urn=str(test_datasets["upstream"].urn), source_column="id"
    )
    assert len(column_lineage_results) == 3
    column_lineage_results = sorted(column_lineage_results, key=lambda x: x.hops)
    validate_lineage_results(
        column_lineage_results[0],
        hops=1,
        urn=str(test_datasets["downstream1"].urn),
        paths_len=2,
    )
    validate_lineage_results(
        column_lineage_results[1],
        hops=2,
        urn=str(test_datasets["downstream2"].urn),
        paths_len=3,
    )
    validate_lineage_results(
        column_lineage_results[2],
        hops=3,
        urn=str(test_datasets["downstream3"].urn),
        paths_len=4,
    )


def test_filtered_column_level_lineage(
    test_client: DataHubClient, test_datasets: Dict[str, Dataset]
):
    filtered_column_lineage_results = robust_lineage_retrieval(
        test_client,
        source_urn=str(test_datasets["upstream"].urn),
        source_column="id",
        filter=F.and_(F.platform("mysql"), F.entity_type("dataset")),
    )
    assert len(filtered_column_lineage_results) == 1
    validate_lineage_results(
        filtered_column_lineage_results[0],
        hops=3,
        platform="mysql",
        urn=str(test_datasets["downstream3"].urn),
        paths_len=4,
    )


def test_column_level_lineage_from_schema_field(
    test_client: DataHubClient, test_datasets: Dict[str, Dataset]
):
    source_schema_field = SchemaFieldUrn(test_datasets["upstream"].urn, "id")

    column_lineage_results = robust_lineage_retrieval(
        test_client, source_urn=str(source_schema_field), source_column=None
    )
    assert len(column_lineage_results) == 3
    column_lineage_results = sorted(column_lineage_results, key=lambda x: x.hops)
    validate_lineage_results(
        column_lineage_results[0],
        hops=1,
        urn=str(test_datasets["downstream1"].urn),
        paths_len=2,
    )
    validate_lineage_results(
        column_lineage_results[1],
        hops=2,
        urn=str(test_datasets["downstream2"].urn),
        paths_len=3,
    )
    validate_lineage_results(
        column_lineage_results[2],
        hops=3,
        urn=str(test_datasets["downstream3"].urn),
        paths_len=4,
    )
