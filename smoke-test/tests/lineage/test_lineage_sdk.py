import logging
import time
from typing import Dict, Generator, List, Optional, Set

import pytest

from datahub.configuration.common import GraphError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sdk.dataset import Dataset
from datahub.sdk.lineage_client import LineageResult
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_filters import Filter, FilterDsl as F
from tests.utilities.domains import Domain
from tests.utils import get_sleep_info, wait_for_writes_to_sync

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)


def _wait_for_downstream_lineage(
    test_client: DataHubClient,
    upstream_urn: str,
    expected_urns: Set[str],
) -> None:
    """Poll until table-level downstream lineage is visible in the graph index."""
    sleep_sec, sleep_times = get_sleep_info()
    last_urns: Set[str] = set()
    for attempt in range(sleep_times):
        try:
            results = test_client.lineage.get_lineage(
                source_urn=upstream_urn,
                direction="downstream",
                max_hops=3,
            )
            last_urns = {r.urn for r in results}
        except GraphError:
            last_urns = set()
        if expected_urns <= last_urns:
            return
        if attempt < sleep_times - 1:
            time.sleep(sleep_sec)
    raise AssertionError(
        f"Downstream lineage for {upstream_urn} did not include "
        f"{sorted(expected_urns)} (last={sorted(last_urns)})"
    )


def _column_paths_match(
    results: List[LineageResult], expected_path_lens: Dict[str, int]
) -> bool:
    if len(results) != len(expected_path_lens):
        return False
    by_urn = {result.urn: result for result in results}
    if set(by_urn) != set(expected_path_lens):
        return False
    for urn, paths_len in expected_path_lens.items():
        paths = by_urn[urn].paths
        if paths is None or len(paths) != paths_len:
            return False
    return True


def _path_lens(results: List[LineageResult]) -> Dict[str, Optional[int]]:
    return {
        result.urn: None if result.paths is None else len(result.paths)
        for result in results
    }


def _wait_for_column_lineage(
    test_client: DataHubClient, datasets: Dict[str, Dataset]
) -> None:
    """Poll until column paths match what the column-lineage tests assert.

    Table edges become searchable before column path lengths, so waiting only
    for downstream dataset URNs still leaves those asserts racing the graph index.
    """
    upstream = str(datasets["upstream"].urn)
    field_urn = str(SchemaFieldUrn(datasets["upstream"].urn, "id"))
    expected = {
        str(datasets["downstream1"].urn): 2,
        str(datasets["downstream2"].urn): 3,
        str(datasets["downstream3"].urn): 4,
    }
    mysql_only = {str(datasets["downstream3"].urn): 4}
    mysql_filter: Filter = F.and_(F.platform("mysql"), F.entity_type("dataset"))
    sleep_sec, sleep_times = get_sleep_info()
    last_error: Optional[GraphError] = None
    last_summary = ""
    for attempt in range(sleep_times):
        try:
            unfiltered = test_client.lineage.get_lineage(
                source_urn=upstream,
                source_column="id",
                direction="downstream",
                max_hops=3,
            )
            from_field = test_client.lineage.get_lineage(
                source_urn=field_urn,
                direction="downstream",
                max_hops=3,
            )
            filtered = test_client.lineage.get_lineage(
                source_urn=upstream,
                source_column="id",
                direction="downstream",
                max_hops=3,
                filter=mysql_filter,
            )
            last_error = None
            if (
                _column_paths_match(unfiltered, expected)
                and _column_paths_match(from_field, expected)
                and _column_paths_match(filtered, mysql_only)
            ):
                return
            last_summary = (
                f"column={_path_lens(unfiltered)} field={_path_lens(from_field)} "
                f"mysql={_path_lens(filtered)}"
            )
        except GraphError as exc:
            last_error = exc
            last_summary = ""
            logger.warning("Column lineage query failed during wait; retrying: %s", exc)
        if attempt < sleep_times - 1:
            time.sleep(sleep_sec)

    msg = f"Column lineage paths for {upstream} were not visible ({last_summary})"
    if last_error is not None:
        msg = f"{msg}; last error: {last_error}"
    raise AssertionError(msg)


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

    for entity in datasets.values():
        test_client._graph.delete_entity(str(entity.urn), hard=True)
    for entity in datasets.values():
        test_client.entities.upsert(entity)

    # Add lineage
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

    expected_downstream = {
        str(datasets["downstream1"].urn),
        str(datasets["downstream2"].urn),
        str(datasets["downstream3"].urn),
    }
    _wait_for_downstream_lineage(
        test_client,
        str(datasets["upstream"].urn),
        expected_downstream,
    )
    _wait_for_column_lineage(test_client, datasets)

    yield datasets

    # Cleanup
    for entity in datasets.values():
        try:
            test_client._graph.delete_entity(str(entity.urn), hard=True)
        except Exception as e:
            raise Exception(f"Could not delete entity {entity.urn}: {e}")


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
    table_lineage_results = test_client.lineage.get_lineage(
        source_urn=str(test_datasets["upstream"].urn),
        direction="downstream",
        max_hops=3,
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
    column_lineage_results = test_client.lineage.get_lineage(
        source_urn=str(test_datasets["upstream"].urn),
        source_column="id",
        direction="downstream",
        max_hops=3,
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
    filtered_column_lineage_results = test_client.lineage.get_lineage(
        source_urn=str(test_datasets["upstream"].urn),
        source_column="id",
        direction="downstream",
        max_hops=3,
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
    column_lineage_results = test_client.lineage.get_lineage(
        source_urn=str(source_schema_field), direction="downstream", max_hops=3
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
