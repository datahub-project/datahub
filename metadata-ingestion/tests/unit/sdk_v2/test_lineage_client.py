import pathlib
from typing import Dict, List
from unittest.mock import Mock

import pytest

from datahub.sdk.lineage_client import LineageClient
from datahub.sdk.main_client import DataHubClient
from tests.test_helpers import mce_helpers

_GOLDEN_DIR = pathlib.Path(__file__).parent / "lineage_client_golden"
_GOLDEN_DIR.mkdir(exist_ok=True)


@pytest.fixture
def mock_graph() -> Mock:
    graph = Mock()
    return graph


@pytest.fixture
def client(mock_graph: Mock) -> DataHubClient:
    return DataHubClient(graph=mock_graph)


def assert_client_golden(client: DataHubClient, golden_path: pathlib.Path) -> None:
    mcps = client._graph.emit_mcps.call_args[0][0]  # type: ignore

    mce_helpers.check_goldens_stream(
        outputs=mcps,
        golden_path=golden_path,
        ignore_order=False,
    )


def test_add_dataset_transform_lineage_basic(client: DataHubClient) -> None:
    """Test basic lineage without column mapping or query."""
    lineage_client = LineageClient(client=client)

    # Basic lineage test
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"

    lineage_client.add_dataset_transform_lineage(
        upstream=upstream,
        downstream=downstream,
    )
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_basic_golden.json")


def test_add_dataset_transform_lineage_complete(client: DataHubClient) -> None:
    """Test complete lineage with column mapping and query."""
    lineage_client = LineageClient(client=client)

    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"
    query_text = (
        "SELECT us_col1 as ds_col1, us_col2 + us_col3 as ds_col2 FROM upstream_table"
    )
    column_lineage: Dict[str, List[str]] = {
        "ds_col1": ["us_col1"],  # Simple 1:1 mapping
        "ds_col2": ["us_col2", "us_col3"],  # 2:1 mapping
    }

    lineage_client.add_dataset_transform_lineage(
        upstream=upstream,
        downstream=downstream,
        query_text=query_text,
        column_lineage=column_lineage,
    )
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_complete_golden.json")
