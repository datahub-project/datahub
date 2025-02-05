import pathlib
from unittest.mock import Mock

import pytest

from datahub.errors import SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.sdk.main_client import DataHubClient

_GOLDEN_DIR = pathlib.Path(__file__).parent / "entity_client_goldens"


@pytest.fixture
def mock_graph() -> Mock:
    graph = Mock(spec=DataHubGraph)
    graph.exists.return_value = False
    return graph


@pytest.fixture
def client(mock_graph: Mock) -> DataHubClient:
    return DataHubClient(graph=mock_graph)


def test_client_creation(mock_graph: Mock) -> None:
    assert DataHubClient(graph=mock_graph)
    assert DataHubClient(server="https://example.com", token="token")


def test_client_init_errors(mock_graph: Mock) -> None:
    config = DatahubClientConfig(server="https://example.com", token="token")

    with pytest.raises(SdkUsageError):
        DataHubClient(server="https://example.com", graph=mock_graph)  # type: ignore
    with pytest.raises(SdkUsageError):
        DataHubClient(server="https://example.com", config=config)  # type: ignore
    with pytest.raises(SdkUsageError):
        DataHubClient(config=config, graph=mock_graph)  # type: ignore
    with pytest.raises(SdkUsageError):
        DataHubClient()  # type: ignore
