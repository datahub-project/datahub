from unittest.mock import Mock

import pytest

from datahub.errors import ItemNotFoundError, MultipleItemsFoundError, SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk.main_client import DataHubClient


@pytest.fixture
def mock_graph() -> Mock:
    graph = Mock(spec=DataHubGraph)
    graph.exists.return_value = False
    return graph


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


def test_resolve_user(mock_graph: Mock) -> None:
    client = DataHubClient(graph=mock_graph)

    # This test doesn't really validate the graphql query or vars.
    # It probably makes more sense to test via smoke-tests.

    mock_graph.get_urns_by_filter.return_value = []
    with pytest.raises(ItemNotFoundError):
        client.resolve.user(name="User")

    mock_graph.get_urns_by_filter.return_value = ["urn:li:corpuser:user"]
    assert client.resolve.user(name="User") == CorpUserUrn("urn:li:corpuser:user")

    mock_graph.get_urns_by_filter.return_value = [
        "urn:li:corpuser:user",
        "urn:li:corpuser:user2",
    ]
    with pytest.raises(MultipleItemsFoundError):
        client.resolve.user(name="User")
