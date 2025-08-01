from unittest.mock import Mock

import pytest

from datahub.errors import SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient


@pytest.fixture
def mock_graph() -> Mock:
    graph = Mock(spec=DataHubGraph)
    graph.exists.return_value = False
    return graph


@pytest.fixture
def client(mock_graph: Mock) -> DataHubClient:
    return DataHubClient(graph=mock_graph)


def test_use_assertions_client_fails_if_not_installed(
    client: DataHubClient, mock_graph: Mock
) -> None:
    mock_graph.exists.return_value = True

    with pytest.raises(
        SdkUsageError,
        match="AssertionsClient is not installed, please install it with `pip install acryl-datahub-cloud`",
    ):
        client.assertions.get_assertions(urn="urn:li:assertion:123")
