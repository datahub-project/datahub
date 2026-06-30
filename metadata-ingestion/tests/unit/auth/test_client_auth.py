import pytest

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig


def test_graph_builds_session_auth_from_auth_config():
    config = DatahubClientConfig(
        server="http://gms",
        auth={"type": "static", "config": {"token": "abc"}},
    )
    graph = DataHubGraph(config)
    assert graph._session.auth is not None
    assert "Authorization" not in graph._session.headers


def test_token_and_auth_mutually_exclusive():
    with pytest.raises((ConfigurationError, ValueError)):
        DatahubClientConfig(
            server="http://gms",
            token="x",
            auth={"type": "static", "config": {"token": "y"}},
        )
