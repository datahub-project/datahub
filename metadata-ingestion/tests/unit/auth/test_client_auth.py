import pytest

from datahub.cli.datapack.loader import _build_datapack_sink_config
from datahub.configuration.common import ConfigurationError
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.emitter.token_provider import StaticTokenProvider, TokenProviderAuth
from datahub.ingestion.auth.registry import AuthConfig
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.sink.datahub_rest import DatahubRestSink, DatahubRestSinkConfig


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


def test_rest_sink_emitter_carries_auth():
    # Regression: DatahubRestSink._make_emitter must forward config.auth to the
    # emitter, or the default ingestion sink silently emits unauthenticated in
    # OAuth mode even though the sink config carries a resolved auth provider.
    auth = AuthConfig(type="static", config={"token": "t"})
    sink_config = DatahubRestSinkConfig(server="http://gms:8080", auth=auth)
    emitter = DatahubRestSink._make_emitter(
        sink_config, auth=DatahubRestSink._resolve_auth(sink_config)
    )
    session_auth = emitter._session.auth
    assert isinstance(session_auth, TokenProviderAuth)
    # Assert the token from *this* config reached the emitter, not merely that
    # some TokenProviderAuth was installed (which would pass even if the config
    # were ignored).
    assert session_auth._provider.get_token().token == "t"


def test_datapack_sink_config_carries_auth():
    # Regression: the datapack loader builds its own sink config from the client
    # config; dropping auth there made demo-data / datapack loads emit
    # unauthenticated in OAuth mode.
    auth = AuthConfig(type="static", config={"token": "t"})
    client_config = DatahubClientConfig(server="http://gms:8080", auth=auth)
    sink_config = _build_datapack_sink_config(client_config)
    assert sink_config["auth"] is client_config.auth
    assert "token" not in sink_config


def test_from_emitter_carries_session_auth():
    # Regression: DataHubGraph.from_emitter rebuilds the client config with only
    # the static token; the resolved requests auth object must be carried onto
    # the new graph's session or an OAuth emitter's graph loses credentials.
    auth = TokenProviderAuth(StaticTokenProvider("tok"))
    emitter = DataHubRestEmitter("http://gms:8080", auth=auth)
    graph = DataHubGraph.from_emitter(emitter)
    assert graph._session.auth is auth
