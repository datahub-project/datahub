import pytest

from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig


@pytest.fixture
def system_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "DATAHUB_GMS_TOKEN",
        "DATAHUB_AUTH_TYPE",
        "DATAHUB_GMS_HOST",
        "DATAHUB_GMS_PORT",
        "DATAHUB_GMS_PROTOCOL",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DATAHUB_SYSTEM_CLIENT_ID", "system")
    monkeypatch.setenv("DATAHUB_SYSTEM_CLIENT_SECRET", "secret")
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://gms:8080")


@pytest.mark.usefixtures("system_credentials")
@pytest.mark.parametrize(
    "server", ["http://gms:8080", "http://gms:8080/", "http://GMS:8080"]
)
def test_system_credentials_sent_to_configured_server(server: str) -> None:
    emitter = DataHubRestEmitter(gms_server=server)
    assert emitter._session.headers.get("Authorization") == "Basic system:secret"


@pytest.mark.usefixtures("system_credentials")
@pytest.mark.parametrize(
    "server", ["http://other:8080", "http://gms:9090", "https://other.example"]
)
def test_system_credentials_not_sent_to_other_server(server: str) -> None:
    emitter = DataHubRestEmitter(gms_server=server)
    assert "Authorization" not in emitter._session.headers


@pytest.mark.usefixtures("system_credentials")
def test_system_credentials_not_sent_without_configured_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATAHUB_GMS_URL")
    emitter = DataHubRestEmitter(gms_server="http://gms:8080")
    assert "Authorization" not in emitter._session.headers


@pytest.mark.usefixtures("system_credentials")
def test_explicit_token_sent_to_any_server() -> None:
    emitter = DataHubRestEmitter(gms_server="http://other:8080", token="tok")
    assert emitter._session.headers.get("Authorization") == "Bearer tok"


@pytest.mark.usefixtures("system_credentials")
def test_server_from_env_gets_system_credentials() -> None:
    emitter = DataHubRestEmitter(gms_server="__from_env__")
    assert emitter._session.headers.get("Authorization") == "Basic system:secret"


@pytest.mark.usefixtures("system_credentials")
def test_graph_follows_the_same_rule() -> None:
    graph = DataHubGraph(DatahubClientConfig(server="http://gms:8080"))
    assert graph._session.headers.get("Authorization") == "Basic system:secret"
    other = DataHubGraph(DatahubClientConfig(server="http://other:8080"))
    assert "Authorization" not in other._session.headers
