import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Iterator, List, Optional

import pytest
import requests_mock
from pydantic import SecretStr, ValidationError

from datahub.ingestion.source.boomi_data_integration.client import (
    BoomiDataIntegrationApiError,
    BoomiDataIntegrationClient,
)
from datahub.ingestion.source.boomi_data_integration.config import (
    BoomiDataIntegrationSourceConfig,
)
from datahub.ingestion.source.boomi_data_integration.report import (
    BoomiDataIntegrationSourceReport,
)

ACCOUNT_ID = "acct-0001"
TOKEN = SecretStr("token-a")


def _config(**overrides: object) -> BoomiDataIntegrationSourceConfig:
    config: Dict[str, object] = {
        "account_id": ACCOUNT_ID,
        "api_token": "token-a",
        "max_requests_per_second": 50,
    }
    config.update(overrides)
    return BoomiDataIntegrationSourceConfig.model_validate(config)


def _client(**overrides: object) -> BoomiDataIntegrationClient:
    return BoomiDataIntegrationClient(
        _config(**overrides), BoomiDataIntegrationSourceReport()
    )


def _river(river_id: str) -> Dict[str, object]:
    return {"river_cross_id": river_id, "name": river_id, "river_type": "actions"}


def _page(items: List[Dict[str, object]], next_page: Optional[str]) -> Dict:
    return {"items": items, "next_page": next_page, "current_page_size": len(items)}


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({}, "https://api.rivery.io"),
        ({"region": "eu"}, "https://api.eu-west-1.rivery.io"),
        ({"region": "il"}, "https://api.il-central-1.rivery.io"),
        ({"region": "au"}, "https://api.ap-southeast-2.rivery.io"),
        (
            {"region": "eu", "api_url": "https://proxy.example.com/"},
            "https://proxy.example.com",
        ),
    ],
)
def test_api_url_resolution(overrides: Dict[str, object], expected: str) -> None:
    client = _client(**overrides)
    assert client.base_url == f"{expected}/v1/accounts/{ACCOUNT_ID}"


@pytest.mark.parametrize(
    "tokens",
    [
        {},
        {"api_token": "a", "environment_tokens": {"env-1": "b"}},
        {"environment_tokens": {}},
    ],
)
def test_config_requires_exactly_one_token_mode(tokens: Dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        BoomiDataIntegrationSourceConfig.model_validate(
            {"account_id": ACCOUNT_ID, **tokens}
        )


def test_config_rejects_non_http_api_url() -> None:
    with pytest.raises(ValidationError):
        _config(api_url="api.rivery.io")


def test_list_rivers_follows_pages_with_bearer_token() -> None:
    client = _client()
    url = f"{client.base_url}/environments/env-1/rivers"
    with requests_mock.Mocker() as mock:
        mock.get(f"{url}?page=1", json=_page([_river("a"), _river("b")], "next"))
        mock.get(f"{url}?page=2", json=_page([_river("c")], None))
        rivers = [r.river_cross_id for r in client.list_rivers("env-1", TOKEN)]
        history = mock.request_history

    assert rivers == ["a", "b", "c"]
    assert [r.qs["items_per_page"] for r in history] == [["500"], ["500"]]
    assert all(r.headers["Authorization"] == "Bearer token-a" for r in history)
    assert client.report.api_calls == 2


def test_invalid_record_is_skipped_with_warning() -> None:
    client = _client()
    with requests_mock.Mocker() as mock:
        mock.get(
            f"{client.base_url}/environments/env-1/rivers",
            json=_page([_river("a"), {"name": "no id"}], None),
        )
        rivers = [r.river_cross_id for r in client.list_rivers("env-1", TOKEN)]

    assert rivers == ["a"]
    assert client.report.invalid_records == 1
    assert len(client.report.warnings) == 1


def test_http_error_carries_status_code() -> None:
    client = _client()
    with requests_mock.Mocker() as mock:
        mock.get(
            f"{client.base_url}/environments/env-1/rivers/r-1",
            status_code=400,
            json={"detail": "bad request"},
        )
        with pytest.raises(BoomiDataIntegrationApiError) as excinfo:
            client.get_river("env-1", "r-1", TOKEN)

    assert excinfo.value.status_code == 400


class _ThrottlingHandler(BaseHTTPRequestHandler):
    statuses: List[int] = []

    def do_GET(self) -> None:
        status = self.statuses.pop(0)
        body = json.dumps(_page([_river("a")], None)).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        # Keeps urllib3's backoff out of the test's runtime.
        self.send_header("Retry-After", "0")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture
def throttling_server() -> Iterator[ThreadingHTTPServer]:
    # requests_mock replaces the transport adapter, so retries need a real socket.
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ThrottlingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()


def test_retries_after_429_and_5xx(throttling_server: ThreadingHTTPServer) -> None:
    _ThrottlingHandler.statuses = [429, 502, 200]
    host, port = throttling_server.server_address[:2]
    client = _client(api_url=f"http://{host!s}:{port}")

    assert [r.river_cross_id for r in client.list_rivers("env-1", TOKEN)] == ["a"]
    assert _ThrottlingHandler.statuses == []
