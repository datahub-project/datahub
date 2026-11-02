import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator, List

import pytest
import requests_mock

from datahub.ingestion.source.boomi.client import BoomiApiError, BoomiClient
from datahub.ingestion.source.boomi.config import BoomiSourceConfig
from datahub.ingestion.source.boomi.report import BoomiSourceReport

ACCOUNT_ID = "acme-AB1CD2"
BASE_URL = f"https://api.boomi.com/api/rest/v1/{ACCOUNT_ID}"


def _client(api_url: str = "https://api.boomi.com") -> BoomiClient:
    config = BoomiSourceConfig.model_validate(
        {
            "account_id": ACCOUNT_ID,
            "username": "svc@example.com",
            "api_token": "secret-token",
            "api_url": api_url,
            "max_requests_per_second": 10,
        }
    )
    return BoomiClient(config, BoomiSourceReport())


def _folder(folder_id: str) -> dict:
    return {"id": folder_id, "name": folder_id, "fullPath": f"Acme/{folder_id}"}


def test_query_follows_query_tokens_until_exhausted() -> None:
    client = _client()
    with requests_mock.Mocker() as mock:
        mock.post(
            f"{BASE_URL}/Folder/query",
            json={"result": [_folder("a")], "queryToken": "token-1"},
        )
        mock.post(
            f"{BASE_URL}/Folder/queryMore",
            [
                {"json": {"result": [_folder("b")], "queryToken": "token-2"}},
                {"json": {"result": [_folder("c")]}},
            ],
        )
        folders = [f.id for f in client.list_folders()]
        history = mock.request_history

    assert folders == ["a", "b", "c"]
    assert [r.text for r in history[1:]] == ["token-1", "token-2"]
    assert all(r.headers["Content-Type"] == "text/plain" for r in history[1:])
    expected_auth = base64.b64encode(
        b"BOOMI_TOKEN.svc@example.com:secret-token"
    ).decode()
    assert history[0].headers["Authorization"] == f"Basic {expected_auth}"
    assert client.report.api_calls == 3


def test_process_query_sends_branch_filter() -> None:
    client = _client()
    with requests_mock.Mocker() as mock:
        mock.post(f"{BASE_URL}/ComponentMetadata/query", json={"result": []})
        assert list(client.list_processes("main")) == []
        body = mock.request_history[0].json()

    nested = body["QueryFilter"]["expression"]["nestedExpression"]
    assert body["QueryFilter"]["expression"]["operator"] == "and"
    assert {"property": "branchName", "operator": "EQUALS", "argument": ["main"]} in (
        nested
    )
    assert {"property": "type", "operator": "EQUALS", "argument": ["process"]} in (
        nested
    )


def test_invalid_record_is_skipped_with_warning() -> None:
    client = _client()
    with requests_mock.Mocker() as mock:
        mock.post(
            f"{BASE_URL}/Folder/query",
            json={"result": [_folder("a"), {"id": "no-name"}]},
        )
        folders = [f.id for f in client.list_folders()]

    assert folders == ["a"]
    assert client.report.invalid_records == 1
    assert len(client.report.warnings) == 1


def test_http_error_carries_status_code() -> None:
    client = _client()
    with requests_mock.Mocker() as mock:
        mock.post(f"{BASE_URL}/DeployedPackage/query", status_code=403, text="denied")
        with pytest.raises(BoomiApiError) as excinfo:
            list(client.list_active_process_deployments())

    assert excinfo.value.status_code == 403


class _ThrottlingHandler(BaseHTTPRequestHandler):
    statuses: List[int] = []

    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        status = self.statuses.pop(0)
        body = json.dumps({"result": [_folder("a")]}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
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


def test_retries_after_503(throttling_server: ThreadingHTTPServer) -> None:
    _ThrottlingHandler.statuses = [503, 200]
    host, port = throttling_server.server_address[:2]
    client = _client(api_url=f"http://{host!s}:{port}")

    assert [f.id for f in client.list_folders()] == ["a"]
    assert _ThrottlingHandler.statuses == []
