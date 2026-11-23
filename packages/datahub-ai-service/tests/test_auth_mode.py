import http.server
import json
import threading
from typing import Iterator, Optional

import pytest

from datahub_ai_service.mcp.auth_mode import GmsAuth, GmsAuthDetector, probe_gms_auth
from datahub_core_mcp_tools.gms_server_info import GmsServerInfo


class _Graphql(http.server.BaseHTTPRequestHandler):
    status = 200
    body: object = {}

    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("content-length", 0)))
        payload = json.dumps(type(self).body).encode()
        self.send_response(type(self).status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_: object) -> None:
        pass


@pytest.fixture
def gms_url() -> Iterator[str]:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Graphql)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


SYSTEM_USER = {
    "data": {"me": {"corpUser": {"urn": "urn:li:corpuser:__datahub_system"}}}
}


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (401, {}, GmsAuth.ON),
        (403, {}, GmsAuth.ON),
        (200, SYSTEM_USER, GmsAuth.OFF),
        # A 200 alone proves nothing: the body has to name a user.
        (200, {"errors": [{"message": "boom"}], "data": None}, GmsAuth.UNKNOWN),
        (200, {"data": {"me": None}}, GmsAuth.UNKNOWN),
        (200, ["not", "an", "object"], GmsAuth.UNKNOWN),
        (500, {}, GmsAuth.UNKNOWN),
    ],
)
def test_probe(gms_url: str, status: int, body: object, expected: GmsAuth) -> None:
    _Graphql.status, _Graphql.body = status, body

    assert probe_gms_auth(gms_url, timeout_seconds=5) is expected


def test_probe_of_an_unreachable_gms() -> None:
    assert probe_gms_auth("http://127.0.0.1:1", timeout_seconds=1) is GmsAuth.UNKNOWN


class _Gms:
    def __init__(self) -> None:
        self.auth_enabled: Optional[bool] = None
        self.down = False
        self.probe_result = GmsAuth.ON
        self.probes = 0
        self.now = 0.0

    def info(self) -> GmsServerInfo:
        if self.down:
            raise ConnectionError("down")
        return GmsServerInfo(
            version="v1.7.0", server_env="core", auth_enabled=self.auth_enabled
        )

    def probe(self, url: str, timeout: float) -> GmsAuth:
        self.probes += 1
        return self.probe_result


@pytest.fixture
def gms() -> _Gms:
    return _Gms()


@pytest.fixture
def detector(gms: _Gms) -> GmsAuthDetector:
    return GmsAuthDetector(
        "http://gms",
        gms.info,
        timeout_seconds=1,
        probe=gms.probe,
        clock=lambda: gms.now,
    )


def test_config_field_wins_over_the_probe(gms: _Gms, detector: GmsAuthDetector) -> None:
    gms.auth_enabled = False

    assert detector.detect() is GmsAuth.OFF
    assert detector.last is GmsAuth.OFF
    assert gms.probes == 0


def test_older_gms_is_probed_once_a_minute(
    gms: _Gms, detector: GmsAuthDetector
) -> None:
    assert detector.detect() is GmsAuth.ON
    gms.probe_result = GmsAuth.OFF
    gms.now = 59
    assert detector.detect() is GmsAuth.ON
    gms.now = 61
    assert detector.detect() is GmsAuth.OFF
    assert gms.probes == 2


def test_unclear_answers_are_asked_again_soon(
    gms: _Gms, detector: GmsAuthDetector
) -> None:
    gms.probe_result = GmsAuth.UNKNOWN
    assert detector.detect() is GmsAuth.UNKNOWN
    gms.probe_result = GmsAuth.ON
    gms.now = 4
    assert detector.detect() is GmsAuth.UNKNOWN
    gms.now = 6
    assert detector.detect() is GmsAuth.ON
    assert gms.probes == 2


def test_unreadable_config_is_unknown(gms: _Gms, detector: GmsAuthDetector) -> None:
    gms.auth_enabled = True
    assert detector.detect() is GmsAuth.ON
    gms.down = True

    assert detector.detect() is GmsAuth.UNKNOWN
    assert detector.last is GmsAuth.UNKNOWN
