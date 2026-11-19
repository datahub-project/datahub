import http.server
import threading
import time
from typing import Iterator

import pytest
import requests
from datahub.configuration.common import OperationalError

from datahub_ai_service.mcp.client_factory import (
    CallerClientFactory,
    ToolDeadlineExceeded,
)


class _Gms(http.server.BaseHTTPRequestHandler):
    """Answers each POST from a script of (status, delay) and counts requests."""

    script: list[tuple[int, float]] = []
    requests = 0

    def do_POST(self) -> None:
        cls = type(self)
        status, delay = cls.script[min(cls.requests, len(cls.script) - 1)]
        cls.requests += 1
        self.rfile.read(int(self.headers.get("content-length", 0)))
        time.sleep(delay)
        body = b'{"data": {"me": {"corpUser": {"urn": "urn:li:corpuser:a"}}}}'
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        if status == 429:
            self.send_header("retry-after", "0")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_: object) -> None:
        pass


@pytest.fixture
def gms() -> Iterator[str]:
    _Gms.script, _Gms.requests = [], 0
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Gms)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def _me(factory: CallerClientFactory, **build: float) -> object:
    client = factory.build("token", **build)
    try:
        return client._graph.execute_graphql("query getMe { me { corpUser { urn } } }")
    finally:
        client._graph.close()


@pytest.mark.parametrize(
    ("script", "requests", "succeeds"),
    [
        ([(200, 0)], 1, True),
        # GMS may have run it: a 5xx is never replayed, so a mutation runs once.
        ([(500, 0), (200, 0)], 1, False),
        ([(503, 0), (200, 0)], 1, False),
        # GMS refused it before running it: safe to send again.
        ([(429, 0), (429, 0), (200, 0)], 3, True),
    ],
)
def test_only_requests_gms_never_ran_are_retried(
    gms: str, script: list[tuple[int, float]], requests: int, succeeds: bool
) -> None:
    _Gms.script = script
    factory = CallerClientFactory(gms, timeout_seconds=5)

    if succeeds:
        assert _me(factory) == {"me": {"corpUser": {"urn": "urn:li:corpuser:a"}}}
    else:
        with pytest.raises(OperationalError):
            _me(factory)
    assert _Gms.requests == requests


def test_a_read_timeout_is_not_replayed(gms: str) -> None:
    _Gms.script = [(200, 1.0), (200, 0)]

    with pytest.raises(requests.RequestException):
        _me(CallerClientFactory(gms, timeout_seconds=0.3))
    assert _Gms.requests == 1


def test_past_the_time_limit_no_request_is_sent(gms: str) -> None:
    _Gms.script = [(200, 0)]
    now = [100.0]
    factory = CallerClientFactory(gms, timeout_seconds=5, clock=lambda: now[0])
    client = factory.build("token", time_limit_seconds=10)

    assert client._graph.execute_graphql("query q { me { corpUser { urn } } }")
    now[0] = 111.0
    with pytest.raises(ToolDeadlineExceeded, match="10s limit"):
        client._graph.execute_graphql("query q { me { corpUser { urn } } }")
    assert _Gms.requests == 1


def test_a_request_in_flight_gets_only_the_time_left(gms: str) -> None:
    _Gms.script = [(200, 1.0)]
    factory = CallerClientFactory(gms, timeout_seconds=30)

    started = time.monotonic()
    with pytest.raises(requests.RequestException):
        _me(factory, time_limit_seconds=0.3)
    assert time.monotonic() - started < 0.9
