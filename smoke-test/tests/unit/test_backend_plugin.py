"""Tests for utilities.backend_plugin: selection by SMOKE_BACKEND / SMOKE_BACKEND_MODE."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

pytest_plugins = ["pytester"]

_INI = """
[pytest]
markers =
    requires(*capabilities): backend capabilities
    docker_canary: Docker image proof
"""

_TESTS = """
import pytest

pytestmark = pytest.mark.requires("integrations")

@pytest.mark.requires("kafka")
def test_kafka():
    pass

@pytest.mark.docker_canary
def test_canary_proof():
    pass

def test_integrations_only():
    pass
"""


@pytest.fixture
def layout(pytester: pytest.Pytester) -> pytest.Pytester:
    pytester.makeini(_INI)
    pytester.makepyfile(test_lanes=_TESTS)
    return pytester


def _run(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
    backend: str = "",
    mode: str = "",
) -> pytest.RunResult:
    monkeypatch.setenv("SMOKE_BACKEND", backend)
    monkeypatch.setenv("SMOKE_BACKEND_MODE", mode)
    # No GMS here: keep the local-lane transport check from probing one.
    monkeypatch.delenv("DATAHUB_GMS_URL", raising=False)
    monkeypatch.delenv("DATAHUB_FRONTEND_URL", raising=False)
    return pytester.runpytest("-p", "utilities.backend_plugin", "-v")


def test_docker_lane_runs_everything_by_default(
    layout: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    _run(layout, monkeypatch).assert_outcomes(passed=3)


def test_local_lane_deselects_what_it_cannot_run(
    layout: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = _run(layout, monkeypatch, backend="local")

    # The module-level integrations requirement merges with the test's own
    # kafka one; integrations alone is available locally.
    result.assert_outcomes(passed=2, deselected=1)
    result.stdout.fnmatch_lines(["*test_integrations_only PASSED*"])
    result.stdout.no_fnmatch_line("*test_kafka*")


def test_canary_runs_local_blocked_and_docker_canary_tests(
    layout: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = _run(layout, monkeypatch, backend="docker", mode="canary")

    result.assert_outcomes(passed=2, deselected=1)
    result.stdout.fnmatch_lines(["*test_kafka PASSED*", "*test_canary_proof PASSED*"])


def test_unknown_capability_is_a_usage_error_on_every_lane(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytester.makeini(_INI)
    pytester.makepyfile(
        test_typo="""
import pytest

@pytest.mark.requires("kafak")
def test_typo():
    pass
"""
    )

    assert _run(pytester, monkeypatch).ret == pytest.ExitCode.USAGE_ERROR


@pytest.mark.parametrize("backend,mode", [("kubernetes", ""), ("local", "canary")])
def test_invalid_backend_settings_are_a_usage_error(
    layout: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, backend: str, mode: str
) -> None:
    result = _run(layout, monkeypatch, backend=backend, mode=mode)

    assert result.ret == pytest.ExitCode.USAGE_ERROR


@pytest.mark.parametrize(
    "gms_config,expected",
    [
        ({"pgQueue": {"enabled": True}}, pytest.ExitCode.OK),
        ({"kafka": {}}, pytest.ExitCode.USAGE_ERROR),
    ],
)
def test_local_lane_refuses_a_kafka_backed_gms(
    layout: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
    gms_config: dict,
    expected: pytest.ExitCode,
) -> None:
    body = json.dumps(gms_config).encode()

    class ConfigHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: object) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), ConfigHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        monkeypatch.setenv("SMOKE_BACKEND", "local")
        monkeypatch.setenv("SMOKE_BACKEND_MODE", "select")
        monkeypatch.setenv("DATAHUB_GMS_URL", f"http://127.0.0.1:{server.server_port}")
        result = layout.runpytest("-p", "utilities.backend_plugin")
    finally:
        server.shutdown()

    assert result.ret == expected
