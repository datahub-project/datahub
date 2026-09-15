# Importing the SDK must not initialize Sentry.
#
# `Telemetry.__init__` calls `sentry_sdk.init()` when SENTRY_DSN is set, and
# sentry's init imports every auto-enabling integration module. Building the
# singleton at module scope ran that inside whatever import first reached this
# module -- `datahub.ingestion.graph.client` pulls it in via rest_emitter ->
# server_config_util -- so a third-party integration importing back into
# `datahub` would hit a partially initialized module. This has happened: the
# sentry `openai_agents` integration probes with a bare `import agents`, which
# a local package can answer to.
#
# Subprocesses: sentry's client is process-global and `sys.modules` must be
# pristine for each case.

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

# Parses fine, routes nowhere: init() never contacts it and no event is sent.
FAKE_SENTRY_DSN = "https://public@127.0.0.1:1/1"


def _run(code: str) -> subprocess.CompletedProcess:
    import os

    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env={**os.environ, "SENTRY_DSN": FAKE_SENTRY_DSN},
        timeout=120,
    )


def test_importing_graph_client_does_not_init_sentry():
    result = _run(
        """
import sentry_sdk

calls = []
_real_init = sentry_sdk.init
sentry_sdk.init = lambda *a, **kw: calls.append(1)

import datahub.ingestion.graph.client  # noqa: F401

assert not calls, "importing graph.client called sentry_sdk.init()"
print("NO_INIT_AT_IMPORT")
"""
    )
    assert result.returncode == 0, result.stderr
    assert "NO_INIT_AT_IMPORT" in result.stdout


def test_telemetry_singleton_is_built_on_first_use_and_reused():
    result = _run(
        """
from datahub.telemetry import telemetry as t

assert t._telemetry_instance is None, "singleton was built at import time"
first = t.get_telemetry_instance()
assert t._telemetry_instance is first
assert t.get_telemetry_instance() is first, "expected a cached singleton"
print("LAZY_SINGLETON")
"""
    )
    assert result.returncode == 0, result.stderr
    assert "LAZY_SINGLETON" in result.stdout


def test_module_attribute_access_still_resolves_telemetry_instance():
    # PEP 562 compat: both spellings used across the codebase keep working.
    result = _run(
        """
from datahub.telemetry import telemetry as t
from datahub.telemetry.telemetry import telemetry_instance

assert telemetry_instance is t.get_telemetry_instance()
assert t.telemetry_instance is telemetry_instance
print("COMPAT_OK")
"""
    )
    assert result.returncode == 0, result.stderr
    assert "COMPAT_OK" in result.stdout
