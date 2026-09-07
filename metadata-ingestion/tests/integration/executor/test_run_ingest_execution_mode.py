"""End-to-end tests: subprocess timeout and help-probe behavior determine execution mode."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any, Callable

import pytest
import requests

from tests.integration.executor.conftest import RUN_ID, _execution_source_types
from tests.integration.executor.gms_stub import RecordingGmsServer


def _seed_prior_scheduled_execution_request(gms_url: str) -> None:
    """Simulate the executor having already written execution request input for this run."""
    payload = {
        "proposals": [
            {
                "entityType": "dataHubExecutionRequest",
                "entityUrn": f"urn:li:dataHubExecutionRequest:{RUN_ID}",
                "changeType": "UPSERT",
                "aspectName": "dataHubExecutionRequestInput",
                "aspect": {
                    "json": {
                        "task": "DataHubIngestionTask",
                        "source": {
                            "type": "SCHEDULED_INGESTION_SOURCE",
                            "ingestionSource": (
                                "urn:li:dataHubIngestionSource:"
                                "e2efa415-cafe-cafe-cafe-cafecafecafe"
                            ),
                        },
                    }
                },
            }
        ],
        "async": "false",
    }
    resp = requests.post(
        f"{gms_url}/aspects?action=ingestProposalBatch",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    resp.raise_for_status()


def _help_probe_run(delay_seconds: float | None = None) -> Callable[..., Any]:
    """Return a ``subprocess.run`` replacement that controls --help probe behaviour.

    ``delay_seconds=None`` (default)
        Always raises ``TimeoutExpired`` immediately — simulates a hard probe failure.
        Used by the degraded-path tests.

    ``delay_seconds=<float>`` (e.g. 11.0)
        Sleeps for *delay_seconds* then returns a synthetic ``CompletedProcess`` whose
        stdout contains ``--report-to``, simulating a slow but successful probe.

        Regression guard: if a ``timeout`` kwarg smaller than *delay_seconds* is present
        (i.e. someone re-introduces ``timeout=10``), this mock raises ``TimeoutExpired``
        after sleeping for *timeout* seconds — exactly replicating the original production
        bug — and the companion test will fail, catching the regression.

    All non-help ``subprocess.run`` calls are forwarded to the real implementation so
    that actual ingestion executes normally.
    """
    real_run = subprocess.run

    def _inner(cmd: Any, *args: Any, **kwargs: Any) -> Any:
        parts = [str(x) for x in cmd]
        if (
            len(parts) >= 4
            and "ingest" in parts
            and "run" in parts
            and "--help" in parts
        ):
            if delay_seconds is None:
                raise subprocess.TimeoutExpired(cmd, timeout=10)
            timeout = kwargs.get("timeout")
            if timeout is not None and timeout < delay_seconds:
                time.sleep(timeout)
                raise subprocess.TimeoutExpired(cmd, timeout=timeout)
            time.sleep(delay_seconds)
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=0,
                stdout="Usage: datahub ingest run [OPTIONS]\n  --report-to PATH\n",
                stderr="",
            )
        return real_run(cmd, *args, **kwargs)

    return _inner


@pytest.mark.integration
@pytest.mark.degraded
def test_subprocess_timeout_triggers_cli_execution_mode(
    gms_server: RecordingGmsServer,
    run_ingestion: Callable[..., tuple[str, Path]],
    tmp_path: Path,
) -> None:
    """Degraded mode: when the --help probe fails (here: TimeoutExpired), CLI mode is used.

    The desired path is executor mode via --report-to. Any exception from the probe
    subprocess (timeout, binary error, OSError, etc.) causes check_report_to_support()
    to return False and fall back to CLI_INGESTION_SOURCE instead of crashing.
    """
    blob, _ = run_ingestion(gms_server, tmp_path, subprocess_patch=_help_probe_run())
    source_types = _execution_source_types(gms_server)
    assert "CLI_INGESTION_SOURCE" in source_types, (
        f"Expected dataHubExecutionRequestInput with CLI_INGESTION_SOURCE; "
        f"got source types: {source_types!r}; captured (truncated): {blob[:2000]!r}"
    )


@pytest.mark.integration
@pytest.mark.degraded
def test_subprocess_timeout_overwrites_scheduled_execution_request(
    gms_server: RecordingGmsServer,
    run_ingestion: Callable[..., tuple[str, Path]],
    tmp_path: Path,
) -> None:
    """Degraded mode: a failed probe must overwrite any prior scheduled execution request with CLI mode.

    The desired path is executor mode via --report-to. This test verifies that when the
    probe fails, the CLI overwrite appears after the seed write in capture order —
    graceful degradation, not silent data loss.
    """
    blob, _ = run_ingestion(
        gms_server,
        tmp_path,
        subprocess_patch=_help_probe_run(),
        seed_fn=_seed_prior_scheduled_execution_request,
    )
    source_types = _execution_source_types(gms_server)
    assert "SCHEDULED_INGESTION_SOURCE" in source_types, (
        "Seed should appear first; captured (truncated): {blob[:2500]!r}"
    )
    assert "CLI_INGESTION_SOURCE" in source_types, (
        f"Expected dataHubExecutionRequestInput with CLI_INGESTION_SOURCE after scheduled seed; "
        f"got source types: {source_types!r}; captured (truncated): {blob[:2500]!r}"
    )
    assert source_types.index("SCHEDULED_INGESTION_SOURCE") < source_types.index(
        "CLI_INGESTION_SOURCE"
    ), "Scheduled write must precede CLI overwrite in capture order"


@pytest.mark.integration
def test_successful_help_probe_preserves_executor_mode(
    gms_server: RecordingGmsServer,
    run_ingestion: Callable[..., tuple[str, Path]],
    tmp_path: Path,
) -> None:
    """When the help probe succeeds, --report-to is passed and no CLI_INGESTION_SOURCE is emitted."""
    blob, report_out = run_ingestion(gms_server, tmp_path)
    assert "CLI_INGESTION_SOURCE" not in blob, (
        "Did not expect CLI execution request input when --report-to is used; "
        f"captured POST payloads (truncated): {blob[:2000]!r}"
    )
    assert report_out.is_file()
    report = json.loads(report_out.read_text(encoding="utf-8"))
    assert isinstance(report, dict)


@pytest.mark.integration
@pytest.mark.slow
def test_slow_help_probe_does_not_fall_back_to_cli_mode(
    gms_server: RecordingGmsServer,
    run_ingestion: Callable[..., tuple[str, Path]],
    tmp_path: Path,
) -> None:
    """Regression guard: a slow but successful --help probe must NOT trigger CLI fallback.

    The original bug (present in acryl-executor ≥ 0.3.0) used timeout=10 on the
    subprocess.run call in check_report_to_support(). On cold container starts, datahub
    ingest run --help could exceed 10 seconds, raising TimeoutExpired and silently falling
    back to CLI_INGESTION_SOURCE instead of SCHEDULED_INGESTION_SOURCE.

    This test uses an 11-second artificial delay (just over the old threshold). With the
    current code (no timeout), the probe completes slowly but successfully, and --report-to
    is passed to the CLI (report file written; no CLI_INGESTION_SOURCE in captured GMS posts).

    Failure mode: if timeout=10 is re-introduced to check_report_to_support(), the
    _help_probe_run mock will raise TimeoutExpired after 10 seconds, the run will
    fall back to CLI_INGESTION_SOURCE, and this test will fail — catching the regression.
    """
    blob, report_out = run_ingestion(
        gms_server, tmp_path, subprocess_patch=_help_probe_run(delay_seconds=11.0)
    )
    source_types = _execution_source_types(gms_server)
    assert "CLI_INGESTION_SOURCE" not in source_types, (
        f"Slow probe must not fall back to CLI mode; "
        f"got source types: {source_types!r}; captured (truncated): {blob[:2000]!r}"
    )
    # --report-to <file> replaces the CLI default (report_to=datahub), so only the file
    # reporter runs and no dataHubExecutionRequestInput posts reach GMS (Pipeline._configure_reporting).
    # The datahub reporter would emit CLI_INGESTION_SOURCE, not SCHEDULED_INGESTION_SOURCE.
    assert report_out.is_file(), (
        "--report-to must have been passed (report file must exist)"
    )
