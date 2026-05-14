"""Unit tests for UpgradeBlockingPhase and its parse_indices_state helper."""

from __future__ import annotations

import io
import subprocess
from unittest.mock import MagicMock

import pytest

from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.upgrade_blocking import (
    UpgradeBlockingPhase,
    parse_indices_state,
)


class TestParseIndicesState:
    def test_full_payload(self) -> None:
        payload = {
            "datasetindex_v2": {
                "nextIndexName": "datasetindex_v2_1714000000",
                "oldBackingIndexName": "datasetindex_v2_old",
                "reindexStartTime": 1714000000,
                "sourceDocCount": 100,
                "taskId": "abc:42",
                "requiresDataBackfill": True,
                "status": "COMPLETED",
            },
            "dashboardindex_v2": {
                "nextIndexName": "dashboardindex_v2_1714000001",
                "oldBackingIndexName": "dashboardindex_v2_old",
                "reindexStartTime": 1714000001,
                "sourceDocCount": 0,
                "taskId": "",
                "requiresDataBackfill": False,
                "status": "COMPLETED",
            },
        }
        out = parse_indices_state(payload)
        assert {s.alias for s in out} == {"datasetindex_v2", "dashboardindex_v2"}
        ds = next(s for s in out if s.alias == "datasetindex_v2")
        assert ds.next_index_name == "datasetindex_v2_1714000000"
        assert ds.old_backing_index_name == "datasetindex_v2_old"
        assert ds.reindex_start_time == 1714000000
        assert ds.source_doc_count == 100
        assert ds.task_id == "abc:42"
        assert ds.requires_data_backfill is True
        assert ds.status == "COMPLETED"

    def test_missing_fields_default_to_none_or_zero(self) -> None:
        payload = {"datasetindex_v2": {"status": "IN_PROGRESS"}}
        out = parse_indices_state(payload)
        assert len(out) == 1
        s = out[0]
        assert s.alias == "datasetindex_v2"
        assert s.status == "IN_PROGRESS"
        assert s.next_index_name is None
        assert s.old_backing_index_name is None
        assert s.reindex_start_time is None
        assert s.source_doc_count == 0
        assert s.task_id is None
        assert s.requires_data_backfill is False

    def test_empty_payload_returns_empty_list(self) -> None:
        assert parse_indices_state({}) == []

    def test_non_dict_alias_value_skipped(self) -> None:
        payload = {
            "datasetindex_v2": "not a dict",
            "dashboardindex_v2": {"status": "COMPLETED"},
        }
        out = parse_indices_state(payload)
        assert [s.alias for s in out] == ["dashboardindex_v2"]


def _popen_with_lines(lines: list[str], returncode: int = 0) -> MagicMock:
    """Build a MagicMock subprocess.Popen that yields ``lines`` on stdout and exits cleanly."""
    proc = MagicMock()
    proc.stdout = io.StringIO("\n".join(lines) + ("\n" if lines else ""))
    proc.returncode = returncode

    def wait(timeout: float | None = None) -> int:
        proc.returncode = returncode
        return returncode

    proc.wait.side_effect = wait
    proc.kill = MagicMock()
    return proc


@pytest.fixture
def docker() -> MagicMock:
    d = MagicMock()
    d.get_service_env.return_value = {
        "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "k",
        "DATAHUB_TOKEN_SERVICE_SALT": "s",
    }
    return d


@pytest.fixture
def mysql() -> MagicMock:
    m = MagicMock()
    m.find_upgrade_result_with_field.return_value = (None, None)
    return m


@pytest.fixture
def phase(docker: MagicMock, mysql: MagicMock) -> UpgradeBlockingPhase:
    return UpgradeBlockingPhase(
        docker=docker,
        mysql=mysql,
        gms_service="datahub-gms-debug",
        upgrade_service="system-update-debug",
        timeout_s=60,
    )


class TestUpgradeBlockingPhaseHappyPath:
    def test_records_alias_swaps_and_indices_state(
        self, phase: UpgradeBlockingPhase, docker: MagicMock, mysql: MagicMock
    ) -> None:
        docker.run_upgrade_job.return_value = _popen_with_lines(
            [
                "INFO  Starting incremental reindex",
                "INFO  Alias swapped: datasetindex_v2 -> datasetindex_v2_1714",
                "INFO  Index dashboardindex_v2 had 0 docs, next index created as empty, alias swapped",
                "INFO  job done",
            ],
            returncode=0,
        )
        mysql.find_upgrade_result_with_field.return_value = (
            "system-update-blocking",
            {
                "indicesState": {
                    "datasetindex_v2": {
                        "nextIndexName": "datasetindex_v2_1714",
                        "status": "COMPLETED",
                        "sourceDocCount": 100,
                    },
                    "dashboardindex_v2": {
                        "nextIndexName": "dashboardindex_v2_1714b",
                        "status": "COMPLETED",
                        "sourceDocCount": 0,
                    },
                }
            },
        )

        ctx = TestContext()
        result = phase.run(ctx)

        assert result.status == "passed", result.error
        assert ctx.upgrade_blocking is not None
        ub = ctx.upgrade_blocking
        assert ub.upgrade_id == "system-update-blocking"
        assert ("datasetindex_v2", "datasetindex_v2_1714") in ub.alias_swaps_observed
        # Empty-source variant has no next-index name in the log line; recorded as empty string.
        assert ("dashboardindex_v2", "") in ub.alias_swaps_observed
        assert {i.alias for i in ub.indices} == {"datasetindex_v2", "dashboardindex_v2"}

    def test_no_reindex_needed_still_passes(
        self, phase: UpgradeBlockingPhase, docker: MagicMock, mysql: MagicMock
    ) -> None:
        docker.run_upgrade_job.return_value = _popen_with_lines(
            ["INFO  No indices require incremental reindex", "INFO  done"],
            returncode=0,
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.upgrade_blocking is not None
        assert ctx.upgrade_blocking.alias_swaps_observed == []
        assert ctx.upgrade_blocking.indices == []


class TestUpgradeBlockingPhaseFailures:
    def test_nonzero_exit_marks_failed(
        self, phase: UpgradeBlockingPhase, docker: MagicMock, mysql: MagicMock
    ) -> None:
        docker.run_upgrade_job.return_value = _popen_with_lines(
            ["ERROR  upgrade crashed"],
            returncode=1,
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "rc=1" in (result.error or "")
        assert ctx.upgrade_blocking is None

    def test_alias_swap_failure_in_log_marks_failed(
        self, phase: UpgradeBlockingPhase, docker: MagicMock, mysql: MagicMock
    ) -> None:
        docker.run_upgrade_job.return_value = _popen_with_lines(
            [
                "INFO  Starting incremental reindex",
                "ERROR  Alias swap failed for datasetindex_v2 -> datasetindex_v2_1714: doc count mismatch",
                "INFO  job done",
            ],
            returncode=0,
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "alias swap failure" in (result.error or "")


class TestUpgradeBlockingPhaseTimeout:
    def test_timeout_kills_process_and_returns_failed(
        self, docker: MagicMock, mysql: MagicMock
    ) -> None:
        proc = MagicMock()
        proc.stdout = io.StringIO("")
        # First call (with timeout kwarg) raises; second call (after kill, no timeout) succeeds.
        proc.wait.side_effect = [
            subprocess.TimeoutExpired(cmd="x", timeout=1),
            None,
        ]
        proc.kill = MagicMock()
        proc.returncode = -9
        docker.run_upgrade_job.return_value = proc

        phase = UpgradeBlockingPhase(
            docker=docker,
            mysql=mysql,
            gms_service="datahub-gms-debug",
            timeout_s=1,
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "system-update exited stdout but did not finish reaping" in (
            result.error or ""
        )
        proc.kill.assert_called_once()

    def test_deadline_hits_mid_stream_kills_process(
        self, docker: MagicMock, mysql: MagicMock
    ) -> None:
        """Simulate a slow-emitting process: long sleep between two lines.

        The phase has timeout_s=0 (deadline already expired before the first
        line arrives), so the per-line check fires and kills the proc.
        """
        proc = MagicMock()
        proc.stdout = io.StringIO("line1\nline2\nline3\n")
        proc.returncode = -9

        def wait(timeout: float | None = None) -> int:
            return -9

        proc.wait.side_effect = wait
        proc.kill = MagicMock()
        docker.run_upgrade_job.return_value = proc

        phase = UpgradeBlockingPhase(
            docker=docker,
            mysql=mysql,
            gms_service="datahub-gms-debug",
            timeout_s=0,  # deadline already passed
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "timed out" in (result.error or "").lower()
        proc.kill.assert_called_once()

    def test_watchdog_terminates_silent_child_and_marks_timeout(
        self, docker: MagicMock, mysql: MagicMock
    ) -> None:
        """B4 — child emits zero lines and stdout would otherwise block forever.

        The watchdog thread must terminate proc once the deadline expires,
        causing stdout to close and the for-loop to exit on EOF. Phase must
        surface this as ``failed`` + "timed out" within roughly deadline+0.5s.
        """
        import threading
        import time as _time

        # stdout that blocks on the first readline until proc.terminate is
        # called. We simulate this with a Lock that's released by terminate.
        block = threading.Event()

        class BlockingStdout:
            def __iter__(self):
                return self

            def __next__(self):
                # First call blocks until terminate is observed, then EOF.
                block.wait(timeout=3)
                raise StopIteration

        proc = MagicMock()
        proc.stdout = BlockingStdout()
        proc.returncode = -15

        # Simulate Popen.poll(): None while alive, then -15 after terminate.
        terminated = threading.Event()

        def poll() -> int | None:
            return -15 if terminated.is_set() else None

        proc.poll.side_effect = poll

        def terminate() -> None:
            terminated.set()
            block.set()  # unblocks the BlockingStdout iterator → loop exits

        proc.terminate.side_effect = terminate
        proc.wait.return_value = -15
        proc.kill = MagicMock()
        docker.run_upgrade_job.return_value = proc

        phase = UpgradeBlockingPhase(
            docker=docker,
            mysql=mysql,
            gms_service="datahub-gms-debug",
            timeout_s=1,  # short deadline
        )
        ctx = TestContext()
        t_start = _time.monotonic()
        result = phase.run(ctx)
        elapsed = _time.monotonic() - t_start

        assert result.status == "failed", result.error
        assert "timed out" in (result.error or "").lower()
        # Phase should return within ~2s (1s deadline + small overhead),
        # not hang forever on the silent child.
        assert elapsed < 3.0, f"phase hung for {elapsed:.1f}s"
        proc.terminate.assert_called()


class TestUpgradeBlockingPhaseImageTag:
    def test_passes_new_image_tag_via_compose_env(
        self, docker: MagicMock, mysql: MagicMock
    ) -> None:
        docker.run_upgrade_job.return_value = _popen_with_lines(
            ["INFO  No indices require incremental reindex"], returncode=0
        )
        phase = UpgradeBlockingPhase(
            docker=docker,
            mysql=mysql,
            gms_service="datahub-gms-debug",
            upgrade_service="system-update-debug",
            timeout_s=60,
            new_image_tag="v1.6.0",
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed", result.error
        # Verify compose_env was passed through with the new tag
        call_kwargs = docker.run_upgrade_job.call_args.kwargs
        assert "compose_env" in call_kwargs
        assert call_kwargs["compose_env"]["DATAHUB_UPDATE_VERSION"] == "v1.6.0"
        assert call_kwargs["compose_env"]["DATAHUB_VERSION"] == "v1.6.0"

    def test_default_new_image_tag_is_debug(
        self, docker: MagicMock, mysql: MagicMock
    ) -> None:
        # Backward-compat: phase constructor without new_image_tag uses "debug"
        # which matches the compose YAML's existing fallback.
        docker.run_upgrade_job.return_value = _popen_with_lines(
            ["INFO  done"], returncode=0
        )
        phase = UpgradeBlockingPhase(
            docker=docker,
            mysql=mysql,
            gms_service="datahub-gms-debug",
            timeout_s=60,
        )
        ctx = TestContext()
        phase.run(ctx)
        compose_env = docker.run_upgrade_job.call_args.kwargs["compose_env"]
        assert compose_env["DATAHUB_UPDATE_VERSION"] == "debug"


class TestTokenServiceSecretsInComposeEnv:
    """G11 — token-service secrets must be forwarded via compose_env, not just
    container -e flags. Docker Compose parses ALL services in the YAML at
    process-launch time and emits one warning per ``${DATAHUB_TOKEN_SERVICE_*}``
    substitution in sibling services when the var is absent from the parent
    env. Without this fix the upgrade phase produces ~80 cosmetic warnings.
    """

    def test_compose_env_includes_token_service_keys(
        self, phase: UpgradeBlockingPhase, docker: MagicMock, mysql: MagicMock
    ) -> None:
        docker.run_upgrade_job.return_value = _popen_with_lines(
            ["INFO  done"], returncode=0
        )
        ctx = TestContext()
        phase.run(ctx)
        compose_env = docker.run_upgrade_job.call_args.kwargs["compose_env"]
        # Read from the fixture's get_service_env return value.
        assert compose_env["DATAHUB_TOKEN_SERVICE_SIGNING_KEY"] == "k"
        assert compose_env["DATAHUB_TOKEN_SERVICE_SALT"] == "s"

    def test_env_overrides_includes_g20a_reindex_flags(
        self, phase: UpgradeBlockingPhase, docker: MagicMock, mysql: MagicMock
    ) -> None:
        """G20a — both ZDU reindex flags must reach the system-update JVM as
        -e env_overrides (the system-update compose service has no env_file
        directive, so zdu-test.env doesn't propagate to it). Selects
        BuildIndicesIncrementalStep (persists state) over the legacy path.
        """
        docker.run_upgrade_job.return_value = _popen_with_lines(
            ["INFO  done"], returncode=0
        )
        ctx = TestContext()
        phase.run(ctx)
        env_overrides = docker.run_upgrade_job.call_args.kwargs["env_overrides"]
        assert (
            env_overrides["ELASTICSEARCH_BUILD_INDICES_INCREMENTAL_REINDEX_ENABLED"]
            == "true"
        )
        assert env_overrides["ELASTICSEARCH_INDEX_BUILDER_MAPPINGS_REINDEX"] == "true"

    def test_env_overrides_still_includes_token_keys(
        self, phase: UpgradeBlockingPhase, docker: MagicMock, mysql: MagicMock
    ) -> None:
        # The container itself must also receive the secrets (Spring config
        # reads System.getenv). Adding compose_env shouldn't remove them from -e.
        docker.run_upgrade_job.return_value = _popen_with_lines(
            ["INFO  done"], returncode=0
        )
        ctx = TestContext()
        phase.run(ctx)
        env_overrides = docker.run_upgrade_job.call_args.kwargs["env_overrides"]
        assert env_overrides["DATAHUB_TOKEN_SERVICE_SIGNING_KEY"] == "k"
        assert env_overrides["DATAHUB_TOKEN_SERVICE_SALT"] == "s"
