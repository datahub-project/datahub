"""Unit tests for UpgradeNonBlockingPhase — uses mocked Docker/DataHub/MySQL clients."""

from __future__ import annotations

from queue import Queue
from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.context import TestContext
from tests.zdu.framework.log_monitor import SweepEvent, SweepState
from tests.zdu.framework.phases.upgrade_nonblocking import UpgradeNonBlockingPhase


@pytest.fixture
def config() -> ZDUTestConfig:
    cfg = ZDUTestConfig()
    cfg.sweep_timeout_s = 30
    cfg.reader_workers = 0
    cfg.writer_workers = 0
    return cfg


@pytest.fixture
def docker() -> MagicMock:
    m = MagicMock()
    m.get_service_env.return_value = {
        "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "k",
        "DATAHUB_TOKEN_SERVICE_SALT": "s",
    }
    proc = MagicMock()
    proc.stdout = iter([])
    m.run_upgrade_job.return_value = proc
    return m


@pytest.fixture
def datahub() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mysql() -> MagicMock:
    m = MagicMock()
    m.find_upgrade_result_with_field.return_value = (
        "urn:li:dataHubUpgrade:SystemUpdateNonBlocking",
        {
            "indicesState": {
                "dashboardindex_v2": {
                    "nextIndexName": "dashboardindex_v2_new",
                    "oldBackingIndexName": "dashboardindex_v2_old",
                    "sourceDocCount": 100,
                    "status": "DUAL_WRITE_DISABLED",
                }
            }
        },
    )
    return m


@pytest.fixture
def phase(
    config: ZDUTestConfig, docker: MagicMock, datahub: MagicMock, mysql: MagicMock
) -> UpgradeNonBlockingPhase:
    return UpgradeNonBlockingPhase(
        config=config, docker=docker, datahub=datahub, mysql=mysql
    )


class TestUpgradeNonBlockingPhase:
    def test_passes_explicit_nonblocking_extra_args_to_run_upgrade_job(
        self, phase: UpgradeNonBlockingPhase, docker: MagicMock
    ) -> None:
        # Arrange a sweep that completes immediately by injecting a COMPLETED event.
        completed = SweepEvent(
            state=SweepState.COMPLETED,
            source="datahub-upgrade",
            timestamp=__import__("datetime").datetime.utcnow(),
            message="done",
            total_migrated=0,
        )
        ctx = TestContext()

        with patch.object(
            UpgradeNonBlockingPhase, "_drain_nonblocking_queue", return_value=([], {})
        ):
            with patch(
                "tests.zdu.framework.phases.upgrade_nonblocking.LogMonitor"
            ) as mon_cls:
                mon = mon_cls.return_value
                mon.start.return_value = None
                # Simulate the sweep_queue.get returning COMPLETED on first poll.
                with patch.object(Queue, "get", return_value=completed):
                    phase.run(ctx)

        # Verify run_upgrade_job was called with -u SystemUpdateNonBlocking
        calls = docker.run_upgrade_job.call_args_list
        assert len(calls) >= 1
        for call in calls:
            kwargs = call.kwargs
            assert kwargs.get("extra_args") == ["-u", "SystemUpdateNonBlocking"]

    def test_captures_upgrade_nonblocking_result_on_completion(
        self,
        phase: UpgradeNonBlockingPhase,
        mysql: MagicMock,
    ) -> None:
        completed = SweepEvent(
            state=SweepState.COMPLETED,
            source="datahub-upgrade",
            timestamp=__import__("datetime").datetime.utcnow(),
            message="done",
            total_migrated=42,
        )
        ctx = TestContext()

        with patch(
            "tests.zdu.framework.phases.upgrade_nonblocking.LogMonitor"
        ) as mon_cls:
            mon = mon_cls.return_value
            mon.start.return_value = None
            with patch.object(Queue, "get", return_value=completed):
                # Pre-stuff the nonblocking queue
                with patch.object(
                    UpgradeNonBlockingPhase,
                    "_drain_nonblocking_queue",
                    return_value=(
                        ["dashboardindex_v2_old"],
                        {"dashboardindex_v2_new": (100, 200)},
                    ),
                ):
                    result = phase.run(ctx)

        assert result.status == "passed"
        assert ctx.upgrade_nonblocking is not None
        assert ctx.upgrade_nonblocking.dual_write_disabled_indices == [
            "dashboardindex_v2_old"
        ]
        assert ctx.upgrade_nonblocking.catch_up_windows == {
            "dashboardindex_v2_new": (100, 200)
        }
        # indicesState parsed from the mocked DataHubUpgradeResult fixture
        assert len(ctx.upgrade_nonblocking.indices) == 1
        assert ctx.upgrade_nonblocking.indices[0].status == "DUAL_WRITE_DISABLED"
        assert (
            ctx.upgrade_nonblocking.upgrade_id
            == "urn:li:dataHubUpgrade:SystemUpdateNonBlocking"
        )
        # Sweep total still recorded (preserves Suite N behavior)
        assert ctx.sweep_total_migrated == 42

    def test_compose_env_includes_token_service_keys(
        self, phase: UpgradeNonBlockingPhase, docker: MagicMock
    ) -> None:
        """G11 — compose_env forwards DATAHUB_TOKEN_SERVICE_{SIGNING_KEY,SALT}
        so Compose's YAML interpolation doesn't warn about sibling services'
        ``${DATAHUB_TOKEN_SERVICE_*}`` substitutions (was ~80 warnings/run).
        """
        completed = SweepEvent(
            state=SweepState.COMPLETED,
            source="datahub-upgrade",
            timestamp=__import__("datetime").datetime.utcnow(),
            message="done",
            total_migrated=0,
        )
        ctx = TestContext()
        with patch.object(
            UpgradeNonBlockingPhase, "_drain_nonblocking_queue", return_value=([], {})
        ):
            with patch(
                "tests.zdu.framework.phases.upgrade_nonblocking.LogMonitor"
            ) as mon_cls:
                mon_cls.return_value.start.return_value = None
                with patch.object(Queue, "get", return_value=completed):
                    phase.run(ctx)

        # Every run_upgrade_job invocation (initial + any retry) must include
        # the token-service keys in compose_env.
        for call in docker.run_upgrade_job.call_args_list:
            compose_env = call.kwargs.get("compose_env")
            assert compose_env is not None, (
                "run_upgrade_job called without compose_env — Compose YAML "
                "substitution will emit ~80 cosmetic warnings per run"
            )
            assert compose_env["DATAHUB_TOKEN_SERVICE_SIGNING_KEY"] == "k"
            assert compose_env["DATAHUB_TOKEN_SERVICE_SALT"] == "s"

    def test_capture_result_skipped_when_mysql_returns_none(
        self,
        phase: UpgradeNonBlockingPhase,
        mysql: MagicMock,
    ) -> None:
        # MySQL has no upgrade result yet (fresh deployment) — phase still passes,
        # ctx.upgrade_nonblocking gets an empty-indices result.
        mysql.find_upgrade_result_with_field.return_value = (None, None)
        completed = SweepEvent(
            state=SweepState.COMPLETED,
            source="datahub-upgrade",
            timestamp=__import__("datetime").datetime.utcnow(),
            message="done",
            total_migrated=0,
        )
        ctx = TestContext()

        with patch(
            "tests.zdu.framework.phases.upgrade_nonblocking.LogMonitor"
        ) as mon_cls:
            mon_cls.return_value.start.return_value = None
            with patch.object(Queue, "get", return_value=completed):
                with patch.object(
                    UpgradeNonBlockingPhase,
                    "_drain_nonblocking_queue",
                    return_value=([], {}),
                ):
                    result = phase.run(ctx)

        assert result.status == "passed"
        assert ctx.upgrade_nonblocking is not None
        assert ctx.upgrade_nonblocking.indices == []
        assert ctx.upgrade_nonblocking.upgrade_id is None
        assert ctx.upgrade_nonblocking.raw is None
