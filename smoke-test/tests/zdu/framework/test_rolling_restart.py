"""Unit tests for RollingRestartPhase — uses mocked DockerComposeClient."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.rolling_restart import (
    RollingRestartPhase,
    _compose_env_for_service,
)


class TestComposeEnvForService:
    def test_gms_service_sets_gms_and_global_vars(self) -> None:
        env = _compose_env_for_service("datahub-gms-debug", "v1.6.0")
        assert env["DATAHUB_GMS_VERSION"] == "v1.6.0"
        assert env["DATAHUB_VERSION"] == "v1.6.0"

    def test_mae_service_sets_mae_and_global_vars(self) -> None:
        env = _compose_env_for_service("datahub-mae-consumer-debug", "v1.6.0")
        assert env["DATAHUB_MAE_VERSION"] == "v1.6.0"
        assert env["DATAHUB_VERSION"] == "v1.6.0"

    def test_mce_service_sets_mce_and_global_vars(self) -> None:
        env = _compose_env_for_service("datahub-mce-consumer-debug", "v1.6.0")
        assert env["DATAHUB_MCE_VERSION"] == "v1.6.0"
        assert env["DATAHUB_VERSION"] == "v1.6.0"

    def test_unknown_service_falls_back_to_global_only(self) -> None:
        env = _compose_env_for_service("custom-service", "v1.6.0")
        # Unknown service: only global DATAHUB_VERSION + auth-bypass override
        # (which keeps METADATA_SERVICE_AUTH_ENABLED=false across the restart
        # so post-restart phases can keep ingesting without a token), plus the
        # G20a mappings-reindex flag.
        assert env == {
            "DATAHUB_VERSION": "v1.6.0",
            "METADATA_SERVICE_AUTH_ENABLED": "false",
        }

    def test_metadata_service_auth_disabled_across_restart(self) -> None:
        """Without this override, Compose's `${VAR:-true}` substitution
        re-enables auth on the recreated GMS — and any post-restart REST
        write (inject_traffic_dual, runtime_migration) hits 401 because
        the user's token's MySQL state was wiped by the SetupOldStack
        nuke phase.
        """
        env = _compose_env_for_service("datahub-gms-debug", "v1.6.0")
        assert env["METADATA_SERVICE_AUTH_ENABLED"] == "false"

    def test_zdu_reindex_flags_NOT_in_compose_env(self) -> None:
        """G20a — reindex flags reach the recreated GMS via env_file
        (zdu-test.env, loaded by ${DATAHUB_LOCAL_COMMON_ENV} substitution
        on the GMS compose service), NOT via compose_env. This test pins
        the design: compose_env is for YAML `${...}` substitution; container
        env comes from env_file.
        """
        env = _compose_env_for_service("datahub-gms-debug", "v1.6.0")
        assert "ELASTICSEARCH_BUILD_INDICES_INCREMENTAL_REINDEX_ENABLED" not in env
        assert "ELASTICSEARCH_INDEX_BUILDER_MAPPINGS_REINDEX" not in env


@pytest.fixture
def docker() -> MagicMock:
    d = MagicMock()
    # All 3 services present in the running stack by default (production-shape
    # profile). Tests that exercise the absent-service skip path override this.
    d.get_all_service_images.return_value = {
        "datahub-gms-debug": "acryldata/datahub-gms:v1.5.0",
        "datahub-mae-consumer-debug": "acryldata/datahub-mae-consumer:v1.5.0",
        "datahub-mce-consumer-debug": "acryldata/datahub-mce-consumer:v1.5.0",
    }
    d.get_service_env.return_value = {
        "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "test-key",
        "DATAHUB_TOKEN_SERVICE_SALT": "test-salt",
    }
    return d


@pytest.fixture
def phase(docker: MagicMock) -> RollingRestartPhase:
    return RollingRestartPhase(
        docker=docker,
        mae_service="datahub-mae-consumer-debug",
        services_in_order=[
            "datahub-gms-debug",
            "datahub-mae-consumer-debug",
            "datahub-mce-consumer-debug",
        ],
        new_image_tag="v1.6.0",
        dual_write_timeout_s=5,
    )


def _dual_write_log_lines_factory(lines: list[str]):
    """Return a side_effect callable yielding a fresh iter() each call.

    The mock's ``return_value`` is one iterator shared across calls; once the
    first tail consumes it, later tails see an empty stream. G20d adds a
    second concurrent tail (MAE + GMS), so we need each call to get its own
    iterator. The "first timestamp per index wins" merge in
    ``_capture_dual_write_start_times`` makes the duplicate stream a safe
    no-op (both threads converge on the same dict).
    """

    def _factory(*_args, **_kwargs):
        return iter(lines)

    return _factory


class TestRollingRestartPhaseHappyPath:
    def test_recreates_services_in_order(
        self, phase: RollingRestartPhase, docker: MagicMock
    ) -> None:
        docker.tail_service_logs.side_effect = _dual_write_log_lines_factory(
            [
                "INFO  Recorded dual-write start time for index 'datasetindex_v2' "
                "(entity 'dataset'): 1714000000000",
                "INFO  Recorded dual-write start time for index 'dashboardindex_v2' "
                "(entity 'dashboard'): 1714000001000",
                "INFO  unrelated MAE chatter",
            ]
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        # Services were recreated in the configured order
        recreate_calls = [
            c.kwargs.get("service") or c.args[0]
            for c in docker.recreate_service.call_args_list
        ]
        assert recreate_calls == [
            "datahub-gms-debug",
            "datahub-mae-consumer-debug",
            "datahub-mce-consumer-debug",
        ]
        # Each recreate carried the right per-service compose_env
        gms_env = docker.recreate_service.call_args_list[0].kwargs["compose_env"]
        assert gms_env["DATAHUB_GMS_VERSION"] == "v1.6.0"
        assert gms_env["DATAHUB_VERSION"] == "v1.6.0"
        # ctx.rolling_restart populated with both index → T1 mappings
        assert ctx.rolling_restart is not None
        assert ctx.rolling_restart.dual_write_start_times == {
            "datasetindex_v2": 1714000000000,
            "dashboardindex_v2": 1714000001000,
        }
        assert ctx.rolling_restart.services_restarted == [
            "datahub-gms-debug",
            "datahub-mae-consumer-debug",
            "datahub-mce-consumer-debug",
        ]

    def test_no_dual_write_log_lines_yields_empty_dict(
        self, phase: RollingRestartPhase, docker: MagicMock
    ) -> None:
        docker.tail_service_logs.side_effect = _dual_write_log_lines_factory(
            ["INFO  startup boilerplate", "INFO  MAE ready"]
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.rolling_restart is not None
        assert ctx.rolling_restart.dual_write_start_times == {}

    def test_dual_write_timeout_does_not_hang_on_quiet_log_stream(
        self, docker: MagicMock
    ) -> None:
        """If MAE goes quiet (blocking readline never returns), the deadline must fire.

        Simulates a tail_service_logs that yields one line then blocks forever
        by using a generator that never returns.
        """

        def quiet_log_stream(*_args, **_kwargs):
            yield (
                "INFO  Recorded dual-write start time for index 'datasetindex_v2' "
                "(entity 'dataset'): 1714000000000"
            )
            # Block forever — simulates MAE going quiet on its log stream
            time.sleep(60)

        docker.tail_service_logs.side_effect = quiet_log_stream

        phase = RollingRestartPhase(
            docker=docker,
            mae_service="datahub-mae-consumer-debug",
            new_image_tag="v1.6.0",
            dual_write_timeout_s=1,  # 1-second deadline
            recreate_timeout_s=5,
        )
        ctx = TestContext()

        start = time.monotonic()
        result = phase.run(ctx)
        duration = time.monotonic() - start

        # Phase must return within ~2s (1s deadline + small overhead),
        # NOT hang for the full 60s sleep.
        assert duration < 5, f"phase hung for {duration:.1f}s — deadline didn't fire"
        assert result.status == "passed"
        # The first event was captured before the deadline fired
        assert ctx.rolling_restart is not None
        assert ctx.rolling_restart.dual_write_start_times == {
            "datasetindex_v2": 1714000000000,
        }


class TestDualWriteTailG20d:
    """G20d — dual-tail (MAE + GMS) for the debug-profile collapsed case."""

    def test_collapsed_debug_profile_tails_gms_when_mae_absent(
        self, docker: MagicMock
    ) -> None:
        # Debug profile shape: MAE is embedded in GMS, no standalone
        # datahub-mae-consumer-debug service in the running stack.
        docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:zdu-new-abc",
        }
        captured_tail_targets: list[str] = []

        def _tail(service: str, **_kwargs):
            captured_tail_targets.append(service)
            return iter(
                [
                    "INFO  Recorded dual-write start time for index "
                    "'dashboardindex_v2' (entity 'dashboard'): 1714000000000",
                ]
                if service == "datahub-gms-debug"
                else []
            )

        docker.tail_service_logs.side_effect = _tail
        phase = RollingRestartPhase(
            docker=docker,
            mae_service="datahub-mae-consumer-debug",
            gms_service="datahub-gms-debug",
            services_in_order=["datahub-gms-debug"],
            new_image_tag="v1.6.0",
            dual_write_timeout_s=2,
            recreate_timeout_s=5,
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        # GMS was the only candidate present; only it got tailed.
        assert captured_tail_targets == ["datahub-gms-debug"]
        # The dual-write line emitted by GMS-embedded MAE was captured.
        assert ctx.rolling_restart is not None
        assert ctx.rolling_restart.dual_write_start_times == {
            "dashboardindex_v2": 1714000000000,
        }

    def test_both_services_present_first_timestamp_wins(
        self, docker: MagicMock
    ) -> None:
        # Production-shape profile: both MAE and GMS present. The "first
        # timestamp per index wins" merge keeps the result deterministic
        # even though both threads see the same line via the mock.
        docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:v1.5.0",
            "datahub-mae-consumer-debug": "acryldata/datahub-mae-consumer:v1.5.0",
            "datahub-mce-consumer-debug": "acryldata/datahub-mce-consumer:v1.5.0",
        }

        def _tail(service: str, **_kwargs):
            # Both services emit the SAME index but at different timestamps.
            # MAE first (earlier), GMS later (would lose the merge race).
            if service == "datahub-mae-consumer-debug":
                return iter(
                    [
                        "INFO  Recorded dual-write start time for index "
                        "'dashboardindex_v2' (entity 'dashboard'): 1000"
                    ]
                )
            return iter(
                [
                    "INFO  Recorded dual-write start time for index "
                    "'dashboardindex_v2' (entity 'dashboard'): 9999"
                ]
            )

        docker.tail_service_logs.side_effect = _tail
        phase = RollingRestartPhase(
            docker=docker,
            mae_service="datahub-mae-consumer-debug",
            gms_service="datahub-gms-debug",
            services_in_order=[
                "datahub-gms-debug",
                "datahub-mae-consumer-debug",
                "datahub-mce-consumer-debug",
            ],
            new_image_tag="v1.6.0",
            dual_write_timeout_s=2,
            recreate_timeout_s=5,
        )
        ctx = TestContext()
        phase.run(ctx)
        assert ctx.rolling_restart is not None
        # Only one entry per index — duplicates dropped by the merge.
        assert "dashboardindex_v2" in ctx.rolling_restart.dual_write_start_times
        # Either timestamp is correct — first wins, and which thread wins
        # depends on scheduler. The deterministic invariant is the merge
        # didn't produce duplicates or pile both values into one key.
        assert ctx.rolling_restart.dual_write_start_times["dashboardindex_v2"] in (
            1000,
            9999,
        )

    def test_no_mae_no_gms_present_logs_warning_and_returns_empty(
        self, docker: MagicMock
    ) -> None:
        # Defensive: a stack with neither service present (e.g. consumer-only
        # restart in a future profile) must not crash — just return empty.
        docker.get_all_service_images.return_value = {
            "kafka-broker": "confluent/kafka:7.0.0",
        }
        docker.tail_service_logs.side_effect = AssertionError(
            "tail_service_logs should not be called when no candidate present"
        )
        phase = RollingRestartPhase(
            docker=docker,
            mae_service="datahub-mae-consumer-debug",
            gms_service="datahub-gms-debug",
            services_in_order=["datahub-gms-debug"],
            new_image_tag="v1.6.0",
            dual_write_timeout_s=1,
            recreate_timeout_s=5,
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.rolling_restart is not None
        assert ctx.rolling_restart.dual_write_start_times == {}


class TestRollingRestartPhaseFailures:
    def test_recreate_service_failure_aborts_phase(
        self, phase: RollingRestartPhase, docker: MagicMock
    ) -> None:
        docker.recreate_service.side_effect = [
            None,  # gms ok
            RuntimeError("image not found: datahub-mae-consumer:missing"),
            None,  # mce — should never run
        ]
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "image not found" in (result.error or "")
        # Only gms was recorded as restarted; mae's failure aborts before mce
        assert docker.recreate_service.call_count == 2

    def test_health_timeout_propagates(
        self, phase: RollingRestartPhase, docker: MagicMock
    ) -> None:
        docker.recreate_service.side_effect = TimeoutError(
            "Service datahub-gms-debug not healthy after 120s"
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "not healthy" in (result.error or "")
