"""Unit tests for CleanupPhase (G5 — restore GMS to pre-test tag)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.context import PrepareOldStackResult, TestContext
from tests.zdu.framework.phases.cleanup import CleanupPhase


@pytest.fixture
def phase() -> CleanupPhase:
    docker = MagicMock()
    docker.get_service_env.return_value = {
        "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "test-key",
        "DATAHUB_TOKEN_SERVICE_SALT": "test-salt",
    }
    return CleanupPhase(
        docker=docker,
        gms_service="datahub-gms-debug",
        health_url="http://localhost:8080",
        timeout_s=10,
    )


class TestSkipsWhenNoSnapshot:
    def test_skips_when_prepare_old_stack_is_none(self, phase) -> None:
        ctx = TestContext()  # ctx.prepare_old_stack = None
        result = phase.run(ctx)
        assert result.status == "skipped"
        phase._docker.recreate_service.assert_not_called()


class TestDigestFallsBackToRestTag:
    """Pre-test image was a digest (sha256:...) → can't derive a tag, so
    cleanup falls back to the canonical rest_tag rather than abandoning
    the user on whatever the test left running."""

    def test_digest_pre_test_image_uses_rest_tag(self, phase) -> None:
        ctx = TestContext()
        ctx.prepare_old_stack = PrepareOldStackResult(
            old_image_tag="zdu-old-abc12345",
            current_images={"datahub-gms-debug": "sha256:abc12345"},
            services_inspected=["datahub-gms-debug"],
            recreated_services=[],
            health_check_passed=True,
            duration_s=0.0,
        )
        # Current GMS is on a NEW tag, NOT on debug — so cleanup should restore.
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:zdu-new-deadbeef",
        }
        with patch.object(phase, "_wait_for_health", return_value=True):
            result = phase.run(ctx)
        assert result.status == "passed"
        assert result.details["original_tag"] == "debug"
        assert "rest_tag fallback" in result.details["tag_source"]


class TestZduArtifactFallsBackToRestTag:
    """G9 — Back-to-back ZDU runs leave the pre-test snapshot pointing at
    a previous test's zdu-{old,new}-* output. Restoring to that would just
    leave the user on more test residue. Cleanup must fall back to rest_tag.
    """

    def test_pretest_zdu_new_artifact_falls_back_to_rest_tag(self, phase) -> None:
        ctx = TestContext()
        ctx.prepare_old_stack = PrepareOldStackResult(
            old_image_tag="zdu-old-abc12345",
            # Pre-test state was itself a previous ZDU run's NEW image.
            current_images={
                "datahub-gms-debug": "acryldata/datahub-gms:zdu-new-deadbeef"
            },
            services_inspected=["datahub-gms-debug"],
            recreated_services=["datahub-gms-debug"],
            health_check_passed=True,
            duration_s=0.0,
        )
        # Post-test, GMS is on yet another NEW tag.
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:zdu-new-cafebabe",
        }
        with patch.object(phase, "_wait_for_health", return_value=True):
            result = phase.run(ctx)
        assert result.status == "passed"
        # Must restore to "debug" (rest_tag), not zdu-new-deadbeef.
        assert result.details["original_tag"] == "debug"
        assert "test artifact" in result.details["tag_source"]
        env = phase._docker.recreate_service.call_args.kwargs["compose_env"]
        assert env["DATAHUB_VERSION"] == "debug"
        assert env["DATAHUB_GMS_VERSION"] == "debug"

    def test_pretest_zdu_old_artifact_also_falls_back(self, phase) -> None:
        ctx = TestContext()
        ctx.prepare_old_stack = PrepareOldStackResult(
            old_image_tag="zdu-old-cafe1234",
            current_images={
                "datahub-gms-debug": "acryldata/datahub-gms:zdu-old-deadbeef"
            },
            services_inspected=["datahub-gms-debug"],
            recreated_services=["datahub-gms-debug"],
            health_check_passed=True,
            duration_s=0.0,
        )
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:zdu-new-cafebabe",
        }
        with patch.object(phase, "_wait_for_health", return_value=True):
            result = phase.run(ctx)
        assert result.status == "passed"
        assert result.details["original_tag"] == "debug"

    def test_custom_rest_tag_via_constructor(self) -> None:
        # User can override rest_tag (e.g. to pin to a release tag).
        docker = MagicMock()
        docker.get_service_env.return_value = {
            "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "k",
            "DATAHUB_TOKEN_SERVICE_SALT": "s",
        }
        docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:zdu-new-x",
        }
        from tests.zdu.framework.phases.cleanup import CleanupPhase

        phase = CleanupPhase(
            docker=docker,
            gms_service="datahub-gms-debug",
            health_url="http://localhost:8080",
            timeout_s=10,
            rest_tag="v1.5.0",  # custom
        )
        ctx = TestContext()
        ctx.prepare_old_stack = PrepareOldStackResult(
            old_image_tag="zdu-old-abc",
            current_images={"datahub-gms-debug": "acryldata/datahub-gms:zdu-new-y"},
            services_inspected=["datahub-gms-debug"],
            recreated_services=["datahub-gms-debug"],
            health_check_passed=True,
            duration_s=0.0,
        )
        with patch.object(phase, "_wait_for_health", return_value=True):
            result = phase.run(ctx)
        assert result.details["original_tag"] == "v1.5.0"


class TestNoOpWhenAlreadyOnOriginal:
    def test_passes_without_recreate_when_gms_already_on_original_tag(
        self, phase
    ) -> None:
        ctx = TestContext()
        ctx.prepare_old_stack = PrepareOldStackResult(
            old_image_tag="zdu-old-abc12345",
            current_images={"datahub-gms-debug": "acryldata/datahub-gms:debug"},
            services_inspected=["datahub-gms-debug"],
            recreated_services=[],
            health_check_passed=True,
            duration_s=0.0,
        )
        # `get_all_service_images` returns the CURRENT (post-test) state.
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:debug",  # already debug
        }
        result = phase.run(ctx)
        assert result.status == "passed"
        assert result.details["restored"] is False
        phase._docker.recreate_service.assert_not_called()


class TestRestoresWhenMismatched:
    def test_recreates_with_original_tag_and_full_passthrough(self, phase) -> None:
        ctx = TestContext()
        ctx.prepare_old_stack = PrepareOldStackResult(
            old_image_tag="zdu-old-abc12345",
            # Pre-test state was on the "debug" tag.
            current_images={"datahub-gms-debug": "acryldata/datahub-gms:debug"},
            services_inspected=["datahub-gms-debug"],
            recreated_services=["datahub-gms-debug"],
            health_check_passed=True,
            duration_s=0.0,
        )
        # Post-test, GMS is on a NEW tag (the test left it there).
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:zdu-new-deadbeef",
        }
        with patch.object(phase, "_wait_for_health", return_value=True):
            result = phase.run(ctx)
        assert result.status == "passed"
        assert result.details["original_tag"] == "debug"
        assert result.details["restored"] is True

        # recreate_service must be called with the original tag + passthrough
        # secrets + an empty.env override (so AUTH_POLICIES_ENABLED comes back
        # to the dev default).
        phase._docker.recreate_service.assert_called_once()
        call = phase._docker.recreate_service.call_args
        env = call.kwargs["compose_env"]
        assert env["DATAHUB_VERSION"] == "debug"
        assert env["DATAHUB_GMS_VERSION"] == "debug"
        assert env["DATAHUB_LOCAL_COMMON_ENV"] == "empty.env"
        assert env["DATAHUB_TOKEN_SERVICE_SIGNING_KEY"] == "test-key"
        assert env["DATAHUB_TOKEN_SERVICE_SALT"] == "test-salt"


class TestHealthCheckTimeout:
    def test_fails_when_gms_does_not_come_back_healthy(self, phase) -> None:
        ctx = TestContext()
        ctx.prepare_old_stack = PrepareOldStackResult(
            old_image_tag="zdu-old-abc12345",
            current_images={"datahub-gms-debug": "acryldata/datahub-gms:debug"},
            services_inspected=["datahub-gms-debug"],
            recreated_services=["datahub-gms-debug"],
            health_check_passed=True,
            duration_s=0.0,
        )
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:zdu-new-deadbeef",
        }
        with patch.object(phase, "_wait_for_health", return_value=False):
            result = phase.run(ctx)
        assert result.status == "failed"
        assert "did not come back healthy" in (result.error or "")


class TestHealthCheckLogging:
    """G13 — _wait_for_health must emit a terminal log line on success and on
    timeout. Prior code silently returned True/False, leaving a 20s+ gap in
    the run log between the "polling" line and the next phase boundary.
    """

    def test_success_logs_health_confirmation(self, phase, caplog) -> None:
        ctx = TestContext()
        ctx.prepare_old_stack = PrepareOldStackResult(
            old_image_tag="zdu-old-abc12345",
            current_images={
                "datahub-gms-debug": "acryldata/datahub-gms:zdu-new-deadbeef"
            },
            services_inspected=["datahub-gms-debug"],
            recreated_services=["datahub-gms-debug"],
            health_check_passed=True,
            duration_s=0.0,
        )
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:zdu-new-deadbeef",
        }
        # Stub urlopen so the very first poll succeeds.
        fake_resp = MagicMock()
        fake_resp.status = 200
        fake_resp.__enter__ = MagicMock(return_value=fake_resp)
        fake_resp.__exit__ = MagicMock(return_value=False)
        with caplog.at_level("INFO", logger="tests.zdu.framework.phases.cleanup"):
            with patch(
                "tests.zdu.framework.phases.cleanup.urllib.request.urlopen",
                return_value=fake_resp,
            ):
                result = phase.run(ctx)
        assert result.status == "passed"
        assert any(
            "GMS healthy at" in rec.message and "/health after" in rec.message
            for rec in caplog.records
        ), f"missing GMS-healthy log line in {[r.message for r in caplog.records]}"

    def test_timeout_logs_failure(self, phase, caplog) -> None:
        ctx = TestContext()
        ctx.prepare_old_stack = PrepareOldStackResult(
            old_image_tag="zdu-old-abc12345",
            current_images={"datahub-gms-debug": "acryldata/datahub-gms:debug"},
            services_inspected=["datahub-gms-debug"],
            recreated_services=["datahub-gms-debug"],
            health_check_passed=True,
            duration_s=0.0,
        )
        # Force a fresh recreate path so _wait_for_health is invoked.
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:zdu-new-deadbeef",
        }
        # _wait_for_health must complete quickly — patch time.monotonic to push
        # the deadline past immediately on the second call, and stub urlopen to
        # always raise (no healthy response will ever come).
        import urllib.error

        with caplog.at_level("ERROR", logger="tests.zdu.framework.phases.cleanup"):
            with patch(
                "tests.zdu.framework.phases.cleanup.urllib.request.urlopen",
                side_effect=urllib.error.URLError("boom"),
            ):
                with patch(
                    "tests.zdu.framework.phases.cleanup.time.sleep", return_value=None
                ):
                    with patch(
                        "tests.zdu.framework.phases.cleanup.time.monotonic",
                        side_effect=[0.0, 0.0, 9999.0, 9999.0],
                    ):
                        result = phase.run(ctx)
        assert result.status == "failed"
        assert any("did not become healthy" in rec.message for rec in caplog.records), (
            f"missing timeout log line in {[r.message for r in caplog.records]}"
        )


class TestZduSkipCleanupEnvVar:
    """The ZDU_SKIP_CLEANUP=1 env var should add 'cleanup' to config.skip_phases."""

    def test_skip_cleanup_env_var_appends_to_skip_phases(self, monkeypatch) -> None:
        from tests.zdu.framework.config import ZDUTestConfig

        monkeypatch.setenv("ZDU_SKIP_CLEANUP", "1")
        cfg = ZDUTestConfig.from_env()
        assert "cleanup" in cfg.skip_phases

    def test_no_env_var_means_cleanup_not_skipped(self, monkeypatch) -> None:
        from tests.zdu.framework.config import ZDUTestConfig

        monkeypatch.delenv("ZDU_SKIP_CLEANUP", raising=False)
        cfg = ZDUTestConfig.from_env()
        assert "cleanup" not in cfg.skip_phases
