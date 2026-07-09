"""Unit tests for PrepareOldStackPhase."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.prepare_old_stack import PrepareOldStackPhase


@pytest.fixture
def phase() -> PrepareOldStackPhase:
    docker = MagicMock()
    docker.get_all_service_images.return_value = {}
    # Default passthrough — tests that exercise the recreation path expect
    # both secrets to be present so the fail-fast guard does not fire.
    # Tests verifying the missing-passthrough path override this.
    docker.get_service_env.return_value = {
        "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "test-signing-key",
        "DATAHUB_TOKEN_SERVICE_SALT": "test-salt",
    }
    return PrepareOldStackPhase(
        docker=docker,
        services_to_restart=(
            "datahub-gms-debug",
            "datahub-mae-consumer-debug",
            "datahub-mce-consumer-debug",
        ),
        gms_service="datahub-gms-debug",
        health_url="http://localhost:8080",
        timeout_s=180,
    )


class TestSkipsWhenDisabled:
    def test_skips_when_prepare_old_stack_false(self, phase) -> None:
        cfg = ZDUTestConfig(prepare_old_stack=False)
        ctx = TestContext()
        result = phase.run(ctx, cfg)
        assert result.status == "skipped"
        assert ctx.prepare_old_stack is None


class TestSkipsWhenTagIsDefault:
    def test_skips_when_old_image_tag_is_debug(self, phase) -> None:
        # If Phase 0 didn't run, old_image_tag is the default 'debug' —
        # nothing meaningful to restart to. Skip with a clear log.
        cfg = ZDUTestConfig(prepare_old_stack=True, old_image_tag="debug")
        ctx = TestContext()
        result = phase.run(ctx, cfg)
        assert result.status == "skipped"
        assert "debug" in (result.details or {}).get("reason", "").lower()


class TestNoRestartWhenAlreadyOnOld:
    def test_passes_without_restart_when_image_matches(self, phase) -> None:
        cfg = ZDUTestConfig(prepare_old_stack=True, old_image_tag="zdu-old-abc12345")
        ctx = TestContext()
        # The running GMS already uses the OLD tag in its image string.
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:zdu-old-abc12345",
            "datahub-mae-consumer-debug": "acryldata/datahub-mae-consumer:zdu-old-abc12345",
            "datahub-mce-consumer-debug": "acryldata/datahub-mce-consumer:zdu-old-abc12345",
        }
        with patch.object(phase, "_wait_for_health", return_value=True):
            result = phase.run(ctx, cfg)
        assert result.status == "passed"
        assert ctx.prepare_old_stack is not None
        assert ctx.prepare_old_stack.recreated_services == []
        phase._docker.recreate_service.assert_not_called()


class TestRecreatesWhenMismatched:
    def test_recreates_all_services_when_running_debug(self, phase) -> None:
        cfg = ZDUTestConfig(prepare_old_stack=True, old_image_tag="zdu-old-abc12345")
        ctx = TestContext()
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:debug",
            "datahub-mae-consumer-debug": "acryldata/datahub-mae-consumer:debug",
            "datahub-mce-consumer-debug": "acryldata/datahub-mce-consumer:debug",
        }
        with patch.object(phase, "_wait_for_health", return_value=True):
            result = phase.run(ctx, cfg)
        assert result.status == "passed"
        assert phase._docker.recreate_service.call_count == 3
        assert ctx.prepare_old_stack is not None
        assert ctx.prepare_old_stack.recreated_services == list(
            phase._services_to_restart
        )


class TestPartialMismatch:
    def test_recreates_only_mismatched_services(self, phase) -> None:
        cfg = ZDUTestConfig(prepare_old_stack=True, old_image_tag="zdu-old-abc12345")
        ctx = TestContext()
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:debug",  # mismatch
            "datahub-mae-consumer-debug": "acryldata/datahub-mae-consumer:zdu-old-abc12345",  # match
            "datahub-mce-consumer-debug": "acryldata/datahub-mce-consumer:debug",  # mismatch
        }
        with patch.object(phase, "_wait_for_health", return_value=True):
            result = phase.run(ctx, cfg)
        assert result.status == "passed"
        assert ctx.prepare_old_stack is not None
        recreated = ctx.prepare_old_stack.recreated_services
        assert "datahub-gms-debug" in recreated
        assert "datahub-mce-consumer-debug" in recreated
        assert "datahub-mae-consumer-debug" not in recreated


class TestHealthCheckTimeout:
    def test_returns_failed_when_health_check_times_out(self, phase) -> None:
        cfg = ZDUTestConfig(prepare_old_stack=True, old_image_tag="zdu-old-abc12345")
        ctx = TestContext()
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:debug",
        }
        with patch.object(phase, "_wait_for_health", return_value=False):
            result = phase.run(ctx, cfg)
        assert result.status == "failed"
        assert "health" in (result.error or "").lower()


class TestPassthroughEnvForwarded:
    """Token-service secrets are extracted from the running GMS and forwarded
    to each recreate_service call so the cascade-launched system-update-debug
    can satisfy ${DATAHUB_TOKEN_SERVICE_SIGNING_KEY} / ${...SALT}.
    """

    def test_recreate_calls_include_token_service_secrets(self, phase) -> None:
        cfg = ZDUTestConfig(prepare_old_stack=True, old_image_tag="zdu-old-abc12345")
        ctx = TestContext()
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:debug",
            "datahub-mae-consumer-debug": "acryldata/datahub-mae-consumer:debug",
            "datahub-mce-consumer-debug": "acryldata/datahub-mce-consumer:debug",
        }
        with patch.object(phase, "_wait_for_health", return_value=True):
            result = phase.run(ctx, cfg)
        assert result.status == "passed"
        # Every recreate_service call must include the token-service secrets
        # plus the version env vars — without them, system-update-debug crashes.
        for call_obj in phase._docker.recreate_service.call_args_list:
            compose_env = call_obj.kwargs["compose_env"]
            assert (
                compose_env["DATAHUB_TOKEN_SERVICE_SIGNING_KEY"] == "test-signing-key"
            )
            assert compose_env["DATAHUB_TOKEN_SERVICE_SALT"] == "test-salt"
            assert compose_env["DATAHUB_VERSION"] == "zdu-old-abc12345"


class TestSkipsAbsentServices:
    """The debug compose profile collapses MAE+MCE consumers into GMS, so
    `docker compose up -d datahub-mae-consumer-debug` raises "no such service".
    Phase 0.5 must skip services that are not in the current running stack
    rather than blindly attempting to recreate them.
    """

    def test_only_recreates_present_services(self, phase) -> None:
        cfg = ZDUTestConfig(prepare_old_stack=True, old_image_tag="zdu-old-abc12345")
        ctx = TestContext()
        # Debug profile: only GMS is present; MAE+MCE collapsed in-process.
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:debug",
        }
        with patch.object(phase, "_wait_for_health", return_value=True):
            result = phase.run(ctx, cfg)
        assert result.status == "passed"
        # Only GMS recreated; MAE+MCE skipped because they are absent.
        assert phase._docker.recreate_service.call_count == 1
        call_obj = phase._docker.recreate_service.call_args_list[0]
        assert call_obj.kwargs["service"] == "datahub-gms-debug"


class TestFailFastOnMissingPassthrough:
    """When the token-service secrets are missing from the running GMS, the
    phase refuses to recreate — recreation would silently produce a broken
    stack (system-update-debug crashes at Spring init) which is much harder
    to diagnose than a clean phase failure here.
    """

    def test_fails_when_signing_key_missing(self, phase) -> None:
        cfg = ZDUTestConfig(prepare_old_stack=True, old_image_tag="zdu-old-abc12345")
        ctx = TestContext()
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:debug",
        }
        # Signing key missing → fail-fast before recreation.
        phase._docker.get_service_env.return_value = {
            "DATAHUB_TOKEN_SERVICE_SALT": "test-salt",
        }
        result = phase.run(ctx, cfg)
        assert result.status == "failed"
        assert "DATAHUB_TOKEN_SERVICE_SIGNING_KEY" in (result.error or "")
        phase._docker.recreate_service.assert_not_called()

    def test_fails_when_both_keys_empty_string(self, phase) -> None:
        # docker inspect can return entries with empty values (the Compose YAML
        # used ${VAR:-} substitution and VAR wasn't set). Treat empty as missing.
        cfg = ZDUTestConfig(prepare_old_stack=True, old_image_tag="zdu-old-abc12345")
        ctx = TestContext()
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:debug",
        }
        phase._docker.get_service_env.return_value = {
            "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "",
            "DATAHUB_TOKEN_SERVICE_SALT": "",
        }
        result = phase.run(ctx, cfg)
        assert result.status == "failed"
        assert "SIGNING_KEY" in (result.error or "") or "SALT" in (result.error or "")
        phase._docker.recreate_service.assert_not_called()
