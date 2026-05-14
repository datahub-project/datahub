"""Unit tests for NukeAndRedeployPhase (G19c)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.nuke_and_redeploy import NukeAndRedeployPhase


@pytest.fixture
def docker() -> MagicMock:
    d = MagicMock()
    d.get_service_env.return_value = {
        "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "captured-key",
        "DATAHUB_TOKEN_SERVICE_SALT": "captured-salt",
    }
    return d


@pytest.fixture
def phase(docker: MagicMock) -> NukeAndRedeployPhase:
    return NukeAndRedeployPhase(
        docker=docker,
        gms_service="datahub-gms-debug",
        health_url="http://localhost:8080",
        down_timeout_s=10,
        up_timeout_s=10,
        health_timeout_s=10,
    )


class TestSkipsWhenCleanBuildDisabled:
    def test_skipped_when_clean_build_false(self, phase, docker) -> None:
        cfg = ZDUTestConfig(clean_build=False)
        result = phase.run(TestContext(), cfg)
        assert result.status == "skipped"
        assert "ZDU_CLEAN_BUILD" in (result.details or {}).get("reason", "")
        docker.down_and_wipe.assert_not_called()
        docker.up_stack.assert_not_called()


class TestSkipsWhenOldTagIsDebug:
    """G19c requires a distinct OLD tag from Phase 0. Without that, the
    nuke would just bounce the dev stack on `debug` — destructive without
    buying any test signal.
    """

    def test_skipped_when_old_tag_is_debug(self, phase, docker) -> None:
        cfg = ZDUTestConfig(clean_build=True, old_image_tag="debug")
        result = phase.run(TestContext(), cfg)
        assert result.status == "skipped"
        assert "no distinct OLD tag" in (result.details or {}).get("reason", "")
        docker.down_and_wipe.assert_not_called()


class TestHappyPath:
    def test_captures_secrets_wipes_redeploys_and_waits_for_health(
        self, phase, docker, monkeypatch
    ) -> None:
        cfg = ZDUTestConfig(clean_build=True, old_image_tag="zdu-old-abc12345")
        monkeypatch.setenv("DATAHUB_LOCAL_COMMON_ENV", "zdu-test.env")

        with patch.object(phase, "_wait_for_health", return_value=True):
            result = phase.run(TestContext(), cfg)

        assert result.status == "passed", result.error
        # Capture token-service secrets BEFORE the nuke.
        docker.get_service_env.assert_called_once()
        assert (
            docker.get_service_env.call_args.args[1].__class__ is list
            or docker.get_service_env.call_args.args[1]
        )
        # Nuke runs before redeploy and wipes ES + MySQL volumes. Phase
        # passes SHORT names; DockerComposeClient prefixes them with the
        # live compose project name (was hardcoded "datahub_" — broke on
        # non-default clone dirs).
        docker.down_and_wipe.assert_called_once()
        assert sorted(docker.down_and_wipe.call_args.kwargs["wipe_volumes"]) == [
            "mysqldata",
            "osdata",
        ]
        docker.up_stack.assert_called_once()
        # Redeploy compose_env carries GMS-specific tag + preserved secrets + authz env.
        # NOT DATAHUB_VERSION (that cascades to frontend/actions which don't have OLD tag).
        compose_env = docker.up_stack.call_args.kwargs["compose_env"]
        assert "DATAHUB_VERSION" not in compose_env, (
            "DATAHUB_VERSION must NOT be set — it cascades to non-built services"
        )
        assert compose_env["DATAHUB_GMS_VERSION"] == "zdu-old-abc12345"
        assert compose_env["DATAHUB_TOKEN_SERVICE_SIGNING_KEY"] == "captured-key"
        assert compose_env["DATAHUB_TOKEN_SERVICE_SALT"] == "captured-salt"
        assert compose_env["DATAHUB_LOCAL_COMMON_ENV"] == "zdu-test.env"
        # G20a — reindex flags do NOT go through compose_env. They reach the
        # JVM via env_file (zdu-test.env, loaded by the compose YAML's
        # ${DATAHUB_LOCAL_COMMON_ENV} substitution). Asserting absence here
        # keeps the contract honest.
        assert (
            "ELASTICSEARCH_BUILD_INDICES_INCREMENTAL_REINDEX_ENABLED" not in compose_env
        )
        # Bring up the FULL profile (services=None). Otherwise kafka-broker
        # gets skipped (not in GMS's depends_on) and system-update-debug
        # crashes on Kafka producer init. Frontend/actions use :debug defaults.
        assert docker.up_stack.call_args.kwargs["services"] is None


class TestMissingLocalCommonEnv:
    def test_skips_authz_env_when_not_set(self, phase, docker, monkeypatch) -> None:
        # No DATAHUB_LOCAL_COMMON_ENV in env → compose_env shouldn't carry it.
        monkeypatch.delenv("DATAHUB_LOCAL_COMMON_ENV", raising=False)
        cfg = ZDUTestConfig(clean_build=True, old_image_tag="zdu-old-abc12345")

        with patch.object(phase, "_wait_for_health", return_value=True):
            phase.run(TestContext(), cfg)

        compose_env = docker.up_stack.call_args.kwargs["compose_env"]
        assert "DATAHUB_LOCAL_COMMON_ENV" not in compose_env


class TestPartialSecretCapture:
    """If only one of SIGNING_KEY/SALT is available from the running stack,
    the missing key is filled in from docker/.local-secrets.env (the
    canonical persisted source maintained by Gradle's build). Phase proceeds
    without warning.
    """

    def test_partial_capture_filled_from_secrets_file(
        self, docker, tmp_path, monkeypatch
    ) -> None:
        docker.get_service_env.return_value = {
            "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "k-from-running-stack",
            # SALT missing from running stack
        }
        # Stub the secrets file path to point at a fixture we control.
        fake_secrets = tmp_path / ".local-secrets.env"
        fake_secrets.write_text(
            "DATAHUB_TOKEN_SERVICE_SIGNING_KEY=k-from-file\n"
            "DATAHUB_TOKEN_SERVICE_SALT=salt-from-file\n"
        )
        monkeypatch.setattr(
            "tests.zdu.framework.phases.nuke_and_redeploy._LOCAL_SECRETS_FILE",
            fake_secrets,
        )
        phase = NukeAndRedeployPhase(
            docker=docker,
            gms_service="datahub-gms-debug",
            health_url="http://localhost:8080",
        )
        cfg = ZDUTestConfig(clean_build=True, old_image_tag="zdu-old-abc12345")

        with patch.object(phase, "_wait_for_health", return_value=True):
            result = phase.run(TestContext(), cfg)

        assert result.status == "passed"
        compose_env = docker.up_stack.call_args.kwargs["compose_env"]
        # Running-stack value wins for keys it captured; file fills the rest.
        assert (
            compose_env["DATAHUB_TOKEN_SERVICE_SIGNING_KEY"] == "k-from-running-stack"
        )
        assert compose_env["DATAHUB_TOKEN_SERVICE_SALT"] == "salt-from-file"


class TestHealthCheckFailure:
    def test_fails_when_gms_does_not_come_back(self, phase, docker) -> None:
        cfg = ZDUTestConfig(clean_build=True, old_image_tag="zdu-old-abc12345")
        with patch.object(phase, "_wait_for_health", return_value=False):
            result = phase.run(TestContext(), cfg)
        assert result.status == "failed"
        assert "did not become healthy" in (result.error or "")
        # Nuke + up still ran — only the health gate failed.
        docker.down_and_wipe.assert_called_once()
        docker.up_stack.assert_called_once()


class TestCleanBuildEnvVars:
    """clean_build is default-ON. ZDU_SKIP_NUKE opts out; ZDU_FORCE_NUKE
    overrides the opt-out (useful when both are set in CI / shell)."""

    def test_default_is_on(self, monkeypatch) -> None:
        monkeypatch.delenv("ZDU_SKIP_NUKE", raising=False)
        monkeypatch.delenv("ZDU_FORCE_NUKE", raising=False)
        cfg = ZDUTestConfig.from_env()
        assert cfg.clean_build is True

    def test_skip_nuke_opts_out(self, monkeypatch) -> None:
        monkeypatch.setenv("ZDU_SKIP_NUKE", "1")
        monkeypatch.delenv("ZDU_FORCE_NUKE", raising=False)
        cfg = ZDUTestConfig.from_env()
        assert cfg.clean_build is False

    def test_force_nuke_overrides_skip(self, monkeypatch) -> None:
        monkeypatch.setenv("ZDU_SKIP_NUKE", "1")
        monkeypatch.setenv("ZDU_FORCE_NUKE", "1")
        cfg = ZDUTestConfig.from_env()
        assert cfg.clean_build is True


class TestRefreshTokenEnvVar:
    def test_default_is_on(self, monkeypatch) -> None:
        monkeypatch.delenv("ZDU_SKIP_TOKEN_REFRESH", raising=False)
        cfg = ZDUTestConfig.from_env()
        assert cfg.refresh_token is True

    def test_opt_out(self, monkeypatch) -> None:
        monkeypatch.setenv("ZDU_SKIP_TOKEN_REFRESH", "1")
        cfg = ZDUTestConfig.from_env()
        assert cfg.refresh_token is False
