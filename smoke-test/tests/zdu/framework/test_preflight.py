"""Unit tests for PreflightPhase (G1/G2/G3 — setup-time failure surfacing)."""

from __future__ import annotations

import urllib.error
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.preflight import PreflightPhase


@pytest.fixture
def phase(tmp_path: Path) -> PreflightPhase:
    return PreflightPhase(project_dir=str(tmp_path))


def _ok_urlopen() -> MagicMock:
    resp = MagicMock()
    resp.status = 200
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=resp)
    cm.__exit__ = MagicMock(return_value=False)
    return cm


def _write_bypass_env(tmp_path: Path) -> str:
    env_file = tmp_path / "zdu-test.env"
    env_file.write_text(
        "AUTH_POLICIES_ENABLED=false\n"
        "REST_API_AUTHORIZATION_ENABLED=false\n"
        "METADATA_SERVICE_AUTH_ENABLED=false\n"
    )
    return "zdu-test.env"


class TestAllChecksPass:
    def test_passes_when_stack_up_token_set_and_env_correct(
        self, phase: PreflightPhase, tmp_path: Path, monkeypatch
    ) -> None:
        env_file_name = _write_bypass_env(tmp_path)
        monkeypatch.setenv("DATAHUB_LOCAL_COMMON_ENV", env_file_name)
        cfg = ZDUTestConfig(
            gms_token="test-token", clean_build=False, refresh_token=False
        )
        ctx = TestContext()
        with patch(
            "tests.zdu.framework.phases.preflight.urllib.request.urlopen",
            return_value=_ok_urlopen(),
        ):
            result = phase.run(ctx, cfg)
        assert result.status == "passed"


class TestGmsUnreachable:
    def test_fails_when_health_endpoint_unreachable(
        self, phase: PreflightPhase, tmp_path: Path, monkeypatch
    ) -> None:
        env_file_name = _write_bypass_env(tmp_path)
        monkeypatch.setenv("DATAHUB_LOCAL_COMMON_ENV", env_file_name)
        cfg = ZDUTestConfig(
            gms_token="test-token", clean_build=False, refresh_token=False
        )
        ctx = TestContext()
        with patch(
            "tests.zdu.framework.phases.preflight.urllib.request.urlopen",
            side_effect=urllib.error.URLError("Connection refused"),
        ):
            result = phase.run(ctx, cfg)
        assert result.status == "failed"
        assert "datahub-dev.sh start" in (result.error or "")


class TestTokenMissing:
    def test_fails_when_token_none(
        self, phase: PreflightPhase, tmp_path: Path, monkeypatch
    ) -> None:
        env_file_name = _write_bypass_env(tmp_path)
        monkeypatch.setenv("DATAHUB_LOCAL_COMMON_ENV", env_file_name)
        cfg = ZDUTestConfig(gms_token=None, clean_build=False, refresh_token=False)
        ctx = TestContext()
        with patch(
            "tests.zdu.framework.phases.preflight.urllib.request.urlopen",
            return_value=_ok_urlopen(),
        ):
            result = phase.run(ctx, cfg)
        assert result.status == "failed"
        assert "DATAHUB_GMS_TOKEN" in (result.error or "")
        assert "datahub init" in (result.error or "")

    def test_fails_when_token_empty_string(
        self, phase: PreflightPhase, tmp_path: Path, monkeypatch
    ) -> None:
        env_file_name = _write_bypass_env(tmp_path)
        monkeypatch.setenv("DATAHUB_LOCAL_COMMON_ENV", env_file_name)
        cfg = ZDUTestConfig(gms_token="", clean_build=False, refresh_token=False)
        ctx = TestContext()
        with patch(
            "tests.zdu.framework.phases.preflight.urllib.request.urlopen",
            return_value=_ok_urlopen(),
        ):
            result = phase.run(ctx, cfg)
        assert result.status == "failed"
        assert "DATAHUB_GMS_TOKEN" in (result.error or "")


class TestAuthzBypassEnvUnset:
    def test_fails_when_local_common_env_unset(
        self, phase: PreflightPhase, monkeypatch
    ) -> None:
        monkeypatch.delenv("DATAHUB_LOCAL_COMMON_ENV", raising=False)
        cfg = ZDUTestConfig(
            gms_token="test-token", clean_build=False, refresh_token=False
        )
        ctx = TestContext()
        with patch(
            "tests.zdu.framework.phases.preflight.urllib.request.urlopen",
            return_value=_ok_urlopen(),
        ):
            result = phase.run(ctx, cfg)
        assert result.status == "failed"
        assert "DATAHUB_LOCAL_COMMON_ENV" in (result.error or "")
        assert "zdu-test.env" in (result.error or "")

    def test_fails_when_local_common_env_set_to_empty_env(
        self, phase: PreflightPhase, monkeypatch
    ) -> None:
        monkeypatch.setenv("DATAHUB_LOCAL_COMMON_ENV", "empty.env")
        cfg = ZDUTestConfig(
            gms_token="test-token", clean_build=False, refresh_token=False
        )
        ctx = TestContext()
        with patch(
            "tests.zdu.framework.phases.preflight.urllib.request.urlopen",
            return_value=_ok_urlopen(),
        ):
            result = phase.run(ctx, cfg)
        assert result.status == "failed"
        assert "DATAHUB_LOCAL_COMMON_ENV" in (result.error or "")


class TestAuthzBypassEnvFileBroken:
    def test_fails_when_env_file_does_not_exist(
        self, phase: PreflightPhase, monkeypatch
    ) -> None:
        monkeypatch.setenv("DATAHUB_LOCAL_COMMON_ENV", "nonexistent.env")
        cfg = ZDUTestConfig(
            gms_token="test-token", clean_build=False, refresh_token=False
        )
        ctx = TestContext()
        with patch(
            "tests.zdu.framework.phases.preflight.urllib.request.urlopen",
            return_value=_ok_urlopen(),
        ):
            result = phase.run(ctx, cfg)
        assert result.status == "failed"
        assert "no such file" in (result.error or "")

    def test_fails_when_env_file_missing_bypass_keys(
        self, phase: PreflightPhase, tmp_path: Path, monkeypatch
    ) -> None:
        # File exists but lacks the required bypass keys.
        bad_env = tmp_path / "bad.env"
        bad_env.write_text("SOME_OTHER_KEY=value\n")
        monkeypatch.setenv("DATAHUB_LOCAL_COMMON_ENV", "bad.env")
        cfg = ZDUTestConfig(
            gms_token="test-token", clean_build=False, refresh_token=False
        )
        ctx = TestContext()
        with patch(
            "tests.zdu.framework.phases.preflight.urllib.request.urlopen",
            return_value=_ok_urlopen(),
        ):
            result = phase.run(ctx, cfg)
        assert result.status == "failed"
        assert "missing required bypass keys" in (result.error or "")


class TestMultipleFailuresReportedTogether:
    def test_all_failures_collected_in_single_run(
        self, phase: PreflightPhase, monkeypatch
    ) -> None:
        # Everything broken at once. With clean_build=False + refresh_token=False
        # we force ALL checks to fire (the default-on flags skip GMS-reachable
        # and token-set so SetupOldStackPhase can fix them; here we test the
        # "user has explicitly opted out of both" path).
        monkeypatch.delenv("DATAHUB_LOCAL_COMMON_ENV", raising=False)
        cfg = ZDUTestConfig(gms_token=None, clean_build=False, refresh_token=False)
        ctx = TestContext()
        with patch(
            "tests.zdu.framework.phases.preflight.urllib.request.urlopen",
            side_effect=urllib.error.URLError("Connection refused"),
        ):
            result = phase.run(ctx, cfg)
        assert result.status == "failed"
        # All three remediation messages should be present.
        err = result.error or ""
        assert "datahub-dev.sh start" in err
        assert "DATAHUB_GMS_TOKEN" in err
        assert "DATAHUB_LOCAL_COMMON_ENV" in err
        assert result.details.get("failed_checks") == 3


class TestConditionalSkipping:
    """G19c-v2 — when SetupOldStackPhase's clean_build / refresh_token defaults
    are on, preflight skips the GMS-reachable + token-set checks so the
    setup phase can recover from a fully-down stack or missing token.
    """

    def test_gms_check_skipped_when_clean_build_on(
        self, phase: PreflightPhase, tmp_path: Path, monkeypatch
    ) -> None:
        env_file_name = _write_bypass_env(tmp_path)
        monkeypatch.setenv("DATAHUB_LOCAL_COMMON_ENV", env_file_name)
        # clean_build=True (default) → GMS-reachable check should NOT fire.
        cfg = ZDUTestConfig(
            gms_token="test-token", clean_build=True, refresh_token=False
        )
        ctx = TestContext()
        # urlopen patched to raise — if the check ran, preflight would fail.
        with patch(
            "tests.zdu.framework.phases.preflight.urllib.request.urlopen",
            side_effect=urllib.error.URLError("Connection refused"),
        ):
            result = phase.run(ctx, cfg)
        assert result.status == "passed", result.error

    def test_token_check_skipped_when_refresh_on(
        self, phase: PreflightPhase, tmp_path: Path, monkeypatch
    ) -> None:
        env_file_name = _write_bypass_env(tmp_path)
        monkeypatch.setenv("DATAHUB_LOCAL_COMMON_ENV", env_file_name)
        # refresh_token=True (default) → token-set check should NOT fire.
        cfg = ZDUTestConfig(gms_token=None, clean_build=False, refresh_token=True)
        ctx = TestContext()
        with patch(
            "tests.zdu.framework.phases.preflight.urllib.request.urlopen",
            return_value=_ok_urlopen(),
        ):
            result = phase.run(ctx, cfg)
        assert result.status == "passed", result.error

    def test_both_defaults_on_only_authz_env_checked(
        self, phase: PreflightPhase, tmp_path: Path, monkeypatch
    ) -> None:
        # New default: both clean_build and refresh_token are ON. Preflight
        # reduces to just the DATAHUB_LOCAL_COMMON_ENV check. With nothing
        # else set up (down stack, no token), preflight should still pass
        # because the only remaining check is the env-file check.
        env_file_name = _write_bypass_env(tmp_path)
        monkeypatch.setenv("DATAHUB_LOCAL_COMMON_ENV", env_file_name)
        cfg = ZDUTestConfig()  # all defaults
        ctx = TestContext()
        with patch(
            "tests.zdu.framework.phases.preflight.urllib.request.urlopen",
            side_effect=urllib.error.URLError("Connection refused"),
        ):
            result = phase.run(ctx, cfg)
        assert result.status == "passed", result.error
