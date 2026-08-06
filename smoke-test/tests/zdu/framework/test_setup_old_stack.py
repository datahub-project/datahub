"""Unit tests for SetupOldStackPhase (G19c-v2 orchestrator)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.base import PhaseResult
from tests.zdu.framework.phases.setup_old_stack import SetupOldStackPhase


def _phase_result(
    name: str,
    status: Literal["passed", "failed", "skipped"] = "passed",
    **details,
) -> PhaseResult:
    return PhaseResult(
        phase_name=name,
        status=status,
        started_at=datetime.utcnow(),
        duration_s=0.1,
        details=details,
    )


@pytest.fixture
def build_phase() -> MagicMock:
    m = MagicMock()
    m.run.return_value = _phase_result(
        "build_images",
        old_image_tag="zdu-old-abc12345",
        new_image_tag="zdu-new-def67890",
        cache_hit=True,
    )
    return m


@pytest.fixture
def nuke_phase() -> MagicMock:
    m = MagicMock()
    m.run.return_value = _phase_result(
        "nuke_and_redeploy",
        old_image_tag="zdu-old-abc12345",
        wiped=True,
    )
    return m


@pytest.fixture
def setup_phase(build_phase, nuke_phase) -> SetupOldStackPhase:
    return SetupOldStackPhase(
        build_phase=build_phase,
        nuke_phase=nuke_phase,
        gms_url="http://localhost:8080",
    )


class TestDefaultAllSubStepsRun:
    """Defaults: clean_build=True, refresh_token=True → all 3 sub-steps fire."""

    def test_all_three_sub_steps_called(
        self, setup_phase, build_phase, nuke_phase
    ) -> None:
        cfg = ZDUTestConfig()  # defaults
        with patch.object(
            setup_phase,
            "_refresh_token",
            return_value={"status": "passed", "token_length": 42},
        ) as mock_refresh:
            result = setup_phase.run(TestContext(), cfg)

        assert result.status == "passed"
        build_phase.run.assert_called_once()
        nuke_phase.run.assert_called_once()
        mock_refresh.assert_called_once_with(cfg)
        sub = result.details["sub_results"]
        assert sub["build_images"]["status"] == "passed"
        assert sub["nuke_and_redeploy"]["status"] == "passed"
        assert sub["refresh_token"]["status"] == "passed"


class TestCleanBuildOptOut:
    def test_nuke_skipped_when_clean_build_false(
        self, setup_phase, build_phase, nuke_phase
    ) -> None:
        cfg = ZDUTestConfig(clean_build=False)
        with patch.object(
            setup_phase, "_refresh_token", return_value={"status": "passed"}
        ):
            result = setup_phase.run(TestContext(), cfg)

        assert result.status == "passed"
        build_phase.run.assert_called_once()
        nuke_phase.run.assert_not_called()
        sub = result.details["sub_results"]
        assert sub["nuke_and_redeploy"]["status"] == "skipped"
        assert "clean_build=False" in sub["nuke_and_redeploy"]["reason"]


class TestRefreshTokenOptOut:
    def test_token_refresh_skipped_when_disabled(
        self, setup_phase, build_phase, nuke_phase
    ) -> None:
        cfg = ZDUTestConfig(refresh_token=False)
        with patch.object(setup_phase, "_refresh_token") as mock_refresh:
            result = setup_phase.run(TestContext(), cfg)

        assert result.status == "passed"
        mock_refresh.assert_not_called()
        sub = result.details["sub_results"]
        assert sub["refresh_token"]["status"] == "skipped"


class TestFailurePropagation:
    """Each sub-step's failure aborts the orchestrator with a meaningful error."""

    def test_build_failure_propagates(
        self, setup_phase, build_phase, nuke_phase
    ) -> None:
        build_phase.run.return_value = _phase_result("build_images", status="failed")
        build_phase.run.return_value.error = "gradle build failed"
        cfg = ZDUTestConfig()
        result = setup_phase.run(TestContext(), cfg)
        assert result.status == "failed"
        assert "build_images failed" in (result.error or "")
        # Downstream sub-steps must not have been attempted.
        nuke_phase.run.assert_not_called()

    def test_nuke_failure_propagates(
        self, setup_phase, build_phase, nuke_phase
    ) -> None:
        nuke_phase.run.return_value = _phase_result(
            "nuke_and_redeploy", status="failed"
        )
        nuke_phase.run.return_value.error = "compose down -v failed"
        cfg = ZDUTestConfig()
        with patch.object(setup_phase, "_refresh_token") as mock_refresh:
            result = setup_phase.run(TestContext(), cfg)
        assert result.status == "failed"
        assert "nuke_and_redeploy failed" in (result.error or "")
        mock_refresh.assert_not_called()

    def test_token_refresh_failure_is_best_effort(
        self, setup_phase, build_phase, nuke_phase
    ) -> None:
        """Token refresh is now best-effort. Since nuke preserves MySQL, the
        existing ~/.datahubenv token usually still works against the
        redeployed stack — a failed refresh shouldn't abort the orchestrator.
        """
        cfg = ZDUTestConfig()
        with patch.object(
            setup_phase,
            "_refresh_token",
            return_value={"status": "failed", "error": "datahub CLI not found"},
        ):
            result = setup_phase.run(TestContext(), cfg)
        # Phase still passes — token failure is logged as a warning but not fatal.
        assert result.status == "passed"
        sub = result.details["sub_results"]
        assert sub["refresh_token"]["status"] == "failed"
        assert sub["refresh_token"]["error"] == "datahub CLI not found"


class TestRefreshTokenSubprocess:
    """The token-refresh sub-step shells out to `datahub init`. Validate the
    contract end-to-end: subprocess call, stdout parse, ~/.datahubenv read,
    config.gms_token mutation.
    """

    def test_writes_refreshed_token_to_config(
        self, setup_phase, tmp_path, monkeypatch
    ) -> None:
        # Stub HOME so we control ~/.datahubenv.
        monkeypatch.setenv("HOME", str(tmp_path))
        (tmp_path / ".datahubenv").write_text("gms:\n  token: fresh-jwt-token\n")
        cfg = ZDUTestConfig()
        cfg.gms_token = "stale-jwt"

        with patch(
            "tests.zdu.framework.phases.setup_old_stack.SetupOldStackPhase._find_datahub_cli",
            return_value="/fake/datahub",
        ):
            with patch(
                "tests.zdu.framework.phases.setup_old_stack.subprocess.run"
            ) as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                result = setup_phase._refresh_token(cfg)

        assert result["status"] == "passed"
        assert cfg.gms_token == "fresh-jwt-token"
        # subprocess was called with expected args.
        cmd = mock_run.call_args.args[0]
        assert cmd[0] == "/fake/datahub"
        assert "init" in cmd
        assert "--username" in cmd and "datahub" in cmd
        assert "--host" in cmd

    def test_fails_when_cli_missing(self, setup_phase) -> None:
        cfg = ZDUTestConfig()
        with patch(
            "tests.zdu.framework.phases.setup_old_stack.SetupOldStackPhase._find_datahub_cli",
            return_value=None,
        ):
            result = setup_phase._refresh_token(cfg)
        assert result["status"] == "failed"
        assert "datahub CLI not found" in result["error"]

    def test_fails_when_subprocess_returns_nonzero(self, setup_phase) -> None:
        cfg = ZDUTestConfig()
        with patch(
            "tests.zdu.framework.phases.setup_old_stack.SetupOldStackPhase._find_datahub_cli",
            return_value="/fake/datahub",
        ):
            with patch(
                "tests.zdu.framework.phases.setup_old_stack.subprocess.run"
            ) as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=1, stdout="", stderr="connection refused"
                )
                with patch("tests.zdu.framework.phases.setup_old_stack.time.sleep"):
                    result = setup_phase._refresh_token(cfg)
        assert result["status"] == "failed"
        assert "rc=1" in result["error"]
        assert "connection refused" in result["error"]

    def test_fails_when_datahubenv_has_no_token_after_init(
        self, setup_phase, tmp_path, monkeypatch
    ) -> None:
        monkeypatch.setenv("HOME", str(tmp_path))
        # datahub init "succeeded" but no .datahubenv written — defensive case.
        cfg = ZDUTestConfig()
        with patch(
            "tests.zdu.framework.phases.setup_old_stack.SetupOldStackPhase._find_datahub_cli",
            return_value="/fake/datahub",
        ):
            with patch(
                "tests.zdu.framework.phases.setup_old_stack.subprocess.run"
            ) as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                result = setup_phase._refresh_token(cfg)
        assert result["status"] == "failed"
        assert "no token: line" in result["error"]
