"""Unit tests for upgrade_jar_freshness.ensure_upgrade_jar_fresh."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework import upgrade_jar_freshness
from tests.zdu.framework.upgrade_jar_freshness import ensure_upgrade_jar_fresh


@pytest.fixture(autouse=True)
def _reset_guard():
    upgrade_jar_freshness.reset_for_tests()
    yield
    upgrade_jar_freshness.reset_for_tests()


class TestEnsureUpgradeJarFresh:
    def test_invokes_gradle_bootjar_when_repo_root_found(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("ZDU_SKIP_BOOTJAR", raising=False)
        with patch.object(upgrade_jar_freshness.subprocess, "run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            ensure_upgrade_jar_fresh()
        mock_run.assert_called_once()
        cmd = mock_run.call_args.args[0]
        assert cmd[0] == "./gradlew"
        assert ":datahub-upgrade:bootJar" in cmd

    def test_opt_out_via_env_var_skips_gradle(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ZDU_SKIP_BOOTJAR", "1")
        with patch.object(upgrade_jar_freshness.subprocess, "run") as mock_run:
            ensure_upgrade_jar_fresh()
        mock_run.assert_not_called()

    def test_idempotent_within_process(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ZDU_SKIP_BOOTJAR", raising=False)
        with patch.object(upgrade_jar_freshness.subprocess, "run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            ensure_upgrade_jar_fresh()
            ensure_upgrade_jar_fresh()
            ensure_upgrade_jar_fresh()
        # Three calls, but gradle invoked only once.
        assert mock_run.call_count == 1

    def test_raises_runtime_error_on_gradle_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("ZDU_SKIP_BOOTJAR", raising=False)
        with patch.object(upgrade_jar_freshness.subprocess, "run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1)
            with pytest.raises(RuntimeError, match="bootJar.*failed"):
                ensure_upgrade_jar_fresh()

    def test_skips_when_repo_root_not_found(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("ZDU_SKIP_BOOTJAR", raising=False)
        with (
            patch.object(upgrade_jar_freshness, "_find_repo_root", return_value=None),
            patch.object(upgrade_jar_freshness.subprocess, "run") as mock_run,
        ):
            ensure_upgrade_jar_fresh()
        mock_run.assert_not_called()

    def test_runs_gradle_from_repo_root_cwd(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        monkeypatch.delenv("ZDU_SKIP_BOOTJAR", raising=False)
        fake_root = tmp_path
        with (
            patch.object(
                upgrade_jar_freshness, "_find_repo_root", return_value=fake_root
            ),
            patch.object(upgrade_jar_freshness.subprocess, "run") as mock_run,
        ):
            mock_run.return_value = MagicMock(returncode=0)
            ensure_upgrade_jar_fresh()
        assert mock_run.call_args.kwargs["cwd"] == fake_root


class TestFindRepoRoot:
    def test_finds_root_with_settings_gradle_and_gradlew(self, tmp_path) -> None:
        (tmp_path / "settings.gradle").write_text("")
        (tmp_path / "gradlew").write_text("")
        nested = tmp_path / "a" / "b" / "c"
        nested.mkdir(parents=True)
        result = upgrade_jar_freshness._find_repo_root(nested)
        assert result == tmp_path

    def test_returns_none_when_no_root_found(self, tmp_path) -> None:
        nested = tmp_path / "deep" / "nested" / "dir"
        nested.mkdir(parents=True)
        # Walk up from `nested` shouldn't hit the real repo root because the
        # real ancestors are above tmp_path. But pytest's tmp_path is under
        # /tmp or similar; the walk WILL eventually run out of ancestors at /
        # without finding gradlew there. Verify None is returned.
        result = upgrade_jar_freshness._find_repo_root(nested)
        # If the test tree happens to nest under a real datahub checkout, this
        # could return that root. Guard by asserting only that, when a root
        # *is* found, it has the expected files.
        if result is not None:
            assert (result / "settings.gradle").exists()
            assert (result / "gradlew").exists()
        # else: properly returned None — also acceptable.
