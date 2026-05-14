"""Unit tests for ZDUTestConfig — defaults and env binding."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.zdu.framework.config import ZDUTestConfig, _resolve_gms_token

# Repo root resolved the same way config.py does it — keeps the test
# decoupled from CWD so pytest can be invoked from anywhere.
_REPO_ROOT = Path(__file__).resolve().parents[4]


class TestOutputPathsAreRepoRootRelative:
    """G10 — report_path / build_dir must resolve absolute regardless of cwd.

    Running ``cd smoke-test && python -m tests.zdu`` used to produce a
    nested ``smoke-test/smoke-test/build/...`` path because the defaults
    were cwd-relative strings. Defaults now anchor to _REPO_ROOT.
    """

    def test_report_path_default_is_absolute(self) -> None:
        cfg = ZDUTestConfig()
        assert os.path.isabs(cfg.report_path), (
            f"report_path must be absolute, got {cfg.report_path!r}"
        )

    def test_report_path_default_lives_under_repo_root(self) -> None:
        cfg = ZDUTestConfig()
        assert (
            Path(cfg.report_path)
            == _REPO_ROOT / "smoke-test/build/zdu-test-report.json"
        )

    def test_build_dir_default_is_absolute(self) -> None:
        cfg = ZDUTestConfig()
        assert os.path.isabs(cfg.build_dir), (
            f"build_dir must be absolute, got {cfg.build_dir!r}"
        )

    def test_build_dir_default_lives_under_repo_root(self) -> None:
        cfg = ZDUTestConfig()
        assert Path(cfg.build_dir) == _REPO_ROOT / "smoke-test/build"

    def test_paths_are_invariant_to_cwd(self, tmp_path, monkeypatch) -> None:
        # Changing cwd must not affect the resolved defaults — this is the
        # exact regression: prior code produced different paths when run from
        # smoke-test/ vs. repo root.
        monkeypatch.chdir(tmp_path)
        cfg = ZDUTestConfig()
        assert Path(cfg.report_path).is_absolute()
        assert Path(cfg.build_dir).is_absolute()
        assert str(tmp_path) not in cfg.report_path
        assert str(tmp_path) not in cfg.build_dir


class TestResolveGmsToken:
    """G18 — token resolution falls back to ~/.datahubenv when the env var
    is unset, eliminating the copy-paste typo path that requiring
    ``export DATAHUB_GMS_TOKEN=<long jwt>`` creates.
    """

    @pytest.fixture(autouse=True)
    def _isolate_home_and_env(self, tmp_path, monkeypatch):
        # Redirect Path.home() so we can write a controlled .datahubenv.
        monkeypatch.setenv("HOME", str(tmp_path))
        # And clear any inherited env so tests don't accidentally use the
        # developer's real token.
        monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
        return tmp_path

    def test_env_var_wins_over_datahubenv(
        self, _isolate_home_and_env, monkeypatch
    ) -> None:
        # Both sources present → env var must win.
        (_isolate_home_and_env / ".datahubenv").write_text(
            "gms:\n  token: from-file-token\n"
        )
        monkeypatch.setenv("DATAHUB_GMS_TOKEN", "from-env-token")
        assert _resolve_gms_token() == "from-env-token"

    def test_falls_back_to_datahubenv_when_env_unset(
        self, _isolate_home_and_env
    ) -> None:
        (_isolate_home_and_env / ".datahubenv").write_text(
            "gms:\n  server: http://localhost:8080\n  token: from-file-token\n"
        )
        assert _resolve_gms_token() == "from-file-token"

    def test_returns_none_when_neither_source_present(
        self, _isolate_home_and_env
    ) -> None:
        # No .datahubenv, no env var. (HOME points at tmp_path with nothing in it.)
        assert _resolve_gms_token() is None

    def test_returns_none_when_datahubenv_has_no_token_line(
        self, _isolate_home_and_env
    ) -> None:
        (_isolate_home_and_env / ".datahubenv").write_text(
            "gms:\n  server: http://localhost:8080\n"
        )
        assert _resolve_gms_token() is None

    def test_returns_none_when_token_value_is_empty(
        self, _isolate_home_and_env
    ) -> None:
        # ``token:`` line with empty value (e.g. user deleted the JWT) must
        # not be treated as a valid token — None lets preflight surface a
        # clear remediation.
        (_isolate_home_and_env / ".datahubenv").write_text("gms:\n  token:   \n")
        assert _resolve_gms_token() is None

    def test_strips_whitespace_from_token(self, _isolate_home_and_env) -> None:
        (_isolate_home_and_env / ".datahubenv").write_text(
            "gms:\n  token:   eyJhbGciOiJI.payload.sig   \n"
        )
        assert _resolve_gms_token() == "eyJhbGciOiJI.payload.sig"

    def test_from_env_picks_up_datahubenv_token(self, _isolate_home_and_env) -> None:
        # End-to-end: ZDUTestConfig.from_env() must surface the fallback.
        (_isolate_home_and_env / ".datahubenv").write_text(
            "gms:\n  token: end-to-end-token\n"
        )
        cfg = ZDUTestConfig.from_env()
        assert cfg.gms_token == "end-to-end-token"


class TestUrnSyncEnabledEnvBinding:
    """D2 — promoted from module-level source toggle to env-bound config."""

    def test_default_is_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ZDU_URN_SYNC_ENABLED", raising=False)
        assert ZDUTestConfig.from_env().urn_sync_enabled is True

    def test_explicit_zero_disables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ZDU_URN_SYNC_ENABLED", "0")
        assert ZDUTestConfig.from_env().urn_sync_enabled is False

    def test_explicit_false_disables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ZDU_URN_SYNC_ENABLED", "false")
        assert ZDUTestConfig.from_env().urn_sync_enabled is False

    def test_unrecognized_value_keeps_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Any value other than the explicit-disable tokens leaves the default
        # in place — accidental typos shouldn't silently disable sync mode.
        monkeypatch.setenv("ZDU_URN_SYNC_ENABLED", "yes-please")
        assert ZDUTestConfig.from_env().urn_sync_enabled is True
