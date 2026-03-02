"""Tests for dh.naming — worktree name derivation and helpers."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

import pytest

from dh.naming import (
    _sanitize_name,
    db_name_for,
    derive_worktree_name,
    es_prefix_for,
    git_root,
    hostname_for,
    kafka_prefix_for,
    namespace_for,
)


class TestSanitizeName:
    def test_lowercase(self):
        assert _sanitize_name("FooBar") == "foobar"

    def test_special_chars_to_hyphens(self):
        assert _sanitize_name("foo_bar.baz") == "foo-bar-baz"

    def test_dedup_hyphens(self):
        assert _sanitize_name("foo--bar---baz") == "foo-bar-baz"

    def test_strip_leading_trailing_hyphens(self):
        assert _sanitize_name("-foo-") == "foo"

    def test_truncation_with_md5(self):
        long_name = "a" * 25
        result = _sanitize_name(long_name)
        assert len(result) <= 20
        # Should be 15-char prefix + hyphen + 4-char md5
        assert result.startswith("a" * 15 + "-")
        assert len(result) == 20

    def test_exactly_20_chars_no_truncation(self):
        name = "a" * 20
        assert _sanitize_name(name) == name

    def test_empty_after_sanitize(self):
        assert _sanitize_name("___") == ""


class TestNamespaceFor:
    def test_basic(self):
        assert namespace_for("main") == "dh-main"


class TestDbNameFor:
    def test_basic(self):
        assert db_name_for("main") == "datahub_main"

    def test_hyphens_to_underscores(self):
        assert db_name_for("my-branch") == "datahub_my_branch"


class TestEsPrefixFor:
    def test_hyphens_to_underscores(self):
        assert es_prefix_for("my-branch") == "my_branch"


class TestKafkaPrefixFor:
    def test_basic(self):
        assert kafka_prefix_for("main") == "main_"

    def test_hyphens_to_underscores(self):
        assert kafka_prefix_for("my-branch") == "my_branch_"


class TestHostnameFor:
    def test_basic(self):
        assert hostname_for("main") == "main.datahub.localhost"


class TestGitRoot:
    def test_success(self, monkeypatch):
        fake_run = MagicMock(
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0, stdout="/home/user/datahub\n", stderr=""
            )
        )
        monkeypatch.setattr("dh.naming.run", fake_run)
        assert git_root() == "/home/user/datahub"
        fake_run.assert_called_once_with(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            check=False,
        )

    def test_not_a_git_repo(self, monkeypatch):
        fake_run = MagicMock(
            return_value=subprocess.CompletedProcess(
                args=[], returncode=128, stdout="", stderr="fatal"
            )
        )
        monkeypatch.setattr("dh.naming.run", fake_run)
        with pytest.raises(SystemExit):
            git_root()


class TestDeriveWorktreeName:
    def test_env_var_override(self, monkeypatch):
        monkeypatch.setenv("DH_WORKTREE_NAME", "MyCustom")
        monkeypatch.setattr(
            "dh.naming.git_root", lambda: "/unused"
        )
        assert derive_worktree_name() == "mycustom"

    def test_strips_datahub_dot_prefix(self, monkeypatch):
        monkeypatch.delenv("DH_WORKTREE_NAME", raising=False)
        monkeypatch.setattr(
            "dh.naming.git_root", lambda: "/home/user/datahub.my-branch"
        )
        assert derive_worktree_name() == "my-branch"

    def test_strips_datahub_dash_prefix(self, monkeypatch):
        monkeypatch.delenv("DH_WORKTREE_NAME", raising=False)
        monkeypatch.setattr(
            "dh.naming.git_root", lambda: "/home/user/datahub-feature"
        )
        assert derive_worktree_name() == "feature"

    def test_plain_datahub_becomes_main(self, monkeypatch):
        monkeypatch.delenv("DH_WORKTREE_NAME", raising=False)
        monkeypatch.setattr(
            "dh.naming.git_root", lambda: "/home/user/datahub"
        )
        assert derive_worktree_name() == "main"

    def test_no_prefix_strip(self, monkeypatch):
        monkeypatch.delenv("DH_WORKTREE_NAME", raising=False)
        monkeypatch.setattr(
            "dh.naming.git_root", lambda: "/home/user/my-project"
        )
        assert derive_worktree_name() == "my-project"
