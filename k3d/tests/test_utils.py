"""Tests for dh.utils — subprocess runner and helpers."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from dh.utils import require_cmd, run


class TestRun:
    def test_success(self, monkeypatch):
        fake = MagicMock(
            return_value=subprocess.CompletedProcess(
                args=["echo", "hi"], returncode=0, stdout="hi\n", stderr=""
            )
        )
        monkeypatch.setattr("subprocess.run", fake)
        result = run(["echo", "hi"], check=False)
        assert result.returncode == 0
        fake.assert_called_once()

    def test_check_failure_exits(self, monkeypatch):
        monkeypatch.setattr(
            "subprocess.run",
            MagicMock(
                side_effect=subprocess.CalledProcessError(
                    1, ["false"], output="", stderr="bad"
                )
            ),
        )
        with pytest.raises(SystemExit):
            run(["false"], check=True)

    def test_capture_output_sets_pipe(self, monkeypatch):
        fake = MagicMock(
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0, stdout="out", stderr=""
            )
        )
        monkeypatch.setattr("subprocess.run", fake)
        run(["test"], capture_output=True, check=False)
        call_kwargs = fake.call_args[1]
        assert call_kwargs["stdout"] == subprocess.PIPE
        assert call_kwargs["stderr"] == subprocess.PIPE

    def test_quiet_sets_devnull(self, monkeypatch):
        fake = MagicMock(
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr=""
            )
        )
        monkeypatch.setattr("subprocess.run", fake)
        run(["test"], quiet=True, check=False)
        call_kwargs = fake.call_args[1]
        assert call_kwargs["stdout"] == subprocess.DEVNULL
        assert call_kwargs["stderr"] == subprocess.DEVNULL

    def test_input_text(self, monkeypatch):
        fake = MagicMock(
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr=""
            )
        )
        monkeypatch.setattr("subprocess.run", fake)
        run(["test"], input_text="hello", check=False)
        call_kwargs = fake.call_args[1]
        assert call_kwargs["input"] == "hello"

    def test_cwd(self, monkeypatch):
        fake = MagicMock(
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr=""
            )
        )
        monkeypatch.setattr("subprocess.run", fake)
        run(["test"], cwd="/tmp", check=False)
        call_kwargs = fake.call_args[1]
        assert call_kwargs["cwd"] == "/tmp"


class TestRequireCmd:
    def test_found(self, monkeypatch):
        monkeypatch.setattr("shutil.which", lambda name: f"/usr/bin/{name}")
        require_cmd("git")  # should not raise

    def test_not_found(self, monkeypatch):
        monkeypatch.setattr("shutil.which", lambda name: None)
        with pytest.raises(SystemExit):
            require_cmd("nonexistent-tool")
