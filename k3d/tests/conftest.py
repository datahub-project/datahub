"""Shared fixtures for k3d CLI tests."""

from __future__ import annotations

import subprocess
from typing import Any
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_run(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Patch dh.utils.run to record calls without executing."""
    fake = MagicMock(
        return_value=subprocess.CompletedProcess(
            args=[], returncode=0, stdout="", stderr=""
        )
    )
    monkeypatch.setattr("dh.utils.run", fake)
    return fake


@pytest.fixture
def profiles_dir(tmp_path):
    """Create a temp profiles directory with the three YAML files."""
    for name in ("minimal", "consumers", "backend"):
        (tmp_path / f"{name}.yaml").write_text(f"# {name} profile values\n")
    return tmp_path
