"""Tests for dh.helm — HelmChartSource and ensure_helm_repo."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest

from dh.helm import HelmChartSource, ensure_helm_repo


class TestHelmChartSource:
    def test_chart_ref_with_path(self):
        src = HelmChartSource(path=Path("/my/chart"))
        assert src.chart_ref("datahub", "datahub") == "/my/chart"

    def test_chart_ref_without_path(self):
        src = HelmChartSource()
        assert src.chart_ref("datahub", "datahub-prerequisites") == "datahub/datahub-prerequisites"

    def test_version_args_with_version(self):
        src = HelmChartSource(version="1.3.0")
        assert src.version_args() == ["--version", "1.3.0"]

    def test_version_args_without_version(self):
        src = HelmChartSource()
        assert src.version_args() == []

    def test_mutual_exclusion(self):
        with pytest.raises(SystemExit):
            HelmChartSource(version="1.3.0", path=Path("/my/chart"))

    def test_both_none_is_ok(self):
        src = HelmChartSource()
        assert src.version is None
        assert src.path is None


class TestEnsureHelmRepo:
    def test_adds_repo_when_missing(self, monkeypatch):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append((cmd, kwargs))
            if cmd[:3] == ["helm", "repo", "list"]:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0, stdout="[]", stderr=""
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )

        monkeypatch.setattr("dh.helm.run", fake_run)
        ensure_helm_repo("datahub", "https://helm.datahubproject.io/")

        cmds = [c[0] for c in calls]
        assert ["helm", "repo", "list", "-o", "json"] in cmds
        assert ["helm", "repo", "add", "datahub", "https://helm.datahubproject.io/"] in cmds
        assert ["helm", "repo", "update", "datahub"] in cmds

    def test_skips_add_when_present(self, monkeypatch):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append((cmd, kwargs))
            if cmd[:3] == ["helm", "repo", "list"]:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0, stdout='[{"name":"datahub"}]', stderr=""
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )

        monkeypatch.setattr("dh.helm.run", fake_run)
        ensure_helm_repo("datahub", "https://helm.datahubproject.io/")

        cmds = [c[0] for c in calls]
        assert ["helm", "repo", "add", "datahub", "https://helm.datahubproject.io/"] not in cmds
        assert ["helm", "repo", "update", "datahub"] in cmds
