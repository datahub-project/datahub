"""Tests for dh.infra — infra lifecycle."""

from __future__ import annotations

import subprocess
from unittest.mock import patch

import pytest

from dh.config import K3dConfig
from dh.helm import HelmChartSource


class TestInfraUp:
    @patch("dh.infra.wait_for_pods")
    @patch("dh.infra.require_cluster")
    @patch("dh.infra.require_cmd")
    @patch("dh.infra.ensure_helm_repo")
    def test_creates_namespace_and_runs_helm(
        self, mock_ensure, mock_require_cmd, mock_require_cluster, mock_wait, monkeypatch
    ):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append((cmd, kwargs))
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="ns yaml", stderr=""
            )

        monkeypatch.setattr("dh.infra.run", fake_run)

        from dh.infra import infra_up

        cfg = K3dConfig()
        infra_up(cfg, HelmChartSource())

        cmds = [c[0] for c in calls]

        # Should create namespace
        ns_cmds = [c for c in cmds if "namespace" in c and "create" in c]
        assert len(ns_cmds) == 1

        # Should run helm upgrade --install
        helm_cmds = [c for c in cmds if "helm" in c and "upgrade" in c]
        assert len(helm_cmds) == 1
        helm_cmd = helm_cmds[0]
        assert "--install" in helm_cmd
        assert "datahub-infra" in helm_cmd

    @patch("dh.infra.wait_for_pods")
    @patch("dh.infra.require_cluster")
    @patch("dh.infra.require_cmd")
    def test_skips_repo_with_local_path(
        self, mock_require_cmd, mock_require_cluster, mock_wait, monkeypatch, tmp_path
    ):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="ns yaml", stderr=""
            )

        monkeypatch.setattr("dh.infra.run", fake_run)

        from dh.infra import infra_up

        cfg = K3dConfig()
        infra_up(cfg, HelmChartSource(path=tmp_path))

        # ensure_helm_repo should NOT have been called (patched separately, so check helm cmd uses path)
        helm_cmds = [c for c in calls if "helm" in c and "upgrade" in c]
        assert len(helm_cmds) == 1
        assert str(tmp_path) in helm_cmds[0]


class TestInfraDown:
    @patch("dh.infra.require_cluster")
    @patch("dh.infra.require_cmd")
    def test_warns_on_active_worktrees(self, mock_cmd, mock_cluster, monkeypatch, capsys):
        call_count = [0]

        def fake_run(cmd, **kwargs):
            call_count[0] += 1
            if "get" in cmd and "namespaces" in cmd and "-l" in cmd:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0,
                    stdout="namespace/dh-mybranch\n", stderr=""
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )

        monkeypatch.setattr("dh.infra.run", fake_run)

        from dh.infra import infra_down

        with pytest.raises(SystemExit):
            infra_down(K3dConfig(), force=False)

        captured = capsys.readouterr()
        assert "Active worktree" in captured.err

    @patch("dh.infra.require_cluster")
    @patch("dh.infra.require_cmd")
    def test_force_deletes_with_active_worktrees(self, mock_cmd, mock_cluster, monkeypatch):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            if "get" in cmd and "namespaces" in cmd and "-l" in cmd:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0,
                    stdout="namespace/dh-mybranch\n", stderr=""
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )

        monkeypatch.setattr("dh.infra.run", fake_run)

        from dh.infra import infra_down

        infra_down(K3dConfig(), force=True)

        # Should proceed to delete
        delete_cmds = [c for c in calls if "delete" in c and "namespace" in c]
        assert len(delete_cmds) == 1

    @patch("dh.infra.require_cluster")
    @patch("dh.infra.require_cmd")
    def test_noop_when_namespace_missing(self, mock_cmd, mock_cluster, monkeypatch, capsys):
        def fake_run(cmd, **kwargs):
            if "get" in cmd and "namespaces" in cmd and "-l" in cmd:
                # No active worktrees
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0, stdout="", stderr=""
                )
            if "get" in cmd and "namespace" in cmd:
                # Infra namespace doesn't exist
                return subprocess.CompletedProcess(
                    args=cmd, returncode=1, stdout="", stderr=""
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )

        monkeypatch.setattr("dh.infra.run", fake_run)

        from dh.infra import infra_down

        infra_down(K3dConfig(), force=False)

        captured = capsys.readouterr()
        assert "does not exist" in captured.err


class TestInfraStatus:
    @patch("dh.infra.require_cluster")
    def test_exists(self, mock_cluster, monkeypatch, capsys):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )

        monkeypatch.setattr("dh.infra.run", fake_run)

        from dh.infra import infra_status

        infra_status(K3dConfig())

        captured = capsys.readouterr()
        assert "Infra pods" in captured.out

    @patch("dh.infra.require_cluster")
    def test_missing(self, mock_cluster, monkeypatch, capsys):
        monkeypatch.setattr(
            "dh.infra.run",
            lambda cmd, **kw: subprocess.CompletedProcess(
                args=cmd, returncode=1, stdout="", stderr=""
            ),
        )

        from dh.infra import infra_status

        infra_status(K3dConfig())

        captured = capsys.readouterr()
        assert "does not exist" in captured.err
