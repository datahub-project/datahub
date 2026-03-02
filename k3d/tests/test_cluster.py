"""Tests for dh.cluster — cluster lifecycle."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from dh.config import K3dConfig


class TestClusterUp:
    @patch("dh.cluster.require_cmd")
    def test_already_exists(self, mock_require, monkeypatch, capsys):
        def fake_run(cmd, **kwargs):
            if cmd == ["k3d", "cluster", "list", "datahub-dev"]:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0, stdout="", stderr=""
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )

        monkeypatch.setattr("dh.cluster.run", fake_run)

        from dh.cluster import cluster_up

        cfg = K3dConfig()
        cluster_up(cfg)

        captured = capsys.readouterr()
        assert "already exists" in captured.out

    @patch("dh.cluster.expand_template", return_value="# config")
    @patch("dh.cluster.require_cmd")
    def test_creates_when_missing(self, mock_require, mock_expand, monkeypatch, capsys):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            if "cluster" in cmd and "list" in cmd:
                # First call: cluster doesn't exist
                return subprocess.CompletedProcess(
                    args=cmd, returncode=1, stdout="", stderr=""
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )

        monkeypatch.setattr("dh.cluster.run", fake_run)

        from dh.cluster import cluster_up

        cfg = K3dConfig()
        cluster_up(cfg)

        # Should have called k3d cluster create
        create_cmds = [c for c in calls if "create" in c]
        assert len(create_cmds) == 1

        # Should wait for traefik
        wait_cmds = [c for c in calls if "traefik" in str(c)]
        assert len(wait_cmds) == 1


class TestClusterDown:
    @patch("dh.cluster.require_cmd")
    def test_warns_when_missing(self, mock_require, monkeypatch, capsys):
        monkeypatch.setattr(
            "dh.cluster.run",
            lambda cmd, **kw: subprocess.CompletedProcess(
                args=cmd, returncode=1, stdout="", stderr=""
            ),
        )

        from dh.cluster import cluster_down

        cluster_down(K3dConfig())

        captured = capsys.readouterr()
        assert "does not exist" in captured.err

    @patch("dh.cluster.require_cmd")
    def test_deletes_when_exists(self, mock_require, monkeypatch):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )

        monkeypatch.setattr("dh.cluster.run", fake_run)

        from dh.cluster import cluster_down

        cluster_down(K3dConfig())

        delete_cmds = [c for c in calls if "delete" in c]
        assert len(delete_cmds) == 1
        assert "datahub-dev" in delete_cmds[0]


class TestClusterStatus:
    @patch("dh.cluster.require_cmd")
    def test_exists(self, mock_require, monkeypatch, capsys):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )

        monkeypatch.setattr("dh.cluster.run", fake_run)

        from dh.cluster import cluster_status

        cluster_status(K3dConfig())

        # Should call list twice: once quiet check, once to display
        list_cmds = [c for c in calls if "list" in c]
        assert len(list_cmds) == 2

    @patch("dh.cluster.require_cmd")
    def test_missing(self, mock_require, monkeypatch, capsys):
        monkeypatch.setattr(
            "dh.cluster.run",
            lambda cmd, **kw: subprocess.CompletedProcess(
                args=cmd, returncode=1, stdout="", stderr=""
            ),
        )

        from dh.cluster import cluster_status

        cluster_status(K3dConfig())

        captured = capsys.readouterr()
        assert "does not exist" in captured.err
