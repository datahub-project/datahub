"""Tests for dh.deploy — deploy orchestration."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dh.deploy import _debug_set_args


class TestDebugSetArgs:
    def test_returns_set_pairs(self):
        result = _debug_set_args()
        # Should be a flat list of --set, value, --set, value, ...
        assert len(result) % 2 == 0
        for i in range(0, len(result), 2):
            assert result[i] == "--set"

    def test_gms_indices_9_through_12(self):
        result = _debug_set_args()
        values = [result[i] for i in range(1, len(result), 2)]
        gms_values = [v for v in values if v.startswith("datahub-gms.")]
        # Indices 9, 10, 11, 12 = 4 name + 4 value = 8 entries
        assert len(gms_values) == 8
        indices = set()
        for v in gms_values:
            idx = int(v.split("[")[1].split("]")[0])
            indices.add(idx)
        assert indices == {9, 10, 11, 12}

    def test_frontend_index_5(self):
        result = _debug_set_args()
        values = [result[i] for i in range(1, len(result), 2)]
        fe_values = [v for v in values if v.startswith("datahub-frontend.")]
        # Index 5: name + value = 2 entries
        assert len(fe_values) == 2
        indices = set()
        for v in fe_values:
            idx = int(v.split("[")[1].split("]")[0])
            indices.add(idx)
        assert indices == {5}

    def test_jdwp_ports(self):
        result = _debug_set_args()
        values = [result[i] for i in range(1, len(result), 2)]
        jdwp_values = [v for v in values if "agentlib" in v]
        # One for GMS (5001), one for Frontend (5002)
        assert any("5001" in v for v in jdwp_values)
        assert any("5002" in v for v in jdwp_values)


def _make_deploy_fake_run():
    """Create a fake run() that records commands and returns plausible results."""
    captured = []

    def fake_run(cmd, **kwargs):
        captured.append(cmd)
        stdout = ""
        returncode = 0

        # _check_collision: "kubectl get namespace dh-test" → not found (no collision)
        if cmd[:3] == ["kubectl", "get", "namespace"] and "jsonpath" not in str(cmd):
            returncode = 1

        # _ensure_database needs a mysql pod name
        if "jsonpath" in str(cmd) and "mysql" in str(cmd):
            stdout = "mysql-pod-0"

        # _ensure_mysql_secret: dry-run creates yaml
        if "--dry-run=client" in cmd:
            stdout = "apiVersion: v1\nkind: Secret\n"

        return subprocess.CompletedProcess(
            args=cmd, returncode=returncode, stdout=stdout, stderr=""
        )

    return captured, fake_run


class TestDeploySetOrdering:
    """Verify that --set args are ordered: profiles → debug → user."""

    def _extract_helm_cmd_set_args(self, helm_cmd: list[str]) -> list[str]:
        """Extract only the --set value args from a helm command."""
        result = []
        for i, arg in enumerate(helm_cmd):
            if arg == "--set" and i + 1 < len(helm_cmd):
                result.append(helm_cmd[i + 1])
        return result

    @patch("dh.deploy.wait_for_pods")
    @patch("dh.deploy.wait_for_jobs")
    @patch("dh.deploy.expand_template", return_value="# rendered values")
    @patch("dh.deploy.git_root", return_value="/fake/root")
    @patch("dh.deploy.derive_worktree_name", return_value="test")
    @patch("dh.deploy.require_cmd")
    @patch("dh.deploy.require_infra")
    @patch("dh.deploy.ensure_helm_repo")
    def test_user_set_args_come_last(
        self,
        mock_ensure,
        mock_infra,
        mock_require_cmd,
        mock_derive,
        mock_git_root,
        mock_expand,
        mock_wait_jobs,
        mock_wait_pods,
        monkeypatch,
    ):
        """User --set args should override profiles and debug."""
        captured_cmds, fake_run = _make_deploy_fake_run()
        monkeypatch.setattr("dh.deploy.run", fake_run)

        from dh.config import K3dConfig
        from dh.deploy import deploy
        from dh.helm import HelmChartSource

        cfg = K3dConfig()
        deploy(
            cfg,
            HelmChartSource(),
            extra_set_args=("global.my.key=userval",),
            debug=True,
            profiles=("minimal",),
        )

        # Find the helm upgrade --install command
        helm_cmds = [c for c in captured_cmds if "helm" in c and "upgrade" in c]
        assert len(helm_cmds) == 1
        helm_cmd = helm_cmds[0]

        set_values = self._extract_helm_cmd_set_args(helm_cmd)
        assert len(set_values) > 0

        # User value should be last
        assert set_values[-1] == "global.my.key=userval"

        # Profile set args should come before debug
        profile_idx = None
        debug_idx = None
        user_idx = None
        for i, v in enumerate(set_values):
            if "extraEnvs[3]" in v:
                profile_idx = i
            if "JAVA_TOOL_OPTIONS" in v and debug_idx is None:
                debug_idx = i
            if v == "global.my.key=userval":
                user_idx = i

        assert profile_idx is not None
        assert debug_idx is not None
        assert user_idx is not None
        assert profile_idx < debug_idx < user_idx


class TestDeployValuesOrdering:
    """Verify values file ordering: template → profile → override → user."""

    @patch("dh.deploy.wait_for_pods")
    @patch("dh.deploy.wait_for_jobs")
    @patch("dh.deploy.expand_template", return_value="# rendered values")
    @patch("dh.deploy.git_root", return_value="/fake/root")
    @patch("dh.deploy.derive_worktree_name", return_value="test")
    @patch("dh.deploy.require_cmd")
    @patch("dh.deploy.require_infra")
    @patch("dh.deploy.ensure_helm_repo")
    def test_values_file_order(
        self,
        mock_ensure,
        mock_infra,
        mock_require_cmd,
        mock_derive,
        mock_git_root,
        mock_expand,
        mock_wait_jobs,
        mock_wait_pods,
        monkeypatch,
        tmp_path,
    ):
        captured_cmds, fake_run = _make_deploy_fake_run()
        monkeypatch.setattr("dh.deploy.run", fake_run)

        user_values = tmp_path / "user.yaml"
        user_values.write_text("# user values")

        from dh.config import K3dConfig
        from dh.deploy import deploy
        from dh.helm import HelmChartSource

        cfg = K3dConfig()
        deploy(
            cfg,
            HelmChartSource(),
            extra_values_files=(str(user_values),),
        )

        helm_cmds = [c for c in captured_cmds if "helm" in c and "upgrade" in c]
        assert len(helm_cmds) == 1
        helm_cmd = helm_cmds[0]

        # Extract values files in order
        values_files = []
        for i, arg in enumerate(helm_cmd):
            if arg == "--values" and i + 1 < len(helm_cmd):
                values_files.append(helm_cmd[i + 1])

        # First should be the template-rendered file, last should be user file
        assert len(values_files) >= 2
        assert values_files[-1] == str(user_values)


class TestDeployBackendProfile:
    """Backend profile should suppress frontend URL in output."""

    @patch("dh.deploy.wait_for_pods")
    @patch("dh.deploy.wait_for_jobs")
    @patch("dh.deploy.expand_template", return_value="# rendered values")
    @patch("dh.deploy.git_root", return_value="/fake/root")
    @patch("dh.deploy.derive_worktree_name", return_value="test")
    @patch("dh.deploy.require_cmd")
    @patch("dh.deploy.require_infra")
    @patch("dh.deploy.ensure_helm_repo")
    def test_backend_hides_frontend_url(
        self,
        mock_ensure,
        mock_infra,
        mock_require_cmd,
        mock_derive,
        mock_git_root,
        mock_expand,
        mock_wait_jobs,
        mock_wait_pods,
        monkeypatch,
        capsys,
    ):
        _, fake_run = _make_deploy_fake_run()
        monkeypatch.setattr("dh.deploy.run", fake_run)

        from dh.config import K3dConfig
        from dh.deploy import deploy
        from dh.helm import HelmChartSource

        cfg = K3dConfig()
        deploy(cfg, HelmChartSource(), profiles=("backend",))

        captured = capsys.readouterr()
        assert "Frontend disabled" in captured.out
        assert "yarn start" in captured.out


class TestDeployLocal:
    """Verify --local builds modified services and adds per-service helm overrides."""

    @patch("dh.deploy.wait_for_pods")
    @patch("dh.deploy.wait_for_jobs")
    @patch("dh.deploy.git_root", return_value="/fake/root")
    @patch("dh.deploy.derive_worktree_name", return_value="test")
    @patch("dh.deploy.require_cmd")
    @patch("dh.deploy.require_infra")
    @patch("dh.deploy.ensure_helm_repo")
    def test_local_calls_build_and_adds_overrides(
        self,
        mock_ensure,
        mock_infra,
        mock_require_cmd,
        mock_derive,
        mock_git_root,
        mock_wait_jobs,
        mock_wait_pods,
        monkeypatch,
    ):
        """use_local should call build_and_import_modified and add per-service --set args."""
        captured_cmds, fake_run = _make_deploy_fake_run()
        monkeypatch.setattr("dh.deploy.run", fake_run)

        captured_tpl_vars = {}

        def fake_expand_template(path, tpl_vars):
            captured_tpl_vars.update(tpl_vars)
            return "# rendered values"

        monkeypatch.setattr("dh.deploy.expand_template", fake_expand_template)

        mock_build = MagicMock(return_value=["gms", "frontend"])
        monkeypatch.setattr("dh.images.build_and_import_modified", mock_build)

        from dh.config import K3dConfig
        from dh.deploy import deploy
        from dh.helm import HelmChartSource

        cfg = K3dConfig()
        deploy(cfg, HelmChartSource(), use_local=True)

        # build_and_import_modified should be called
        mock_build.assert_called_once_with(cfg, "test")

        # DH_VERSION stays as the default published version (not local tag)
        assert captured_tpl_vars["DH_VERSION"] == cfg.datahub_version

        # No DH_IMAGE_PULL_POLICY in template vars (removed)
        assert "DH_IMAGE_PULL_POLICY" not in captured_tpl_vars

        # Per-service --set overrides should be in the helm command
        helm_cmds = [c for c in captured_cmds if "helm" in c and "upgrade" in c]
        assert len(helm_cmds) == 1
        helm_cmd = helm_cmds[0]
        set_values = []
        for i, arg in enumerate(helm_cmd):
            if arg == "--set" and i + 1 < len(helm_cmd):
                set_values.append(helm_cmd[i + 1])

        assert "datahub-gms.image.tag=local-test" in set_values
        assert "datahub-gms.image.pullPolicy=Never" in set_values
        assert "datahub-frontend.image.tag=local-test" in set_values
        assert "datahub-frontend.image.pullPolicy=Never" in set_values

    @patch("dh.deploy.wait_for_pods")
    @patch("dh.deploy.wait_for_jobs")
    @patch("dh.deploy.git_root", return_value="/fake/root")
    @patch("dh.deploy.derive_worktree_name", return_value="test")
    @patch("dh.deploy.require_cmd")
    @patch("dh.deploy.require_infra")
    @patch("dh.deploy.ensure_helm_repo")
    def test_no_local_no_overrides(
        self,
        mock_ensure,
        mock_infra,
        mock_require_cmd,
        mock_derive,
        mock_git_root,
        mock_wait_jobs,
        mock_wait_pods,
        monkeypatch,
    ):
        """Without --local, no per-service image overrides should be added."""
        captured_cmds, fake_run = _make_deploy_fake_run()
        monkeypatch.setattr("dh.deploy.run", fake_run)
        monkeypatch.setattr(
            "dh.deploy.expand_template", lambda p, v: "# rendered values"
        )

        from dh.config import K3dConfig
        from dh.deploy import deploy
        from dh.helm import HelmChartSource

        deploy(K3dConfig(), HelmChartSource())

        helm_cmds = [c for c in captured_cmds if "helm" in c and "upgrade" in c]
        assert len(helm_cmds) == 1
        helm_cmd = helm_cmds[0]
        set_values = []
        for i, arg in enumerate(helm_cmd):
            if arg == "--set" and i + 1 < len(helm_cmd):
                set_values.append(helm_cmd[i + 1])

        # No image overrides
        assert not any("pullPolicy=Never" in v for v in set_values)
