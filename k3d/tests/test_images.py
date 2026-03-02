"""Tests for dh.images — Gradle-delegated image build and import."""

from __future__ import annotations

import subprocess
from unittest.mock import patch

import pytest

from dh.images import (
    SERVICE_DEPLOYMENTS,
    SERVICE_DOCKER_MODULES,
    SERVICE_HELM_OVERRIDES,
    SERVICE_IMAGES,
    SERVICE_SOURCE_PATHS,
    helm_set_args_for_local,
)


class TestServiceDictConsistency:
    def test_all_docker_modules_have_images(self):
        assert set(SERVICE_DOCKER_MODULES.keys()) == set(SERVICE_IMAGES.keys())

    def test_helm_overrides_subset_of_docker_modules(self):
        assert set(SERVICE_HELM_OVERRIDES.keys()).issubset(
            set(SERVICE_DOCKER_MODULES.keys())
        )

    def test_source_paths_subset_of_docker_modules(self):
        assert set(SERVICE_SOURCE_PATHS.keys()).issubset(
            set(SERVICE_DOCKER_MODULES.keys())
        )

    def test_deployments_subset_of_docker_modules(self):
        assert set(SERVICE_DEPLOYMENTS.keys()).issubset(
            set(SERVICE_DOCKER_MODULES.keys())
        )


class TestHelmSetArgsForLocal:
    def test_generates_tag_and_pull_policy(self):
        args = helm_set_args_for_local(["gms"], "local-test")
        assert "--set" in args
        assert "datahub-gms.image.tag=local-test" in args
        assert "datahub-gms.image.pullPolicy=Never" in args

    def test_multiple_services(self):
        args = helm_set_args_for_local(["gms", "frontend"], "local-test")
        assert "datahub-gms.image.tag=local-test" in args
        assert "datahub-frontend.image.tag=local-test" in args

    def test_upgrade_has_two_overrides(self):
        args = helm_set_args_for_local(["upgrade"], "local-test")
        assert "datahubSystemUpdate.image.tag=local-test" in args
        assert "datahubUpgrade.image.tag=local-test" in args

    def test_empty_services(self):
        args = helm_set_args_for_local([], "local-test")
        assert args == []


class TestDetectModifiedServices:
    @patch("dh.images.run")
    def test_metadata_service_change_triggers_gms(self, mock_run):
        mock_run.side_effect = [
            # merge-base
            subprocess.CompletedProcess(
                args=[], returncode=0, stdout="abc123\n", stderr=""
            ),
            # committed changes
            subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="metadata-service/war/src/main/java/Foo.java\n",
                stderr="",
            ),
            # uncommitted changes
            subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr=""
            ),
        ]
        from dh.images import detect_modified_services

        result = detect_modified_services("/fake/root")
        assert "gms" in result

    @patch("dh.images.run")
    def test_frontend_change_triggers_frontend(self, mock_run):
        mock_run.side_effect = [
            subprocess.CompletedProcess(
                args=[], returncode=0, stdout="abc123\n", stderr=""
            ),
            subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="datahub-frontend/play/build.gradle\n",
                stderr="",
            ),
            subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr=""
            ),
        ]
        from dh.images import detect_modified_services

        result = detect_modified_services("/fake/root")
        assert "frontend" in result

    @patch("dh.images.run")
    def test_gradle_change_triggers_all(self, mock_run):
        mock_run.side_effect = [
            subprocess.CompletedProcess(
                args=[], returncode=0, stdout="abc123\n", stderr=""
            ),
            subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="gradle/docker/docker.gradle\n",
                stderr="",
            ),
            subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr=""
            ),
        ]
        from dh.images import detect_modified_services

        result = detect_modified_services("/fake/root")
        assert set(result) == set(SERVICE_SOURCE_PATHS.keys())

    @patch("dh.images.run")
    def test_no_changes_returns_empty(self, mock_run):
        mock_run.side_effect = [
            subprocess.CompletedProcess(
                args=[], returncode=0, stdout="abc123\n", stderr=""
            ),
            subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr=""
            ),
            subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr=""
            ),
        ]
        from dh.images import detect_modified_services

        result = detect_modified_services("/fake/root")
        assert result == []

    @patch("dh.images.warn")
    @patch("dh.images.run")
    def test_no_merge_base_returns_all(self, mock_run, mock_warn):
        mock_run.side_effect = [
            # main fails
            subprocess.CompletedProcess(
                args=[], returncode=1, stdout="", stderr=""
            ),
            # master fails
            subprocess.CompletedProcess(
                args=[], returncode=1, stdout="", stderr=""
            ),
        ]
        from dh.images import detect_modified_services

        result = detect_modified_services("/fake/root")
        assert set(result) == set(SERVICE_DOCKER_MODULES.keys())
        mock_warn.assert_called_once()


class TestBuildAndImportModified:
    @patch("dh.images.require_cmd")
    @patch("dh.images.git_root", return_value="/fake/root")
    @patch("dh.images._get_version_tag", return_value="v1.0.0")
    @patch("dh.images._retag_and_import")
    @patch("dh.images.run")
    def test_default_builds_gms_and_frontend(
        self,
        mock_run,
        mock_retag,
        mock_version,
        mock_root,
        mock_require,
        monkeypatch,
    ):
        monkeypatch.delenv("DH_LOCAL_SERVICES", raising=False)

        from dh.config import K3dConfig
        from dh.images import build_and_import_modified

        result = build_and_import_modified(K3dConfig(), "test")

        assert result == ["gms", "frontend"]
        mock_run.assert_called_once()
        gradle_cmd = mock_run.call_args[0][0]
        assert gradle_cmd[0] == "./gradlew"
        assert ":metadata-service:war:docker" in gradle_cmd
        assert ":datahub-frontend:docker" in gradle_cmd
        assert mock_retag.call_count == 2

    @patch("dh.images.require_cmd")
    @patch("dh.images.git_root", return_value="/fake/root")
    @patch("dh.images.detect_modified_services")
    @patch("dh.images._get_version_tag", return_value="v1.0.0")
    @patch("dh.images._retag_and_import")
    @patch("dh.images.run")
    def test_env_override(
        self,
        mock_run,
        mock_retag,
        mock_version,
        mock_detect,
        mock_root,
        mock_require,
        monkeypatch,
    ):
        monkeypatch.setenv("DH_LOCAL_SERVICES", "gms,upgrade")

        from dh.config import K3dConfig
        from dh.images import build_and_import_modified

        result = build_and_import_modified(K3dConfig(), "test")

        assert result == ["gms", "upgrade"]
        mock_detect.assert_not_called()

    @patch("dh.images.require_cmd")
    @patch("dh.images.git_root", return_value="/fake/root")
    @patch("dh.images.detect_modified_services", return_value=[])
    @patch("dh.images.run")
    def test_empty_env_falls_through_to_detection(
        self,
        mock_run,
        mock_detect,
        mock_root,
        mock_require,
        monkeypatch,
    ):
        monkeypatch.setenv("DH_LOCAL_SERVICES", "")

        from dh.config import K3dConfig
        from dh.images import build_and_import_modified

        result = build_and_import_modified(K3dConfig(), "test")

        assert result == []
        mock_detect.assert_called_once()
        mock_run.assert_not_called()
