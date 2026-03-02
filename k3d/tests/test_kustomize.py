"""Tests for dh.kustomize — kustomize pipeline."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


class TestDeployWithKustomize:
    def test_pipeline_sequence(self, monkeypatch, tmp_path):
        """Verify: helm template → file write → kustomize → kubectl apply."""
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append((list(cmd), kwargs))
            if cmd[0] == "helm":
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0,
                    stdout="---\napiVersion: v1\nkind: ConfigMap\n", stderr=""
                )
            if "kustomize" in cmd:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0,
                    stdout="---\nkustomized output\n", stderr=""
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )

        monkeypatch.setattr("dh.kustomize.run", fake_run)

        # Create a minimal kustomize overlay
        overlay = tmp_path / "overlay"
        overlay.mkdir()
        (overlay / "kustomization.yaml").write_text(
            "apiVersion: kustomize.config.k8s.io/v1beta1\n"
            "kind: Kustomization\n"
            "resources:\n"
            "  - placeholder.yaml\n"
        )

        from dh.kustomize import deploy_with_kustomize

        deploy_with_kustomize(
            ["helm", "template", "myrelease", "datahub/datahub"],
            overlay,
            "my-namespace",
        )

        # Should have 3 run calls: helm template, kustomize, kubectl apply
        assert len(calls) == 3

        # 1st: helm template
        assert calls[0][0][0] == "helm"
        assert "capture_output" in calls[0][1] and calls[0][1]["capture_output"]

        # 2nd: kubectl kustomize
        assert calls[1][0][0] == "kubectl"
        assert "kustomize" in calls[1][0]

        # 3rd: kubectl apply
        assert calls[2][0][0] == "kubectl"
        assert "apply" in calls[2][0]
        assert "-n" in calls[2][0]
        assert "my-namespace" in calls[2][0]
        assert calls[2][1].get("input_text") == "---\nkustomized output\n"

    def test_injects_resource_into_kustomization(self, monkeypatch, tmp_path):
        """Verify that all.yaml gets injected as a resource."""
        kustomization_content = None

        def fake_run(cmd, **kwargs):
            nonlocal kustomization_content
            if "kustomize" in cmd:
                # Read the kustomization.yaml to verify injection
                overlay_dir = Path(cmd[-1])
                kustomization_content = (overlay_dir / "kustomization.yaml").read_text()
            return subprocess.CompletedProcess(
                args=cmd, returncode=0,
                stdout="rendered", stderr=""
            )

        monkeypatch.setattr("dh.kustomize.run", fake_run)

        overlay = tmp_path / "overlay"
        overlay.mkdir()
        (overlay / "kustomization.yaml").write_text(
            "apiVersion: kustomize.config.k8s.io/v1beta1\n"
            "kind: Kustomization\n"
            "resources:\n"
            "  - placeholder.yaml\n"
        )

        from dh.kustomize import deploy_with_kustomize

        deploy_with_kustomize(
            ["helm", "template", "rel", "chart"],
            overlay,
            "ns",
        )

        assert kustomization_content is not None
        assert "all.yaml" in kustomization_content
