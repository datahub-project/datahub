"""Kustomize overlay pipeline — render Helm template then apply kustomize patches."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from dh.utils import info, run


def deploy_with_kustomize(
    helm_template_cmd: list[str],
    kustomize_dir: Path,
    namespace: str,
) -> None:
    """Render a Helm chart via ``helm template``, merge a kustomize overlay, and apply.

    1. ``helm template ...`` → capture stdout → write as ``all.yaml``
    2. Copy the user's overlay dir, inject ``all.yaml`` as a resource
    3. ``kubectl kustomize <work_dir>`` → ``kubectl apply -n <ns> -f -``
    """
    info("Rendering Helm template for kustomize...")
    rendered = run(helm_template_cmd, capture_output=True)

    with tempfile.TemporaryDirectory() as work_dir:
        work = Path(work_dir)

        # Write rendered manifests
        (work / "all.yaml").write_text(rendered.stdout)

        # Copy user's overlay into work dir
        overlay_dest = work / "overlay"
        shutil.copytree(kustomize_dir, overlay_dest)

        # Inject all.yaml as a resource in the kustomization
        kustomization = overlay_dest / "kustomization.yaml"
        content = kustomization.read_text()
        # Add our rendered manifests as a resource
        content = content.replace(
            "resources:", f"resources:\n  - {work / 'all.yaml'}"
        )
        kustomization.write_text(content)

        info("Building kustomize output...")
        kustomized = run(
            ["kubectl", "kustomize", str(overlay_dest)], capture_output=True
        )

        info(f"Applying kustomized manifests to namespace '{namespace}'...")
        run(
            ["kubectl", "apply", "-n", namespace, "-f", "-"],
            input_text=kustomized.stdout,
        )
