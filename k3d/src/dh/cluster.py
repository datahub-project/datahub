"""Cluster lifecycle management — replaces cluster.sh."""

from __future__ import annotations

import tempfile
from pathlib import Path

from dh.config import K3dConfig
from dh.template import expand_template
from dh.utils import info, ok, require_cmd, run, warn


def cluster_up(cfg: K3dConfig) -> None:
    require_cmd("k3d")
    require_cmd("kubectl")

    result = run(
        ["k3d", "cluster", "list", cfg.cluster_name], check=False, quiet=True
    )
    if result.returncode == 0:
        ok(f"Cluster '{cfg.cluster_name}' already exists.")
        return

    info(f"Creating k3d cluster '{cfg.cluster_name}'...")

    config_content = expand_template(
        cfg.script_dir / "k3d-config.yaml",
        {"K3D_HTTP_PORT": cfg.http_port, "K3D_HTTPS_PORT": cfg.https_port},
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=True
    ) as tmp:
        tmp.write(config_content)
        tmp.flush()
        run(["k3d", "cluster", "create", "--config", tmp.name])

    ok(f"Cluster '{cfg.cluster_name}' is ready.")

    info("Waiting for Traefik ingress controller...")
    run(
        [
            "kubectl",
            "wait",
            "--for=condition=available",
            "deployment/traefik",
            "-n",
            "kube-system",
            "--timeout=120s",
        ],
        check=False,
        quiet=True,
    )
    ok("Traefik is running.")


def cluster_down(cfg: K3dConfig) -> None:
    require_cmd("k3d")

    result = run(
        ["k3d", "cluster", "list", cfg.cluster_name], check=False, quiet=True
    )
    if result.returncode != 0:
        warn(f"Cluster '{cfg.cluster_name}' does not exist.")
        return

    info(f"Deleting k3d cluster '{cfg.cluster_name}'...")
    run(["k3d", "cluster", "delete", cfg.cluster_name])
    ok("Cluster deleted.")


def cluster_status(cfg: K3dConfig) -> None:
    require_cmd("k3d")
    result = run(
        ["k3d", "cluster", "list", cfg.cluster_name], check=False, quiet=True
    )
    if result.returncode == 0:
        run(["k3d", "cluster", "list", cfg.cluster_name])
    else:
        warn(f"Cluster '{cfg.cluster_name}' does not exist.")
