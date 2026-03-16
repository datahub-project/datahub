"""Shared infrastructure (MySQL, Kafka, Elasticsearch) lifecycle — replaces infra.sh."""

from __future__ import annotations

import click

from dh.config import K3dConfig
from dh.helm import HelmChartSource, ensure_helm_repo
from dh.utils import die, info, ok, require_cmd, require_cluster, run, wait_for_pods, warn


def infra_up(cfg: K3dConfig, chart_source: HelmChartSource) -> None:
    require_cmd("helm")
    require_cmd("kubectl")
    require_cluster(cfg.cluster_name)

    if not chart_source.path:
        ensure_helm_repo(cfg.helm_repo_name, cfg.helm_repo_url)

    info(f"Deploying shared infrastructure to namespace '{cfg.infra_namespace}'...")

    # Create namespace idempotently
    ns_yaml = run(
        ["kubectl", "create", "namespace", cfg.infra_namespace, "--dry-run=client", "-o", "yaml"],
        capture_output=True,
    )
    run(["kubectl", "apply", "-f", "-"], input_text=ns_yaml.stdout)

    chart_ref = chart_source.chart_ref(cfg.helm_repo_name, "datahub-prerequisites")
    helm_cmd = [
        "helm",
        "upgrade",
        "--install",
        cfg.infra_release,
        chart_ref,
        "--namespace",
        cfg.infra_namespace,
        "--values",
        str(cfg.script_dir / "values" / "infra-prerequisites.yaml"),
        *chart_source.version_args(),
        "--wait",
        "--timeout",
        "10m",
    ]
    run(helm_cmd)

    wait_for_pods(cfg.infra_namespace, 300)
    ok(f"Shared infrastructure is running in '{cfg.infra_namespace}'.")


def infra_down(cfg: K3dConfig, force: bool = False) -> None:
    require_cmd("helm")
    require_cmd("kubectl")
    require_cluster(cfg.cluster_name)

    # Check for active worktree deployments
    result = run(
        [
            "kubectl",
            "get",
            "namespaces",
            "-l",
            "datahub.io/component=worktree",
            "-o",
            "name",
        ],
        capture_output=True,
        check=False,
    )
    active_ns = result.stdout.strip() if result.returncode == 0 else ""
    if active_ns:
        active_list = ", ".join(
            line.replace("namespace/", "") for line in active_ns.splitlines() if line
        )
        warn(f"Active worktree deployments still exist: {active_list}")
        warn("Tear them down first with 'dh teardown' from each worktree, or pass --force.")
        if not force:
            die("Infra teardown aborted — active deployments exist. Use --force to override.")

    ns_check = run(
        ["kubectl", "get", "namespace", cfg.infra_namespace],
        check=False,
        quiet=True,
    )
    if ns_check.returncode != 0:
        warn(f"Infra namespace '{cfg.infra_namespace}' does not exist.")
        return

    info("Removing shared infrastructure...")
    run(
        ["helm", "uninstall", cfg.infra_release, "--namespace", cfg.infra_namespace],
        check=False,
    )
    run(["kubectl", "delete", "namespace", cfg.infra_namespace, "--wait=false"])
    ok("Shared infrastructure removed.")


def infra_status(cfg: K3dConfig) -> None:
    require_cluster(cfg.cluster_name)
    ns_check = run(
        ["kubectl", "get", "namespace", cfg.infra_namespace],
        check=False,
        quiet=True,
    )
    if ns_check.returncode == 0:
        click.echo()
        click.echo(click.style(f"Infra pods ({cfg.infra_namespace}):", bold=True))
        run(["kubectl", "get", "pods", "-n", cfg.infra_namespace, "-o", "wide"])
        click.echo()
        click.echo(click.style("Infra services:", bold=True))
        run(["kubectl", "get", "svc", "-n", cfg.infra_namespace])
    else:
        warn(f"Infra namespace '{cfg.infra_namespace}' does not exist.")
