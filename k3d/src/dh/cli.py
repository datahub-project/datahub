"""Click CLI group wiring all subcommands — replaces dh.sh."""

from __future__ import annotations

import functools
from pathlib import Path
from typing import Optional

import click

from dh.config import K3dConfig
from dh.helm import HelmChartSource


pass_cfg = click.make_pass_decorator(K3dConfig)


@click.group()
@click.pass_context
def main(ctx: click.Context) -> None:
    """CLI for managing DataHub on k3d with multi-worktree support."""
    ctx.ensure_object(K3dConfig)
    ctx.obj = K3dConfig()


# ── Shared decorators ──────────────────────────────────────────────────────


def chart_source_options(fn):  # type: ignore[no-untyped-def]
    """Add --chart-version and --chart-path options to a command."""

    @click.option(
        "--chart-version",
        default=None,
        help="Helm chart version to install (e.g. 1.3.0).",
    )
    @click.option(
        "--chart-path",
        default=None,
        type=click.Path(exists=True, file_okay=False, path_type=Path),
        help="Path to a local Helm chart directory (mutually exclusive with --chart-version).",
    )
    @functools.wraps(fn)
    def wrapper(
        *args: object,
        chart_version: Optional[str],
        chart_path: Optional[Path],
        **kwargs: object,
    ) -> object:
        kwargs["chart_source"] = HelmChartSource(
            version=chart_version, path=chart_path
        )
        return fn(*args, **kwargs)

    return wrapper


# ── Cluster commands ────────────────────────────────────────────────────────


@main.command("cluster-up")
@pass_cfg
def cluster_up_cmd(cfg: K3dConfig) -> None:
    """Create the k3d cluster."""
    from dh.cluster import cluster_up

    cluster_up(cfg)


@main.command("cluster-down")
@pass_cfg
def cluster_down_cmd(cfg: K3dConfig) -> None:
    """Delete the k3d cluster."""
    from dh.cluster import cluster_down

    cluster_down(cfg)


# ── Infra commands ──────────────────────────────────────────────────────────


@main.command("infra-up")
@chart_source_options
@pass_cfg
def infra_up_cmd(cfg: K3dConfig, chart_source: HelmChartSource) -> None:
    """Deploy shared infrastructure (MySQL, Kafka, Elasticsearch)."""
    from dh.infra import infra_up

    infra_up(cfg, chart_source)


@main.command("infra-down")
@click.option("--force", is_flag=True, help="Remove infra even with active deployments.")
@pass_cfg
def infra_down_cmd(cfg: K3dConfig, force: bool) -> None:
    """Remove shared infrastructure."""
    from dh.infra import infra_down

    infra_down(cfg, force=force)


# ── Deploy commands ─────────────────────────────────────────────────────────


@main.command("deploy")
@click.option("--local", "use_local", is_flag=True, help="Build and import local Docker images.")
@click.option("--version", "version", default=None, help="Image tag to use (default: head).")
@click.option(
    "-f",
    "--values",
    "extra_values_files",
    multiple=True,
    type=click.Path(exists=True),
    help="Additional Helm values file (repeatable).",
)
@click.option(
    "--set",
    "extra_set_args",
    multiple=True,
    help="Override individual Helm values (repeatable, e.g. --set key=val).",
)
@click.option(
    "--kustomize",
    "kustomize_dir",
    default=None,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Kustomize overlay directory. NOTE: Helm hooks don't run in kustomize mode.",
)
@click.option("--debug", is_flag=True, help="Enable JDWP debug ports (GMS:5001, Frontend:5002).")
@click.option(
    "--profile",
    "profiles",
    multiple=True,
    type=click.Choice(["minimal", "consumers", "backend"], case_sensitive=False),
    help="Deployment profile (repeatable, cumulative). Options: minimal, consumers, backend.",
)
@chart_source_options
@pass_cfg
def deploy_cmd(
    cfg: K3dConfig,
    chart_source: HelmChartSource,
    use_local: bool,
    version: Optional[str],
    extra_values_files: tuple[str, ...],
    extra_set_args: tuple[str, ...],
    kustomize_dir: Optional[Path],
    debug: bool,
    profiles: tuple[str, ...],
) -> None:
    """Deploy DataHub for the current worktree."""
    from dh.deploy import deploy

    deploy(
        cfg,
        chart_source,
        use_local=use_local,
        version=version,
        extra_values_files=extra_values_files,
        extra_set_args=extra_set_args,
        kustomize_dir=kustomize_dir,
        debug=debug,
        profiles=profiles,
    )


@main.command("teardown")
@pass_cfg
def teardown_cmd(cfg: K3dConfig) -> None:
    """Remove current worktree's DataHub deployment."""
    from dh.deploy import teardown

    teardown(cfg)


# ── Status / list commands ──────────────────────────────────────────────────


@main.command("status")
@click.option(
    "-o",
    "--output",
    "output_format",
    type=click.Choice(["pretty", "json"], case_sensitive=False),
    default="pretty",
    help="Output format (default: pretty).",
)
@pass_cfg
def status_cmd(cfg: K3dConfig, output_format: str) -> None:
    """Show cluster, infra, and deployment status."""
    output_json = output_format == "json"

    if output_json:
        from dh.deploy import deploy_status

        try:
            deploy_status(cfg, output_json=True)
        except SystemExit:
            click.echo("{}")
    else:
        from dh.cluster import cluster_status
        from dh.deploy import deploy_status, list_deployments
        from dh.infra import infra_status

        click.echo()
        click.echo("=== Cluster ===")
        cluster_status(cfg)
        click.echo()
        click.echo("=== Infrastructure ===")
        try:
            infra_status(cfg)
        except SystemExit:
            pass
        click.echo()
        click.echo("=== Deployments ===")
        try:
            list_deployments(cfg)
        except SystemExit:
            pass
        click.echo()
        click.echo("=== Current Worktree ===")
        try:
            deploy_status(cfg)
        except SystemExit:
            pass


@main.command("list")
@pass_cfg
def list_cmd(cfg: K3dConfig) -> None:
    """List all deployed worktree instances."""
    from dh.deploy import list_deployments

    list_deployments(cfg)


# ── Image commands ──────────────────────────────────────────────────────────


@main.command("import-images")
@click.option(
    "--services",
    default="gms,frontend",
    help="Comma-separated services to build and import (default: gms,frontend).",
)
@pass_cfg
def import_images_cmd(cfg: K3dConfig, services: str) -> None:
    """Build and import local images for current worktree."""
    from dh.images import import_specific

    import_specific(cfg, services)


# ── Reload command ─────────────────────────────────────────────────────────


@main.command("reload")
@click.option(
    "--services",
    default="gms,frontend",
    help="Comma-separated services to rebuild and import (default: gms,frontend).",
)
@click.option("--no-build", is_flag=True, help="Skip build, just reimport existing images and restart.")
@pass_cfg
def reload_cmd(cfg: K3dConfig, services: str, no_build: bool) -> None:
    """Rebuild images and restart pods (requires prior deploy --local)."""
    from dh.images import SERVICE_DEPLOYMENTS, build_and_import_services
    from dh.naming import derive_worktree_name, namespace_for
    from dh.utils import die, info, ok, require_cluster, run, wait_for_pods, warn

    require_cluster(cfg.cluster_name)

    name = derive_worktree_name()
    ns = namespace_for(name)
    release_name = f"datahub-{name}"

    # Verify the namespace exists (must have deployed first)
    ns_check = run(["kubectl", "get", "namespace", ns], check=False, quiet=True)
    if ns_check.returncode != 0:
        die(f"Namespace '{ns}' not found. Deploy first with: dh deploy --local")

    info(f"Reloading services ({services}) for '{name}'...")

    build_and_import_services(cfg, services, name, skip_build=no_build)

    # Restart only the deployments that correspond to rebuilt services
    # (like quickstartReload — only restart what changed)
    deployments: list[str] = []
    for svc in services.split(","):
        suffix = SERVICE_DEPLOYMENTS.get(svc.strip())
        if suffix:
            deployments.append(f"{release_name}-{suffix}")

    if deployments:
        info(f"Restarting deployments: {', '.join(deployments)}...")
        for dep in deployments:
            run(
                ["kubectl", "rollout", "restart", f"deployment/{dep}", "-n", ns],
                check=False,
            )
    else:
        warn("No long-running deployments to restart for the specified services.")
        warn("Jobs (upgrade, mysql-setup, etc.) run to completion and don't need restarting.")
        return

    info("Waiting for pods to be ready...")
    wait_for_pods(ns, 300)

    ok(f"Reload complete for '{name}'.")
