"""Click CLI group wiring all subcommands — replaces dh.sh."""

from __future__ import annotations

import functools
import sys
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


def output_option(fn):  # type: ignore[no-untyped-def]
    """Add -o/--output flag and set up OutputContext before the command runs."""

    @click.option(
        "-o",
        "--output",
        "output_format",
        type=click.Choice(["pretty", "json"], case_sensitive=False),
        default="pretty",
        help="Output format (default: pretty). Use 'json' for machine-readable NDJSON output.",
    )
    @functools.wraps(fn)
    def wrapper(*args: object, output_format: str, **kwargs: object) -> object:
        from dh.utils import OutputContext, OutputMode, set_output_context

        ctx = OutputContext(mode=OutputMode(output_format.lower()))
        set_output_context(ctx)
        return fn(*args, **kwargs)

    return wrapper


def dry_run_option(fn):  # type: ignore[no-untyped-def]
    """Add --dry-run flag. Requires output_option to be applied first."""

    @click.option(
        "--dry-run",
        is_flag=True,
        help="Print what would be done without executing any mutations.",
    )
    @functools.wraps(fn)
    def wrapper(*args: object, dry_run: bool, **kwargs: object) -> object:
        from dh.utils import get_output_context

        ctx = get_output_context()
        if ctx:
            ctx.dry_run = dry_run
        kwargs["dry_run"] = dry_run
        return fn(*args, **kwargs)

    return wrapper


# ── Cluster commands ────────────────────────────────────────────────────────


@main.command("cluster-up")
@output_option
@dry_run_option
@pass_cfg
def cluster_up_cmd(cfg: K3dConfig, dry_run: bool) -> None:
    """Create the k3d cluster."""
    from dh.cluster import cluster_up
    from dh.utils import get_output_context

    cluster_up(cfg)

    ctx = get_output_context()
    if ctx and ctx.json_mode:
        ctx.emit_result(True, {"cluster_name": cfg.cluster_name, "dry_run": dry_run})


@main.command("cluster-down")
@output_option
@dry_run_option
@pass_cfg
def cluster_down_cmd(cfg: K3dConfig, dry_run: bool) -> None:
    """Delete the k3d cluster."""
    from dh.cluster import cluster_down
    from dh.utils import get_output_context

    cluster_down(cfg)

    ctx = get_output_context()
    if ctx and ctx.json_mode:
        ctx.emit_result(True, {"cluster_name": cfg.cluster_name, "dry_run": dry_run})


# ── Infra commands ──────────────────────────────────────────────────────────


@main.command("infra-up")
@output_option
@dry_run_option
@chart_source_options
@pass_cfg
def infra_up_cmd(cfg: K3dConfig, chart_source: HelmChartSource, dry_run: bool) -> None:
    """Deploy shared infrastructure (MySQL, Kafka, Elasticsearch)."""
    from dh.infra import infra_up
    from dh.utils import get_output_context

    infra_up(cfg, chart_source)

    ctx = get_output_context()
    if ctx and ctx.json_mode:
        ctx.emit_result(
            True,
            {
                "namespace": cfg.infra_namespace,
                "release": cfg.infra_release,
                "dry_run": dry_run,
            },
        )


@main.command("infra-down")
@output_option
@dry_run_option
@click.option("--force", is_flag=True, help="Remove infra even with active deployments.")
@pass_cfg
def infra_down_cmd(cfg: K3dConfig, force: bool, dry_run: bool) -> None:
    """Remove shared infrastructure."""
    from dh.infra import infra_down
    from dh.utils import get_output_context

    infra_down(cfg, force=force)

    ctx = get_output_context()
    if ctx and ctx.json_mode:
        ctx.emit_result(True, {"namespace": cfg.infra_namespace, "dry_run": dry_run})


# ── Deploy commands ─────────────────────────────────────────────────────────


@main.command("deploy")
@output_option
@dry_run_option
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
    dry_run: bool,
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
    from dh.naming import derive_worktree_name, gms_hostname_for, hostname_for, namespace_for
    from dh.utils import get_output_context

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
        dry_run=dry_run,
    )

    ctx = get_output_context()
    if ctx and ctx.json_mode:
        name = derive_worktree_name()
        ns = namespace_for(name)
        ctx.emit_result(
            True,
            {
                "worktree": name,
                "namespace": ns,
                "url": f"http://{hostname_for(name)}",
                "gms_url": f"http://{gms_hostname_for(name)}",
                "dry_run": dry_run,
            },
        )


@main.command("teardown")
@output_option
@dry_run_option
@pass_cfg
def teardown_cmd(cfg: K3dConfig, dry_run: bool) -> None:
    """Remove current worktree's DataHub deployment."""
    from dh.deploy import teardown
    from dh.naming import derive_worktree_name, namespace_for
    from dh.utils import get_output_context

    name = derive_worktree_name()
    ns = namespace_for(name)

    teardown(cfg)

    ctx = get_output_context()
    if ctx and ctx.json_mode:
        ctx.emit_result(True, {"worktree": name, "namespace": ns, "dry_run": dry_run})


# ── Status / list commands ──────────────────────────────────────────────────


@main.command("status")
@output_option
@pass_cfg
def status_cmd(cfg: K3dConfig) -> None:
    """Show cluster, infra, and deployment status."""
    from dh.utils import get_output_context

    ctx = get_output_context()

    if ctx and ctx.json_mode:
        from dh.deploy import collect_status_json
        from dh.naming import derive_worktree_name, gms_hostname_for, hostname_for, namespace_for
        from dh.utils import run

        # Check cluster exists first (read-only, won't call die())
        cluster_check = run(
            ["k3d", "cluster", "list", cfg.cluster_name], check=False, quiet=True
        )
        if cluster_check.returncode != 0:
            ctx.emit_result(
                False,
                {"deployed": False},
                error=f"Cluster '{cfg.cluster_name}' not found. Run: dh cluster-up",
            )
            sys.exit(1)

        name = derive_worktree_name()
        ns = namespace_for(name)
        hostname = hostname_for(name)
        gms_hostname = gms_hostname_for(name)

        ns_check = run(["kubectl", "get", "namespace", ns], check=False, quiet=True)
        if ns_check.returncode != 0:
            ctx.emit_result(
                True,
                {"deployed": False, "worktree": name, "namespace": ns},
            )
            return

        data = collect_status_json(cfg, name, ns, hostname, gms_hostname)
        ctx.emit_result(True, {"deployed": True, **data})
        return

    # Pretty mode
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
@output_option
@pass_cfg
def list_cmd(cfg: K3dConfig) -> None:
    """List all deployed worktree instances."""
    from dh.deploy import list_deployments
    from dh.utils import get_output_context

    deployments = list_deployments(cfg)

    ctx = get_output_context()
    if ctx and ctx.json_mode:
        ctx.emit_result(True, {"deployments": deployments})


# ── Image commands ──────────────────────────────────────────────────────────


@main.command("import-images")
@output_option
@dry_run_option
@click.option(
    "--services",
    default="gms,frontend",
    help="Comma-separated services to build and import (default: gms,frontend).",
)
@pass_cfg
def import_images_cmd(cfg: K3dConfig, services: str, dry_run: bool) -> None:
    """Build and import local images for current worktree."""
    from dh.images import import_specific
    from dh.naming import derive_worktree_name
    from dh.utils import get_output_context

    built = import_specific(cfg, services)

    ctx = get_output_context()
    if ctx and ctx.json_mode:
        name = derive_worktree_name()
        ctx.emit_result(
            True,
            {"services": built, "tag": f"local-{name}", "dry_run": dry_run},
        )


# ── Reload command ─────────────────────────────────────────────────────────


@main.command("reload")
@output_option
@dry_run_option
@click.option(
    "--services",
    default="gms,frontend",
    help="Comma-separated services to rebuild and import (default: gms,frontend).",
)
@click.option("--no-build", is_flag=True, help="Skip build, just reimport existing images and restart.")
@pass_cfg
def reload_cmd(cfg: K3dConfig, services: str, no_build: bool, dry_run: bool) -> None:
    """Rebuild images and restart pods (requires prior deploy --local)."""
    from dh.images import SERVICE_DEPLOYMENTS, build_and_import_services
    from dh.naming import derive_worktree_name, namespace_for
    from dh.utils import die, get_output_context, info, ok, require_cluster, run, wait_for_pods, warn

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
        ctx = get_output_context()
        if ctx and ctx.json_mode:
            ctx.emit_result(True, {"services": services.split(","), "namespace": ns, "dry_run": dry_run})
        return

    info("Waiting for pods to be ready...")
    wait_for_pods(ns, 300)

    ok(f"Reload complete for '{name}'.")

    ctx = get_output_context()
    if ctx and ctx.json_mode:
        ctx.emit_result(
            True,
            {
                "services": services.split(","),
                "deployments": deployments,
                "namespace": ns,
                "dry_run": dry_run,
            },
        )


# ── Describe command ───────────────────────────────────────────────────────


@main.command("describe")
@click.argument("command_name", required=False)
def describe_cmd(command_name: Optional[str]) -> None:
    """Output machine-readable JSON schema for CLI commands.

    Pass a COMMAND_NAME to describe a specific command, or omit to list all.
    """
    import json

    if command_name:
        cmd = main.commands.get(command_name)
        if not cmd:
            click.echo(
                json.dumps({"error": f"Unknown command: {command_name}. Run 'dh describe' for a list."})
            )
            sys.exit(1)
        click.echo(json.dumps({command_name: _describe_command(cmd)}, indent=2))
    else:
        result = {name: _describe_command(cmd) for name, cmd in sorted(main.commands.items())}
        click.echo(json.dumps(result, indent=2))


def _describe_command(cmd: click.Command) -> dict:
    """Extract a machine-readable schema from a Click command."""
    import json

    def _safe(val: object) -> object:
        """Return val if JSON-serializable, else its string repr."""
        try:
            json.dumps(val)
            return val
        except (TypeError, ValueError):
            return str(val)

    options = []
    for param in cmd.params:
        opt: dict = {
            "name": param.name,
            "help": getattr(param, "help", "") or "",
        }
        if isinstance(param, click.Option):
            opt["flags"] = param.opts
            opt["default"] = _safe(param.default)
            opt["required"] = param.required
            if isinstance(param.type, click.Choice):
                opt["choices"] = list(param.type.choices)
            elif param.is_flag:
                opt["is_flag"] = True
            else:
                opt["type"] = str(param.type)
        elif isinstance(param, click.Argument):
            opt["kind"] = "argument"
            opt["required"] = param.required
        options.append(opt)
    return {
        "help": cmd.help or "",
        "options": options,
    }
