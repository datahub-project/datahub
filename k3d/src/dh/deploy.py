"""Per-worktree DataHub deployment lifecycle — replaces deploy.sh."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Optional

import click

from dh.config import K3dConfig
from dh.helm import HelmChartSource, ensure_helm_repo
from dh.kustomize import deploy_with_kustomize
from dh.naming import (
    db_name_for,
    derive_worktree_name,
    es_prefix_for,
    gms_hostname_for,
    git_root,
    hostname_for,
    kafka_prefix_for,
    namespace_for,
)
from dh.template import expand_template
from dh.utils import (
    die,
    get_output_context,
    info,
    ok,
    require_cmd,
    require_infra,
    run,
    wait_for_jobs,
    wait_for_pods,
    warn,
)


def deploy(
    cfg: K3dConfig,
    chart_source: HelmChartSource,
    *,
    use_local: bool = False,
    version: Optional[str] = None,
    extra_values_files: tuple[str, ...] = (),
    extra_set_args: tuple[str, ...] = (),
    kustomize_dir: Optional[Path] = None,
    debug: bool = False,
    profiles: tuple[str, ...] = (),
    dry_run: bool = False,
) -> None:
    require_cmd("helm")
    require_cmd("kubectl")
    require_infra(cfg.cluster_name, cfg.infra_namespace)

    if not chart_source.path:
        ensure_helm_repo(cfg.helm_repo_name, cfg.helm_repo_url)

    effective_version = version or cfg.datahub_version

    name = derive_worktree_name()
    _check_collision(name)

    ns = namespace_for(name)
    db_name = db_name_for(name)
    es_prefix = es_prefix_for(name)
    kafka_prefix = kafka_prefix_for(name)
    hostname = hostname_for(name)
    gms_hostname = gms_hostname_for(name)
    release_name = f"datahub-{name}"

    local_tag = f"local-{name}"
    local_services: list[str] = []
    if use_local:
        from dh.images import build_and_import_modified

        local_services = build_and_import_modified(cfg, name)

    info(f"Deploying DataHub for worktree '{name}'...")
    info(f"Namespace:  {ns}")
    info(f"Database:   {db_name}")
    info(f"ES prefix:  {es_prefix}")
    info(f"Kafka pfx:  {kafka_prefix}")
    info(f"Hostname:   {hostname}")
    info(f"Version:    {effective_version}")
    if local_services:
        info(f"Local:      {', '.join(local_services)} (tag: {local_tag})")

    # Create namespace with labels and annotations
    root = git_root()
    ns_yaml = run(
        ["kubectl", "create", "namespace", ns, "--dry-run=client", "-o", "yaml"],
        capture_output=True,
    )
    run(["kubectl", "apply", "-f", "-"], input_text=ns_yaml.stdout)
    run(
        [
            "kubectl",
            "label",
            "namespace",
            ns,
            "datahub.io/component=worktree",
            f"datahub.io/worktree-name={name}",
            "--overwrite",
        ]
    )
    run(
        [
            "kubectl",
            "annotate",
            "namespace",
            ns,
            f"datahub.io/git-root={root}",
            "--overwrite",
        ]
    )

    _ensure_database(cfg, db_name)
    _ensure_mysql_secret(cfg, ns)

    # Build template variables
    tpl_vars = {
        "DH_WORKTREE_NAME": name,
        "DH_NAMESPACE": ns,
        "DH_DB_NAME": db_name,
        "DH_ES_PREFIX": es_prefix,
        "DH_KAFKA_PREFIX": kafka_prefix,
        "DH_HOSTNAME": hostname,
        "DH_VERSION": effective_version,
        "DH_RELEASE_NAME": release_name,
        "DH_MYSQL_HOST": cfg.mysql_host,
        "DH_MYSQL_PORT": cfg.mysql_port,
        "DH_KAFKA_HOST": cfg.kafka_host,
        "DH_KAFKA_PORT": cfg.kafka_port,
        "DH_ES_HOST": cfg.elasticsearch_host,
        "DH_ES_PORT": cfg.elasticsearch_port,
    }

    values_content = expand_template(
        cfg.script_dir / "values" / "datahub-worktree.yaml.tpl",
        tpl_vars,
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=True
    ) as values_file:
        values_file.write(values_content)
        values_file.flush()

        chart_ref = chart_source.chart_ref(cfg.helm_repo_name, "datahub")

        # Build the values file list
        values_files = [values_file.name]

        # Apply profile values files (before per-worktree overrides)
        profile_set_args: list[str] = []
        if profiles:
            from dh.profiles import resolve_profiles

            profile_values, profile_set_args = resolve_profiles(
                profiles, cfg.script_dir / "values" / "profiles",
            )
            for pf in profile_values:
                info(f"Using profile: {pf.stem}")
                values_files.append(str(pf))

        # Auto-detect per-worktree override file
        override_file = cfg.script_dir / "values" / "overrides" / f"{name}.yaml"
        if override_file.is_file():
            info(f"Using per-worktree overrides: {override_file}")
            values_files.append(str(override_file))

        # Append user-specified values files
        values_files.extend(extra_values_files)

        # Build --set arguments (order: local → profiles → debug → user, so user always wins)
        set_args: list[str] = []
        if local_services:
            from dh.images import helm_set_args_for_local

            set_args.extend(helm_set_args_for_local(local_services, local_tag))
        set_args.extend(profile_set_args)
        if debug:
            set_args.extend(_debug_set_args())
        for s in extra_set_args:
            set_args.extend(["--set", s])

        if kustomize_dir:
            # Kustomize mode: helm template → kustomize → kubectl apply
            # NOTE: Helm hooks don't run in kustomize mode.
            helm_tpl_cmd = [
                "helm",
                "template",
                release_name,
                chart_ref,
                "--namespace",
                ns,
                *chart_source.version_args(),
            ]
            for vf in values_files:
                helm_tpl_cmd.extend(["--values", vf])
            helm_tpl_cmd.extend(set_args)

            deploy_with_kustomize(helm_tpl_cmd, kustomize_dir, ns)
        else:
            # Standard Helm deploy
            helm_cmd = [
                "helm",
                "upgrade",
                "--install",
                release_name,
                chart_ref,
                "--namespace",
                ns,
                *chart_source.version_args(),
            ]
            for vf in values_files:
                helm_cmd.extend(["--values", vf])
            helm_cmd.extend(set_args)
            helm_cmd.extend(["--timeout", "20m"])

            info("Running helm upgrade --install...")
            run(helm_cmd)

    # Create GMS ingress (not part of the Helm chart)
    _ensure_gms_ingress(ns, release_name, gms_hostname)

    # Wait for setup jobs first, then services
    info("Waiting for setup jobs...")
    wait_for_jobs(ns, 300)

    info("Waiting for services...")
    wait_for_pods(ns, 300)

    has_frontend = "backend" not in profiles

    ok(f"DataHub '{name}' deployed!")

    ctx = get_output_context()
    if not (ctx and ctx.json_mode):
        click.echo()
        if has_frontend:
            click.echo(f"  Frontend:  {click.style(f'http://{hostname}', bold=True)}")
        else:
            click.echo(
                "  Frontend disabled — run locally: cd datahub-web-react && yarn start"
            )
        click.echo(
            f"  GMS:       {click.style(f'http://{gms_hostname}', bold=True)}"
        )
        if debug:
            click.echo()
            click.echo(click.style("  Debug ports (JDWP):", bold=True))
            click.echo(
                f"  GMS:       {click.style(f'kubectl port-forward -n {ns} deployment/{release_name}-datahub-gms 5001:5001', bold=True)}"
            )
            if has_frontend:
                click.echo(
                    f"  Frontend:  {click.style(f'kubectl port-forward -n {ns} deployment/{release_name}-datahub-frontend 5002:5002', bold=True)}"
                )
        click.echo()


def teardown(cfg: K3dConfig) -> None:
    require_cmd("helm")
    require_cmd("kubectl")

    from dh.utils import require_cluster

    require_cluster(cfg.cluster_name)

    name = derive_worktree_name()
    ns = namespace_for(name)
    release_name = f"datahub-{name}"
    db_name = db_name_for(name)
    es_prefix = es_prefix_for(name)

    ns_check = run(["kubectl", "get", "namespace", ns], check=False, quiet=True)
    if ns_check.returncode != 0:
        warn(f"Namespace '{ns}' does not exist. Nothing to tear down.")
        return

    info(f"Tearing down DataHub '{name}' (namespace: {ns})...")

    run(
        ["helm", "uninstall", release_name, "--namespace", ns],
        check=False,
    )
    _drop_database(cfg, db_name)
    _delete_es_indices(cfg, es_prefix)
    run(["kubectl", "delete", "namespace", ns, "--wait=false"])
    ok(f"Teardown complete for '{name}'.")


def list_deployments(cfg: K3dConfig) -> list[dict]:
    """List all deployed worktree instances.

    Returns a list of dicts with keys: worktree, namespace, git_root.
    Also prints a human-readable table in pretty mode.
    """
    from dh.utils import require_cluster

    require_cluster(cfg.cluster_name)

    result = run(
        [
            "kubectl",
            "get",
            "namespaces",
            "-l",
            "datahub.io/component=worktree",
            "-o",
            "jsonpath="
            "{range .items[*]}"
            "{.metadata.labels.datahub\\.io/worktree-name}"
            "\\t{.metadata.name}"
            "\\t{.metadata.annotations.datahub\\.io/git-root}"
            "\\n{end}",
        ],
        capture_output=True,
        check=False,
    )

    output = result.stdout.strip() if result.returncode == 0 else ""

    ctx = get_output_context()
    if not output:
        if not (ctx and ctx.json_mode):
            click.echo("No DataHub worktree deployments found.")
        return []

    # kubectl jsonpath may return literal \t and \n instead of real tab/newline
    output = output.replace("\\t", "\t").replace("\\n", "\n")

    deployments: list[dict] = []
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) == 3:
            deployments.append(
                {"worktree": parts[0], "namespace": parts[1], "git_root": parts[2]}
            )

    if not (ctx and ctx.json_mode):
        click.echo(click.style("Active DataHub deployments:", bold=True))
        click.echo()
        click.echo(f"  {'WORKTREE':<20} {'NAMESPACE':<25} GIT ROOT")
        click.echo(f"  {'--------':<20} {'---------':<25} --------")
        for d in deployments:
            click.echo(f"  {d['worktree']:<20} {d['namespace']:<25} {d['git_root']}")
        click.echo()

    return deployments


def deploy_status(cfg: K3dConfig, output_json: bool = False) -> None:
    from dh.utils import require_cluster

    require_cluster(cfg.cluster_name)

    name = derive_worktree_name()
    ns = namespace_for(name)
    hostname = hostname_for(name)
    gms_hostname = gms_hostname_for(name)

    ns_check = run(["kubectl", "get", "namespace", ns], check=False, quiet=True)
    if ns_check.returncode != 0:
        if output_json:
            click.echo("{}")
        else:
            warn(f"No deployment found for worktree '{name}' (namespace: {ns}).")
        return

    if output_json:
        data = collect_status_json(cfg, name, ns, hostname, gms_hostname)
        import json
        click.echo(json.dumps(data, indent=2))
    else:
        click.echo()
        click.echo(click.style(f"DataHub '{name}' ({ns}):", bold=True))
        click.echo(f"  Frontend: http://{hostname}")
        click.echo(f"  GMS:      http://{gms_hostname}")
        click.echo()
        run(["kubectl", "get", "pods", "-n", ns, "-o", "wide"])
        click.echo()
        run(["kubectl", "get", "ingress", "-n", ns], check=False)


def collect_status_json(
    cfg: K3dConfig,
    name: str,
    ns: str,
    hostname: str,
    gms_hostname: str = "",
) -> dict:
    """Collect full deployment status as a structured dict for JSON output."""
    import json as _json

    data: dict = {
        "worktree": name,
        "namespace": ns,
        "hostname": hostname,
        "gms_hostname": gms_hostname,
        "url": f"http://{hostname}",
        "gms_url": f"http://{gms_hostname}" if gms_hostname else "",
        "cluster": {},
        "infra": {},
        "pods": [],
        "jobs": [],
        "ingress": [],
    }

    # Cluster info
    result = run(
        ["k3d", "cluster", "list", cfg.cluster_name, "-o", "json"],
        capture_output=True,
        check=False,
    )
    if result.returncode == 0:
        try:
            data["cluster"] = _json.loads(result.stdout)
        except _json.JSONDecodeError:
            pass

    # Infra pods
    result = run(
        ["kubectl", "get", "pods", "-n", cfg.infra_namespace, "-o", "json"],
        capture_output=True,
        check=False,
    )
    if result.returncode == 0:
        try:
            infra_pods = _json.loads(result.stdout).get("items", [])
            data["infra"] = {
                "namespace": cfg.infra_namespace,
                "pods": _summarize_pods(infra_pods),
            }
        except _json.JSONDecodeError:
            pass

    # Worktree pods
    result = run(
        ["kubectl", "get", "pods", "-n", ns, "-o", "json"],
        capture_output=True,
        check=False,
    )
    if result.returncode == 0:
        try:
            pods = _json.loads(result.stdout).get("items", [])
            data["pods"] = _summarize_pods(pods)
        except _json.JSONDecodeError:
            pass

    # Jobs
    result = run(
        ["kubectl", "get", "jobs", "-n", ns, "-o", "json"],
        capture_output=True,
        check=False,
    )
    if result.returncode == 0:
        try:
            jobs = _json.loads(result.stdout).get("items", [])
            data["jobs"] = _summarize_jobs(jobs)
        except _json.JSONDecodeError:
            pass

    # Ingress
    result = run(
        ["kubectl", "get", "ingress", "-n", ns, "-o", "json"],
        capture_output=True,
        check=False,
    )
    if result.returncode == 0:
        try:
            ingresses = _json.loads(result.stdout).get("items", [])
            data["ingress"] = [
                {
                    "name": ing.get("metadata", {}).get("name"),
                    "hosts": [
                        rule.get("host")
                        for rule in ing.get("spec", {}).get("rules", [])
                    ],
                }
                for ing in ingresses
            ]
        except _json.JSONDecodeError:
            pass

    # Compute overall health
    data["healthy"] = _is_healthy(data)

    return data


def _summarize_pods(pods: list[dict]) -> list[dict]:
    """Extract a concise summary from raw pod JSON."""
    summaries = []
    for pod in pods:
        meta = pod.get("metadata", {})
        status = pod.get("status", {})
        phase = status.get("phase", "Unknown")

        containers = []
        for cs in status.get("containerStatuses", []):
            state_keys = cs.get("state", {})
            current_state = "unknown"
            reason = ""
            if "running" in state_keys:
                current_state = "running"
            elif "waiting" in state_keys:
                current_state = "waiting"
                reason = state_keys["waiting"].get("reason", "")
            elif "terminated" in state_keys:
                current_state = "terminated"
                reason = state_keys["terminated"].get("reason", "")

            containers.append({
                "name": cs.get("name"),
                "ready": cs.get("ready", False),
                "state": current_state,
                "reason": reason,
                "restarts": cs.get("restartCount", 0),
                "image": cs.get("image", ""),
            })

        summaries.append({
            "name": meta.get("name"),
            "phase": phase,
            "ready": all(c["ready"] for c in containers) if containers else (phase == "Succeeded"),
            "containers": containers,
        })
    return summaries


def _summarize_jobs(jobs: list[dict]) -> list[dict]:
    """Extract a concise summary from raw job JSON."""
    summaries = []
    for job in jobs:
        meta = job.get("metadata", {})
        status = job.get("status", {})
        conditions = status.get("conditions", [])

        state = "active"
        if status.get("succeeded", 0) > 0:
            state = "completed"
        elif any(c.get("type") == "Failed" and c.get("status") == "True" for c in conditions):
            state = "failed"

        summaries.append({
            "name": meta.get("name"),
            "state": state,
            "succeeded": status.get("succeeded", 0),
            "failed": status.get("failed", 0),
            "active": status.get("active", 0),
        })
    return summaries


_EXPECTED_SERVICES = ("datahub-gms", "datahub-frontend", "datahub-actions")


def _is_healthy(data: dict) -> bool:
    """Determine if the DataHub deployment is healthy.

    Requires all expected services (GMS, frontend, actions) to be running
    and ready. Without them, the deployment is not considered healthy even
    if setup jobs have completed.
    """
    pods = data.get("pods", [])

    # All expected services must be present and ready
    for svc in _EXPECTED_SERVICES:
        if not any(
            svc in pod.get("name", "")
            and pod.get("ready")
            and pod.get("phase") == "Running"
            for pod in pods
        ):
            return False

    # All non-succeeded pods must be ready
    for pod in pods:
        if pod.get("phase") == "Succeeded":
            continue
        if not pod.get("ready"):
            return False

    # No failed jobs
    for job in data.get("jobs", []):
        if job.get("state") == "failed":
            return False

    return True


# ── Debug helpers ──────────────────────────────────────────────────────────

# JDWP agent string — commas escaped for Helm --set
_JDWP = r"-agentlib:jdwp=transport=dt_socket\,server=y\,suspend=n\,address=*:{port}"


def _debug_set_args() -> list[str]:
    """Return Helm --set flags for JDWP and debug env vars."""
    sets = [
        # GMS debug (indices 9–12, after 9 base extraEnvs)
        "datahub-gms.extraEnvs[9].name=JAVA_TOOL_OPTIONS",
        f"datahub-gms.extraEnvs[9].value={_JDWP.format(port=5001)}",
        "datahub-gms.extraEnvs[10].name=SEARCH_SERVICE_ENABLE_CACHE",
        "datahub-gms.extraEnvs[10].value=false",
        "datahub-gms.extraEnvs[11].name=LINEAGE_SEARCH_CACHE_ENABLED",
        "datahub-gms.extraEnvs[11].value=false",
        "datahub-gms.extraEnvs[12].name=DATAHUB_SERVER_TYPE",
        "datahub-gms.extraEnvs[12].value=dev",
        # Frontend debug (index 5, after 5 base extraEnvs)
        "datahub-frontend.extraEnvs[5].name=JAVA_TOOL_OPTIONS",
        f"datahub-frontend.extraEnvs[5].value={_JDWP.format(port=5002)}",
    ]
    result: list[str] = []
    for s in sets:
        result.extend(["--set", s])
    return result


# ── Internal helpers ────────────────────────────────────────────────────────


def _ensure_gms_ingress(ns: str, release_name: str, gms_hostname: str) -> None:
    """Create an ingress for GMS (not provided by the Helm chart)."""
    ingress_yaml = f"""\
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: {release_name}-datahub-gms
  namespace: {ns}
  annotations:
    traefik.ingress.kubernetes.io/router.entrypoints: web,websecure
  labels:
    app.kubernetes.io/name: datahub-gms
    app.kubernetes.io/instance: {release_name}
spec:
  rules:
    - host: {gms_hostname}
      http:
        paths:
          - path: /
            pathType: ImplementationSpecific
            backend:
              service:
                name: {release_name}-datahub-gms
                port:
                  number: 8080
"""
    run(["kubectl", "apply", "-f", "-"], input_text=ingress_yaml)


def _check_collision(name: str) -> None:
    """Ensure no other git root is using this worktree name."""
    ns = namespace_for(name)
    root = git_root()

    ns_check = run(["kubectl", "get", "namespace", ns], check=False, quiet=True)
    if ns_check.returncode != 0:
        return

    result = run(
        [
            "kubectl",
            "get",
            "namespace",
            ns,
            "-o",
            "jsonpath={.metadata.annotations.datahub\\.io/git-root}",
        ],
        capture_output=True,
        check=False,
    )
    existing_root = result.stdout.strip() if result.returncode == 0 else ""
    if existing_root and existing_root != root:
        die(
            f"Namespace '{ns}' is already used by git root '{existing_root}'.\n"
            "Use DH_WORKTREE_NAME=<unique-name> to override."
        )


def _ensure_database(cfg: K3dConfig, db_name: str) -> None:
    info(f"Ensuring MySQL database '{db_name}' exists with schema...")
    result = run(
        [
            "kubectl",
            "get",
            "pod",
            "-n",
            cfg.infra_namespace,
            "-l",
            "app.kubernetes.io/name=mysql",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        die(f"Cannot find MySQL pod in '{cfg.infra_namespace}'.")
    mysql_pod = result.stdout.strip()

    sql = f"""\
CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
GRANT ALL ON `{db_name}`.* TO 'datahub'@'%';
FLUSH PRIVILEGES;
USE `{db_name}`;
CREATE TABLE IF NOT EXISTS metadata_aspect_v2 (
  urn varchar(500) NOT NULL,
  aspect varchar(200) NOT NULL,
  version bigint(20) NOT NULL,
  metadata longtext NOT NULL,
  systemmetadata longtext,
  createdon datetime(6) NOT NULL,
  createdby varchar(255) NOT NULL,
  createdfor varchar(255) DEFAULT NULL,
  PRIMARY KEY (urn, aspect, version),
  INDEX timeIndex (createdon)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;"""

    run(
        [
            "kubectl",
            "exec",
            "-n",
            cfg.infra_namespace,
            mysql_pod,
            "--",
            "mysql",
            "-u",
            "root",
            "-pdatahub",
            "-e",
            sql,
        ],
        quiet=True,
    )
    ok(f"Database '{db_name}' ready.")


def _drop_database(cfg: K3dConfig, db_name: str) -> None:
    info(f"Dropping MySQL database '{db_name}'...")
    result = run(
        [
            "kubectl",
            "get",
            "pod",
            "-n",
            cfg.infra_namespace,
            "-l",
            "app.kubernetes.io/name=mysql",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return
    mysql_pod = result.stdout.strip()

    run(
        [
            "kubectl",
            "exec",
            "-n",
            cfg.infra_namespace,
            mysql_pod,
            "--",
            "mysql",
            "-u",
            "root",
            "-pdatahub",
            "-e",
            f"DROP DATABASE IF EXISTS `{db_name}`;",
        ],
        check=False,
        quiet=True,
    )
    ok(f"Database '{db_name}' dropped.")


def _delete_es_indices(cfg: K3dConfig, prefix: str) -> None:
    info(f"Deleting Elasticsearch indices with prefix '{prefix}_'...")
    result = run(
        [
            "kubectl",
            "get",
            "pod",
            "-n",
            cfg.infra_namespace,
            "-l",
            "app=elasticsearch-master",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return
    es_pod = result.stdout.strip()

    run(
        [
            "kubectl",
            "exec",
            "-n",
            cfg.infra_namespace,
            es_pod,
            "--",
            "curl",
            "-s",
            "-X",
            "DELETE",
            f"http://localhost:9200/{prefix}_*",
        ],
        check=False,
        quiet=True,
    )
    ok("Elasticsearch indices cleaned up.")


def _ensure_mysql_secret(cfg: K3dConfig, namespace: str) -> None:
    secret_yaml = run(
        [
            "kubectl",
            "create",
            "secret",
            "generic",
            "mysql-secrets",
            "--namespace",
            namespace,
            "--from-literal=mysql-root-password=datahub",
            "--from-literal=mysql-password=datahub",
            "--dry-run=client",
            "-o",
            "yaml",
        ],
        capture_output=True,
    )
    run(["kubectl", "apply", "-f", "-"], input_text=secret_yaml.stdout)
