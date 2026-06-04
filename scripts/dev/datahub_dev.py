#!/usr/bin/env python3
"""
datahub-dev: Agent-friendly development tooling for DataHub.

A stdlib-only Python CLI (no third-party deps, no venv needed) that provides
machine-readable environment status, smart rebuilds, targeted test execution,
feature flag management, and recovery tools.

Usage:
    python3 scripts/dev/datahub_dev.py status
    python3 scripts/dev/datahub_dev.py wait [--timeout 300]
    python3 scripts/dev/datahub_dev.py setup [module]
    python3 scripts/dev/datahub_dev.py frontend
    python3 scripts/dev/datahub_dev.py docs [--build]
    python3 scripts/dev/datahub_dev.py rebuild [--wait] [--module gms]
    python3 scripts/dev/datahub_dev.py test <path> [pytest-args...]
    python3 scripts/dev/datahub_dev.py flag list
    python3 scripts/dev/datahub_dev.py flag get <name>
    python3 scripts/dev/datahub_dev.py env set KEY=VALUE
    python3 scripts/dev/datahub_dev.py env restart
    python3 scripts/dev/datahub_dev.py sync-flags
    python3 scripts/dev/datahub_dev.py reset
    python3 scripts/dev/datahub_dev.py nuke [--keep-data]
    python3 scripts/dev/datahub_dev.py instances list
    python3 scripts/dev/datahub_dev.py instances clean
    python3 scripts/dev/datahub_dev.py shell-env

Remote runner (set DATAHUB_RUNNER or 'runner' in ~/.datahub/dev/config.json):
    python3 scripts/dev/datahub_dev.py setup --remote   # one-time remote init
    python3 scripts/dev/datahub_dev.py start            # sync + start remote + tunnel
"""

import argparse
import dataclasses
import datetime
import hashlib
import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Plugin extension dataclasses
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class ServiceConfig:
    name: str  # short label used in JSON output
    health_url: str  # URL that must return 200 for "healthy"
    status_url: Optional[str] = None  # if set, JSON body shown in status output
    required: bool = True  # if True, must be up for overall ready=True


@dataclasses.dataclass
class DevToolingConfig:
    # Maps repo-path prefix → (gradle_task_or_None, docker_service_name_or_None)
    module_to_container: Dict[str, Tuple[Optional[str], Optional[str]]]

    # Short alias → docker service name  (used by `rebuild --module <alias>`)
    rebuild_module_aliases: Dict[str, str]

    # Ordered list of services; cmd_status and cmd_wait iterate this
    services: List[ServiceConfig]

    # Gradle task paths (all overridable by fork)
    gradle_reload_task: str
    gradle_reload_env_task: str
    gradle_quickstart_task: str
    gradle_smoke_install_task: str
    gradle_sync_flags_task: str


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DOCKER_DIR = REPO_ROOT / "docker"
SMOKE_TEST_DIR = REPO_ROOT / "smoke-test"


def _get_branch_slug() -> str:
    """Return a filesystem-safe slug for the current git branch.

    Uses this to name the managed env file so that each branch gets its own
    isolated config. Falls back to 'local' on detached HEAD or any error
    (CI, fresh clone, non-git directory, etc.).
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            timeout=5,
        )
        branch = result.stdout.strip()
        if not branch or branch == "HEAD":  # detached HEAD
            return "local"
        # Replace anything that isn't alphanumeric or a dash, then collapse runs of dashes
        slug = re.sub(r"[^a-zA-Z0-9-]", "-", branch)
        slug = re.sub(r"-+", "-", slug).strip("-")
        return slug or "local"
    except Exception:
        return "local"


# Branch-specific env file: docker/datahub-dev-<branch-slug>.env
# Each branch gets its own isolated config. Computed once at startup.
DEV_ENV_FILE = DOCKER_DIR / f"datahub-dev-{_get_branch_slug()}.env"

# Sentinel file written by cmd_env_restart on success. Ends in .env so it's
# covered by the *.env entry in .gitignore — no new gitignore rules needed.
DEV_ENV_SENTINEL = DEV_ENV_FILE.with_name(DEV_ENV_FILE.stem + ".applied.env")

DEFAULT_TIMEOUT = 300  # seconds
POLL_INTERVAL = 3  # seconds


# ---------------------------------------------------------------------------
# Worktree identity & multi-instance support
# ---------------------------------------------------------------------------


def _get_worktree_id() -> str:
    """Return a stable 6-char hex ID derived from the repo root path.

    Each git worktree has a unique absolute path, so this uniquely identifies
    the worktree regardless of which branch is checked out.
    """
    return hashlib.sha256(str(REPO_ROOT).encode()).hexdigest()[:6]


WORKTREE_ID: str = _get_worktree_id()

# When DATAHUB_REMOTE_EXEC=1 this script is running ON the remote machine
# (invoked by the runner's "exec" verb).  Runner logic is suppressed so the
# script behaves as a plain local executor on that machine.
REMOTE_EXEC: bool = os.environ.get("DATAHUB_REMOTE_EXEC") == "1"

# Path to the runner executable.  Cleared when running remotely so a runner
# configured on the remote machine doesn't cause recursive proxying.
RUNNER: str = "" if REMOTE_EXEC else os.environ.get("DATAHUB_RUNNER", "")

# Docker Compose project name. Allows explicit override (power-user escape
# hatch); defaults to a per-worktree identifier so all Docker objects
# (containers, volumes, networks) are automatically namespaced.
COMPOSE_PROJECT: str = os.environ.get("COMPOSE_PROJECT_NAME") or f"dh-{WORKTREE_ID}"

# Config and instance registry live under ~/.datahub/dev/ so they survive
# across worktrees and are independent of the repo directory.
DATAHUB_DEV_DIR: Path = Path.home() / ".datahub" / "dev"
INSTANCES_FILE: Path = DATAHUB_DEV_DIR / "instances.json"
CONFIG_FILE: Path = DATAHUB_DEV_DIR / "config.json"

# Default host-port for each mapped service (slot 0).  Slot N adds N * SLOT_OFFSET.
PORT_BASE: Dict[str, int] = {
    "DATAHUB_MAPPED_GMS_PORT": 8080,
    "DATAHUB_MAPPED_FRONTEND_PORT": 9002,
    "DATAHUB_MAPPED_MYSQL_PORT": 3306,
    "DATAHUB_MAPPED_ELASTIC_PORT": 9200,
    "DATAHUB_MAPPED_KAFKA_BROKER_PORT": 9092,
    "DATAHUB_MAPPED_GMS_DEBUG_PORT": 5001,
    "DATAHUB_MAPPED_FRONTEND_DEBUG_PORT": 5002,
    "DATAHUB_MAPPED_UPGRADE_DEBUG_PORT": 5003,
    "DATAHUB_MAPPED_GMS_MANAGEMENT_PORT": 4319,
    "DATAHUB_MAPPED_NEO4J_HTTP_PORT": 7474,
    "DATAHUB_MAPPED_NEO4J_BOLT_PORT": 7687,
    "DATAHUB_MAPPED_LOCALSTACK_PORT": 4566,
    "DATAHUB_MAPPED_MAE_CONSUMER_PORT": 9091,
    "DATAHUB_MAPPED_MCE_CONSUMER_PORT": 9090,
    "DATAHUB_MAPPED_MCE_DEBUG_PORT": 5009,
}
SLOT_OFFSET: int = 1000


def _load_registry() -> Dict[str, Any]:
    """Read ~/.datahub/dev/instances.json; return empty structure if absent."""
    if not INSTANCES_FILE.exists():
        return {"version": 1, "instances": {}}
    try:
        data = json.loads(INSTANCES_FILE.read_text())
        if "instances" not in data:
            data["instances"] = {}
        return data
    except (json.JSONDecodeError, OSError):
        return {"version": 1, "instances": {}}


def _save_registry(data: Dict[str, Any]) -> None:
    """Atomically write the instance registry."""
    DATAHUB_DEV_DIR.mkdir(parents=True, exist_ok=True)
    tmp = INSTANCES_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n")
    tmp.rename(INSTANCES_FILE)


def _load_dev_config() -> Dict[str, Any]:
    """Read ~/.datahub/dev/config.json; return defaults if absent.

    Keys:
      max_local_instances   — concurrent local docker slots (default 2)
      max_remote_instances  — concurrent remote runner slots (default 10)
      runner                — path to runner executable (read by datahub-dev.sh)
    """
    defaults: Dict[str, Any] = {"max_local_instances": 2, "max_remote_instances": 10}
    if not CONFIG_FILE.exists():
        return defaults
    try:
        data = json.loads(CONFIG_FILE.read_text())
        return {**defaults, **data}
    except (json.JSONDecodeError, OSError):
        return defaults


def _get_instance() -> Optional[Dict[str, Any]]:
    """Return this worktree's instance record from the registry, or None."""
    return _load_registry()["instances"].get(WORKTREE_ID)


def _compute_ports(slot: int) -> Dict[str, int]:
    """Compute port assignments for a slot. Slot 0 returns the defaults."""
    return {k: v + slot * SLOT_OFFSET for k, v in PORT_BASE.items()}


def _is_port_free(port: int) -> bool:
    """Return True when no process is listening on the given port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        try:
            s.connect(("127.0.0.1", port))
            return False
        except (ConnectionRefusedError, OSError):
            return True


def _pick_slot(registry: Dict[str, Any], remote: bool = False) -> int:
    """Return the lowest available slot for a local or remote instance.

    Local slots:  0 … max_local_instances-1   (ports 8080, 18080, …)
    Remote slots: max_local … max_local+max_remote-1  (tunnel ports only;
                  docker on the remote machine always runs on default ports)
    """
    dev_config = _load_dev_config()
    max_local = int(dev_config.get("max_local_instances", 2))
    max_remote = int(dev_config.get("max_remote_instances", 10))

    slot_start = max_local if remote else 0
    slot_end = max_local + max_remote if remote else max_local

    used_slots = {
        v["slot"]
        for k, v in registry.get("instances", {}).items()
        if k != WORKTREE_ID and isinstance(v.get("slot"), int)
    }
    for slot in range(slot_start, slot_end):
        if slot not in used_slots:
            return slot

    kind = "remote" if remote else "local"
    limit = max_remote if remote else max_local
    key = "max_remote_instances" if remote else "max_local_instances"
    raise RuntimeError(
        f"All {limit} {kind} instance slots are in use. "
        f"Stop an existing instance or increase {key} in "
        "~/.datahub/dev/config.json."
    )


def _register_remote_instance(runner: str, slot: int) -> Dict[str, Any]:
    """Write a remote instance entry into the LOCAL registry.

    For remote instances the "ports" field holds the LOCAL tunnel ports
    (slot-offset).  Docker on the remote machine always runs on PORT_BASE
    defaults — the slot offset is only used for local port-forwarding.
    """
    registry = _load_registry()
    local_ports = _compute_ports(slot)  # slot-offset → used as tunnel local ports
    instance: Dict[str, Any] = {
        "worktree_path": str(REPO_ROOT),
        "project_name": COMPOSE_PROJECT,
        "type": "remote",
        "runner": runner,
        "slot": slot,
        "ports": local_ports,
        "created_at": datetime.datetime.utcnow().isoformat() + "Z",
    }
    registry["instances"][WORKTREE_ID] = instance
    _save_registry(registry)
    return instance


def _compute_tunnel_pairs(local_ports: Dict[str, int]) -> List[str]:
    """Build local:remote port-pair strings for the runner's tunnel verb.

    The remote container always binds PORT_BASE defaults; the slot offset
    determines only the local (forwarded) port.
    """
    pairs = []
    for key in sorted(local_ports):
        local_port = local_ports[key]
        remote_port = PORT_BASE.get(key, local_port)
        pairs.append(f"{local_port}:{remote_port}")
    return pairs


def _register_instance(slot: int, ports: Dict[str, int]) -> Dict[str, Any]:
    """Add or update this worktree's LOCAL instance entry in the registry."""
    # Re-read to catch concurrent writes before committing.
    registry = _load_registry()
    instance: Dict[str, Any] = {
        "worktree_path": str(REPO_ROOT),
        "project_name": COMPOSE_PROJECT,
        "type": "local",
        "slot": slot,
        "ports": ports,
        "created_at": datetime.datetime.utcnow().isoformat() + "Z",
    }
    registry["instances"][WORKTREE_ID] = instance
    _save_registry(registry)
    return instance


def _count_running_dh_instances() -> int:
    """Count dh-* compose projects (excluding our own) with running containers."""
    result = subprocess.run(
        ["docker", "ps", "--format", "{{json .}}"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        return 0
    projects: Set[str] = set()
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            container = json.loads(line)
        except json.JSONDecodeError:
            continue
        labels_str = container.get("Labels", "")
        for label in labels_str.split(","):
            if label.startswith("com.docker.compose.project="):
                proj = label.split("=", 1)[1]
                if proj.startswith("dh-") and proj != COMPOSE_PROJECT:
                    projects.add(proj)
                break
    return len(projects)


def _write_ports_to_env_file(ports: Dict[str, int]) -> None:
    """Upsert all port env vars into DEV_ENV_FILE so docker compose picks them up."""
    if not DEV_ENV_FILE.exists():
        DEV_ENV_FILE.touch()
    existing_lines = DEV_ENV_FILE.read_text().splitlines()
    port_keys = set(ports.keys())
    new_lines = [
        line
        for line in existing_lines
        if not any(line.strip().startswith(f"{k}=") for k in port_keys)
    ]
    for k, v in sorted(ports.items()):
        new_lines.append(f"{k}={v}")
    DEV_ENV_FILE.write_text("\n".join(new_lines) + "\n")


def _gms_url() -> str:
    """Return the GMS base URL for this worktree, reading the instance registry."""
    inst = _get_instance()
    port = inst["ports"].get("DATAHUB_MAPPED_GMS_PORT", 8080) if inst else 8080
    return os.environ.get("DATAHUB_GMS_URL", f"http://localhost:{port}")


def _frontend_url() -> str:
    """Return the frontend base URL for this worktree, reading the instance registry."""
    inst = _get_instance()
    port = inst["ports"].get("DATAHUB_MAPPED_FRONTEND_PORT", 9002) if inst else 9002
    return os.environ.get("DATAHUB_FRONTEND_URL", f"http://localhost:{port}")


# ---------------------------------------------------------------------------
# Plugin config: default + extension point
# ---------------------------------------------------------------------------


def _default_config() -> DevToolingConfig:
    return DevToolingConfig(
        module_to_container={
            "metadata-service/": (":metadata-service:war:bootJar", "datahub-gms"),
            "metadata-models/": (None, None),  # triggers full rebuild
            "datahub-frontend/": (":datahub-frontend:dist", "datahub-frontend-react"),
            "datahub-web-react/": (
                ":datahub-web-react:distZip",
                "datahub-frontend-react",
            ),
            "datahub-graphql-core/": (
                ":metadata-service:war:bootJar",
                "datahub-gms",
            ),
            "metadata-jobs/mce-consumer-job/": (
                ":metadata-jobs:mce-consumer-job:bootJar",
                "datahub-mce-consumer",
            ),
            "metadata-jobs/mae-consumer-job/": (
                ":metadata-jobs:mae-consumer-job:bootJar",
                "datahub-mae-consumer",
            ),
            "metadata-io/": (":metadata-service:war:bootJar", "datahub-gms"),
        },
        rebuild_module_aliases={
            "gms": "datahub-gms",
            "frontend": "datahub-frontend-react",
            "mce": "datahub-mce-consumer",
            "mae": "datahub-mae-consumer",
        },
        services=[
            ServiceConfig(
                "gms",
                f"{_gms_url()}/health",
                status_url=f"{_gms_url()}/health/detailed",
                required=True,
            ),
            ServiceConfig("frontend", f"{_frontend_url()}/admin", required=True),
        ],
        gradle_reload_task=":docker:reload",
        gradle_reload_env_task=":docker:reloadEnv",
        gradle_quickstart_task="quickstartDebug",
        gradle_smoke_install_task=":smoke-test:installDev",
        gradle_sync_flags_task=":metadata-service:configuration:generateFlagClassification",
    )


def _load_config() -> DevToolingConfig:
    """Load default config and optionally extend it via a local plugin file.

    If ``scripts/dev/datahub_dev_ext.py`` exists next to this file, it is
    imported and its ``extend_config(config)`` function is called so that
    forks can overlay service lists, module aliases, and Gradle task names
    without modifying this file.
    """
    config = _default_config()
    ext_path = Path(__file__).parent / "datahub_dev_ext.py"
    if ext_path.exists():
        try:
            import importlib.util

            spec = importlib.util.spec_from_file_location("datahub_dev_ext", ext_path)
            mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
            spec.loader.exec_module(mod)  # type: ignore[union-attr]
            if hasattr(mod, "extend_config"):
                config = mod.extend_config(config)
        except Exception as e:
            _log(f"WARNING: Failed to load datahub_dev_ext.py: {e}")
    return config


def _log(msg: str) -> None:
    """Log to stderr so stdout stays clean for JSON output."""
    print(msg, file=sys.stderr)


CONFIG: DevToolingConfig = _load_config()


def _rebuild_config_services() -> None:
    """Re-point CONFIG.services at the current instance's allocated ports.

    Called after port allocation in cmd_start() so that cmd_wait() health-checks
    the correct ports when a new instance is being registered for the first time.
    """
    gms = _gms_url()
    frontend = _frontend_url()
    for svc in CONFIG.services:
        if svc.name == "gms":
            svc.health_url = f"{gms}/health"
            if svc.status_url:
                svc.status_url = f"{gms}/health/detailed"
        elif svc.name == "frontend":
            svc.health_url = f"{frontend}/admin"


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _http_get(url: str, timeout: int = 5) -> Tuple[int, str]:
    """Make an HTTP GET, return (status_code, body). Returns (-1, error) on failure."""
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace") if e.fp else ""
    except Exception as e:
        return -1, str(e)


def _dev_env() -> Dict[str, str]:
    """Return a copy of os.environ with DATAHUB_LOCAL_COMMON_ENV and
    COMPOSE_PROJECT_NAME injected.

    Every subprocess (Gradle, docker compose, pytest) picks up:
    - DATAHUB_LOCAL_COMMON_ENV: the per-branch env file read by docker compose profiles
    - COMPOSE_PROJECT_NAME: the per-worktree project name read by Gradle's docker-compose
      plugin and by docker compose itself
    """
    env = os.environ.copy()
    if not DEV_ENV_FILE.exists():
        DEV_ENV_FILE.touch()
    env["DATAHUB_LOCAL_COMMON_ENV"] = str(DEV_ENV_FILE)
    # Only inject if not already set (respects explicit user override)
    env.setdefault("COMPOSE_PROJECT_NAME", COMPOSE_PROJECT)
    return env


def _run(
    cmd: List[str],
    capture: bool = True,
    cwd: Optional[Path] = None,
    timeout: int = 600,
    env: Optional[Dict[str, str]] = None,
) -> subprocess.CompletedProcess:
    """Run a subprocess command with DATAHUB_LOCAL_COMMON_ENV injected."""
    return subprocess.run(
        cmd,
        capture_output=capture,
        text=True,
        cwd=cwd or REPO_ROOT,
        timeout=timeout,
        env=env or _dev_env(),
    )


def _run_docker_compose_ps() -> List[Dict[str, Any]]:
    """Get docker compose container info as JSON list."""
    result = _run(
        ["docker", "compose", "-p", COMPOSE_PROJECT, "ps", "--format", "json", "-a"]
    )
    if result.returncode != 0:
        return []
    containers = []
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, list):
                containers.extend(obj)
            else:
                containers.append(obj)
        except json.JSONDecodeError:
            continue
    return containers


def _find_conflicting_projects(our_ports: Set[int]) -> Dict[str, List[str]]:
    """Find non-DataHub compose projects occupying ports this instance needs.

    Returns a dict mapping project name -> list of conflicting host ports.

    Skips:
    - Our own COMPOSE_PROJECT (already running / mid-restart)
    - Any project whose name starts with "dh-" — those are sibling worktrees
      managed by this tool; stopping them would disrupt another developer's session.

    Legacy "datahub" projects (pre-isolation) are treated as external conflicts
    and will be auto-stopped, providing a clean migration path.
    """
    result = _run(["docker", "ps", "--format", "{{json .}}"])
    if result.returncode != 0:
        return {}

    port_pattern = re.compile(r":(\d+)->")
    conflicts: Dict[str, List[str]] = {}

    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            container = json.loads(line)
        except json.JSONDecodeError:
            continue

        ports_str = container.get("Ports", "")
        labels_str = container.get("Labels", "")

        # Extract host-side ports that overlap with the ports we need
        host_ports = port_pattern.findall(ports_str)
        matching = [p for p in host_ports if int(p) in our_ports]
        if not matching:
            continue

        # Extract compose project name from labels
        project = None
        for label in labels_str.split(","):
            if label.startswith("com.docker.compose.project="):
                project = label.split("=", 1)[1]
                break

        # Skip ourselves and sibling dh-* worktrees
        if not project or project == COMPOSE_PROJECT or project.startswith("dh-"):
            continue

        conflicts.setdefault(project, []).extend(matching)

    return {proj: sorted(set(ports)) for proj, ports in conflicts.items()}


def _stop_conflicting_projects(conflicts: Dict[str, List[str]]) -> None:
    """Stop compose projects that conflict with our ports."""
    for project, ports in conflicts.items():
        port_list = ", ".join(ports)
        _log(
            f"Stopping conflicting project '{project}' (occupying ports: {port_list})..."
        )
        result = _run(
            ["docker", "compose", "-p", project, "down"],
            capture=False,
            timeout=120,
        )
        if result.returncode != 0:
            _log(
                f"Warning: failed to stop project '{project}' — you may see port conflicts."
            )


def _get_container_info(containers: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Parse docker compose ps JSON into structured service info."""
    services = {}
    for c in containers:
        name = c.get("Service", c.get("Name", "unknown"))
        state = c.get("State", "unknown")
        health = c.get("Health", "")
        status_str = c.get("Status", "")

        # Parse restart count from status string (e.g., "Restarting (1) 5 seconds ago")
        restart_count = 0
        restart_match = re.search(r"Restarting \((\d+)\)", status_str)
        if restart_match:
            restart_count = int(restart_match.group(1))

        services[name] = {
            "state": state,
            "health": health,
            "status": status_str,
            "restart_count": restart_count,
        }
    return services


def _suggest_recovery(
    services: Dict[str, Dict[str, Any]], gms_ok: bool
) -> Optional[str]:
    """Analyze service state and suggest recovery action."""
    all_down = (
        all(s["state"] != "running" for s in services.values()) if services else True
    )
    any_crash_loop = any(s["restart_count"] > 3 for s in services.values())
    # Only flag exits with non-zero codes as problems. Containers like system-update
    # exit with code 0 normally (one-shot init containers).
    any_bad_exit = any(
        s["state"] == "exited" and "Exited (0)" not in s.get("status", "")
        for s in services.values()
    )

    if all_down and not services:
        return "No DataHub containers found. Run: ./gradlew quickstartDebug"
    if all_down:
        return "All services are down. Try: python3 scripts/datahub_dev.py nuke --keep-data"
    if any_crash_loop:
        return "Services are crash-looping. Try: python3 scripts/datahub_dev.py reset"
    if any_bad_exit:
        return "Some services have exited with errors. Try: python3 scripts/datahub_dev.py reset"
    if not gms_ok:
        return "GMS is not healthy. It may still be bootstrapping — wait a minute. If it persists: python3 scripts/datahub_dev.py reset"
    return None


# ---------------------------------------------------------------------------
# Command: status
# ---------------------------------------------------------------------------


def cmd_status(args: argparse.Namespace) -> int:
    """Check environment status and output structured JSON."""
    containers = _run_docker_compose_ps()
    services = _get_container_info(containers)

    # Health-check each configured service
    service_health: Dict[str, Dict[str, Any]] = {}
    for svc in CONFIG.services:
        status_code, _ = _http_get(svc.health_url)
        healthy = status_code == 200
        entry: Dict[str, Any] = {"healthy": healthy, "status_code": status_code}
        if svc.status_url:
            status_detail_code, status_detail_body = _http_get(svc.status_url)
            if status_detail_code == 200:
                try:
                    entry["status_detail"] = json.loads(status_detail_body)
                except json.JSONDecodeError:
                    pass
        service_health[svc.name] = entry

    # Overall readiness: all required services must be healthy
    ready = all(
        service_health[svc.name]["healthy"] for svc in CONFIG.services if svc.required
    )

    gms_ok = service_health.get("gms", {}).get("healthy", False)
    suggestion = _suggest_recovery(services, gms_ok)

    result: Dict[str, Any] = {
        "ready": ready,
        **service_health,
        "services": services,
    }
    if suggestion:
        result["suggestion"] = suggestion

    print(json.dumps(result, indent=2))
    return 0 if ready else 1


# ---------------------------------------------------------------------------
# Command: wait
# ---------------------------------------------------------------------------


def cmd_wait(args: argparse.Namespace) -> int:
    """Block until the DataHub stack is ready or timeout."""
    timeout = args.timeout
    start = time.time()
    _log(f"Waiting up to {timeout}s for DataHub to become ready...")

    required = [svc for svc in CONFIG.services if svc.required]

    while True:
        elapsed = time.time() - start
        if elapsed > timeout:
            _log(f"Timeout after {timeout}s. DataHub is not ready.")
            # Print final status to stdout
            cmd_status(args)
            return 1

        checks_ok = [False] * len(required)
        for i, svc in enumerate(required):
            code, _ = _http_get(svc.health_url)
            checks_ok[i] = code == 200

        if all(checks_ok):
            _log(f"DataHub is ready! ({elapsed:.0f}s)")
            cmd_status(args)
            return 0

        status_parts = [
            f"{svc.name}={'ok' if ok else 'waiting'}"
            for svc, ok in zip(required, checks_ok)
        ]
        _log(f"  [{elapsed:.0f}s] {', '.join(status_parts)}")

        time.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# Command: rebuild
# ---------------------------------------------------------------------------


def _detect_changed_modules() -> List[Tuple[str, str, str]]:
    """Use git diff to detect which modules changed, return list of (module_path, gradle_task, container)."""
    result = _run(["git", "diff", "--name-only", "HEAD"])
    if result.returncode != 0:
        # Fallback: also check unstaged
        result = _run(["git", "diff", "--name-only"])

    # Combine staged + unstaged changes
    staged = _run(["git", "diff", "--name-only", "--cached"])
    changed_files = set()
    if result.stdout:
        changed_files.update(result.stdout.strip().splitlines())
    if staged.stdout:
        changed_files.update(staged.stdout.strip().splitlines())

    # Also check untracked files
    untracked = _run(["git", "ls-files", "--others", "--exclude-standard"])
    if untracked.stdout:
        changed_files.update(untracked.stdout.strip().splitlines())

    if not changed_files:
        return []

    matched = []
    full_rebuild = False
    for module_prefix, (gradle_task, container) in CONFIG.module_to_container.items():
        for f in changed_files:
            if f.startswith(module_prefix):
                if gradle_task is None:
                    # metadata-models change => full rebuild
                    full_rebuild = True
                    break
                matched.append((module_prefix, gradle_task, container))
                break
        if full_rebuild:
            break

    if full_rebuild:
        # Return all buildable modules
        return [
            (mp, gt, ct)
            for mp, (gt, ct) in CONFIG.module_to_container.items()
            if gt is not None
        ]

    return matched


def cmd_rebuild(args: argparse.Namespace) -> int:
    """Smart incremental rebuild of changed modules.

    Uses ``./gradlew :docker:reload`` which builds changed modules via its
    ``prepareAll*`` dependency AND restarts only the affected containers —
    all in a single Gradle invocation so the task graph correctly detects
    which ``dockerPrepare`` tasks were not up-to-date.
    """
    # Module info is only used for logging; reload handles detection itself
    if args.module:
        module_names = CONFIG.rebuild_module_aliases
        if args.module not in module_names:
            _log(
                f"Unknown module: {args.module}. Valid: {', '.join(module_names.keys())}"
            )
            return 1
        _log(f"Rebuilding: {module_names[args.module]}")
    else:
        modules = _detect_changed_modules()
        if modules:
            containers = list(dict.fromkeys(m[2] for m in modules))
            _log(f"Changed modules detected: {', '.join(containers)}")
        else:
            _log("No changed modules detected, but running reload to be safe.")

    # Single Gradle invocation: builds changed modules AND restarts their
    # containers.  The reload task depends on prepareAll* which triggers
    # the build, then inspects which dockerPrepare tasks were not
    # up-to-date to decide which containers to restart.
    gradle_cmd = [
        "./gradlew",
        CONFIG.gradle_reload_task,
        "-x",
        "test",
        "-x",
        "check",
    ]
    _log(f"Running: {' '.join(gradle_cmd)}")
    build_start = time.time()
    result = _run(gradle_cmd, capture=False, timeout=600)
    build_time = time.time() - build_start

    if result.returncode != 0:
        _log(f"Rebuild+reload failed after {build_time:.0f}s")
        return 1

    _log(f"Rebuild+reload succeeded in {build_time:.0f}s")

    # Step 3: Optionally wait for readiness
    if args.wait:
        _log("Waiting for services to become ready...")
        wait_args = argparse.Namespace(timeout=args.timeout)
        return cmd_wait(wait_args)

    _log("Rebuild complete. Use 'datahub-dev wait' to check readiness.")
    return 0


# ---------------------------------------------------------------------------
# Command: test
# ---------------------------------------------------------------------------


def cmd_test(args: argparse.Namespace) -> int:
    """Run targeted smoke tests."""
    test_path = args.path
    extra_args = args.pytest_args or []

    # Reject paths that escape the smoke-test directory (path traversal guard)
    resolved = (SMOKE_TEST_DIR / test_path).resolve()
    if not str(resolved).startswith(str(SMOKE_TEST_DIR.resolve())):
        _log(f"ERROR: test path '{test_path}' escapes the smoke-test directory.")
        return 1

    # Pre-check: is the stack ready?
    _log("Checking stack health...")
    gms_status, _ = _http_get(f"{_gms_url()}/health", timeout=3)
    if gms_status != 200:
        _log(f"WARNING: GMS is not healthy (status={gms_status}). Tests may fail.")
        _log("Run 'python3 scripts/datahub_dev.py wait' to wait for readiness.")

    # Check smoke-test venv exists
    venv_python = SMOKE_TEST_DIR / "venv" / "bin" / "python"
    sentinel = SMOKE_TEST_DIR / "venv" / ".build_install_dev_sentinel"
    if not venv_python.exists() or not sentinel.exists():
        _log("Smoke test venv not found. Setting it up...")
        setup_result = _run(
            [
                "./gradlew",
                CONFIG.gradle_smoke_install_task,
            ],
            capture=False,
            timeout=300,
        )
        if setup_result.returncode != 0:
            _log("Failed to set up smoke-test venv.")
            return 1

    # Set up environment variables (replicate set-test-env-vars.sh and set-cypress-creds.sh)
    env = _dev_env()
    gms_base_path = env.get("DATAHUB_GMS_BASE_PATH", "")
    instance_gms_url = _gms_url()
    if gms_base_path == "/" or not gms_base_path:
        env.setdefault(
            "DATAHUB_KAFKA_SCHEMA_REGISTRY_URL",
            f"{instance_gms_url}/schema-registry/api",
        )
        env.setdefault("DATAHUB_GMS_URL", instance_gms_url)
    else:
        env.setdefault(
            "DATAHUB_KAFKA_SCHEMA_REGISTRY_URL",
            f"{instance_gms_url}{gms_base_path}/schema-registry/api",
        )
        env.setdefault("DATAHUB_GMS_URL", f"{instance_gms_url}{gms_base_path}")

    # Cypress creds (used by some test fixtures)
    env.setdefault("ADMIN_USERNAME", "datahub")
    env.setdefault("ADMIN_PASSWORD", "datahub")
    env.setdefault("CYPRESS_ADMIN_USERNAME", env.get("ADMIN_USERNAME", "datahub"))
    env.setdefault("CYPRESS_ADMIN_PASSWORD", env.get("ADMIN_PASSWORD", "datahub"))
    env.setdefault("CYPRESS_ADMIN_DISPLAYNAME", "DataHub")

    # Disable telemetry during tests
    env["DATAHUB_TELEMETRY_ENABLED"] = "false"

    # Build pytest command
    pytest_cmd = [
        str(venv_python),
        "-m",
        "pytest",
        test_path,
        "-vv",
        "--tb=short",
        "--no-header",
        "-q",
    ]

    # Add agent-mode reporting if enabled
    if env.get("AGENT_MODE") == "1":
        report_path = SMOKE_TEST_DIR / "build" / "test-report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        pytest_cmd.extend(
            [
                f"--agent-report={report_path}",
            ]
        )

    pytest_cmd.extend(extra_args)

    _log(f"Running: {' '.join(pytest_cmd)}")
    result = _run(pytest_cmd, capture=False, cwd=SMOKE_TEST_DIR, env=env)

    # If agent mode, print the JSON report path
    if env.get("AGENT_MODE") == "1":
        report_path = SMOKE_TEST_DIR / "build" / "test-report.json"
        if report_path.exists():
            _log(f"Agent report: {report_path}")

    return result.returncode


# ---------------------------------------------------------------------------
# Command: flag
# ---------------------------------------------------------------------------

GENERATED_MANIFEST = REPO_ROOT / "scripts" / "generated" / "flag-classification.json"


def _load_flag_classification() -> Dict[str, Any]:
    """Load the auto-generated flag classification manifest.

    The manifest is produced by the generateFlagClassification Gradle task.
    If missing, returns an empty structure and logs a hint to regenerate.
    """
    if not GENERATED_MANIFEST.exists():
        _log("WARNING: flag-classification.json not generated yet.")
        _log(
            "Run: ./gradlew :metadata-service:configuration:generateFlagClassification"
        )
        _log("Or:  scripts/datahub-dev.sh sync-flags")
        return {"dynamic": {}, "static": {}}
    with open(GENERATED_MANIFEST) as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            _log("ERROR: flag-classification.json is corrupted (partial write?).")
            _log(
                "Re-run: ./gradlew :metadata-service:configuration:generateFlagClassification"
            )
            return {"dynamic": {}, "static": {}}


def cmd_flag_list(args: argparse.Namespace) -> int:
    """List all feature flags and their current live values."""
    status_code, body = _http_get(f"{_gms_url()}/openapi/operations/dev/featureFlags")
    if status_code != 200:
        _log(
            f"Failed to get feature flags (status={status_code}). Is GMS running with DEV_TOOLING_ENABLED=true?"
        )
        return 1

    try:
        flags = json.loads(body)
        print(json.dumps(flags, indent=2))
    except json.JSONDecodeError:
        print(body)
    return 0


def cmd_flag_get(args: argparse.Namespace) -> int:
    """Get a specific feature flag value."""
    status_code, body = _http_get(f"{_gms_url()}/openapi/operations/dev/featureFlags")
    if status_code != 200:
        _log(f"Failed to get feature flags (status={status_code})")
        return 1
    try:
        flags = json.loads(body)
        if args.name in flags:
            print(json.dumps({args.name: flags[args.name]}, indent=2))
            return 0
        else:
            _log(
                f"Flag '{args.name}' not found. Available: {', '.join(sorted(flags.keys()))}"
            )
            return 1
    except json.JSONDecodeError:
        _log(f"Invalid response: {body}")
        return 1


# ---------------------------------------------------------------------------
# Command: env
# ---------------------------------------------------------------------------


def cmd_env_set(args: argparse.Namespace) -> int:
    """Set an environment variable for DataHub containers.

    Writes to DEV_ENV_FILE (docker/datahub-dev.env). Every subprocess this
    tool spawns already has DATAHUB_LOCAL_COMMON_ENV pointing at that file,
    so Gradle / docker compose will pick the value up automatically.
    """
    key_value = args.assignment
    if "=" not in key_value:
        _log("Usage: datahub-dev env set KEY=VALUE")
        return 1

    key, value = key_value.split("=", 1)
    if "\n" in key or "\n" in value:
        _log("ERROR: Key and value must not contain newlines.")
        return 1

    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
        _log(f"ERROR: Invalid key '{key}'. Keys must match [A-Za-z_][A-Za-z0-9_]*")
        return 1

    if not DEV_ENV_FILE.exists():
        DEV_ENV_FILE.touch()

    # Read existing content
    existing_lines = DEV_ENV_FILE.read_text().splitlines()

    # Update or add the key
    found = False
    new_lines = []
    for line in existing_lines:
        if line.strip().startswith(f"{key}="):
            new_lines.append(f"{key}={value}")
            found = True
        else:
            new_lines.append(line)

    if not found:
        new_lines.append(f"{key}={value}")

    DEV_ENV_FILE.write_text("\n".join(new_lines) + "\n")

    _log(f"Set {key}={value} in {DEV_ENV_FILE}")
    _log("Run: scripts/datahub-dev.sh env restart  (to apply)")
    return 0


def cmd_env_list(args: argparse.Namespace) -> int:
    """List env vars and whether a restart is needed to apply them."""
    vars_dict: Dict[str, str] = {}
    if DEV_ENV_FILE.exists():
        lines = [line for line in DEV_ENV_FILE.read_text().splitlines() if line.strip()]
        vars_dict = dict(line.split("=", 1) for line in lines if "=" in line)

    # pending_restart: true if env file is newer than the sentinel (or sentinel absent)
    if DEV_ENV_SENTINEL.exists() and DEV_ENV_FILE.exists():
        pending_restart = (
            DEV_ENV_FILE.stat().st_mtime > DEV_ENV_SENTINEL.stat().st_mtime
        )
    elif not vars_dict:
        pending_restart = False  # nothing set, nothing pending
    else:
        pending_restart = True  # vars exist but no successful restart recorded

    print(
        json.dumps(
            {
                "file": str(DEV_ENV_FILE.relative_to(REPO_ROOT)),
                "pending_restart": pending_restart,
                "vars": vars_dict,
            },
            indent=2,
        )
    )
    return 0


def cmd_env_restart(args: argparse.Namespace) -> int:
    """Restart containers to pick up new environment variables."""
    _log("Restarting containers with new environment via Gradle reloadEnv...")
    result = _run(
        [
            "./gradlew",
            CONFIG.gradle_reload_env_task,
        ],
        capture=False,
        timeout=300,
    )
    if result.returncode != 0:
        _log("Gradle reloadEnv failed. Falling back to docker compose recreate...")
        fallback = _run(
            [
                "docker",
                "compose",
                "-p",
                COMPOSE_PROJECT,
                "up",
                "-d",
                "--force-recreate",
            ],
            capture=False,
            timeout=120,
        )
        if fallback.returncode != 0:
            _log("ERROR: Both Gradle reloadEnv and docker compose recreate failed.")
            return 1

    DEV_ENV_SENTINEL.write_text(str(time.time()))
    _log("Containers restarting. Waiting for readiness...")
    wait_args = argparse.Namespace(timeout=DEFAULT_TIMEOUT)
    return cmd_wait(wait_args)


def cmd_env_clean(args: argparse.Namespace) -> int:
    """Remove env files whose branch no longer exists locally.

    Lists all docker/datahub-dev-*.env files and removes any whose branch
    slug doesn't match a current local branch. The 'local' fallback file
    (detached HEAD / CI) is always kept.
    """
    # Exclude sentinel files (*.applied.env) — those are handled alongside their env files
    env_files = sorted(
        f
        for f in DOCKER_DIR.glob("datahub-dev-*.env")
        if not f.name.endswith(".applied.env")
    )
    if not env_files:
        _log("No datahub-dev-*.env files found.")
        return 0

    # Build the set of slugs for all current local branches
    result = _run(["git", "branch", "--format=%(refname:short)"])
    branches = result.stdout.strip().splitlines() if result.returncode == 0 else []
    active_slugs: set = set()
    for branch in branches:
        slug = re.sub(r"[^a-zA-Z0-9-]", "-", branch.strip())
        slug = re.sub(r"-+", "-", slug).strip("-")
        if slug:
            active_slugs.add(f"datahub-dev-{slug}.env")
    active_slugs.add("datahub-dev-local.env")  # always keep the fallback

    to_keep = [f for f in env_files if f.name in active_slugs]
    to_remove = [f for f in env_files if f.name not in active_slugs]

    if to_keep:
        _log(f"Keeping ({len(to_keep)}):")
        for f in to_keep:
            marker = " (current)" if f == DEV_ENV_FILE else ""
            _log(f"  {f.name}{marker}")

    if not to_remove:
        _log("Nothing to clean.")
        return 0

    _log(f"{'Would remove' if args.dry_run else 'Removing'} ({len(to_remove)}):")
    for f in to_remove:
        _log(f"  {f.name}")

    if args.dry_run:
        _log("Dry run — pass without --dry-run to delete.")
        return 0

    for f in to_remove:
        f.unlink()
        sentinel = f.with_name(f.stem + ".applied.env")
        if sentinel.exists():
            sentinel.unlink()
    _log("Done.")
    return 0


# ---------------------------------------------------------------------------
# Command: stop
# ---------------------------------------------------------------------------


def cmd_stop(args: argparse.Namespace) -> int:
    """Stop all DataHub services without restarting."""
    _log("Stopping DataHub services...")
    result = _run(
        ["docker", "compose", "-p", COMPOSE_PROJECT, "down"],
        capture=False,
        timeout=120,
    )
    if result.returncode != 0:
        _log("Failed to stop DataHub services.")
        return 1
    _log("DataHub stopped.")
    return 0


# ---------------------------------------------------------------------------
# Command: reset
# ---------------------------------------------------------------------------


def cmd_reset(args: argparse.Namespace) -> int:
    """Soft reset: restart services without losing data."""
    _log("Performing soft reset (no data loss)...")

    # Step 1: Stop all DataHub services
    _log("Stopping DataHub services...")
    _run(
        ["docker", "compose", "-p", COMPOSE_PROJECT, "down"],
        capture=False,
        timeout=120,
    )

    # Step 2: Start everything back up
    _log("Starting DataHub services...")
    _run(
        ["docker", "compose", "-p", COMPOSE_PROJECT, "up", "-d"],
        capture=False,
        timeout=120,
    )

    # Step 3: Wait for readiness
    _log("Waiting for services to become ready...")
    wait_args = argparse.Namespace(timeout=DEFAULT_TIMEOUT)
    return cmd_wait(wait_args)


# ---------------------------------------------------------------------------
# Command: nuke
# ---------------------------------------------------------------------------


def cmd_nuke(args: argparse.Namespace) -> int:
    """Hard reset: remove containers (and optionally volumes) and restart from scratch."""
    keep_data = args.keep_data

    if keep_data:
        _log("Nuking containers (keeping data volumes)...")
    else:
        _log("NUCLEAR OPTION: Removing ALL containers AND data volumes...")
        _log(
            "WARNING: This will destroy all local metadata. Recovery takes 10-20 minutes."
        )

    # Step 1: Find and remove containers
    _log("Removing containers...")
    result = _run(
        [
            "docker",
            "ps",
            "-a",
            "--filter",
            f"label=com.docker.compose.project={COMPOSE_PROJECT}",
            "-q",
        ],
    )
    container_ids = result.stdout.strip().splitlines() if result.stdout.strip() else []
    if container_ids:
        _run(["docker", "rm", "-f"] + container_ids, capture=False)
        _log(f"Removed {len(container_ids)} containers.")
    else:
        _log("No containers found.")

    # Step 2: Remove volumes (unless --keep-data)
    if not keep_data:
        _log("Removing volumes...")
        vol_result = _run(
            [
                "docker",
                "volume",
                "ls",
                "--filter",
                f"label=com.docker.compose.project={COMPOSE_PROJECT}",
                "-q",
            ],
        )
        volume_ids = (
            vol_result.stdout.strip().splitlines() if vol_result.stdout.strip() else []
        )
        if volume_ids:
            _run(["docker", "volume", "rm", "-f"] + volume_ids, capture=False)
            _log(f"Removed {len(volume_ids)} volumes.")

    # Step 3: Remove networks
    _log("Removing networks...")
    net_result = _run(
        [
            "docker",
            "network",
            "ls",
            "--filter",
            f"label=com.docker.compose.project={COMPOSE_PROJECT}",
            "-q",
        ],
    )
    network_ids = (
        net_result.stdout.strip().splitlines() if net_result.stdout.strip() else []
    )
    if network_ids:
        for nid in network_ids:
            _run(["docker", "network", "rm", nid], capture=False)

    # Step 4: Restart from scratch
    _log("Restarting DataHub from scratch via Gradle...")
    start_args = argparse.Namespace(timeout=600)
    return cmd_start(start_args)


# ---------------------------------------------------------------------------
# Command: setup
# ---------------------------------------------------------------------------

# Maps short names to Gradle tasks. None = special-cased (e.g. frontend uses yarn).
SETUP_MODULES: Dict[str, Optional[str]] = {
    "ingestion": ":metadata-ingestion:installDev",
    "smoke-test": ":smoke-test:installDev",
    "airflow-plugin": ":metadata-ingestion-modules:airflow-plugin:installDev",
    "gx-plugin": ":metadata-ingestion-modules:gx-plugin:installDev",
    "dagster-plugin": ":metadata-ingestion-modules:dagster-plugin:installDev",
    "prefect-plugin": ":metadata-ingestion-modules:prefect-plugin:installDev",
    "actions": ":datahub-actions:installDev",
    "frontend": None,
}
DEFAULT_SETUP_MODULE = "ingestion"


def cmd_setup(args: argparse.Namespace) -> int:
    """Install dev dependencies for a module, or initialise a remote environment."""
    if getattr(args, "remote", False):
        if not RUNNER:
            _log(
                "ERROR: No runner configured. "
                "Set DATAHUB_RUNNER or add 'runner' to ~/.datahub/dev/config.json."
            )
            return 1
        _log("Initialising remote environment via runner (may take several minutes)...")
        rc = _run([RUNNER, "init"], capture=False, timeout=1200).returncode
        if rc == 0:
            _log("Remote environment ready.")
            _log("Next: datahub-dev.sh start  (syncs code and starts DataHub remotely)")
        return rc

    module = args.module

    if module == "frontend":
        _log("Running yarn install in datahub-web-react/...")
        result = _run(
            ["yarn", "install"],
            capture=False,
            cwd=REPO_ROOT / "datahub-web-react",
            timeout=300,
        )
        if result.returncode != 0:
            _log("yarn install failed.")
            return 1
        _log("Frontend dependencies installed.")
        return 0

    gradle_task = SETUP_MODULES[module]
    _log(f"Setting up {module} via {gradle_task}...")
    result = _run(
        ["./gradlew", gradle_task],
        capture=False,
        timeout=600,
    )
    if result.returncode != 0:
        _log(f"Setup for {module} failed.")
        return 1

    # Print concrete verification so the agent trusts the result
    venv_dir = _resolve_venv_dir(module)
    if venv_dir and venv_dir.exists():
        _log(f"Setup for {module} complete. venv: {venv_dir}")
        if module == "ingestion":
            cli = venv_dir / "bin" / "datahub"
            if cli.exists():
                ver = _run([str(cli), "version"], timeout=10)
                if ver.returncode == 0:
                    _log(f"datahub CLI ready: {ver.stdout.strip()}")
    else:
        _log(f"Setup for {module} complete.")
    return 0


def _resolve_venv_dir(module: str) -> Optional[Path]:
    """Return the venv path for a setup module, or None if not applicable."""
    module_dirs: Dict[str, Path] = {
        "ingestion": REPO_ROOT / "metadata-ingestion" / "venv",
        "smoke-test": REPO_ROOT / "smoke-test" / "venv",
        "airflow-plugin": REPO_ROOT
        / "metadata-ingestion-modules"
        / "airflow-plugin"
        / "venv",
        "gx-plugin": REPO_ROOT / "metadata-ingestion-modules" / "gx-plugin" / "venv",
        "dagster-plugin": REPO_ROOT
        / "metadata-ingestion-modules"
        / "dagster-plugin"
        / "venv",
        "prefect-plugin": REPO_ROOT
        / "metadata-ingestion-modules"
        / "prefect-plugin"
        / "venv",
        "actions": REPO_ROOT / "datahub-actions" / "venv",
    }
    return module_dirs.get(module)


# ---------------------------------------------------------------------------
# Command: frontend
# ---------------------------------------------------------------------------


def cmd_frontend(args: argparse.Namespace) -> int:
    """Start the frontend dev server (yarn start in datahub-web-react/)."""
    _log("Starting frontend dev server...")
    try:
        result = subprocess.run(
            ["yarn", "start"],
            cwd=REPO_ROOT / "datahub-web-react",
            text=True,
        )
        return result.returncode
    except KeyboardInterrupt:
        _log("\nFrontend dev server stopped.")
        return 0


# ---------------------------------------------------------------------------
# Command: docs
# ---------------------------------------------------------------------------


def cmd_docs(args: argparse.Namespace) -> int:
    """Start the docs dev server (Docusaurus)."""
    docs_dir = REPO_ROOT / "docs-website"
    if args.build:
        _log("Building and starting docs dev server (full regeneration)...")
        return _run(["./gradlew", ":docs-website:yarnStart"], cwd=REPO_ROOT).returncode
    _log("Starting docs dev server on http://localhost:3001 ...")
    _log("  (Use --build for full regeneration including docGen)")
    try:
        result = subprocess.run(["yarn", "start"], cwd=docs_dir, text=True)
        return result.returncode
    except KeyboardInterrupt:
        _log("\nDocs dev server stopped.")
        return 0


# ---------------------------------------------------------------------------
# Command: start
# ---------------------------------------------------------------------------


# Env vars written to DEV_ENV_FILE (and therefore propagated to GMS) when
# `start --ai` is used.  Ollama listens on the Docker service name "ollama"
# inside the compose network; the test runner on the host uses localhost:11434.
_AI_SEMANTIC_SEARCH_VARS = {
    "EMBEDDING_PROVIDER_TYPE": "local",
    # Enables the kNN index mapping in OpenSearch/Elasticsearch
    "ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED": "true",
    # Enables the SemanticSearchService bean and semanticSearchAcrossEntities GraphQL field
    "SEARCH_SERVICE_SEMANTIC_SEARCH_ENABLED": "true",
}

# Default endpoint / model for the managed Ollama container (Docker-internal hostname).
_MANAGED_OLLAMA_ENDPOINT = "http://ollama:11434/v1/embeddings"
_DEFAULT_EMBEDDING_MODEL = "nomic-embed-text"

# Keys cleared by --no-ai
_AI_ALL_KEYS = list(_AI_SEMANTIC_SEARCH_VARS.keys()) + [
    "LOCAL_EMBEDDING_ENDPOINT",
    "LOCAL_EMBEDDING_MODEL",
]


def _write_env_var(key: str, value: str) -> None:
    """Write a single KEY=VALUE into DEV_ENV_FILE, replacing any existing entry."""
    if not DEV_ENV_FILE.exists():
        DEV_ENV_FILE.touch()
    existing_lines = DEV_ENV_FILE.read_text().splitlines()
    found = False
    new_lines = []
    for line in existing_lines:
        if line.strip().startswith(f"{key}="):
            new_lines.append(f"{key}={value}")
            found = True
        else:
            new_lines.append(line)
    if not found:
        new_lines.append(f"{key}={value}")
    DEV_ENV_FILE.write_text("\n".join(new_lines) + "\n")


def _clear_env_var(key: str) -> None:
    """Remove a KEY entry from DEV_ENV_FILE (no-op if the key is not present)."""
    if not DEV_ENV_FILE.exists():
        return
    existing_lines = DEV_ENV_FILE.read_text().splitlines()
    new_lines = [
        line for line in existing_lines if not line.strip().startswith(f"{key}=")
    ]
    DEV_ENV_FILE.write_text("\n".join(new_lines) + "\n")


def _wait_for_ollama_model_ready(endpoint: str, model: str, timeout: int = 180) -> bool:
    """
    Poll the embeddings endpoint until the model responds successfully.

    Returns True when ready, False on timeout.  This detects both Ollama not
    yet running AND the model still being loaded (cold GGUF load from disk).
    """
    import json
    import time
    import urllib.error
    import urllib.request

    payload = json.dumps({"model": model, "input": "warmup"}).encode()
    deadline = time.monotonic() + timeout
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        try:
            req = urllib.request.Request(
                endpoint,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10):
                return True
        except Exception:
            elapsed = int(time.monotonic() - (deadline - timeout))
            if attempt == 1 or elapsed % 15 == 0:
                _log(
                    f"  Waiting for Ollama model '{model}' to load... ({elapsed}s elapsed)"
                )
            time.sleep(3)
    return False


def _cmd_start_remote(args: argparse.Namespace) -> int:
    """Start DataHub on a remote machine via the configured runner.

    Flow (all on the local machine):
      1. Allocate a remote slot in the LOCAL registry (tunnel ports).
      2. runner sync  — push code to the remote.
      3. runner exec  — run datahub-dev.sh start on the remote (blocks until ready).
      4. runner tunnel — forward remote default ports to local slot ports.
    """
    registry = _load_registry()
    instance = registry["instances"].get(WORKTREE_ID)

    if instance is None or instance.get("type") != "remote":
        dev_config = _load_dev_config()
        max_remote = int(dev_config.get("max_remote_instances", 10))
        # Count already-registered remote slots (we can't easily check live status)
        remote_count = sum(
            1
            for v in registry.get("instances", {}).values()
            if v.get("type") == "remote" and v.get("worktree_path") != str(REPO_ROOT)
        )
        if remote_count >= max_remote:
            _log(
                f"ERROR: {max_remote} remote instance slots are in use "
                "(limit: max_remote_instances in ~/.datahub/dev/config.json)."
            )
            _log("  Run: datahub-dev.sh instances list")
            return 1
        try:
            slot = _pick_slot(registry, remote=True)
        except RuntimeError as e:
            _log(f"ERROR: {e}")
            return 1
        instance = _register_remote_instance(RUNNER, slot)
        _rebuild_config_services()
        gms_local = instance["ports"]["DATAHUB_MAPPED_GMS_PORT"]
        fe_local = instance["ports"]["DATAHUB_MAPPED_FRONTEND_PORT"]
        _log(f"Registered remote instance '{COMPOSE_PROJECT}' (slot {slot})")
        _log(f"  Local GMS:      http://localhost:{gms_local}")
        _log(f"  Local Frontend: http://localhost:{fe_local}")

    # 1. Resume compute (no-op if already running; starts a stopped instance)
    _log("Ensuring remote instance is running...")
    _run([RUNNER, "resume"], capture=False, timeout=300)
    # resume is best-effort — if the instance was already running the runner
    # exits 0 immediately; if it was stopped it waits for SSH readiness.

    # 2. Sync code
    _log("Syncing code to remote...")
    sync_rc = _run([RUNNER, "sync"], capture=False, timeout=300).returncode
    if sync_rc != 0:
        _log("ERROR: runner sync failed.")
        return sync_rc

    # 2. Start on remote — DATAHUB_REMOTE_EXEC=1 suppresses runner detection there
    _log("Starting DataHub on remote machine (this may take a few minutes)...")
    start_argv = ["env", "DATAHUB_REMOTE_EXEC=1", "scripts/dev/datahub-dev.sh", "start"]
    if getattr(args, "ai", False):
        start_argv.append("--ai")
    if getattr(args, "no_ai", False):
        start_argv.append("--no-ai")
    ep = getattr(args, "embeddings_endpoint", None)
    if ep:
        start_argv += ["--embeddings-endpoint", ep]
    em = getattr(args, "embeddings_model", None)
    if em:
        start_argv += ["--embeddings-model", em]
    start_argv += ["--timeout", str(getattr(args, "timeout", DEFAULT_TIMEOUT))]

    start_rc = _run(
        [RUNNER, "exec", "--"] + start_argv, capture=False, timeout=1500
    ).returncode
    if start_rc != 0:
        _log("Remote start failed. Check output above.")
        return start_rc

    # 3. Tunnel — map local slot ports to remote default ports
    tunnel_pairs = _compute_tunnel_pairs(instance["ports"])
    _log(f"Setting up port forwarding: {', '.join(tunnel_pairs)}")
    tunnel_rc = _run(
        [RUNNER, "tunnel"] + tunnel_pairs, capture=False, timeout=60
    ).returncode
    if tunnel_rc != 0:
        _log(
            "WARNING: runner tunnel returned non-zero. "
            "You may need to set up port forwarding manually."
        )

    gms_port = instance["ports"]["DATAHUB_MAPPED_GMS_PORT"]
    fe_port = instance["ports"]["DATAHUB_MAPPED_FRONTEND_PORT"]
    _log(f"\nDataHub is ready on remote.  (instance: {COMPOSE_PROJECT})")
    _log(f"  UI:  http://localhost:{fe_port}")
    _log(f"  GMS: http://localhost:{gms_port}")
    _log("  Set CLI env: eval $(datahub-dev.sh shell-env)")
    return 0


def cmd_start(args: argparse.Namespace) -> int:
    """Start (or restart) DataHub via quickstartDebug, then wait for readiness."""
    # Proxy to remote machine when a runner is configured.
    if RUNNER:
        return _cmd_start_remote(args)

    # --- Per-worktree instance registration -----------------------------------
    registry = _load_registry()
    dev_config = _load_dev_config()
    instance = registry["instances"].get(WORKTREE_ID)

    if instance is None:
        # This worktree has never started before — allocate a local slot and ports.
        max_local = int(dev_config.get("max_local_instances", 2))
        running_count = _count_running_dh_instances()
        if running_count >= max_local:
            _log(
                f"ERROR: {max_local} local DataHub instance(s) already running "
                "(limit: max_local_instances in ~/.datahub/dev/config.json)."
            )
            _log("  Stop an existing instance first, or increase max_local_instances.")
            _log("  Run: datahub-dev.sh instances list")
            return 1
        try:
            slot = _pick_slot(registry, remote=False)
        except RuntimeError as e:
            _log(f"ERROR: {e}")
            return 1
        ports = _compute_ports(slot)
        instance = _register_instance(slot, ports)
        _write_ports_to_env_file(ports)
        # Update CONFIG.services URLs now that we know the allocated ports.
        _rebuild_config_services()
        _log(f"Registered worktree '{REPO_ROOT.name}' as instance '{COMPOSE_PROJECT}'")
        _log(f"  GMS:      http://localhost:{ports['DATAHUB_MAPPED_GMS_PORT']}")
        _log(f"  Frontend: http://localhost:{ports['DATAHUB_MAPPED_FRONTEND_PORT']}")
    # --------------------------------------------------------------------------

    # Conflict detection: stop any non-DataHub process occupying our ports.
    our_ports: Set[int] = set(instance["ports"].values())
    conflicts = _find_conflicting_projects(our_ports)
    if conflicts:
        _stop_conflicting_projects(conflicts)

    task = CONFIG.gradle_quickstart_task
    ai_mode = getattr(args, "ai", False)
    no_ai_mode = getattr(args, "no_ai", False)
    embeddings_endpoint: str | None = getattr(args, "embeddings_endpoint", None)
    embeddings_model: str = (
        getattr(args, "embeddings_model", None) or _DEFAULT_EMBEDDING_MODEL
    )

    if no_ai_mode:
        _log("Clearing AI embedding env vars from dev env file...")
        for k in _AI_ALL_KEYS:
            _clear_env_var(k)
            _log(f"  cleared {k}")
        _log("AI mode disabled. Run 'env restart' to apply.")

    elif ai_mode:
        byo_mode = embeddings_endpoint is not None

        if byo_mode:
            # BYO server: use the developer's own endpoint, no managed Ollama container.
            _log(f"AI mode (BYO embeddings server): {embeddings_endpoint}")
            for k, v in _AI_SEMANTIC_SEARCH_VARS.items():
                _write_env_var(k, v)
            _write_env_var("LOCAL_EMBEDDING_ENDPOINT", embeddings_endpoint)
            _write_env_var("LOCAL_EMBEDDING_MODEL", embeddings_model)
            _log(f"  LOCAL_EMBEDDING_ENDPOINT={embeddings_endpoint}")
            _log(f"  LOCAL_EMBEDDING_MODEL={embeddings_model}")
        else:
            # Managed Ollama: start the built-in container on the debug-ai profile.
            task = "quickstartDebugAi"
            _log("AI mode (managed Ollama): writing embedding env vars...")
            for k, v in _AI_SEMANTIC_SEARCH_VARS.items():
                _write_env_var(k, v)
            _write_env_var("LOCAL_EMBEDDING_ENDPOINT", _MANAGED_OLLAMA_ENDPOINT)
            _write_env_var("LOCAL_EMBEDDING_MODEL", embeddings_model)
            _log(f"  LOCAL_EMBEDDING_ENDPOINT={_MANAGED_OLLAMA_ENDPOINT}")
            _log(f"  LOCAL_EMBEDDING_MODEL={embeddings_model}")
            _log(
                "  Ollama will start alongside GMS (profile: debug-ai). "
                "Model pull + warmup may take a few minutes on first run."
            )
            _log(
                "  To reach Ollama from the host (smoke tests), use: "
                "LOCAL_EMBEDDING_ENDPOINT=http://localhost:11434/v1/embeddings"
            )

    _log(f"Starting DataHub via {task}...")
    result = _run(
        ["./gradlew", task],
        capture=False,
        timeout=1200,
    )
    if result.returncode != 0:
        _log(f"{task} failed. Check the output above for errors.")
        return 1

    _log("Waiting for services to become ready...")
    wait_args = argparse.Namespace(timeout=args.timeout)
    rc = cmd_wait(wait_args)
    if rc != 0:
        return rc

    # Print per-instance connection info so it's easy to find after a long startup.
    _log(f"\nDataHub is ready.  (instance: {COMPOSE_PROJECT})")
    _log(f"  UI:  {_frontend_url()}")
    _log(f"  GMS: {_gms_url()}")
    _log("  Set CLI env: eval $(datahub-dev.sh shell-env)")

    # In managed Ollama mode, wait for the model to be fully loaded before
    # returning — eliminates cold-start delay on the first semantic search query.
    if ai_mode and not no_ai_mode and embeddings_endpoint is None:
        host_endpoint = "http://localhost:11434/v1/embeddings"
        _log(f"Waiting for Ollama model '{embeddings_model}' to be ready...")
        if _wait_for_ollama_model_ready(host_endpoint, embeddings_model, timeout=300):
            _log(
                f"Ollama model '{embeddings_model}' is ready — semantic search enabled."
            )
        else:
            _log(
                f"WARNING: Timed out waiting for Ollama model '{embeddings_model}'. "
                "Semantic search may be slow on the first query while the model loads."
            )

    return 0


# ---------------------------------------------------------------------------
# Command: sync-flags
# ---------------------------------------------------------------------------


def cmd_sync_flags(args: argparse.Namespace) -> int:
    """Regenerate scripts/generated/flag-classification.json from FeatureFlags.java."""
    _log("Regenerating flag-classification.json...")
    result = _run(
        [
            "./gradlew",
            CONFIG.gradle_sync_flags_task,
        ],
        capture=False,
        timeout=300,
    )
    if result.returncode != 0:
        _log("Failed to generate flag-classification.json.")
        return 1
    if GENERATED_MANIFEST.exists():
        _log(f"Generated: {GENERATED_MANIFEST}")
    return 0


# ---------------------------------------------------------------------------
# Command: suspend
# ---------------------------------------------------------------------------


def cmd_suspend(args: argparse.Namespace) -> int:
    """Stop DataHub containers and suspend the remote compute instance.

    Calls runner suspend, which stops Docker and then halts the EC2 instance
    (or scales a K8s deployment to zero). No compute charges while suspended.
    Data is preserved on the persistent volume.

    Resume with: datahub-dev.sh start
    """
    if not RUNNER:
        _log(
            "ERROR: No runner configured — suspend only applies to remote instances. "
            "Use 'stop' to halt local DataHub containers."
        )
        return 1
    _log("Suspending remote instance (stopping DataHub + halting compute)...")
    rc = _run([RUNNER, "suspend"], capture=False, timeout=180).returncode
    if rc == 0:
        _log("Suspended. No compute charges until next 'datahub-dev.sh start'.")
    return rc


# ---------------------------------------------------------------------------
# Command: instances
# ---------------------------------------------------------------------------


def _cmd_instances_list(args: argparse.Namespace) -> int:
    """Print all registered worktree instances (local and remote) as JSON."""
    registry = _load_registry()
    instances = registry.get("instances", {})
    if not instances:
        _log("No registered instances.")
        print(json.dumps([], indent=2))
        return 0

    rows = []
    for wt_id, inst in instances.items():
        project = inst.get("project_name", f"dh-{wt_id}")
        kind = inst.get("type", "local")

        if kind == "remote":
            # Ask the remote whether it's healthy rather than querying local docker.
            runner_path = inst.get("runner", "")
            running: Optional[bool] = None
            if runner_path:
                rc = _run(
                    [runner_path, "exec", "--", "scripts/dev/datahub-dev.sh", "status"],
                    timeout=15,
                ).returncode
                running = rc == 0
        else:
            # Local: query docker compose directly.
            ps = _run(
                ["docker", "compose", "-p", project, "ps", "--format", "json", "-a"]
            )
            running = False
            if ps.returncode == 0 and ps.stdout.strip():
                for line in ps.stdout.strip().splitlines():
                    try:
                        obj = json.loads(line)
                        containers = obj if isinstance(obj, list) else [obj]
                        if any(c.get("State") == "running" for c in containers):
                            running = True
                            break
                    except json.JSONDecodeError:
                        continue

        gms_port = inst.get("ports", {}).get("DATAHUB_MAPPED_GMS_PORT")
        fe_port = inst.get("ports", {}).get("DATAHUB_MAPPED_FRONTEND_PORT")
        rows.append(
            {
                "id": wt_id,
                "current": wt_id == WORKTREE_ID,
                "type": kind,
                "project": project,
                "worktree_path": inst.get("worktree_path", "?"),
                "slot": inst.get("slot"),
                # For remote: these are local tunnel ports.
                # For local: these are docker host ports.
                "gms_port": gms_port,
                "frontend_port": fe_port,
                "runner": inst.get("runner"),
                "running": running,
                "created_at": inst.get("created_at"),
            }
        )

    print(json.dumps(rows, indent=2))
    return 0


def _cmd_instances_clean(args: argparse.Namespace) -> int:
    """Remove registry entries for worktrees that no longer exist on disk."""
    registry = _load_registry()
    instances = registry.get("instances", {})
    to_remove = [
        wt_id
        for wt_id, inst in instances.items()
        if inst.get("worktree_path") and not Path(inst["worktree_path"]).exists()
    ]
    if not to_remove:
        _log("No stale instances found.")
        return 0
    for wt_id in to_remove:
        inst = instances.pop(wt_id)
        _log(f"Removed stale instance {wt_id} ({inst.get('worktree_path', '?')})")
    registry["instances"] = instances
    _save_registry(registry)
    _log(f"Cleaned {len(to_remove)} stale instance(s).")
    return 0


# ---------------------------------------------------------------------------
# Command: shell-env
# ---------------------------------------------------------------------------


def cmd_shell_env(args: argparse.Namespace) -> int:
    """Print export statements for the current worktree's DataHub CLI environment.

    Usage: eval $(datahub-dev.sh shell-env)
    """
    gms = _gms_url()
    print(f"export DATAHUB_GMS_URL={gms}")

    # Propagate token if present in the dev env file.
    if DEV_ENV_FILE.exists():
        for line in DEV_ENV_FILE.read_text().splitlines():
            stripped = line.strip()
            if stripped.startswith("DATAHUB_GMS_TOKEN="):
                token = stripped.split("=", 1)[1]
                if token:
                    print(f"export DATAHUB_GMS_TOKEN={token}")
                break

    return 0


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="datahub-dev",
        description="Agent-friendly development tooling for DataHub",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # status
    subparsers.add_parser("status", help="Check environment status (JSON output)")

    # setup
    setup_p = subparsers.add_parser(
        "setup",
        help="Install dev dependencies for a module, or init a remote environment",
    )
    setup_p.add_argument(
        "module",
        nargs="?",
        default=DEFAULT_SETUP_MODULE,
        choices=list(SETUP_MODULES.keys()),
        help=f"Module to set up (default: {DEFAULT_SETUP_MODULE})",
    )
    setup_p.add_argument(
        "--remote",
        action="store_true",
        help=(
            "Initialise the remote environment via the configured runner "
            "(calls runner init). Use once before the first 'start'."
        ),
    )

    # frontend
    subparsers.add_parser("frontend", help="Start the frontend dev server (yarn start)")

    # docs
    docs_p = subparsers.add_parser(
        "docs", help="Start documentation dev server (Docusaurus)"
    )
    docs_p.add_argument(
        "--build",
        action="store_true",
        help="Full rebuild including docGen before starting",
    )

    # start
    start_p = subparsers.add_parser(
        "start", help="Start DataHub (quickstartDebug) and wait for readiness"
    )
    start_p.add_argument(
        "--ai",
        action="store_true",
        help=(
            "Enable AI / semantic search features. By default starts a managed Ollama "
            "container (quickstartDebugAi profile). Use --embeddings-endpoint to bring "
            "your own server instead."
        ),
    )
    start_p.add_argument(
        "--no-ai",
        action="store_true",
        dest="no_ai",
        help=(
            "Clear all AI embedding env vars set by a previous --ai run. "
            "Run 'env restart' afterwards to apply."
        ),
    )
    start_p.add_argument(
        "--embeddings-endpoint",
        dest="embeddings_endpoint",
        metavar="URL",
        help=(
            "URL of an existing OpenAI-compatible embeddings server "
            "(e.g. http://localhost:11434/v1/embeddings). "
            "When set with --ai, skips the managed Ollama container and uses this server instead. "
            "Works with Ollama, LM Studio, llama.cpp, or any /v1/embeddings endpoint."
        ),
    )
    start_p.add_argument(
        "--embeddings-model",
        dest="embeddings_model",
        metavar="MODEL",
        default=_DEFAULT_EMBEDDING_MODEL,
        help=(
            f"Embedding model to use (default: {_DEFAULT_EMBEDDING_MODEL}). "
            "In managed mode, this model is pulled and warmed up in the Ollama container. "
            "In BYO mode, the model must already be available on the specified server."
        ),
    )
    start_p.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Timeout for readiness wait (default: {DEFAULT_TIMEOUT})",
    )

    # wait
    wait_p = subparsers.add_parser("wait", help="Block until DataHub is ready")
    wait_p.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Timeout in seconds (default: {DEFAULT_TIMEOUT})",
    )

    # rebuild
    rebuild_p = subparsers.add_parser("rebuild", help="Smart incremental rebuild")
    rebuild_p.add_argument(
        "--wait", action="store_true", help="Wait for readiness after rebuild"
    )
    rebuild_p.add_argument(
        "--module",
        type=str,
        choices=list(CONFIG.rebuild_module_aliases.keys()),
        help="Explicitly specify module to rebuild",
    )
    rebuild_p.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="Timeout for wait (default: 300s)",
    )

    # test
    test_p = subparsers.add_parser("test", help="Run targeted smoke tests")
    test_p.add_argument("path", help="Test file or directory path")
    test_p.add_argument("pytest_args", nargs="*", help="Additional pytest arguments")

    # flag
    flag_p = subparsers.add_parser("flag", help="Feature flag inspection (read-only)")
    flag_sub = flag_p.add_subparsers(dest="flag_command")
    flag_sub.add_parser("list", help="List all feature flags and their current values")
    flag_get_p = flag_sub.add_parser("get", help="Get a flag value")
    flag_get_p.add_argument("name", help="Flag name (camelCase)")

    # env
    env_p = subparsers.add_parser("env", help="Environment variable management")
    env_sub = env_p.add_subparsers(dest="env_command")
    env_sub.add_parser("list", help="List env vars currently set for containers")
    env_set_p = env_sub.add_parser("set", help="Set an env var for containers")
    env_set_p.add_argument("assignment", help="KEY=VALUE")
    env_sub.add_parser("restart", help="Restart containers with new env vars")
    env_clean_p = env_sub.add_parser(
        "clean", help="Remove env files for deleted branches"
    )
    env_clean_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be removed without deleting",
    )

    # sync-flags
    subparsers.add_parser(
        "sync-flags",
        help="Regenerate scripts/generated/flag-classification.json from source",
    )

    # stop
    subparsers.add_parser("stop", help="Stop all DataHub services")

    # reset
    subparsers.add_parser("reset", help="Soft reset (restart without data loss)")

    # nuke
    nuke_p = subparsers.add_parser(
        "nuke", help="Hard reset (remove containers, optionally data)"
    )
    nuke_p.add_argument("--keep-data", action="store_true", help="Keep data volumes")

    # suspend
    subparsers.add_parser(
        "suspend",
        help=(
            "Stop DataHub containers and suspend the remote compute instance "
            "(no compute charges until next 'start'). Requires a runner."
        ),
    )

    # instances
    instances_p = subparsers.add_parser(
        "instances", help="Manage worktree DataHub instances"
    )
    instances_sub = instances_p.add_subparsers(dest="instances_command")
    instances_sub.add_parser(
        "list",
        help="List all registered instances with status, ports, and paths",
    )
    instances_sub.add_parser(
        "clean",
        help="Remove registry entries for worktrees that no longer exist on disk",
    )

    # shell-env
    subparsers.add_parser(
        "shell-env",
        help=(
            "Print shell export statements for this instance's CLI environment. "
            "Usage: eval $(datahub-dev.sh shell-env)"
        ),
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    command_map = {
        "status": cmd_status,
        "setup": cmd_setup,
        "frontend": cmd_frontend,
        "docs": cmd_docs,
        "start": cmd_start,
        "stop": cmd_stop,
        "suspend": cmd_suspend,
        "wait": cmd_wait,
        "rebuild": cmd_rebuild,
        "test": cmd_test,
        "sync-flags": cmd_sync_flags,
        "reset": cmd_reset,
        "nuke": cmd_nuke,
        "shell-env": cmd_shell_env,
    }

    if args.command == "flag":
        flag_map = {
            "list": cmd_flag_list,
            "get": cmd_flag_get,
        }
        if not args.flag_command:
            parser.parse_args(["flag", "--help"])
            return 1
        return flag_map[args.flag_command](args)

    if args.command == "env":
        env_map = {
            "list": cmd_env_list,
            "set": cmd_env_set,
            "restart": cmd_env_restart,
            "clean": cmd_env_clean,
        }
        if not args.env_command:
            parser.parse_args(["env", "--help"])
            return 1
        return env_map[args.env_command](args)

    if args.command == "instances":
        instances_map = {
            "list": _cmd_instances_list,
            "clean": _cmd_instances_clean,
        }
        if not args.instances_command:
            parser.parse_args(["instances", "--help"])
            return 1
        return instances_map[args.instances_command](args)

    handler = command_map.get(args.command)
    if handler:
        return handler(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
