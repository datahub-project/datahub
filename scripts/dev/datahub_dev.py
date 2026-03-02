#!/usr/bin/env python3
"""
datahub-dev: Agent-friendly development tooling for DataHub.

A stdlib-only Python CLI (no third-party deps, no venv needed) that provides
machine-readable environment status, smart rebuilds, targeted test execution,
feature flag management, and recovery tools.

Usage:
    python3 scripts/dev/datahub_dev.py status
    python3 scripts/dev/datahub_dev.py wait [--timeout 300]
    python3 scripts/dev/datahub_dev.py rebuild [--wait] [--module gms]
    python3 scripts/dev/datahub_dev.py test <path> [pytest-args...]
    python3 scripts/dev/datahub_dev.py flag list
    python3 scripts/dev/datahub_dev.py flag get <name>
    python3 scripts/dev/datahub_dev.py env set KEY=VALUE
    python3 scripts/dev/datahub_dev.py env restart
    python3 scripts/dev/datahub_dev.py sync-flags
    python3 scripts/dev/datahub_dev.py reset
    python3 scripts/dev/datahub_dev.py nuke [--keep-data]
"""

import argparse
import dataclasses
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


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

GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
FRONTEND_URL = os.environ.get("DATAHUB_FRONTEND_URL", "http://localhost:9002")
COMPOSE_PROJECT = os.environ.get("DOCKER_COMPOSE_PROJECT_NAME", "datahub")

DEFAULT_TIMEOUT = 300  # seconds
POLL_INTERVAL = 3  # seconds


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
                f"{GMS_URL}/health",
                status_url=f"{GMS_URL}/health/detailed",
                required=True,
            ),
            ServiceConfig("frontend", f"{FRONTEND_URL}/admin", required=True),
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
    """Return a copy of os.environ with DATAHUB_LOCAL_COMMON_ENV injected.

    This ensures every subprocess we spawn (Gradle, docker compose, pytest)
    sees the managed env file, so docker compose profiles pick it up via
    ``env_file: ${DATAHUB_LOCAL_COMMON_ENV:-empty.env}``.
    """
    env = os.environ.copy()
    if not DEV_ENV_FILE.exists():
        DEV_ENV_FILE.touch()
    env["DATAHUB_LOCAL_COMMON_ENV"] = str(DEV_ENV_FILE)
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
        "-x",
        "generateGitPropertiesGlobal",
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
    gms_status, _ = _http_get(f"{GMS_URL}/health", timeout=3)
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
                "-x",
                "generateGitPropertiesGlobal",
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
    if gms_base_path == "/" or not gms_base_path:
        env.setdefault(
            "DATAHUB_KAFKA_SCHEMA_REGISTRY_URL",
            "http://localhost:8080/schema-registry/api",
        )
        env.setdefault("DATAHUB_GMS_URL", "http://localhost:8080")
    else:
        env.setdefault(
            "DATAHUB_KAFKA_SCHEMA_REGISTRY_URL",
            f"http://localhost:8080{gms_base_path}/schema-registry/api",
        )
        env.setdefault("DATAHUB_GMS_URL", f"http://localhost:8080{gms_base_path}")

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
            " -x generateGitPropertiesGlobal"
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
                " -x generateGitPropertiesGlobal"
            )
            return {"dynamic": {}, "static": {}}


def cmd_flag_list(args: argparse.Namespace) -> int:
    """List all feature flags and their current live values."""
    status_code, body = _http_get(f"{GMS_URL}/openapi/operations/dev/featureFlags")
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
    status_code, body = _http_get(f"{GMS_URL}/openapi/operations/dev/featureFlags")
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
            "-x",
            "generateGitPropertiesGlobal",
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
# Command: start
# ---------------------------------------------------------------------------


def cmd_start(args: argparse.Namespace) -> int:
    """Start (or restart) DataHub via quickstartDebug, then wait for readiness."""
    _log(f"Starting DataHub via {CONFIG.gradle_quickstart_task}...")
    result = _run(
        [
            "./gradlew",
            CONFIG.gradle_quickstart_task,
            "-x",
            "generateGitPropertiesGlobal",
        ],
        capture=False,
        timeout=1200,
    )
    if result.returncode != 0:
        _log(
            f"{CONFIG.gradle_quickstart_task} failed. Check the output above for errors."
        )
        return 1

    _log("Waiting for services to become ready...")
    wait_args = argparse.Namespace(timeout=args.timeout)
    return cmd_wait(wait_args)


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
            "-x",
            "generateGitPropertiesGlobal",
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

    # start
    start_p = subparsers.add_parser(
        "start", help="Start DataHub (quickstartDebug) and wait for readiness"
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

    # reset
    subparsers.add_parser("reset", help="Soft reset (restart without data loss)")

    # nuke
    nuke_p = subparsers.add_parser(
        "nuke", help="Hard reset (remove containers, optionally data)"
    )
    nuke_p.add_argument("--keep-data", action="store_true", help="Keep data volumes")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    command_map = {
        "status": cmd_status,
        "start": cmd_start,
        "wait": cmd_wait,
        "rebuild": cmd_rebuild,
        "test": cmd_test,
        "sync-flags": cmd_sync_flags,
        "reset": cmd_reset,
        "nuke": cmd_nuke,
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

    handler = command_map.get(args.command)
    if handler:
        return handler(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
