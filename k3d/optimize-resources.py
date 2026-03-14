#!/usr/bin/env python3
"""Resource optimizer for DataHub on k3d.

Binary-searches for the minimum memory and CPU per DataHub component
that still passes the smoke test suite. Supports deployment profiles
(minimal, consumers, backend) and per-trial checkpointing for resume.

Usage:
    uv run --directory k3d python optimize-resources.py
    uv run --directory k3d python optimize-resources.py --dry-run
    uv run --directory k3d python optimize-resources.py --profile consumers
    uv run --directory k3d python optimize-resources.py --components gms
    uv run --directory k3d python optimize-resources.py --no-resume
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import click

# Ensure dh package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from dh.config import K3dConfig
from dh.naming import (
    derive_worktree_name,
    gms_hostname_for,
    hostname_for,
    namespace_for,
)
from dh.utils import err, info, ok, warn

# ── Search space ─────────────────────────────────────────────────────────────

ALL_COMPONENTS: dict[str, dict] = {
    "gms": {
        "helm_prefix": "datahub-gms",
        "has_jvm": True,
        "jvm_heap_ratio": 0.75,  # heap = 75% of memory_limit (leave room for off-heap)
        "dimensions": {
            "memory_limit": {"current": 960, "floor": 512, "step": 64, "unit": "Mi"},
            # JVM: memory_request is always set equal to memory_limit (not searched)
            "cpu_request": {"current": 50, "floor": 25, "step": 25, "unit": "m"},
        },
    },
    "frontend": {
        "helm_prefix": "datahub-frontend",
        "has_jvm": True,
        "jvm_heap_ratio": 0.75,
        "dimensions": {
            "memory_limit": {"current": 384, "floor": 256, "step": 64, "unit": "Mi"},
            # JVM: memory_request is always set equal to memory_limit (not searched)
            "cpu_request": {"current": 75, "floor": 25, "step": 25, "unit": "m"},
        },
    },
    "actions": {
        "helm_prefix": "acryl-datahub-actions",
        "has_jvm": False,
        "dimensions": {
            "memory_limit": {"current": 192, "floor": 128, "step": 64, "unit": "Mi"},
            "memory_request": {"current": 128, "floor": 64, "step": 64, "unit": "Mi"},
            "cpu_request": {"current": 25, "floor": 25, "step": 25, "unit": "m"},
        },
    },
    "mae-consumer": {
        "helm_prefix": "datahub-mae-consumer",
        "has_jvm": True,
        "jvm_heap_ratio": 0.75,
        "dimensions": {
            "memory_limit": {"current": 512, "floor": 256, "step": 64, "unit": "Mi"},
            "cpu_request": {"current": 50, "floor": 25, "step": 25, "unit": "m"},
        },
    },
    "mce-consumer": {
        "helm_prefix": "datahub-mce-consumer",
        "has_jvm": True,
        "jvm_heap_ratio": 0.75,
        "dimensions": {
            "memory_limit": {"current": 512, "floor": 256, "step": 64, "unit": "Mi"},
            "cpu_request": {"current": 50, "floor": 25, "step": 25, "unit": "m"},
        },
    },
}

DIMENSION_ORDER = ["memory_limit", "memory_request", "cpu_request"]

# Profile → component adjustments (add/remove from default set)
_DEFAULT_COMPONENTS = {"gms", "frontend", "actions"}
_PROFILE_EFFECTS: dict[str, dict[str, set[str]]] = {
    "consumers": {"add": {"mae-consumer", "mce-consumer"}},
    "backend": {"remove": {"frontend"}},
    "minimal": {"remove": {"actions"}},
}


def active_components(profiles: tuple[str, ...] = ()) -> list[str]:
    """Return the list of components active for the given profiles."""
    active = set(_DEFAULT_COMPONENTS)
    for p in profiles:
        effects = _PROFILE_EFFECTS.get(p, {})
        active |= effects.get("add", set())
        active -= effects.get("remove", set())
    return [c for c in ALL_COMPONENTS if c in active]


# ── State dataclasses ────────────────────────────────────────────────────────


@dataclass
class Trial:
    id: int
    component: str
    dimension: str
    value: int
    unit: str
    all_overrides: dict[str, str]
    test_passed: Optional[bool]  # None = in progress
    duration_s: float
    timestamp: str
    pod_events: list[str]
    retries_used: int = 0


@dataclass
class SearchState:
    low: int
    high: int
    step: int


@dataclass
class OptimizerState:
    started_at: str
    components_order: list[str]
    current_component: Optional[str]
    current_dimension: Optional[str]
    search_state: Optional[SearchState]
    trials: list[Trial]
    results: dict[str, dict[str, int]]


# ── Checkpoint I/O ───────────────────────────────────────────────────────────


def load_state(path: Path) -> Optional[OptimizerState]:
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return OptimizerState(
        started_at=data["started_at"],
        components_order=data["components_order"],
        current_component=data.get("current_component"),
        current_dimension=data.get("current_dimension"),
        search_state=(
            SearchState(**data["search_state"]) if data.get("search_state") else None
        ),
        trials=[Trial(**t) for t in data.get("trials", [])],
        results=data.get("results", {}),
    )


def save_state(state: OptimizerState, path: Path) -> None:
    data = {
        "started_at": state.started_at,
        "components_order": state.components_order,
        "current_component": state.current_component,
        "current_dimension": state.current_dimension,
        "search_state": asdict(state.search_state) if state.search_state else None,
        "trials": [asdict(t) for t in state.trials],
        "results": state.results,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n")
    tmp.rename(path)


def new_state(components: list[str]) -> OptimizerState:
    return OptimizerState(
        started_at=datetime.now(timezone.utc).isoformat(),
        components_order=components,
        current_component=None,
        current_dimension=None,
        search_state=None,
        trials=[],
        results={},
    )


# ── Helm helpers ─────────────────────────────────────────────────────────────


def get_release_info(namespace: str) -> tuple[str, str, str]:
    """Get (release_name, chart_name, chart_version) from helm list."""
    result = subprocess.run(
        ["helm", "list", "-n", namespace, "-a", "-o", "json"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"helm list failed: {result.stderr}")
    releases = json.loads(result.stdout)
    if not releases:
        raise RuntimeError(f"No helm releases in namespace '{namespace}'")

    # Prefer deployed releases; fall back to any release
    deployed = [r for r in releases if r.get("status") == "deployed"]
    if not deployed:
        # Auto-rollback pending-upgrade to recover from failed optimizer trials
        rel = releases[0]
        warn(f"Release '{rel['name']}' is in '{rel.get('status')}' state, rolling back...")
        subprocess.run(
            ["helm", "rollback", rel["name"], "0", "-n", namespace],
            capture_output=True,
            text=True,
        )
        # Re-fetch
        result = subprocess.run(
            ["helm", "list", "-n", namespace, "-o", "json"],
            capture_output=True,
            text=True,
        )
        releases = json.loads(result.stdout)
        if not releases:
            raise RuntimeError(f"Rollback failed for release in namespace '{namespace}'")
    rel = releases[0]
    # chart field is like "datahub-0.5.0"
    chart_field = rel.get("chart", "")
    # Parse: last dash-separated segment that looks like a version
    parts = chart_field.rsplit("-", 1)
    chart_name = parts[0] if len(parts) == 2 else chart_field
    chart_version = parts[1] if len(parts) == 2 else ""
    return rel["name"], chart_name, chart_version


def get_helm_values(release: str, namespace: str) -> dict:
    """Get current user-supplied helm values for the release."""
    result = subprocess.run(
        ["helm", "get", "values", release, "-n", namespace, "-o", "json"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"helm get values failed: {result.stderr}")
    return json.loads(result.stdout)


def find_java_opts_index(values: dict, helm_prefix: str) -> Optional[int]:
    """Find the index of JAVA_OPTS in the extraEnvs array."""
    extra_envs = values.get(helm_prefix, {}).get("extraEnvs", [])
    for i, env in enumerate(extra_envs):
        if env.get("name") == "JAVA_OPTS":
            return i
    return None


def get_current_java_opts(values: dict, helm_prefix: str) -> Optional[str]:
    """Get the current JAVA_OPTS value."""
    extra_envs = values.get(helm_prefix, {}).get("extraEnvs", [])
    for env in extra_envs:
        if env.get("name") == "JAVA_OPTS":
            return env.get("value", "")
    return None


def derive_jvm_heap(memory_limit: int, heap_ratio: float) -> int:
    """Derive JVM heap as a percentage of memory_limit, rounded down to 64m."""
    heap = int(memory_limit * heap_ratio)
    heap = (heap // 64) * 64
    return max(heap, 64)


def update_java_opts_heap(java_opts: str, new_heap: int) -> str:
    """Replace -Xms/-Xmx values in a JAVA_OPTS string."""
    opts = re.sub(r"-Xms\d+m", f"-Xms{new_heap}m", java_opts)
    opts = re.sub(r"-Xmx\d+m", f"-Xmx{new_heap}m", opts)
    return opts


def get_effective_value(
    state: OptimizerState,
    comp_name: str,
    dim_name: str,
    trial_component: str,
    trial_dimension: str,
    trial_value: int,
) -> int:
    """Get the effective value for a component/dimension, considering the trial."""
    if comp_name == trial_component and dim_name == trial_dimension:
        return trial_value
    comp_results = state.results.get(comp_name, {})
    return comp_results.get(
        dim_name, ALL_COMPONENTS[comp_name]["dimensions"][dim_name]["current"]
    )


def get_deployment_name(namespace: str, comp_name: str) -> Optional[str]:
    """Find the Kubernetes deployment name for a component."""
    prefix = ALL_COMPONENTS[comp_name]["helm_prefix"]
    result = subprocess.run(
        ["kubectl", "get", "deployments", "-n", namespace, "-o", "name"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    for line in result.stdout.strip().splitlines():
        # line is like "deployment.apps/datahub-k3d-quickstart-datahub-gms"
        name = line.split("/")[-1]
        if prefix in name:
            return name
    return None


@dataclass
class TrialPatch:
    """Patch instructions for a single deployment."""
    deploy_name: str
    resources_patch: dict  # strategic merge patch for resources only
    java_opts: Optional[str] = None  # new JAVA_OPTS value (if JVM component)


def build_trial_patches(
    state: OptimizerState,
    trial_component: str,
    trial_dimension: str,
    trial_value: int,
    namespace: str,
    active_comps: list[str] | None = None,
) -> tuple[list[TrialPatch], dict[str, str]]:
    """Build kubectl patches for a trial.

    Returns (patches, summary_dict). Resources are changed via strategic merge
    patch, and JAVA_OPTS via `kubectl set env` to avoid conflicts with
    valueFrom entries in the env array.
    """
    patches: list[TrialPatch] = []
    overrides: dict[str, str] = {}
    comps = active_comps or list(ALL_COMPONENTS.keys())

    for comp_name in comps:
        comp_cfg = ALL_COMPONENTS[comp_name]
        mem_limit = get_effective_value(
            state, comp_name, "memory_limit", trial_component, trial_dimension, trial_value
        )
        # JVM apps: request = limit (JVM pre-allocates heap, so usage is constant)
        if comp_cfg.get("has_jvm"):
            mem_req = mem_limit
        else:
            mem_req = get_effective_value(
                state, comp_name, "memory_request", trial_component, trial_dimension, trial_value
            )
        cpu_req = get_effective_value(
            state, comp_name, "cpu_request", trial_component, trial_dimension, trial_value
        )

        overrides[f"{comp_name}.memory_limit"] = f"{mem_limit}Mi"
        overrides[f"{comp_name}.memory_request"] = f"{mem_req}Mi"
        overrides[f"{comp_name}.cpu_request"] = f"{cpu_req}m"

        deploy_name = get_deployment_name(namespace, comp_name)
        if not deploy_name:
            warn(f"Deployment not found for {comp_name}")
            continue

        # Resource-only strategic merge patch
        resources_patch = {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{
                            "name": comp_cfg["helm_prefix"],
                            "resources": {
                                "limits": {"memory": f"{mem_limit}Mi"},
                                "requests": {"memory": f"{mem_req}Mi", "cpu": f"{cpu_req}m"},
                            },
                        }],
                    }
                }
            }
        }

        # Derive new JAVA_OPTS for JVM components
        java_opts = None
        if comp_cfg.get("has_jvm"):
            heap_ratio = comp_cfg["jvm_heap_ratio"]
            heap = derive_jvm_heap(mem_limit, heap_ratio)
            overrides[f"{comp_name}.jvm_heap"] = f"{heap}m"

            # Read current JAVA_OPTS and update heap values
            result = subprocess.run(
                [
                    "kubectl", "get", "deployment", deploy_name,
                    "-n", namespace, "-o",
                    "jsonpath={.spec.template.spec.containers[0].env}",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0 and result.stdout:
                current_envs = json.loads(result.stdout)
                for env in current_envs:
                    if env.get("name") == "JAVA_OPTS" and env.get("value"):
                        java_opts = update_java_opts_heap(env["value"], heap)
                        break

        patches.append(TrialPatch(
            deploy_name=deploy_name,
            resources_patch=resources_patch,
            java_opts=java_opts,
        ))

    return patches, overrides


class DeployResult:
    """Result of a trial deployment."""

    def __init__(self, success: bool, pod_events: list[str], is_resource_issue: bool):
        self.success = success
        self.pod_events = pod_events
        self.is_resource_issue = is_resource_issue


def deploy_trial(
    namespace: str,
    patches: list[TrialPatch],
    timeout: int = 600,
    active_comps: list[str] | None = None,
) -> DeployResult:
    """Apply kubectl patches to deployments and wait for rollout.

    Uses kubectl patch for resources and kubectl set env for JAVA_OPTS,
    avoiding helm upgrade (which bumps revision and triggers system update
    version checks in GMS).

    Returns a DeployResult that distinguishes resource failures (OOM,
    CrashLoopBackOff) from non-resource failures.
    """
    from dh.utils import wait_for_pods

    info("Deploying trial: kubectl patch deployments ...")

    # Apply patches to all deployments
    for tp in patches:
        # Apply resource patch (strategic merge)
        patch_json = json.dumps(tp.resources_patch)
        result = subprocess.run(
            [
                "kubectl", "patch", "deployment", tp.deploy_name,
                "-n", namespace,
                "--type", "strategic",
                "-p", patch_json,
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            events = check_pod_events(namespace)
            err(f"kubectl patch failed for {tp.deploy_name}: {result.stderr.strip()}")
            return DeployResult(success=False, pod_events=events, is_resource_issue=False)

        # Apply JAVA_OPTS change via kubectl set env (avoids valueFrom conflicts)
        if tp.java_opts is not None:
            result = subprocess.run(
                [
                    "kubectl", "set", "env",
                    f"deployment/{tp.deploy_name}",
                    "-n", namespace,
                    f"JAVA_OPTS={tp.java_opts}",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                events = check_pod_events(namespace)
                err(f"kubectl set env failed for {tp.deploy_name}: {result.stderr.strip()}")
                return DeployResult(success=False, pod_events=events, is_resource_issue=False)

    # Wait for pods to become ready
    try:
        wait_for_pods(namespace, timeout)
    except SystemExit:
        events = check_pod_events(namespace)
        resource_issue = is_resource_failure(events)
        warn("Pods failed to become healthy")
        return DeployResult(success=False, pod_events=events, is_resource_issue=resource_issue)

    # Verify core pods are Ready
    events = check_pod_events(namespace)
    ready = check_core_pods_ready(namespace, active_comps=active_comps)
    if not ready:
        warn("Core DataHub pods not ready after wait")
        resource_issue = is_resource_failure(events)
        return DeployResult(success=False, pod_events=events, is_resource_issue=resource_issue)

    ok("Deployment patched, pods healthy")
    return DeployResult(success=True, pod_events=events, is_resource_issue=False)


def rollback_deployments(namespace: str, active_comps: list[str] | None = None) -> None:
    """Rollback all core deployments to their previous revision."""
    comps = active_comps or list(ALL_COMPONENTS.keys())
    for comp_name in comps:
        deploy_name = get_deployment_name(namespace, comp_name)
        if deploy_name:
            subprocess.run(
                ["kubectl", "rollout", "undo", f"deployment/{deploy_name}", "-n", namespace],
                capture_output=True,
                text=True,
            )


# ── Pod health ───────────────────────────────────────────────────────────────


def check_pod_events(namespace: str) -> list[str]:
    """Check pods for OOM kills, CrashLoopBackOff, and other resource failures."""
    result = subprocess.run(
        ["kubectl", "get", "pods", "-n", namespace, "-o", "json"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return ["kubectl-error"]

    events: list[str] = []
    pods = json.loads(result.stdout).get("items", [])
    for pod in pods:
        pod_name = pod.get("metadata", {}).get("name", "?")
        phase = pod.get("status", {}).get("phase", "Unknown")

        for cs in pod.get("status", {}).get("containerStatuses", []):
            # Check current state
            waiting = cs.get("state", {}).get("waiting", {})
            reason = waiting.get("reason", "")
            if reason in ("CrashLoopBackOff", "OOMKilled"):
                events.append(f"{pod_name}: {reason}")

            # Check last terminated state for OOM (only when reason is
            # explicitly "OOMKilled", NOT generic exit 137 which can also
            # come from liveness probe kills by kubelet)
            terminated = cs.get("lastState", {}).get("terminated", {})
            term_reason = terminated.get("reason", "")
            if term_reason == "OOMKilled":
                events.append(f"{pod_name}: OOMKilled")

            # High restart count suggests resource issues
            restarts = cs.get("restartCount", 0)
            if restarts > 2:
                events.append(f"{pod_name}: {restarts} restarts")

    return events


def _core_pod_prefixes(active_comps: list[str] | None = None) -> tuple[str, ...]:
    """Return pod name prefixes for the active components."""
    comps = active_comps or list(_DEFAULT_COMPONENTS)
    return tuple(ALL_COMPONENTS[c]["helm_prefix"] for c in comps if c in ALL_COMPONENTS)


def check_core_pods_ready(namespace: str, active_comps: list[str] | None = None) -> bool:
    """Check that all active component pods are Running and Ready.

    During rolling updates, both old and new pods may exist. ALL of them
    must be Ready — a new pod at 0/1 means it hasn't started yet.
    """
    result = subprocess.run(
        ["kubectl", "get", "pods", "-n", namespace, "-o", "json"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return False

    pod_prefixes = _core_pod_prefixes(active_comps)
    pods = json.loads(result.stdout).get("items", [])
    found: dict[str, int] = {}  # prefix → count of ready pods
    not_ready: dict[str, list[str]] = {}  # prefix → list of not-ready pod names

    for pod in pods:
        pod_name = pod.get("metadata", {}).get("name", "")
        phase = pod.get("status", {}).get("phase", "")

        # Skip completed pods (jobs)
        if phase == "Succeeded":
            continue

        for prefix in pod_prefixes:
            if prefix in pod_name:
                containers = pod.get("status", {}).get("containerStatuses", [])
                all_ready = (
                    phase == "Running"
                    and containers
                    and all(cs.get("ready", False) for cs in containers)
                )
                if all_ready:
                    found[prefix] = found.get(prefix, 0) + 1
                else:
                    not_ready.setdefault(prefix, []).append(pod_name)
                break

    ok_flag = True
    for prefix in _CORE_POD_PREFIXES:
        ready_count = found.get(prefix, 0)
        bad_pods = not_ready.get(prefix, [])
        if ready_count == 0:
            warn(f"No Ready pod for '{prefix}'")
            ok_flag = False
        if bad_pods:
            warn(f"Not-ready pods for '{prefix}': {', '.join(bad_pods)}")
            ok_flag = False

    return ok_flag


def is_resource_failure(events: list[str]) -> bool:
    """Check if pod events indicate a resource-related failure."""
    resource_indicators = ("OOMKilled", "CrashLoopBackOff", "restarts")
    return any(
        indicator in event for event in events for indicator in resource_indicators
    )


# ── Test runner ──────────────────────────────────────────────────────────────


def get_test_env(project_root: Path, hostname: str, gms_hostname: str) -> dict[str, str]:
    """Build environment variables for running smoke tests."""
    env = os.environ.copy()
    env["DATAHUB_GMS_URL"] = f"http://{gms_hostname}"
    env["DATAHUB_FRONTEND_URL"] = f"http://{hostname}"

    # Parse credentials from user.props
    user_props = project_root / "datahub-frontend" / "conf" / "user.props"
    if user_props.exists():
        for line in user_props.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and ":" in line:
                username, password = line.split(":", 1)
                env["ADMIN_USERNAME"] = username
                env["ADMIN_PASSWORD"] = password
                break

    env["DATAHUB_TELEMETRY_ENABLED"] = "false"
    return env


@dataclass
class JUnitResult:
    """Parsed JUnit XML results separating test failures from collection errors."""
    test_failures: list[str]       # retryable test node IDs (e.g. tests/foo.py::test_bar)
    collection_errors: list[str]   # module names that failed to collect
    tests_ran: int                 # number of actual test cases that executed


def parse_junit_failures(xml_path: Path) -> JUnitResult:
    """Parse failed test node IDs from a JUnit XML file."""
    empty = JUnitResult([], [], 0)
    if not xml_path.exists():
        return empty
    try:
        tree = ET.parse(xml_path)
    except ET.ParseError:
        return empty

    test_failures: list[str] = []
    collection_errors: list[str] = []
    tests_ran = 0
    for testcase in tree.iter("testcase"):
        error_el = testcase.find("error")
        has_failure = testcase.find("failure") is not None
        classname = testcase.get("classname", "")
        name = testcase.get("name", "")

        # Collection errors: classname is empty, error message says "collection failure"
        if error_el is not None and not classname:
            # name is like "tests.test_stateful_ingestion" → "tests/test_stateful_ingestion.py"
            file_path = name.replace(".", "/") + ".py"
            collection_errors.append(file_path)
            continue

        tests_ran += 1
        if has_failure or error_el is not None:
            if classname:
                # classname is "tests.foo.bar.TestClass" → split into
                # module path ("tests/foo/bar.py") and class ("TestClass")
                parts = classname.split(".")
                # Find where module path ends and class name begins:
                # class names start with uppercase by Python convention
                class_parts: list[str] = []
                module_parts = parts[:]
                while module_parts and module_parts[-1][0:1].isupper():
                    class_parts.insert(0, module_parts.pop())
                filepath = "/".join(module_parts) + ".py"
                if class_parts:
                    test_failures.append(f"{filepath}::{'::'.join(class_parts)}::{name}")
                else:
                    test_failures.append(f"{filepath}::{name}")
            else:
                test_failures.append(name)

    return JUnitResult(test_failures, collection_errors, tests_ran)


def load_known_flaky(path: Optional[Path]) -> list[str]:
    """Load known-flaky test node IDs from a file (one per line, # comments)."""
    if not path or not path.exists():
        return []
    tests: list[str] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            tests.append(line)
    return tests


def _run_pytest(
    cmd: list[str],
    cwd: Path,
    env: dict[str, str],
    timeout: int,
) -> Optional[subprocess.CompletedProcess[str]]:
    """Run pytest, returning None on timeout."""
    try:
        return subprocess.run(
            cmd, cwd=cwd, env=env, capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        warn(f"Smoke tests timed out after {timeout}s")
        return None


def run_smoke_tests(
    project_root: Path,
    test_env: dict[str, str],
    known_flaky: list[str],
    test_path: str = "tests/cli/graphql_cmd/test_graphql_cli_smoke.py",
    max_retries: int = 10,
    timeout: int = 300,
) -> tuple[bool, float, list[str], int]:
    """Run smoke tests. Returns (passed, duration_s, failed_tests, retries_used)."""
    smoke_test_dir = project_root / "smoke-test"
    junit_xml = Path("/tmp/optimize-smoke-results.xml")

    # Build deselect args for known-flaky tests
    deselect_args: list[str] = []
    for test_id in known_flaky:
        deselect_args.extend(["--deselect", test_id])

    # Use the venv pytest (smoke-test uses venv/, not .venv/)
    pytest_bin = str(smoke_test_dir / "venv" / "bin" / "pytest")

    # Track files to ignore (from collection errors)
    ignore_args: list[str] = []

    # First run
    cmd = [
        pytest_bin, test_path,
        "--tb=line", "-q",
        f"--junitxml={junit_xml}",
        *deselect_args,
    ]
    info("Running smoke tests...")
    start = time.monotonic()

    result = _run_pytest(cmd, smoke_test_dir, test_env, timeout)
    duration = time.monotonic() - start

    if result is None:
        return False, duration, ["timeout"], 0

    # Print test output summary
    for line in (result.stdout or "").splitlines()[-5:]:
        click.echo(f"  {line}")

    if result.returncode == 0:
        ok(f"All tests passed ({duration:.0f}s)")
        return True, duration, [], 0

    # Some tests failed — parse failures
    junit = parse_junit_failures(junit_xml)

    # If collection errors prevented tests from running, re-run with --ignore
    if junit.collection_errors and junit.tests_ran == 0:
        for err_file in junit.collection_errors:
            warn(f"Collection error (ignoring file): {err_file}")
            ignore_args.extend(["--ignore", err_file])
        info("Re-running tests with broken files ignored...")
        cmd_retry = [
            pytest_bin, test_path,
            "--tb=line", "-q",
            f"--junitxml={junit_xml}",
            *deselect_args,
            *ignore_args,
        ]
        result = _run_pytest(cmd_retry, smoke_test_dir, test_env, timeout)
        duration = time.monotonic() - start
        if result is None:
            return False, duration, ["timeout"], 0
        for line in (result.stdout or "").splitlines()[-5:]:
            click.echo(f"  {line}")
        if result.returncode == 0:
            ok(f"All tests passed ({duration:.0f}s, {len(junit.collection_errors)} file(s) ignored)")
            return True, duration, [], 0
        # Re-parse with the new results
        junit = parse_junit_failures(junit_xml)

    # Log any remaining collection errors
    for ce in junit.collection_errors:
        warn(f"Collection error (ignored): {ce}")

    failed_tests = junit.test_failures
    if not failed_tests and not junit.collection_errors:
        warn("Tests failed but couldn't parse failures from JUnit XML")
        return False, duration, ["unknown"], 0

    if not failed_tests:
        ok(f"All tests passed ({duration:.0f}s, {len(junit.collection_errors)} collection error(s) ignored)")
        return True, duration, [], 0

    click.echo(f"  {len(failed_tests)} test(s) failed")

    # Check if pods are in bad state (resource failure, not flakiness)
    namespace = test_env.get("_OPTIMIZER_NAMESPACE", "")
    if namespace:
        events = check_pod_events(namespace)
        if is_resource_failure(events):
            warn("Pod resource failures detected — not retrying (resource issue)")
            return False, duration, failed_tests, 0

    # Re-run only failed tests (up to max_retries)
    for retry in range(1, max_retries + 1):
        info(f"Re-running {len(failed_tests)} failed test(s) (retry {retry}/{max_retries})...")
        rerun_xml = Path(f"/tmp/optimize-smoke-rerun-{retry}.xml")
        rerun_cmd = [
            pytest_bin,
            *failed_tests,
            "--tb=line", "-q",
            f"--junitxml={rerun_xml}",
            *ignore_args,
        ]
        rerun_result = _run_pytest(rerun_cmd, smoke_test_dir, test_env, timeout)
        if rerun_result is None:
            return False, duration, failed_tests, retry
        for line in (rerun_result.stdout or "").splitlines()[-3:]:
            click.echo(f"  {line}")

        if rerun_result.returncode == 0:
            ok(f"Re-run passed (retry {retry}) — original failures were flaky")
            return True, duration, failed_tests, retry

        # Still failing — check if it's now a resource issue
        if namespace:
            events = check_pod_events(namespace)
            if is_resource_failure(events):
                warn("Pod resource failures on retry — resource issue")
                return False, duration, failed_tests, retry

        rerun_junit = parse_junit_failures(rerun_xml)
        failed_tests = rerun_junit.test_failures or failed_tests

    warn(f"Tests still failing after {max_retries} retries")
    return False, duration, failed_tests, max_retries


# ── Binary search ────────────────────────────────────────────────────────────


def round_to_step(value: int, step: int) -> int:
    """Round value down to the nearest step."""
    return (value // step) * step


def optimize_dimension(
    state: OptimizerState,
    state_path: Path,
    component: str,
    dimension: str,
    namespace: str,
    test_env: dict[str, str],
    known_flaky: list[str],
    active_comps: list[str] | None = None,
    test_path: str = "tests/cli/graphql_cmd/test_graphql_cli_smoke.py",
    dry_run: bool = False,
) -> int:
    """Binary search for the minimum value of a dimension that passes tests."""
    dim_cfg = ALL_COMPONENTS[component]["dimensions"][dimension]
    step = dim_cfg["step"]

    # Resume from checkpoint or start fresh
    if state.search_state:
        low, high = state.search_state.low, state.search_state.high
        info(f"Resuming {component}.{dimension}: [{low}, {high}] step={step}")
    else:
        low = dim_cfg["floor"]
        high = dim_cfg["current"]
        info(f"Optimizing {component}.{dimension}: [{low}, {high}] step={step}")

    if dry_run:
        click.echo(f"  [dry-run] Would binary search [{low}, {high}] step={step}")
        iterations = 0
        l, h = low, high
        while h - l >= step:
            iterations += 1
            mid = round_to_step((l + h) // 2, step)
            l = mid + step  # assume worst case
        click.echo(f"  [dry-run] Estimated {iterations} trials")
        return high

    max_non_resource_retries = 3  # retry non-resource failures before giving up

    while high - low >= step:
        mid = round_to_step((low + high) // 2, step)

        # Save state before trial
        state.search_state = SearchState(low=low, high=high, step=step)
        state.current_component = component
        state.current_dimension = dimension
        save_state(state, state_path)

        # Build patches and deploy (with retries for non-resource failures)
        patches, overrides = build_trial_patches(
            state, component, dimension, mid, namespace, active_comps=active_comps
        )

        passed = False
        pod_events: list[str] = []
        retries = 0

        for attempt in range(max_non_resource_retries + 1):
            trial_id = len(state.trials) + 1
            if attempt > 0:
                info(f"Trial #{trial_id}: {component}.{dimension} = {mid}{dim_cfg['unit']} (retry {attempt} — non-resource failure)")
            else:
                info(f"Trial #{trial_id}: {component}.{dimension} = {mid}{dim_cfg['unit']}")

            trial_start = time.monotonic()
            deploy_result = deploy_trial(namespace, patches, active_comps=active_comps)

            if deploy_result.success:
                # Run smoke tests
                passed, test_duration, failed_tests, retries = run_smoke_tests(
                    Path(state_path).resolve().parent.parent,  # project root
                    test_env,
                    known_flaky,
                    test_path=test_path,
                )
                pod_events = deploy_result.pod_events
                break
            elif deploy_result.is_resource_issue:
                # Confirmed resource failure — this value is too low
                pod_events = deploy_result.pod_events
                passed = False
                break
            else:
                # Non-resource failure — rollback and retry same value
                pod_events = deploy_result.pod_events
                if attempt < max_non_resource_retries:
                    warn(f"Non-resource failure, retrying same value ({attempt + 1}/{max_non_resource_retries})...")
                    rollback_deployments(namespace, active_comps=active_comps)
                    time.sleep(10)  # brief pause before retry
                else:
                    err(f"Non-resource failure persisted after {max_non_resource_retries} retries, treating as failure")
                    passed = False

        trial_duration = time.monotonic() - trial_start

        trial = Trial(
            id=len(state.trials) + 1,
            component=component,
            dimension=dimension,
            value=mid,
            unit=dim_cfg["unit"],
            all_overrides=overrides,
            test_passed=passed,
            duration_s=trial_duration,
            timestamp=datetime.now(timezone.utc).isoformat(),
            pod_events=pod_events,
            retries_used=retries,
        )
        state.trials.append(trial)
        save_state(state, state_path)

        if passed:
            ok(f"Trial #{trial.id}: PASS — {mid}{dim_cfg['unit']} works, trying lower")
            high = mid
        else:
            warn(f"Trial #{trial.id}: FAIL — {mid}{dim_cfg['unit']} too low, trying higher")
            low = mid + step

    info(f"Optimal {component}.{dimension} = {high}{dim_cfg['unit']}")
    return high


def optimize_all(
    state: OptimizerState,
    state_path: Path,
    namespace: str,
    test_env: dict[str, str],
    known_flaky: list[str],
    active_comps: list[str] | None = None,
    test_path: str = "tests/cli/graphql_cmd/test_graphql_cli_smoke.py",
    dry_run: bool = False,
) -> None:
    """Optimize all components and dimensions."""
    for comp_name in state.components_order:
        if comp_name not in ALL_COMPONENTS:
            warn(f"Unknown component '{comp_name}', skipping")
            continue

        # Skip completed components
        comp_results = state.results.get(comp_name, {})
        completed_dims = set(comp_results.keys())

        info(f"{'=' * 60}")
        info(f"Optimizing component: {comp_name}")
        info(f"{'=' * 60}")

        for dim_name in DIMENSION_ORDER:
            if dim_name not in ALL_COMPONENTS[comp_name]["dimensions"]:
                continue
            if dim_name in completed_dims:
                ok(f"  {dim_name} already optimized: {comp_results[dim_name]}")
                continue

            # Check if we're resuming this specific dimension
            if (
                state.current_component == comp_name
                and state.current_dimension == dim_name
            ):
                info(f"Resuming optimization of {comp_name}.{dim_name}")
            else:
                state.search_state = None

            optimal = optimize_dimension(
                state,
                state_path,
                comp_name,
                dim_name,
                namespace,
                test_env,
                known_flaky,
                active_comps=active_comps,
                test_path=test_path,
                dry_run=dry_run,
            )

            # Record result (skip persisting in dry-run mode)
            if not dry_run:
                if comp_name not in state.results:
                    state.results[comp_name] = {}
                state.results[comp_name][dim_name] = optimal
                state.search_state = None
                state.current_dimension = None
                save_state(state, state_path)

        if not dry_run:
            state.current_component = None
            save_state(state, state_path)
        ok(f"Component '{comp_name}' optimization complete")


# ── Output ───────────────────────────────────────────────────────────────────


def print_summary(state: OptimizerState) -> None:
    """Print a summary table of optimization results."""
    click.echo()
    info("Optimization Results")
    click.echo(
        f"  {'Component':<12} {'Dimension':<18} {'Current':>8} {'Optimal':>8} {'Saved':>8}"
    )
    click.echo(f"  {'-' * 12} {'-' * 18} {'-' * 8} {'-' * 8} {'-' * 8}")

    total_mem_saved = 0
    total_cpu_saved = 0

    for comp_name in state.components_order:
        comp_cfg = ALL_COMPONENTS.get(comp_name)
        if not comp_cfg:
            continue
        comp_results = state.results.get(comp_name, {})
        is_jvm = comp_cfg.get("has_jvm", False)

        for dim_name in DIMENSION_ORDER:
            if dim_name not in comp_cfg["dimensions"]:
                # For JVM: show memory_request as derived from memory_limit
                if dim_name == "memory_request" and is_jvm:
                    mem_limit_cfg = comp_cfg["dimensions"]["memory_limit"]
                    current = mem_limit_cfg["current"]
                    optimal = comp_results.get("memory_limit", current)
                    saved = current - optimal
                    click.echo(
                        f"  {comp_name:<12} {dim_name:<18} {current:>6}Mi {optimal:>6}Mi {saved:>6}Mi  (= limit)"
                    )
                    total_mem_saved += saved
                continue
            dim_cfg = comp_cfg["dimensions"][dim_name]
            current = dim_cfg["current"]
            optimal = comp_results.get(dim_name, current)
            saved = current - optimal
            unit = dim_cfg["unit"]

            extra = ""
            if dim_name == "memory_limit" and is_jvm:
                heap = derive_jvm_heap(optimal, comp_cfg["jvm_heap_ratio"])
                extra = f"  (heap: {heap}m)"
            click.echo(
                f"  {comp_name:<12} {dim_name:<18} {current:>6}{unit:>2} {optimal:>6}{unit:>2} {saved:>6}{unit:>2}{extra}"
            )

            if "memory" in dim_name:
                total_mem_saved += saved
            elif "cpu" in dim_name:
                total_cpu_saved += saved

    click.echo()
    ok(f"Total memory saved: {total_mem_saved}Mi")
    ok(f"Total CPU saved: {total_cpu_saved}m")

    # Trial stats
    total_trials = len(state.trials)
    passed = sum(1 for t in state.trials if t.test_passed)
    failed = sum(1 for t in state.trials if t.test_passed is False)
    total_duration = sum(t.duration_s for t in state.trials)
    click.echo()
    ok(f"Trials: {total_trials} total ({passed} passed, {failed} failed)")
    ok(f"Total time: {total_duration / 60:.1f} minutes")


def write_optimized_values(state: OptimizerState, output_path: Path) -> None:
    """Write a Helm values override file with optimal resource settings."""
    lines = [
        "# Optimized resource values for DataHub on k3d.",
        f"# Generated by optimize-resources.py on {datetime.now(timezone.utc).isoformat()}",
        f"# Trials: {len(state.trials)}, started: {state.started_at}",
        "",
    ]

    for comp_name in state.components_order:
        comp_cfg = ALL_COMPONENTS.get(comp_name)
        if not comp_cfg:
            continue
        comp_results = state.results.get(comp_name, {})
        prefix = comp_cfg["helm_prefix"]
        is_jvm = comp_cfg.get("has_jvm", False)

        mem_limit = comp_results.get("memory_limit", comp_cfg["dimensions"]["memory_limit"]["current"])
        # JVM: request = limit (JVM pre-allocates heap)
        if is_jvm:
            mem_req = mem_limit
        else:
            mem_req_cfg = comp_cfg["dimensions"].get("memory_request")
            mem_req = comp_results.get("memory_request", mem_req_cfg["current"] if mem_req_cfg else mem_limit)
        cpu_req = comp_results.get("cpu_request", comp_cfg["dimensions"]["cpu_request"]["current"])

        lines.extend([
            f"{prefix}:",
            f"  resources:",
            f"    limits:",
            f"      memory: {mem_limit}Mi",
            f"    requests:",
            f"      memory: {mem_req}Mi",
            f"      cpu: {cpu_req}m",
        ])

        # JVM heap configuration
        if is_jvm:
            heap = derive_jvm_heap(mem_limit, comp_cfg["jvm_heap_ratio"])
            lines.append(f"  # JVM heap: {heap}m ({int(comp_cfg['jvm_heap_ratio'] * 100)}% of {mem_limit}Mi limit)")

        lines.append("")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))
    ok(f"Wrote optimized values to {output_path}")


# ── CLI ──────────────────────────────────────────────────────────────────────


@click.command()
@click.option(
    "--state-file",
    default="k3d/resource-optimization-state.json",
    help="Path to the checkpoint state file.",
)
@click.option(
    "--resume/--no-resume",
    default=True,
    help="Resume from checkpoint (default: yes).",
)
@click.option(
    "--components",
    default=None,
    help="Comma-separated list of components to optimize (overrides --profile auto-detection).",
)
@click.option(
    "--profile",
    "profiles",
    multiple=True,
    type=click.Choice(["minimal", "consumers", "backend"], case_sensitive=False),
    help="Deployment profile (repeatable). Determines which components to optimize.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be tested without deploying.",
)
@click.option(
    "--known-flaky",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="File listing known-flaky test IDs to skip (one per line).",
)
@click.option(
    "--test-retries",
    default=10,
    help="Max retries for failed tests before declaring failure.",
)
@click.option(
    "--test-path",
    default="tests/cli/graphql_cmd/test_graphql_cli_smoke.py",
    help="Pytest path to run (relative to smoke-test/). Default: GraphQL smoke tests.",
)
def main(
    state_file: str,
    resume: bool,
    components: Optional[str],
    profiles: tuple[str, ...],
    dry_run: bool,
    known_flaky: Optional[Path],
    test_retries: int,
    test_path: str,
) -> None:
    """Find minimum resources for DataHub components that still pass smoke tests."""
    # Resolve active components from profiles (or explicit --components override)
    if components:
        comp_list = [c.strip() for c in components.split(",")]
    else:
        comp_list = active_components(profiles)
    if profiles:
        info(f"Profiles: {', '.join(profiles)}")
    info(f"Components: {', '.join(comp_list)}")

    # Derive deployment info
    name = derive_worktree_name()
    namespace = namespace_for(name)
    hostname = hostname_for(name)
    gms_hostname = gms_hostname_for(name)
    project_root = Path(__file__).resolve().parent.parent
    state_path = project_root / state_file

    info(f"Resource optimizer for '{name}' (namespace: {namespace})")
    click.echo(f"  Frontend: http://{hostname}")
    click.echo(f"  GMS:      http://{gms_hostname}")
    click.echo(f"  State:    {state_path}")

    # Verify deployment exists
    try:
        release, chart_name, chart_version = get_release_info(namespace)
    except RuntimeError as exc:
        err(str(exc))
        err(f"Is DataHub deployed in namespace '{namespace}'? Run: dh deploy")
        sys.exit(1)

    click.echo(f"  Release:  {release} ({chart_name} v{chart_version})")

    # Verify core deployments exist
    for comp_name in comp_list:
        if comp_name in ALL_COMPONENTS:
            deploy_name = get_deployment_name(namespace, comp_name)
            if not deploy_name:
                err(f"Deployment not found for component '{comp_name}'")
                sys.exit(1)
            click.echo(f"  Deployment: {deploy_name}")

    # Load or create state
    if resume:
        state = load_state(state_path)
        if state:
            info(f"Resumed from checkpoint (started: {state.started_at})")
            click.echo(f"  Trials so far: {len(state.trials)}")
            click.echo(f"  Results: {json.dumps(state.results, indent=2)}")
        else:
            state = new_state(comp_list)
            info("No checkpoint found, starting fresh")
    else:
        state = new_state(comp_list)
        info("Starting fresh (--no-resume)")

    # Build test environment
    test_env = get_test_env(project_root, hostname, gms_hostname)
    test_env["_OPTIMIZER_NAMESPACE"] = namespace  # internal, for pod health checks

    # Load known-flaky tests
    flaky_tests = load_known_flaky(known_flaky)
    if flaky_tests:
        info(f"Skipping {len(flaky_tests)} known-flaky test(s)")

    if dry_run:
        info("[DRY RUN] Showing estimated work without deploying")

    # Verify cluster is healthy before starting
    if not dry_run:
        if not check_core_pods_ready(namespace, active_comps=comp_list):
            err("Core pods not ready — fix cluster health before optimizing")
            sys.exit(1)
        ok("Cluster healthy, starting optimization")

    # Run optimization (uses kubectl patch, not helm upgrade)
    optimize_all(
        state,
        state_path,
        namespace,
        test_env,
        flaky_tests,
        active_comps=comp_list,
        test_path=test_path,
        dry_run=dry_run,
    )

    # Print results
    print_summary(state)

    # Write optimized values file
    output_path = project_root / "k3d" / "values" / "optimized-resources.yaml"
    write_optimized_values(state, output_path)


if __name__ == "__main__":
    main()
