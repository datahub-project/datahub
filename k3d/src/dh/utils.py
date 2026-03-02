"""Subprocess runner, output helpers, prerequisite guards, and wait utilities."""

from __future__ import annotations

import shutil
import subprocess
import sys
from typing import Optional

import click


# ── Output helpers ──────────────────────────────────────────────────────────


def info(msg: str) -> None:
    click.echo(click.style("==>", fg="blue") + " " + click.style(msg, bold=True))


def ok(msg: str) -> None:
    click.echo(click.style("==>", fg="green") + " " + msg)


def warn(msg: str) -> None:
    click.echo(
        click.style("==> WARNING:", fg="yellow") + " " + msg,
        err=True,
    )


def err(msg: str) -> None:
    click.echo(
        click.style("==> ERROR:", fg="red") + " " + msg,
        err=True,
    )


def die(msg: str) -> None:
    err(msg)
    sys.exit(1)


# ── Subprocess runner ───────────────────────────────────────────────────────


def run(
    cmd: list[str],
    *,
    check: bool = True,
    capture_output: bool = False,
    quiet: bool = False,
    input_text: Optional[str] = None,
    cwd: Optional[str] = None,
) -> subprocess.CompletedProcess[str]:
    """Run a command, returning CompletedProcess.

    Args:
        cmd: Command and arguments.
        check: Raise on non-zero exit (default True).
        capture_output: Capture stdout/stderr instead of inheriting.
        quiet: Suppress stdout/stderr (redirect to DEVNULL).
        input_text: String to pipe to stdin.
        cwd: Working directory.
    """
    kwargs: dict = dict(encoding="utf-8")
    if capture_output:
        kwargs["stdout"] = subprocess.PIPE
        kwargs["stderr"] = subprocess.PIPE
    elif quiet:
        kwargs["stdout"] = subprocess.DEVNULL
        kwargs["stderr"] = subprocess.DEVNULL
    if input_text is not None:
        kwargs["input"] = input_text
    if cwd is not None:
        kwargs["cwd"] = cwd
    try:
        return subprocess.run(cmd, check=check, **kwargs)
    except subprocess.CalledProcessError as exc:
        stderr_msg = ""
        if exc.stderr:
            stderr_msg = f"\n{exc.stderr.strip()}"
        die(f"Command failed: {' '.join(cmd)}{stderr_msg}")
        raise  # unreachable, but keeps type checker happy


# ── Prerequisite guards ─────────────────────────────────────────────────────


def require_cmd(name: str) -> None:
    if shutil.which(name) is None:
        die(f"'{name}' is required but not found. Install it or run 'devbox shell'.")


def require_cluster(cluster_name: str) -> None:
    result = run(["k3d", "cluster", "list", cluster_name], check=False, quiet=True)
    if result.returncode != 0:
        die(f"Cluster '{cluster_name}' not found. Run: dh cluster-up")
    result = run(["kubectl", "cluster-info"], check=False, quiet=True)
    if result.returncode != 0:
        die("Cannot connect to cluster. Check your kubeconfig.")


def require_infra(cluster_name: str, infra_namespace: str) -> None:
    require_cluster(cluster_name)
    result = run(
        ["kubectl", "get", "namespace", infra_namespace], check=False, quiet=True
    )
    if result.returncode != 0:
        die(f"Infra namespace '{infra_namespace}' not found. Run: dh infra-up")


# ── Wait helpers ────────────────────────────────────────────────────────────


def wait_for_pods(namespace: str, timeout: int = 300) -> None:
    """Wait for all pods to be ready, showing real-time status.

    Polls pod status every few seconds, prints a summary line, and
    detects terminal failures (CrashLoopBackOff, ImagePullBackOff, etc.)
    early so we can surface logs instead of waiting for the full timeout.
    """
    import json as _json
    import time

    _TERMINAL_STATES = {
        "CrashLoopBackOff",
        "ImagePullBackOff",
        "ErrImageNeverPull",
        "InvalidImageName",
        "CreateContainerConfigError",
    }
    _POLL_INTERVAL = 5
    _FAIL_THRESHOLD = 3  # consecutive polls in terminal state before aborting

    info(f"Waiting for pods in '{namespace}' (timeout: {timeout}s)...")
    deadline = time.monotonic() + timeout
    fail_counts: dict[str, int] = {}

    while time.monotonic() < deadline:
        result = run(
            [
                "kubectl", "get", "pods", "-n", namespace,
                "-o", "json",
            ],
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            time.sleep(_POLL_INTERVAL)
            continue

        pods = _json.loads(result.stdout).get("items", [])
        if not pods:
            time.sleep(_POLL_INTERVAL)
            continue

        # Summarize pod statuses
        summary: dict[str, int] = {}
        all_ready = True
        for pod in pods:
            phase = pod.get("status", {}).get("phase", "Unknown")
            pod_name = pod.get("metadata", {}).get("name", "?")

            # Completed pods (jobs) are always considered ready
            if phase == "Succeeded":
                summary[phase] = summary.get(phase, 0) + 1
                continue

            # Check container statuses for detailed state
            container_statuses = pod.get("status", {}).get("containerStatuses", [])
            for cs in container_statuses:
                waiting = cs.get("state", {}).get("waiting", {})
                reason = waiting.get("reason", "")
                if reason in _TERMINAL_STATES:
                    fail_counts[pod_name] = fail_counts.get(pod_name, 0) + 1
                    phase = reason
                    if fail_counts[pod_name] >= _FAIL_THRESHOLD:
                        err(f"Pod '{pod_name}' stuck in {reason}.")
                        _show_failing_pod_logs(namespace, pod_name)
                        die(
                            f"Deploy failed: pod '{pod_name}' is in {reason}. "
                            "Check the logs above for details."
                        )

            if phase not in _TERMINAL_STATES:
                if not all(
                    cs.get("ready", False) for cs in container_statuses
                ):
                    all_ready = False

            summary[phase] = summary.get(phase, 0) + 1

        status_str = "  " + " | ".join(
            f"{status}: {count}" for status, count in sorted(summary.items())
        )
        click.echo(f"\r{status_str}", nl=True)

        if all_ready and all(
            pod.get("status", {}).get("phase") in ("Running", "Succeeded")
            for pod in pods
        ):
            return

        time.sleep(_POLL_INTERVAL)

    warn(f"Timed out waiting for pods in '{namespace}'.")
    run(["kubectl", "get", "pods", "-n", namespace])


def wait_for_jobs(namespace: str, timeout: int = 300) -> None:
    """Wait for all jobs to complete, showing real-time status."""
    import json as _json
    import time

    _POLL_INTERVAL = 5

    info(f"Waiting for jobs in '{namespace}' (timeout: {timeout}s)...")
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        result = run(
            [
                "kubectl", "get", "jobs", "-n", namespace,
                "-o", "json",
            ],
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            time.sleep(_POLL_INTERVAL)
            continue

        jobs = _json.loads(result.stdout).get("items", [])
        if not jobs:
            time.sleep(_POLL_INTERVAL)
            continue

        all_done = True
        for job in jobs:
            job_name = job.get("metadata", {}).get("name", "?")
            status = job.get("status", {})
            succeeded = status.get("succeeded", 0)
            failed = status.get("failed", 0)
            active = status.get("active", 0)
            conditions = status.get("conditions", [])

            # Check for terminal failure
            for cond in conditions:
                if cond.get("type") == "Failed" and cond.get("status") == "True":
                    err(f"Job '{job_name}' failed: {cond.get('reason', '?')}")
                    # Show logs from the job's pod
                    _show_job_pod_logs(namespace, job_name)
                    die(f"Deploy failed: job '{job_name}' failed.")

            if succeeded > 0:
                click.echo(f"  Job {job_name}: completed")
            elif active > 0:
                all_done = False
                click.echo(f"  Job {job_name}: running (active={active})")
            elif failed > 0:
                all_done = False
                click.echo(f"  Job {job_name}: retrying (failed={failed})")
            else:
                all_done = False
                click.echo(f"  Job {job_name}: pending")

        if all_done:
            return

        time.sleep(_POLL_INTERVAL)

    warn(f"Timed out waiting for jobs in '{namespace}'.")
    run(["kubectl", "get", "jobs", "-n", namespace])


def _show_failing_pod_logs(namespace: str, pod_name: str) -> None:
    """Print the last 30 lines of logs for a failing pod."""
    click.echo()
    click.echo(click.style(f"--- Logs for {pod_name} ---", fg="red", bold=True))
    run(
        ["kubectl", "logs", "-n", namespace, pod_name, "--tail=30"],
        check=False,
    )
    click.echo(click.style("--- End logs ---", fg="red", bold=True))
    click.echo()


def _show_job_pod_logs(namespace: str, job_name: str) -> None:
    """Print the last 30 lines of logs for a failed job's pod."""
    result = run(
        [
            "kubectl", "get", "pods", "-n", namespace,
            "-l", f"job-name={job_name}",
            "-o", "jsonpath={.items[0].metadata.name}",
        ],
        capture_output=True,
        check=False,
    )
    if result.returncode == 0 and result.stdout.strip():
        _show_failing_pod_logs(namespace, result.stdout.strip())
