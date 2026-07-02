"""Cgroup-aware CPU detection for multiprocessing worker sizing.

On virtualized EC2/K8s hosts, os.cpu_count() reports the host's physical core
count (e.g. 64) rather than the container's CPU quota (e.g. 2). Spawning a
process pool sized to os.cpu_count() on such a host causes severe CPU throttling
and OOM. This module detects the actual usable CPU budget by consulting cgroup
quotas and CPU affinity before falling back to os.cpu_count().
"""

import logging
import math
import os
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


def _parse_cgroup_v2_cpu_max(content: str) -> Optional[float]:
    """Parse /sys/fs/cgroup/cpu.max content.

    Returns None for unlimited ("max <period>"), or quota/period as float.
    Returns None on malformed input.
    """
    parts = content.strip().split()
    if len(parts) != 2:
        return None
    quota_str, period_str = parts
    if quota_str == "max":
        return None
    try:
        quota = float(quota_str)
        period = float(period_str)
        if quota <= 0 or period <= 0:
            return None
        return quota / period
    except ValueError:
        return None


def _parse_cgroup_v1_cpu_quota(quota_str: str, period_str: str) -> Optional[float]:
    """Parse cgroup v1 cpu.cfs_quota_us and cpu.cfs_period_us values.

    Returns None when quota is -1 (unlimited), or quota/period as float.
    Returns None on malformed input.
    """
    try:
        quota = int(quota_str.strip())
        period = int(period_str.strip())
    except ValueError:
        return None
    if quota <= 0:
        return None
    if period == 0:
        return None
    return quota / period


def _cgroup_cpu_limit() -> Optional[float]:
    """Read the container CPU limit from cgroup files.

    Tries cgroup v2 first (/sys/fs/cgroup/cpu.max), then cgroup v1
    (/sys/fs/cgroup/cpu/cpu.cfs_quota_us + cpu.cfs_period_us).
    Returns None on any file/OS error or when no limit is set.
    """
    # cgroup v2
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            return _parse_cgroup_v2_cpu_max(f.read())
    except OSError:
        pass

    # cgroup v1
    try:
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") as f:
            quota_str = f.read()
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us") as f:
            period_str = f.read()
        return _parse_cgroup_v1_cpu_quota(quota_str, period_str)
    except OSError:
        pass

    return None


def _affinity_cpu_count() -> Optional[int]:
    """Return the number of CPUs in this process's CPU affinity set.

    Uses os.sched_getaffinity (Linux only; respects cpuset/taskset).
    Returns None on platforms where affinity is not available.
    """
    if hasattr(os, "sched_getaffinity"):
        return len(os.sched_getaffinity(0))
    return None


def _os_cpu_count() -> Optional[int]:
    """Thin wrapper over os.cpu_count()."""
    return os.cpu_count()


def get_available_cpu_count() -> int:
    """Return the number of CPUs actually usable by this process.

    Takes the minimum of all available signals — cgroup quota (floored),
    CPU affinity count, and os.cpu_count() — ignoring any that are None.
    Always returns at least 1.
    """
    candidates = []

    cgroup_limit = _cgroup_cpu_limit()
    if cgroup_limit is not None:
        candidates.append(math.floor(cgroup_limit))

    affinity = _affinity_cpu_count()
    if affinity is not None:
        candidates.append(affinity)

    os_count = _os_cpu_count()
    if os_count is not None:
        candidates.append(os_count)

    return max(1, min(candidates)) if candidates else 1


@dataclass
class WorkerDecision:
    """Outcome of resolve_worker_count, including diagnostics for logging/debugging."""

    workers: int
    detected_cpus: int
    requested: Optional[int]
    clamped: bool
    fell_back_to_serial: bool
    reason: str


def resolve_worker_count(
    requested: Optional[int],
    *,
    reserve: int = 1,
    detected_cpus: Optional[int] = None,
) -> WorkerDecision:
    """Decide how many worker processes to spawn, respecting the container's CPU budget.

    Args:
        requested: Caller-requested worker count, or None for auto-detection.
        reserve: Number of cores to hold back for the main/aggregation process.
        detected_cpus: Override for testing; uses get_available_cpu_count() when None.

    Returns:
        WorkerDecision with the final worker count and diagnostic fields.
    """
    detected = detected_cpus if detected_cpus is not None else get_available_cpu_count()
    usable = max(1, detected - reserve)

    # Only fall back to serial when there is strictly fewer than 2 CPUs detected.
    # With exactly 2 CPUs, usable=1 after reserving one core — callers treat
    # workers=1 as a degenerate single-worker pool (not serial), which preserves
    # the multiprocessing code path and avoids a special case in the caller.
    if detected < 2:
        return WorkerDecision(
            workers=1,
            detected_cpus=detected,
            requested=requested,
            clamped=False,
            fell_back_to_serial=True,
            reason=f"Only {detected} CPU(s) detected; falling back to serial execution.",
        )

    if requested is None:
        return WorkerDecision(
            workers=usable,
            detected_cpus=detected,
            requested=None,
            clamped=False,
            fell_back_to_serial=False,
            reason=f"Auto-detected {detected} CPU(s); using {usable} worker(s) (reserving {reserve}).",
        )

    workers = min(requested, usable)
    clamped = requested > usable
    if clamped:
        logger.warning(
            "Requested %d workers but only %d are available after reserving %d core(s) "
            "for the main process (detected %d CPU(s)). Clamping to %d workers. "
            "This host may be a virtualized environment where os.cpu_count() over-reports.",
            requested,
            usable,
            reserve,
            detected,
            workers,
        )
        reason = (
            f"Requested {requested} workers clamped to {workers} "
            f"(detected {detected} CPU(s), {reserve} reserved)."
        )
    else:
        reason = (
            f"Using {workers} of {requested} requested worker(s) "
            f"(detected {detected} CPU(s), {reserve} reserved)."
        )

    return WorkerDecision(
        workers=workers,
        detected_cpus=detected,
        requested=requested,
        clamped=clamped,
        fell_back_to_serial=False,
        reason=reason,
    )
