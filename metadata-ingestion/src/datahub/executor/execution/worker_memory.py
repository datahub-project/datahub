"""Memory context for an OOM-killed ingestion run.

The executor does not sample the ingestion subprocess: the pipeline already
measures its own RSS and prints it in the periodic CLI report. What the executor
adds is the memory limit it runs under, read from its own cgroup (found via
/proc/self/cgroup and walking up to the mount, since in some setups the top of
/sys/fs/cgroup belongs to the whole machine and carries no limit). Every
ingestion subprocess started by one executor shares that limit, so it is a
per-worker figure rather than a per-run one.
"""

import logging
import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import humanfriendly
import psutil

logger = logging.getLogger(__name__)

CGROUP_ROOT = "/sys/fs/cgroup"
PROC_SELF_CGROUP = "/proc/self/cgroup"

# cgroup v1 reports "no limit" as a page-aligned value just under 2**63 rather
# than a sentinel string; anything this large is treated as unlimited.
_V1_NO_LIMIT_BYTES = 1 << 60

# Matches the pipeline's periodic CLI report, e.g. "'peak_memory_usage': '12.3 GB',".
_PEAK_MEMORY_RE = re.compile(r"'peak_memory_usage': '([^']+)'")


@dataclass(frozen=True)
class MemoryLimit:
    bytes: Optional[int]
    source: str  # "cgroup v2", "cgroup v1", "host" or "unknown"

    @property
    def text(self) -> Optional[str]:
        if self.bytes is None:
            return None
        return f"{humanfriendly.format_size(self.bytes)} ({self.source})"


@dataclass
class IngestionMemoryInfo:
    """The peak memory the ingestion subprocess last reported about itself."""

    last_reported_peak: Optional[str] = None

    def record_log_line(self, line: str) -> None:
        if "peak_memory_usage" not in line:
            return
        match = _PEAK_MEMORY_RE.search(line)
        if match:
            self.last_reported_peak = match.group(1)


def _read_int(path: str) -> Optional[int]:
    """None when the file is missing, unreadable, or not a number (e.g. "max")."""
    try:
        with open(path) as f:
            text = f.read(64).strip()
    except OSError:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _parse_proc_cgroup(text: str) -> Tuple[Optional[str], Dict[str, str]]:
    """The cgroup v2 path (the ``0::<path>`` line) and each v1 controller's path."""
    unified_path: Optional[str] = None
    controller_paths: Dict[str, str] = {}
    for line in text.splitlines():
        parts = line.split(":", 2)
        if len(parts) != 3:
            continue
        hierarchy_id, controllers, path = parts
        if hierarchy_id == "0" and controllers == "":
            unified_path = path
        else:
            for controller in controllers.split(","):
                controller_paths[controller] = path
    return unified_path, controller_paths


def _own_cgroup_dirs(mount: str, cgroup_path: Optional[str]) -> List[str]:
    """This process's cgroup directory and its parents, up to and including ``mount``.

    Falls back to ``mount`` alone when the path from /proc/self/cgroup is not
    visible under it, which is the usual case inside a private cgroup namespace:
    the mount is then the container's own cgroup.
    """
    own = mount
    if cgroup_path is not None:
        candidate = os.path.normpath(os.path.join(mount, cgroup_path.lstrip("/")))
        if os.path.commonpath([mount, candidate]) == mount and os.path.isdir(candidate):
            own = candidate
    dirs = [own]
    while dirs[-1] != mount:
        dirs.append(os.path.dirname(dirs[-1]))
    return dirs


def _cgroup_memory_limit(
    cgroup_root: str, proc_self_cgroup: str
) -> Optional[MemoryLimit]:
    try:
        with open(proc_self_cgroup) as f:
            unified_path, controller_paths = _parse_proc_cgroup(f.read(65536))
    except OSError:
        unified_path, controller_paths = None, {}

    root = os.path.normpath(cgroup_root)
    if os.path.exists(os.path.join(root, "cgroup.controllers")):
        dirs = _own_cgroup_dirs(root, unified_path)
        filename, source = "memory.max", "cgroup v2"
    elif os.path.isdir(os.path.join(root, "memory")):
        mount = os.path.join(root, "memory")
        dirs = _own_cgroup_dirs(mount, controller_paths.get("memory"))
        filename, source = "memory.limit_in_bytes", "cgroup v1"
    else:
        return None

    limits = [_read_int(os.path.join(directory, filename)) for directory in dirs]
    effective = [
        limit
        for limit in limits
        if limit is not None and 0 < limit < _V1_NO_LIMIT_BYTES
    ]
    if not effective:
        return None
    return MemoryLimit(bytes=min(effective), source=source)


def read_memory_limit(
    cgroup_root: str = CGROUP_ROOT, proc_self_cgroup: str = PROC_SELF_CGROUP
) -> MemoryLimit:
    """The memory limit this executor runs under: its cgroup's, else the host's RAM.

    Never raises; returns an unknown limit when nothing can be read.
    """
    cgroup_limit: Optional[MemoryLimit] = None
    try:
        cgroup_limit = _cgroup_memory_limit(cgroup_root, proc_self_cgroup)
    except Exception:
        logger.debug("Failed to read the cgroup memory limit", exc_info=True)

    host_bytes: Optional[int] = None
    try:
        host_bytes = int(psutil.virtual_memory().total)
    except Exception:
        logger.debug("Failed to read host memory size", exc_info=True)

    if cgroup_limit is not None and (
        host_bytes is None or (cgroup_limit.bytes or 0) < host_bytes
    ):
        return cgroup_limit
    if host_bytes is not None:
        return MemoryLimit(bytes=host_bytes, source="host")
    return MemoryLimit(bytes=None, source="unknown")
