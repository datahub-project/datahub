"""Locking and eviction for the node-local venv cache.

Split from venv_utils, which promises in its own docstring to work "without
performing any actual venv creation or management". Everything with a side
effect lives here.
"""

import fcntl
import logging
import os
import pathlib
import shutil
from typing import List, Optional, Tuple

from datahub.executor.execution.venv_utils import last_used_at

logger = logging.getLogger(__name__)


class EntryLock:
    """An flock on one cache entry.

    Sound because the cache is node-local: one pod is the only writer. The lock
    lives as long as the file descriptor, so a crashed executor releases every
    lock it held with its fds -- nothing needs cleaning up after a crash.

    Every failure path returns False rather than raising. The cache is an
    optimisation; a task that could have run must never fail because of it.
    """

    def __init__(self, lock_path: pathlib.Path) -> None:
        self._lock_path = lock_path
        self._fd: Optional[int] = None

    @property
    def held(self) -> bool:
        return self._fd is not None

    def acquire(self, *, exclusive: bool, blocking: bool = True) -> bool:
        mode = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        if not blocking:
            mode |= fcntl.LOCK_NB
        try:
            # The cache root does not exist on a fresh pod, and a first run
            # must create it rather than degrade: a missing directory is the
            # normal initial state, not a failure. Nothing else creates it --
            # eviction only reads the directory, and setup_venv reaches this
            # before anything has written to the cache.
            self._lock_path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self._lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        except OSError:
            logger.debug("venv cache: cannot open lock %s", self._lock_path)
            return False
        try:
            fcntl.flock(fd, mode)
        except OSError:
            # BlockingIOError for a contended LOCK_NB, and ENOLCK/EINVAL on a
            # filesystem without lock support. Both mean "no cache", not "fail".
            os.close(fd)
            return False
        self._fd = fd
        return True

    def downgrade_to_shared(self) -> None:
        """Convert an exclusive hold to shared on the same fd.

        Used by the build path: build under exclusive, then hold shared for the
        task's life so eviction stays out without shutting other runs out.
        """
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_SH)
        except OSError:
            logger.debug("venv cache: could not downgrade %s", self._lock_path)

    def release(self) -> None:
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            os.close(fd)
        except OSError:
            pass


def _entry_size(venv: pathlib.Path) -> int:
    """Nominal size of one entry: the sum of its files' sizes.

    NOT the bytes deleting it would reclaim. DataHub defaults uv to
    UV_LINK_MODE=hardlink, so most of a venv's files are hardlinks into uv's
    package cache and are shared with sibling entries -- removing this
    directory drops the directory entries, and the data survives as long as
    anything else links it.

    Deliberately not corrected for that. Over-counting makes the cache reach
    its budget sooner than real disk usage does, so eviction runs earlier than
    strictly necessary and actual usage stays under the configured ceiling;
    under-counting would be the direction that lets a disk fill. Deduplicating
    by inode would measure the cache more accurately and err the less safe way,
    and would still be wrong about links uv holds outside the cache -- which is
    why uv documents `uv cache prune` as a separate operation.

    The consequence to know: DATAHUB_VENV_CACHE_MAX_GB is a budget in nominal
    size, so it does not line up with `du` on the cache directory.
    """
    total = 0
    for dirpath, _dirnames, filenames in os.walk(venv):
        for name in filenames:
            try:
                total += os.lstat(os.path.join(dirpath, name)).st_size
            except OSError:
                continue
    return total


def evict_to_budget(cache_root: pathlib.Path, max_bytes: int) -> int:
    """Remove least-recently-used entries until the cache fits. Returns bytes freed.

    Ordered by the .datahub-venv-last-used marker, never filesystem atime --
    containers mount relatime or noatime, so atime is not a usable signal.

    An entry whose exclusive lock cannot be taken immediately is in use and is
    SKIPPED, never waited on: a build must not block behind an hours-long
    ingestion, and deleting a venv a running task is executing out of is worse
    than exceeding the budget.
    """
    try:
        entries = [p for p in cache_root.iterdir() if p.is_dir()]
    except OSError:
        return 0

    sized: List[Tuple[float, int, pathlib.Path]] = [
        (last_used_at(p), _entry_size(p), p) for p in entries
    ]
    total = sum(size for _, size, _ in sized)
    if total <= max_bytes:
        return 0

    freed = 0
    for _stamp, size, venv in sorted(sized, key=lambda row: row[0]):
        if total - freed <= max_bytes:
            break
        lock = EntryLock(venv.parent / f"{venv.name}.lock")
        if not lock.acquire(exclusive=True, blocking=False):
            logger.debug("venv cache: %s is in use, not evicting", venv)
            continue
        try:
            shutil.rmtree(venv)
            freed += size
            logger.info("venv cache: evicted %s (%d bytes)", venv, size)
        except OSError:
            logger.warning("venv cache: could not evict %s", venv, exc_info=True)
        finally:
            lock.release()
    return freed
