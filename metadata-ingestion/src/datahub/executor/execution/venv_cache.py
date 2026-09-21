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

from datahub.executor.execution.venv_utils import (
    COMPLETE_MARKER,
    ENTRY_PREFIX,
    is_venv_complete,
    last_used_at,
)

# Cached answer to "how big is this entry", written beside the entry itself.
# Safe only because a COMPLETE entry never changes again -- see _entry_size.
SIZE_MARKER = ".datahub-venv-size"

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
        self._unusable = False

    @property
    def held(self) -> bool:
        return self._fd is not None

    @property
    def unusable(self) -> bool:
        """Whether the last failed acquire() failed for a reason retrying cannot fix.

        False after a merely CONTENDED non-blocking acquire -- someone else
        holds the entry, and waiting can still win it. True when the lock file
        could not be opened at all (an unwritable or read-only cache root) or
        the filesystem does not support flock: retrying those only wastes the
        caller's time before it falls back to a per-run venv.
        """
        return self._unusable

    def acquire(self, *, exclusive: bool, blocking: bool = True) -> bool:
        mode = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        if not blocking:
            mode |= fcntl.LOCK_NB
        self._unusable = False
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
            self._unusable = True
            return False
        try:
            fcntl.flock(fd, mode)
        except BlockingIOError:
            # A contended LOCK_NB. Someone else holds the entry right now,
            # which is transient -- the caller may retry.
            os.close(fd)
            return False
        except OSError:
            # ENOLCK/EINVAL on a filesystem without lock support. Means "no
            # cache", not "fail", and never resolves by waiting.
            os.close(fd)
            self._unusable = True
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


def _measure_entry(venv: pathlib.Path) -> int:
    """Sum the entry's apparent file sizes, with one directory read per level.

    os.scandir carries the stat from the directory read, so this costs one
    syscall per directory rather than one per FILE. That is the whole
    difference: the os.walk plus lstat version this replaces took 0.97s over
    four entries where this takes 0.15s, for a byte-identical answer, because
    a venv is tens of thousands of small files.
    """
    total = 0
    stack = [str(venv)]
    while stack:
        try:
            with os.scandir(stack.pop()) as it:
                for entry in it:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(entry.path)
                        else:
                            total += entry.stat(follow_symlinks=False).st_size
                    except OSError:
                        continue
        except OSError:
            continue
    return total


def _entry_size(venv: pathlib.Path) -> int:
    """Nominal size of one entry, measured once and remembered.

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

    The measurement is cached in a file beside the entry, which is sound only
    because a COMPLETE entry is immutable: nothing writes into it after the
    completion marker goes on, so its size cannot change. An incomplete entry
    is measured fresh every time and never remembered -- it is mid-build, and
    remembering a partial size would under-report exactly the entry about to
    grow. That matters because eviction reads every entry on every build, so
    without this the cost is paid again and again for answers that cannot have
    changed.
    """
    hint = venv / SIZE_MARKER
    complete = is_venv_complete(venv)
    if complete:
        try:
            return int(hint.read_text())
        except (OSError, ValueError):
            pass
    total = _measure_entry(venv)
    if complete:
        try:
            hint.write_text(str(total))
        except OSError:
            # A read-only or full filesystem costs a re-measure next time, and
            # nothing else. Never fail eviction over a cache of a cache.
            logger.debug("venv cache: could not record size for %s", venv)
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
        # Only directories this cache created. DATAHUB_VENV_CACHE_PATH is an
        # operator knob returned verbatim by get_venv_cache_path, so the root
        # may be a directory that already holds other things -- a volume mount,
        # or /tmp. Without this filter the first build to cross the budget
        # rmtree's whatever it finds there.
        entries = [
            p
            for p in cache_root.iterdir()
            if p.is_dir() and p.name.startswith(ENTRY_PREFIX)
        ]
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
            # Invalidate before removing. rmtree raises on the FIRST failure,
            # having already deleted an arbitrary prefix of the tree, and
            # traversal order is not defined -- so site-packages can be gone
            # while bin/python and the completion marker survive.
            # is_venv_complete accepts that husk, nothing on the hit path
            # re-validates, and every later run for that key "reuses" a venv
            # with no packages and dies with ModuleNotFoundError. Dropping the
            # marker first makes a partial removal self-invalidating: the next
            # claimant discards and rebuilds it instead.
            (venv / COMPLETE_MARKER).unlink(missing_ok=True)
            shutil.rmtree(venv)
            freed += size
            logger.info("venv cache: evicted %s (%d bytes)", venv, size)
        except OSError:
            logger.warning("venv cache: could not evict %s", venv, exc_info=True)
        finally:
            lock.release()
    return freed
