"""Locking and eviction for the node-local venv cache.

Split from venv_utils, which promises in its own docstring to work "without
performing any actual venv creation or management". Everything with a side
effect lives here.
"""

import errno
import fcntl
import logging
import os
import pathlib
import shutil
import time
from typing import Dict, Optional

from datahub.executor.execution.venv_utils import (
    COMPLETE_MARKER,
    ENTRY_PREFIX,
    last_used_at,
)

logger = logging.getLogger(__name__)

# open() failures that clear by themselves. Everything else -- an unwritable
# or read-only cache root, a filesystem without flock -- stays broken, and
# retrying it only delays the fallback to a per-run venv.
_TRANSIENT_OPEN_ERRNOS = frozenset({errno.EMFILE, errno.ENFILE, errno.ENOMEM})


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
    def lock_path(self) -> pathlib.Path:
        return self._lock_path

    @property
    def held(self) -> bool:
        return self._fd is not None

    @property
    def unusable(self) -> bool:
        """Whether the last failed acquire() failed for a reason retrying cannot fix.

        False after a merely CONTENDED acquire -- someone else
        holds the entry, and waiting can still win it. True when the lock file
        could not be opened at all (an unwritable or read-only cache root) or
        the filesystem does not support flock: retrying those only wastes the
        caller's time before it falls back to a per-run venv.
        """
        return self._unusable

    def acquire(self, *, exclusive: bool) -> bool:
        """Take the lock, or report that it is not available right now.

        Always non-blocking, and there is deliberately no way to ask for
        otherwise. flock is a plain syscall, so a blocking acquire freezes the
        OS thread and with it the whole event loop the task runs on -- no
        in-loop timeout could even fire to rescue it. Waiting is the caller's
        job, as `await asyncio.sleep` between attempts.
        """
        mode = (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB
        self._unusable = False
        try:
            # The cache root does not exist on a fresh pod, and a first run
            # must create it rather than degrade: a missing directory is the
            # normal initial state, not a failure. Nothing else creates it --
            # eviction only reads the directory, and setup_venv reaches this
            # before anything has written to the cache.
            self._lock_path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self._lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        except OSError as e:
            # Descriptor exhaustion is the one open() failure that clears on
            # its own, and it is the one this cache can cause: every retained
            # lock holds an fd for the life of the process. Marking it
            # `unusable` would make a transient condition permanent -- the
            # caller breaks out of its retry loop, and
            # _warn_cache_unavailable_once's lru_cache freezes the operator's
            # picture of a "broken" cache root for the rest of the pod's life.
            self._unusable = e.errno not in _TRANSIENT_OPEN_ERRNOS
            logger.debug(
                "venv cache: cannot open lock %s (%s)",
                self._lock_path,
                errno.errorcode.get(e.errno or 0, e.errno),
            )
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

    def downgrade_to_shared(self) -> bool:
        """Convert an exclusive hold to shared on the same fd.

        Used by the build path: build under exclusive, then hold shared for the
        task's life so eviction stays out without shutting other runs out.

        Returns whether the entry is still protected afterwards. False means
        this lock now guards nothing and the caller must stop claiming it does.

        Non-blocking, because the conversion can genuinely wait. flock has no
        atomic downgrade: the kernel removes the existing lock and only then
        looks for conflicts, so the entry is briefly unlocked and a competitor
        EXCLUSIVE can be granted in that window. A blocking request would then
        sit on somebody else's build or rmtree -- on the event loop thread,
        since flock is a plain syscall -- which is exactly what
        _acquire_cache_entry forbids.

        That same non-atomicity is why failure has to be reported rather than
        logged. The exclusive hold is already gone by the time the conversion
        fails, so a caller that carried on would hold an fd protecting nothing
        while eviction was free to delete the venv its task is executing from.
        """
        if self._fd is None:
            return False
        try:
            fcntl.flock(self._fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
            return True
        except OSError:
            # Deliberately not retried. A competitor holds this entry
            # EXCLUSIVE -- a reader would not have conflicted -- so it is a
            # peer build or an eviction, and neither clears fast enough to spin
            # on. Sleeping is not an option either: this is a sync method on
            # the event loop thread.
            logger.warning(
                "venv cache: lost the hold on %s while downgrading it to "
                "shared; this run continues against an entry eviction is free "
                "to reclaim.",
                self._lock_path,
            )
            self.release()
            return False

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


# Locks deliberately kept for the life of the process, keyed by lock path so
# the same entry is only ever retained once. See retain_lock.
_RETAINED_LOCKS: Dict[str, EntryLock] = {}


def retain_lock(lock: EntryLock) -> None:
    """Keep this entry locked for the rest of the process, at a bounded cost.

    Callers reach this when they must NOT release: a child may still be
    executing out of the venv, and releasing would let the next build's
    evict_stale_entries rmtree a running interpreter.

    Simply dropping the EntryLock reference achieves the lock part and
    nothing else. The object owns a raw os.open fd and has no __del__, so
    each detach adds a descriptor on the same lock file -- one per EVENT, not
    one per entry, and test-connection reaches it on essentially every
    cancellation because the child is signalled without being waited for.
    Descriptors then accumulate for the pod's life until os.open raises
    EMFILE, at which point unrelated sockets and subprocesses start failing
    too.

    Keying on the lock path collapses that back to the cost the callers
    actually reason about -- one unevictable entry, one descriptor -- because
    a second hold on an entry already retained protects nothing the first
    does not.
    """
    if not lock.held:
        return
    key = str(lock.lock_path)
    existing = _RETAINED_LOCKS.get(key)
    if existing is not None and existing.held and existing is not lock:
        # Already protected for the life of the process. This hold is
        # redundant, so give its descriptor back rather than stacking it.
        lock.release()
        return
    _RETAINED_LOCKS[key] = lock


# One pass at a time per cache root. Each pass reads the whole root and then
# deletes; without this, concurrent passes each snapshot the same over-limit
# count and each free the full deficit. See evict_stale_entries.
EVICT_LOCK_NAME = ".datahub-venv-evict.lock"


def evict_stale_entries(
    cache_root: pathlib.Path, *, max_entries: int, max_age_sec: float
) -> int:
    """Trim the cache to `max_entries`, dropping anything unused for
    `max_age_sec` first. Returns how many entries were removed.

    Bounded by COUNT and AGE rather than by bytes, which is a deliberate
    trade. Sizing the cache in bytes means measuring it, and measuring a venv
    means walking tens of thousands of small files -- per entry, on every
    build, because the answer is needed before anything can be deleted. On a
    real datahub venv (~48k files) that is seconds per entry, and it bought a
    number that did not correspond to disk anyway: DataHub defaults uv to
    UV_LINK_MODE=hardlink, so most of a venv is links into uv's package cache
    and deleting the directory reclaims almost nothing. A count is one stat
    per entry, and an operator can check it with `ls`.

    What it gives up: entries vary from a few hundred MB to a few GB, so a
    count does not bound disk tightly. DATAHUB_VENV_CACHE_MAX_ENTRIES should
    be set against the largest connector a node runs.

    Ordered by the .datahub-venv-last-used marker, never filesystem atime --
    containers mount relatime or noatime, so atime is not a usable signal.

    An entry whose exclusive lock cannot be taken immediately is in use and is
    SKIPPED, never waited on: a build must not block behind an hours-long
    ingestion, and deleting a venv a running task is executing out of is worse
    than exceeding the limit.
    """
    # One pass at a time. DefaultExecutor gives each task its own thread and
    # event loop, so several builds on different keys reach this within a
    # second of each other. The per-entry flock only stops two passes picking
    # the SAME victim; without a pass lock each one independently measures the
    # same overshoot and frees it in full, so N passes evict N times what was
    # needed and warm entries well inside the limit are destroyed. Declining
    # is right rather than merely cheap: the peer holding this is doing this
    # call's work, and waiting would mean a blocking flock on the event loop.
    pass_lock = EntryLock(cache_root / EVICT_LOCK_NAME)
    if not pass_lock.acquire(exclusive=True):
        logger.debug("venv cache: another eviction pass is running, skipping")
        return 0
    try:
        return _evict_locked(
            cache_root, max_entries=max_entries, max_age_sec=max_age_sec
        )
    finally:
        pass_lock.release()


def _evict_locked(
    cache_root: pathlib.Path, *, max_entries: int, max_age_sec: float
) -> int:
    try:
        # Only directories this cache created. DATAHUB_VENV_CACHE_PATH is an
        # operator knob returned verbatim by get_venv_cache_path, so the root
        # may be a directory that already holds other things -- a volume mount,
        # or /tmp. Without this filter the first build to cross the limit
        # rmtree's whatever it finds there.
        entries = [
            p
            for p in cache_root.iterdir()
            if p.is_dir() and p.name.startswith(ENTRY_PREFIX)
        ]
    except OSError:
        return 0

    now = time.time()
    oldest_first = sorted(entries, key=last_used_at)

    # One walk, oldest first, applying both rules at once: an entry goes if
    # nothing has used it in max_age_sec, or if the cache is still over its
    # count. Counting down as we go -- rather than slicing a fixed victim list
    # up front -- is what lets the pass keep going past an entry it could not
    # take: a locked entry is in use, and skipping it must not stop the cache
    # being trimmed, only spare that one venv.
    remaining = len(entries)
    evicted = 0
    for venv in oldest_first:
        too_old = now - last_used_at(venv) > max_age_sec
        if not too_old and remaining <= max_entries:
            # Oldest first, so every entry after this one is younger and the
            # count only shrinks. Nothing further can qualify.
            break
        if _remove_entry(venv):
            evicted += 1
            remaining -= 1
    return evicted


def _remove_entry(venv: pathlib.Path) -> bool:
    """Take the entry exclusively and delete it. False when it is in use."""
    lock = EntryLock(venv.parent / f"{venv.name}.lock")
    if not lock.acquire(exclusive=True):
        logger.debug("venv cache: %s is in use, not evicting", venv)
        return False
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
        # The .lock file beside it is deliberately left behind. Unlinking
        # it would break mutual exclusion rather than tidy up: a peer
        # already holding it holds an fd on that inode, so the next two
        # claimants would create a NEW inode and flock a different file
        # from the peer -- two processes each believing they hold the
        # entry. The files are empty and bounded by the number of distinct
        # cache keys, so the inodes are the cheaper side of that trade.
        logger.info("venv cache: evicted %s", venv)
        return True
    except OSError:
        logger.warning("venv cache: could not evict %s", venv, exc_info=True)
        return False
    finally:
        lock.release()
