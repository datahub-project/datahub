"""Locking and eviction for the node-local venv cache.

Split from venv_utils, which promises in its own docstring to work "without
performing any actual venv creation or management". Everything with a side
effect lives here.
"""

import asyncio
import dataclasses
import enum
import errno
import fcntl
import functools
import logging
import os
import pathlib
import shutil
import time
from typing import TYPE_CHECKING, Optional, Union

from datahub.executor.common.env_config import (
    get_venv_cache_latest_ttl_sec,
    get_venv_cache_max_age_sec,
    get_venv_cache_max_entries,
)
from datahub.executor.execution.venv_utils import (
    COMPLETE_MARKER,
    ENTRY_PREFIX,
    CacheName,
    built_at,
    is_venv_complete,
    last_used_at,
    mark_venv_complete,
    touch_last_used,
    venv_location,
)

if TYPE_CHECKING:
    # Type-only: runner imports this module, so a runtime import is a cycle.
    from datahub.executor.execution.runner import SubprocessRunner
    from datahub.executor.execution.venv_config import VenvReference

logger = logging.getLogger(__name__)

# open() failures that clear by themselves. Everything else -- an unwritable
# or read-only cache root, a filesystem without flock -- stays broken, and
# retrying it only delays the fallback to a per-run venv.
_TRANSIENT_OPEN_ERRNOS = frozenset({errno.EMFILE, errno.ENFILE, errno.ENOMEM})


def entry_lock_path(venv_loc: pathlib.Path) -> pathlib.Path:
    """The lock file guarding one cache entry.

    Defined once because eviction and the build path must agree: eviction
    only skips an in-use entry if it takes the SAME lock the user of that
    entry holds. Two independent `parent / f"{name}.lock"` expressions were
    correct only by coincidence, and a change to either would have made
    eviction delete directories out from under running tasks.
    """
    return venv_loc.parent / f"{venv_loc.name}.lock"


class LockOutcome(enum.Enum):
    """Why an acquire did or did not succeed.

    Replaces a bool plus a separate `unusable` flag the caller had to read
    before the next acquire reset it -- two values that could disagree, and a
    second step that was easy to forget.

    CONTENDED is transient: someone holds the entry now and a retry can win
    it. UNAVAILABLE never resolves by waiting -- an unwritable cache root, or
    a filesystem with no flock support -- so the caller should stop retrying
    and fall back to a per-run venv.
    """

    ACQUIRED = "acquired"
    CONTENDED = "contended"
    UNAVAILABLE = "unavailable"

    @property
    def ok(self) -> bool:
        return self is LockOutcome.ACQUIRED


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

    def try_acquire(self, *, exclusive: bool) -> LockOutcome:
        """Take the lock, or report that it is not available right now.

        Always non-blocking, and there is deliberately no way to ask for
        otherwise. flock is a plain syscall, so a blocking acquire freezes the
        OS thread and with it the whole event loop the task runs on -- no
        in-loop timeout could even fire to rescue it. Waiting is the caller's
        job, as `await asyncio.sleep` between attempts.

        Raises if this lock is already held: acquiring twice overwrites
        `_fd`, and the kernel keeps that first hold until the descriptor
        closes, so the entry could never be taken exclusively again -- not by
        a rebuild, and not by eviction, which skips what it cannot take.
        `downgrade_to_shared` is the supported transition.
        """
        if self._fd is not None:
            raise RuntimeError(
                f"EntryLock({self._lock_path}) is already held; acquiring again "
                "would leak the current descriptor and pin the entry forever"
            )
        mode = (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB
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
            logger.debug(
                "venv cache: cannot open lock %s (%s)",
                self._lock_path,
                errno.errorcode.get(e.errno or 0, e.errno),
            )
            # Descriptor exhaustion is the one open() failure that clears on
            # its own, and this cache can cause it: every retained lock holds
            # an fd for the process's life. Calling that UNAVAILABLE would
            # make a transient condition permanent -- the caller stops
            # retrying, and _warn_cache_unavailable_once freezes the
            # operator's picture of a broken root for the rest of the pod.
            if e.errno in _TRANSIENT_OPEN_ERRNOS:
                return LockOutcome.CONTENDED
            return LockOutcome.UNAVAILABLE
        try:
            fcntl.flock(fd, mode)
        except BlockingIOError:
            os.close(fd)
            return LockOutcome.CONTENDED
        except OSError:
            # ENOLCK/EINVAL on a filesystem without lock support.
            os.close(fd)
            return LockOutcome.UNAVAILABLE
        self._fd = fd
        return LockOutcome.ACQUIRED

    @property
    def fileno(self) -> int:
        """The raw descriptor, for handing to a child via `pass_fds`.

        Raises if nothing is held: there is no meaningful "no lock" fd, and
        silently passing -1 would spawn a child that protects nothing.
        """
        if self._fd is None:
            raise RuntimeError("no lock is held")
        return self._fd

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
        """Give up this process's hold by CLOSING, never by LOCK_UN.

        The kernel releases an flock once every descriptor referring to that
        open file description is closed. Closing is therefore correct in
        both situations this lock can be in: when nothing else holds a copy
        the lock drops immediately, and when a child inherited one via
        pass_fds the child's hold survives -- which is exactly what should
        happen, because that child is executing out of the venv.

        LOCK_UN would not be correct in the second case. Duplicate
        descriptors share one description, so unlocking this copy releases
        the CHILD's lock too, leaving a live interpreter in a venv eviction
        is free to delete. Verified: with the child alive, a peer is refused
        after a close and granted after a LOCK_UN. There is no case where
        this class wants LOCK_UN, so it is not used anywhere.
        """
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            os.close(fd)
        except OSError:
            pass


# One pass at a time per cache root. Each pass reads the whole root and then
# deletes; without this, concurrent passes each snapshot the same over-limit
# count and each free the full deficit. See evict_stale_entries.
_CACHE_LOCK_ATTEMPTS = 10
_CACHE_LOCK_RETRY_SEC = 0.15

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

    Two things about WHEN this runs, both of which affect what an operator
    should expect on disk:

    - Only on the build path. A cache taking nothing but hits never trims,
      which is what keeps a warm hit down to one flock and one stat. Space
      is reclaimed when a new venv is built, not as time passes.
    - Before the new venv is created, so the pass only counts entries
      already on disk. The peak is therefore max_entries + 1, not
      max_entries: the build that trims the cache then adds to it.

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
    if not pass_lock.try_acquire(exclusive=True).ok:
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
    # In-use entries DO count toward the limit: they occupy disk, which is
    # what the limit bounds. The consequence to know is that a node running
    # more concurrent tasks than DATAHUB_VENV_CACHE_MAX_ENTRIES will churn --
    # each build evicts a warm idle entry to make room the locked ones are
    # holding, and the hit rate falls. That is a misconfiguration to
    # surface, not a case to silently exempt: exempting them would let the
    # cache grow past the bound an operator sized their volume against.
    remaining = len(entries)
    in_use = 0
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
        else:
            in_use += 1
    if in_use >= max_entries:
        logger.warning(
            "venv cache: %d of %d entries are in use, at or above the limit "
            "of %d, so this pass could not free the space it needed. Raise "
            "DATAHUB_VENV_CACHE_MAX_ENTRIES above the node's concurrency or "
            "the cache will churn.",
            in_use,
            len(entries),
            max_entries,
        )
    return evicted


def _remove_entry(venv: pathlib.Path) -> bool:
    """Take the entry exclusively and delete it. False when it is in use."""
    lock = EntryLock(entry_lock_path(venv))
    if not lock.try_acquire(exclusive=True).ok:
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
        # Marker first, and deliberately NOT restored if the rmtree then
        # fails. Restoring it would rescue a healthy-but-undeletable venv --
        # a root-owned file, EBUSY, an NFS silly-rename -- but there is no
        # cheap way to tell that case apart from a PARTIAL removal, which
        # leaves bin/python and the marker while site-packages is gone.
        # Serving that husk as complete is a silent ModuleNotFoundError on
        # every later run; losing a rebuild is not. The undeletable
        # directory is logged as a warning below so it is at least visible.
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


@dataclasses.dataclass
class CacheStats:
    """Counters an operator can read to tell a working cache from a dead one.

    The cache degrades silently on purpose, which makes it invisible: without
    these, a 90% hit rate and 0% look identical from outside. Kept here as
    plain counters rather than emitted directly -- the Prometheus helper lives
    in the executor service, which depends on this package and not the other
    way round, so it reads these and emits them under its own registry.
    """

    hits: int = 0
    builds: int = 0
    stale_served: int = 0
    evicted: int = 0
    fallbacks: dict = dataclasses.field(default_factory=dict)

    def record_fallback(self, reason: str) -> None:
        self.fallbacks[reason] = self.fallbacks.get(reason, 0) + 1
        logger.info("venv cache: falling back to a per-run venv (%s)", reason)

    def snapshot(self) -> dict:
        return {
            "hits": self.hits,
            "builds": self.builds,
            "stale_served": self.stale_served,
            "evicted": self.evicted,
            "fallbacks": dict(self.fallbacks),
        }


STATS = CacheStats()


@dataclasses.dataclass(frozen=True)
class UncachedVenv:
    """A per-run venv under tmp_dir: never locked, never published.

    Either the version is not cacheable, or the cache could not be used for
    this call. `reason` says which, and reaches the run's own log.
    """

    venv_loc: pathlib.Path
    reason: str
    lock: None = None


@dataclasses.dataclass(frozen=True)
class CacheHit:
    """A complete, fresh entry held SHARED. The caller builds nothing."""

    venv_loc: pathlib.Path
    lock: EntryLock


@dataclasses.dataclass(frozen=True)
class CacheBuild:
    """An entry held EXCLUSIVE that the caller must build and then publish."""

    venv_loc: pathlib.Path
    lock: EntryLock


# Distinct types rather than one record with two booleans: the old shape
# allowed combinations that cannot occur -- an uncached entry holding a lock,
# or a "ready" entry that still had to be built -- and every reader had to
# know which pairs were real.
CacheEntry = Union[UncachedVenv, CacheHit, CacheBuild]


@functools.lru_cache(maxsize=None)
def _warn_cache_unavailable_once(cache_root: str) -> None:
    """Warn that the cache root cannot be used -- once per root, not per task.

    The lru_cache IS the once-ness: this runs on every task in a long-lived
    pod, and a broken cache root stays broken, so logging per call would emit
    the same line for the pod's lifetime.
    """
    logger.warning(
        "venv cache unavailable at %s (unwritable, or a filesystem without "
        "flock support); falling back to per-run venvs",
        cache_root,
    )


def _is_fresh_hit(venv_loc: pathlib.Path, *, moving: bool) -> bool:
    """Whether this entry may be served, rather than merely being complete.

    Completeness alone is the right question for a pinned version: the name
    is a content address, so an entry that finished building is the right
    bytes forever.

    It is not the right question for a moving version, and `latest` is the
    default for every recipe. With no age bound the hit path never
    re-resolves: a pod that starts on Monday keeps executing Monday's
    acryl-datahub until it restarts, so a connector fix published on Tuesday
    silently never arrives -- and because test-connection deliberately shares
    the entry, re-testing after the fix reports the old behaviour too. That is
    the "I shipped the fix, the customer re-ran, still broken" ticket.

    Measured from BUILD time, not last use. touch_last_used fires on every
    hit, so an age taken from it would make the busiest entry -- the shared
    `latest` one -- the one thing that never expires.
    """
    if not is_venv_complete(venv_loc):
        return False
    if not moving:
        return True
    age = time.time() - built_at(venv_loc)
    ttl = get_venv_cache_latest_ttl_sec()
    if age <= ttl:
        return True
    logger.info(
        "venv cache: %s was built from a moving version %.1f hours ago "
        "(TTL %.1f hours); rebuilding so it re-resolves.",
        venv_loc.name,
        age / 3600,
        ttl / 3600,
    )
    return False


def _discard_incomplete(venv_loc: pathlib.Path) -> bool:
    """Clear a half-built entry so a build can start clean. False if it survives.

    Only safe under an EXCLUSIVE hold on the entry.

    The marker goes first, so a removal that fails part-way leaves something
    that reads as incomplete rather than a husk the hit path would serve.

    Errors are NOT ignored, which is the whole point. `ignore_errors=True`
    makes a completely failed discard indistinguishable from a successful
    one, and the build that follows then either dies forever on that key --
    `uv venv` on a surviving non-venv directory fails hard with "exists, but
    it's not a virtual environment" -- or, if pyvenv.cfg happened to survive,
    recreates the venv over the leftovers and gets stamped COMPLETE, so every
    later run reuses a venv that may mix two builds' site-packages and
    nothing on the hit path re-validates. The causes are ordinary: an NFS
    .nfsXXXX silly-rename, a uid mismatch on a shared volume, a read-only
    remount. Reporting the failure lets the caller use a per-run venv, which
    is slower and always correct.
    """
    try:
        (venv_loc / COMPLETE_MARKER).unlink(missing_ok=True)
        shutil.rmtree(venv_loc)
        return True
    except OSError:
        logger.warning(
            "Could not discard the incomplete venv at %s; falling back to a "
            "per-run venv. Clear this directory to re-enable the cache entry.",
            venv_loc,
            exc_info=True,
        )
        return False


async def _acquire_cache_entry(
    cache_name: CacheName, tmp_dir: pathlib.Path
) -> CacheEntry:
    """Resolve a dynamic venv against the cache and take the lock guarding it.

    NO PATH HERE MAY BLOCK INDEFINITELY. flock() is a synchronous syscall, so
    a blocking acquire inside this coroutine freezes the OS thread and with it
    the entire event loop the task runs on -- not even an in-loop timeout could
    fire to rescue it. Every acquire below is non-blocking and all waiting is
    an `await asyncio.sleep`.

    The protocol, in order:

    1. SHARED, non-blocking. A complete, fresh entry is served immediately.
       This is the warm hit, it is the common case, and it must never wait: a
       running task holds its entry SHARED for the whole run, so taking
       EXCLUSIVE here would make two recipes on the same `latest` entry --
       the default, and therefore the norm -- serialize behind the longer one.
    2. Otherwise a build is needed, and building needs EXCLUSIVE. Retry
       non-blocking on a short budget, re-attempting the shared hit each pass:
       a peer that finishes its build downgrades to SHARED and keeps it, so an
       exclusive-only retry could never succeed again once it lost the race.
    3. Whenever EXCLUSIVE is won, re-check INSIDE the lock -- another process
       may have finished building while we waited.
    4. When the budget runs out, someone else is building. Fall back to a
       per-run venv rather than waiting on them.

    Everything inside an EXCLUSIVE hold runs under a handler that releases it.
    This function is called ABOVE setup_venv's own `try`, whose
    `except BaseException` exists precisely to release the lock, so anything
    raised here -- a bad DATAHUB_VENV_CACHE_* value, an eviction pass hitting
    an unreadable root -- would otherwise escape with the entry still locked.
    That key could then never be built, hit or evicted again for the life of
    the process, and every task on it would fall back to a full per-run build.
    """
    venv_name, cacheable, moving = (
        cache_name.name,
        cache_name.cacheable,
        cache_name.moving,
    )
    venv_loc = pathlib.Path(venv_location(venv_name, str(tmp_dir), cacheable=cacheable))
    if not cacheable:
        return UncachedVenv(venv_loc, reason="version is not cacheable")

    def per_run_fallback(reason: str) -> UncachedVenv:
        # Every fallback names itself. The cache degrades silently by design,
        # which left an operator unable to tell a 90% hit rate from 0%: the
        # run log only ever said "Creating new venv", with no hint that the
        # cache had been skipped or why.
        STATS.record_fallback(reason)
        return UncachedVenv(
            pathlib.Path(venv_location(venv_name, str(tmp_dir), cacheable=False)),
            reason=reason,
        )

    lock = EntryLock(entry_lock_path(venv_loc))
    # A complete entry that is only past its TTL. Worth remembering: if no
    # attempt ever wins EXCLUSIVE to refresh it, serving the stale venv beats
    # the per-run build that would otherwise be the fallback. See below.
    stale_but_complete = False
    last = LockOutcome.CONTENDED
    for attempt in range(_CACHE_LOCK_ATTEMPTS):
        last = lock.try_acquire(exclusive=False)
        if last.ok:
            # Guarded like the exclusive branch below. _is_fresh_hit reaches
            # Path.exists(), which re-raises EACCES -- an entry made
            # unreadable by a uid mismatch on a shared volume, or a
            # part-removed tree. This runs ABOVE setup_venv's own try, so an
            # escape here would leak the SHARED hold, and EntryLock has no
            # __del__: every later run on the key would raise again and leak
            # another fd.
            try:
                if _is_fresh_hit(venv_loc, moving=moving):
                    touch_last_used(venv_loc)
                    STATS.hits += 1
                    return CacheHit(venv_loc, lock)
                stale_but_complete = is_venv_complete(venv_loc)
            except BaseException:
                lock.release()
                raise
            # Nothing there yet, a build killed midway, or a moving entry past
            # its TTL. Either way this call has to build, and building needs
            # the entry exclusively.
            lock.release()
        elif last is LockOutcome.UNAVAILABLE:
            break

        last = lock.try_acquire(exclusive=True)
        if last.ok:
            try:
                if _is_fresh_hit(venv_loc, moving=moving):
                    touch_last_used(venv_loc)
                    # flock has no atomic downgrade -- the kernel drops the
                    # exclusive hold before it looks for conflicts -- so a
                    # peer granted EXCLUSIVE in that window leaves us with
                    # nothing. Re-taking SHARED outright is the recovery;
                    # returning a "ready" entry with no lock is not, because
                    # the caller then runs a child out of a directory the
                    # next eviction pass is free to rmtree.
                    if (
                        lock.downgrade_to_shared()
                        or lock.try_acquire(exclusive=False).ok
                    ):
                        # Re-validate: the hold was momentarily gone either
                        # way, and _remove_entry deliberately leaves the
                        # .lock file behind, so a competing eviction can have
                        # deleted the directory while the lock we just took
                        # survived. Without this the child is spawned against
                        # <venv>/bin/python and dies with FileNotFoundError.
                        if is_venv_complete(venv_loc):
                            return CacheHit(venv_loc, lock)
                        lock.release()
                    return per_run_fallback("lost the hold after downgrade")
                # A directory here is a build killed midway, or a moving
                # entry past its TTL. Either way it must be removed rather
                # than built on top of, and if it cannot be removed this key
                # is unusable until an operator clears it.
                #
                # Deliberately NOT on a worker thread, unlike the eviction
                # pass below. This deletes the directory THIS coroutine's
                # lock protects, and awaiting makes the wait cancellable:
                # a cancellation would unwind into the handler below,
                # release the flock, and leave the worker still rmtree-ing
                # while a peer takes the same key and starts `uv venv` into
                # the directory being deleted. Blocking the loop for one
                # half-built venv's unlinks is the lesser cost. (Eviction is
                # safe to thread because each victim is protected by a lock
                # the worker itself takes and releases.)
                if venv_loc.exists() and not _discard_incomplete(venv_loc):
                    lock.release()
                    return per_run_fallback("could not discard an incomplete entry")
                # Eviction runs here and nowhere else: on the build path only,
                # and after we hold this entry, so it cannot select the
                # directory we are about to write into (it skips anything it
                # cannot take exclusively). Also off the loop, and for the
                # same reason -- an age pass can select every entry at once.
                await asyncio.to_thread(
                    evict_stale_entries,
                    venv_loc.parent,
                    max_entries=get_venv_cache_max_entries(),
                    max_age_sec=get_venv_cache_max_age_sec(),
                )
            except BaseException:
                lock.release()
                raise
            STATS.builds += 1
            return CacheBuild(venv_loc, lock)
        if last is LockOutcome.UNAVAILABLE:
            break

        if attempt + 1 < _CACHE_LOCK_ATTEMPTS:
            await asyncio.sleep(_CACHE_LOCK_RETRY_SEC)

    if last is LockOutcome.UNAVAILABLE:
        _warn_cache_unavailable_once(str(venv_loc.parent))
        return per_run_fallback("cache root unusable")

    # Stale-while-in-use. Refreshing an expired moving entry needs EXCLUSIVE,
    # and every in-flight run holds the same entry SHARED for its whole life
    # -- and `latest` is the default, so essentially every run shares one
    # entry. On a pod whose runs overlap continuously there may be no instant
    # inside this budget with zero holders, so the refresh never happens.
    # Falling through to a per-run build there would make the TTL strictly
    # worse than not having one: every task would pay a full build forever,
    # which is the cost this cache exists to remove. Serving the stale venv
    # is the lesser evil, and the refresh still happens on the first attempt
    # that finds the entry idle.
    if stale_but_complete and lock.try_acquire(exclusive=False).ok:
        # Guarded for the same reason the two branches above are: this sits
        # ABOVE setup_venv's own try, and is_venv_complete reaches
        # Path.exists(), which re-raises EACCES -- stranding the hold and
        # its fd for the life of the process.
        try:
            still_there = is_venv_complete(venv_loc)
        except BaseException:
            lock.release()
            raise
        if still_there:
            touch_last_used(venv_loc)
            STATS.stale_served += 1
            logger.info(
                "venv cache entry %s is past its TTL but in use by another "
                "run, so it cannot be rebuilt right now; serving it as-is",
                venv_loc.name,
            )
            return CacheHit(venv_loc, lock)
        lock.release()

    return per_run_fallback("entry held by another build")


def _resolve_existing_venv(entry: CacheEntry, runner: "SubprocessRunner") -> bool:
    """Whether the entry can be returned as-is, without building anything.

    A cache hit is already decided -- in _acquire_cache_entry, under the lock,
    which is the only place is_venv_complete() can be trusted, and so is
    discarding a half-built entry: both need the EXCLUSIVE hold that only
    exists there. What is left here is the non-cached legacy check, where the
    interpreter's presence alone is enough since certain systems clean up
    files in temp directories but not the directories themselves.
    """
    venv_loc = entry.venv_loc
    if isinstance(entry, CacheHit):
        runner._logs.append(f"Reusing cached venv at {venv_loc}.\n")
        return True

    if isinstance(entry, UncachedVenv):
        runner._logs.append(f"Not using the venv cache: {entry.reason}.\n")
        if venv_loc.exists() and (venv_loc / "bin/python").exists():
            runner._logs.append(f"venv at {venv_loc} already exists, skipping setup.\n")
            return True
    return False


def _publish_cache_entry(
    venv_reference: "VenvReference", venv_loc: pathlib.Path
) -> None:
    """Make a freshly built entry reusable, and keep holding it for this run.

    No-op for an uncached venv, which has no lock and nothing to publish.
    """
    if venv_reference.lock is None:
        return

    # LAST, so a build killed before this point leaves an entry that fails
    # is_venv_complete() and is rebuilt rather than reused empty.
    try:
        mark_venv_complete(venv_loc)
    except OSError:
        # The cache is an optimisation: a full or read-only cache filesystem
        # must not fail a build that otherwise succeeded. An unmarked venv just
        # looks incomplete and gets rebuilt next time -- the same degradation
        # touch_last_used already accepts.
        #
        # WARNING rather than DEBUG, because the degradation is not local to
        # this run. This entry stays locked SHARED for the task's whole life,
        # so for as long as that lasts every peer on the same key pays the
        # full retry budget, finds the entry incomplete, cannot take it
        # exclusively, and rebuilds per-run. A cache that has silently stopped
        # caching should be visible without turning on debug logging.
        logger.warning(
            "Could not mark venv complete at %s; this entry will be rebuilt "
            "rather than reused, and peers on the same key will fall back to "
            "per-run venvs while this task holds it.",
            venv_loc,
            exc_info=True,
        )
    touch_last_used(venv_loc)

    # flock has no atomic downgrade, so the exclusive hold is already gone by
    # the time the conversion is refused. Re-requesting SHARED outright is the
    # recovery and nearly always succeeds -- the window is sub-second.
    if (
        venv_reference.lock.downgrade_to_shared()
        or venv_reference.lock.try_acquire(exclusive=False).ok
    ):
        return

    # Nothing left to hold. Carrying the object would only let
    # finalize_task_output release a lock nobody has, and would report the
    # entry as protected when eviction is free to take it. The venv itself is
    # complete and this task is about to run out of it, so the honest state is
    # "usable, unprotected" -- said out loud, because an eviction pass landing
    # here kills the child with an ImportError on a deleted file.
    logger.warning(
        "Lost the venv cache hold on %s after building it; this run continues "
        "against an entry eviction may reclaim.",
        venv_loc,
    )
    venv_reference.lock = None


class VenvCache:
    """The node-local venv cache: entry lifecycle, in one place.

    `acquire` resolves a name to an entry and takes the lock guarding it;
    `publish` marks a built entry reusable. setup_venv is left doing install
    steps only, which is the point -- the policy used to be spread through it.
    """

    def __init__(self, tmp_dir: pathlib.Path) -> None:
        self._tmp_dir = tmp_dir

    async def acquire(self, cache_name: CacheName) -> CacheEntry:
        return await _acquire_cache_entry(cache_name, self._tmp_dir)

    def publish(self, venv_reference: "VenvReference", venv_loc: pathlib.Path) -> None:
        _publish_cache_entry(venv_reference, venv_loc)

    @staticmethod
    def resolve_existing(entry: CacheEntry, runner: "SubprocessRunner") -> bool:
        return _resolve_existing_venv(entry, runner)

    @staticmethod
    def stats() -> dict:
        return STATS.snapshot()
