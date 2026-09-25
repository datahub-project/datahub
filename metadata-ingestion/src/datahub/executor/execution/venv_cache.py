"""Locking and eviction for the node-local venv cache.

Split from venv_utils, which promises in its own docstring to work "without
performing any actual venv creation or management". Everything with a side
effect lives here.
"""

import asyncio
import contextlib
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
from typing import TYPE_CHECKING, Callable, Iterator, Optional, Union

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


def _fd_is_file_at(fd: int, path: pathlib.Path) -> bool:
    """Whether `fd` still refers to the file currently at `path`.

    False once the path has been unlinked or replaced: the descriptor is
    then a hold on an inode nobody else can reach, which excludes nothing.
    """
    try:
        held = os.fstat(fd)
        current = os.stat(path)
    except OSError:
        return False
    return (held.st_ino, held.st_dev) == (current.st_ino, current.st_dev)


class RemovalOutcome(enum.Enum):
    """Why an entry was or was not evicted.

    REMOVED and IN_USE used to share `False` with FAILED, so an undeletable
    directory counted as in use -- and the churn warning then told an
    operator to raise MAX_ENTRIES over a root-owned file or an EBUSY mount,
    which does nothing.
    """

    REMOVED = "removed"
    IN_USE = "in_use"
    FAILED = "failed"


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
        # The hold is only meaningful if this descriptor is still the file
        # AT this path. A sweep may have unlinked it between our open() and
        # our flock(), in which case the next claimant creates a new inode
        # and flocks a different file -- two processes each believing they
        # hold the entry. Re-checking here is what lets _remove_entry unlink
        # the lock at all; without it the file had to be kept forever.
        if not _fd_is_file_at(fd, self._lock_path):
            os.close(fd)
            return LockOutcome.CONTENDED
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

        flock has no atomic downgrade: the kernel drops the existing lock
        and only then looks for conflicts, so the entry is briefly unlocked
        and a competing EXCLUSIVE can win that window. Two consequences, and
        both matter:

        - Non-blocking. A blocking request would sit on somebody else's
          build or rmtree, on the event loop thread.
        - Failure must be REPORTED, not logged: the exclusive hold is
          already gone, so a caller that carried on would hold an fd
          protecting nothing while eviction deleted the venv under its task.
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

        The kernel drops an flock once every descriptor for that open file
        description is closed, so closing is right in both cases: with no
        other copy the lock goes immediately, and a child that inherited one
        via pass_fds keeps its hold -- which is what should happen, since it
        is executing out of the venv.

        LOCK_UN is wrong in the second case: duplicate descriptors share one
        description, so unlocking this copy releases the CHILD's too and
        leaves a live interpreter in a venv eviction may delete.
        """
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            os.close(fd)
        except OSError:
            pass


class _Lease:
    """A lock held for the duration of a `with` block, unless kept.

    Releases on EVERY exit -- normal, exception, cancellation -- unless the
    body calls keep(), which hands ownership to the caller. That is the only
    way a lock leaves the block still held, so "who owns this lock here" is
    answered by reading the block rather than by checking each exit.

    Before this, release was hand-written at eight points inside the acquire
    protocol, three of them in `except BaseException: release; raise`
    handlers that existed only because a stray EACCES from Path.exists()
    would otherwise strand the hold -- and EntryLock has no __del__, so the
    fd leaked with it for the life of the process.
    """

    def __init__(self, lock: "EntryLock", outcome: LockOutcome) -> None:
        self._lock = lock
        self._kept = False
        self.outcome = outcome

    @property
    def ok(self) -> bool:
        return self.outcome.ok

    def keep(self) -> "EntryLock":
        """Transfer the hold to the caller; the block will not release it."""
        if not self.outcome.ok:
            raise RuntimeError("cannot keep a lock that was never acquired")
        self._kept = True
        return self._lock


@contextlib.contextmanager
def _lease(lock: "EntryLock", *, exclusive: bool) -> Iterator[_Lease]:
    """Take `lock` for this block. See _Lease."""
    lease = _Lease(lock, lock.try_acquire(exclusive=exclusive))
    try:
        yield lease
    finally:
        # release() is a no-op when nothing is held, which covers both a
        # failed acquire and a downgrade that lost its hold.
        if not lease._kept:
            lock.release()


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

    Bounded by COUNT and AGE, not bytes. Measuring a venv means walking tens
    of thousands of files per entry on every build (~48k for a real datahub
    venv), and the number would not describe disk anyway: uv defaults to
    UV_LINK_MODE=hardlink, so most of an entry is links into the package
    cache and deleting it reclaims little. The cost is that entries range
    from a few hundred MB to a few GB, so DATAHUB_VENV_CACHE_MAX_ENTRIES
    should be sized against the largest connector a node runs.

    Runs on the build path only, and before the new venv is created -- so a
    cache taking nothing but hits never trims, and the peak on disk is
    max_entries + 1.

    Ordered by the .datahub-venv-last-used marker, never atime: containers
    mount relatime or noatime.

    An entry whose exclusive lock cannot be taken immediately is in use and
    is SKIPPED, never waited on -- a build must not block behind an
    hours-long ingestion, and deleting a venv a task is running from is
    worse than exceeding the limit.
    """
    # One pass at a time. The per-entry flock only stops two passes picking
    # the SAME victim; without this, N concurrent passes each measure the
    # same overshoot and free it in full, destroying warm entries well inside
    # the limit. Declining rather than waiting: the peer holding this is
    # doing this call's work anyway.
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

    # One walk, oldest first, applying both rules: too old, or still over
    # the count. Counting down as we go (rather than slicing a victim list up
    # front) lets the pass continue past an entry it could not take.
    #
    # In-use entries DO count toward the limit -- they occupy the disk the
    # limit bounds. So a node running more concurrent tasks than
    # DATAHUB_VENV_CACHE_MAX_ENTRIES will churn, which is a misconfiguration
    # worth surfacing rather than silently exempting.
    remaining = len(entries)
    in_use = 0
    undeletable = 0
    evicted = 0
    for venv in oldest_first:
        too_old = now - last_used_at(venv) > max_age_sec
        if not too_old and remaining <= max_entries:
            # Oldest first, so every entry after this one is younger and the
            # count only shrinks. Nothing further can qualify.
            break
        outcome = _remove_entry(venv)
        if outcome is RemovalOutcome.REMOVED:
            evicted += 1
            remaining -= 1
            STATS.evicted += 1
        elif outcome is RemovalOutcome.IN_USE:
            in_use += 1
        else:
            undeletable += 1
    if undeletable:
        logger.warning(
            "venv cache: %d entr(ies) could not be deleted (permissions, a "
            "busy mount, or a partial removal). Raising the limit will not "
            "help; the cache root needs attention.",
            undeletable,
        )
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


def _remove_entry(venv: pathlib.Path) -> RemovalOutcome:
    """Take the entry exclusively and delete it, lock file included."""
    lock_path = entry_lock_path(venv)
    lock = EntryLock(lock_path)
    if not lock.try_acquire(exclusive=True).ok:
        logger.debug("venv cache: %s is in use, not evicting", venv)
        return RemovalOutcome.IN_USE
    try:
        # Marker first, so a partial removal is self-invalidating. rmtree
        # raises on its FIRST failure in undefined traversal order, so
        # site-packages can be gone while bin/python and the marker survive
        # -- a husk is_venv_complete accepts and nothing re-validates.
        #
        # Deliberately not restored if the rmtree then fails: that would
        # rescue a healthy-but-undeletable venv, but it cannot be told
        # apart from a partial removal, and serving a husk is a silent
        # ModuleNotFoundError on every later run. Losing a rebuild is not.
        (venv / COMPLETE_MARKER).unlink(missing_ok=True)
        shutil.rmtree(venv)
        # The .lock goes too, under the EXCLUSIVE hold we still have. Safe
        # only because try_acquire re-checks that its descriptor is still
        # the file at this path: a peer that opened the old inode before
        # this unlink fails that check and retries against the new one,
        # rather than flocking a file nobody else can see. Without the
        # unlink these accumulate one per distinct key forever, and every
        # dev-wheel deployment is a new key.
        lock_path.unlink(missing_ok=True)
        logger.info("venv cache: evicted %s", venv)
        return RemovalOutcome.REMOVED
    except OSError:
        logger.warning("venv cache: could not evict %s", venv, exc_info=True)
        return RemovalOutcome.FAILED
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
    re-resolves, so a fix published today never reaches a pod that resolved
    `latest` yesterday -- and since test-connection shares the entry,
    re-testing reports the old behaviour too.

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

    Errors are NOT ignored. `ignore_errors=True` makes a failed discard look
    like a successful one, and the build that follows either dies forever on
    that key (`uv venv` refuses a surviving non-venv directory) or, if
    pyvenv.cfg survived, builds over the leftovers and is stamped COMPLETE --
    so later runs reuse a venv mixing two builds' site-packages. The causes
    are ordinary: an NFS silly-rename, a uid mismatch, a read-only remount.
    Reporting the failure lets the caller fall back to a per-run venv.
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


@dataclasses.dataclass(frozen=True)
class _Attempt:
    """What one pass at the lock produced.

    `entry` non-None means the protocol is finished. Otherwise `outcome`
    says whether waiting could still help, and `stale_but_complete` records
    that the entry exists and is usable but past its TTL -- which only the
    shared pass can observe, and which the tail below may fall back on.
    """

    entry: Optional[CacheEntry] = None
    outcome: LockOutcome = LockOutcome.CONTENDED
    stale_but_complete: bool = False


def _regain_shared_hold(lock: EntryLock, venv_loc: pathlib.Path) -> Optional[CacheHit]:
    """Hold an entry SHARED after inspecting or building it under EXCLUSIVE.

    A lost downgrade (see downgrade_to_shared) is recovered by re-taking
    SHARED outright. Returning an entry with no lock is NOT a recovery: the
    caller would run a child out of a directory the next eviction may rmtree.
    """
    if not (lock.downgrade_to_shared() or lock.try_acquire(exclusive=False).ok):
        return None
    # The hold was momentarily gone either way, and _remove_entry leaves the
    # .lock behind, so a competing eviction may have deleted the directory
    # while the lock we just took survived.
    if is_venv_complete(venv_loc):
        return CacheHit(venv_loc, lock)
    lock.release()
    return None


def _attempt_shared_hit(
    lock: EntryLock, venv_loc: pathlib.Path, *, moving: bool
) -> _Attempt:
    """Step 1: serve a complete, fresh entry, without ever waiting."""
    with _lease(lock, exclusive=False) as lease:
        if not lease.ok:
            return _Attempt(outcome=lease.outcome)
        if _is_fresh_hit(venv_loc, moving=moving):
            touch_last_used(venv_loc)
            STATS.hits += 1
            return _Attempt(CacheHit(venv_loc, lease.keep()), lease.outcome)
        return _Attempt(
            outcome=lease.outcome,
            stale_but_complete=is_venv_complete(venv_loc),
        )


async def _attempt_exclusive_build(
    lock: EntryLock,
    venv_loc: pathlib.Path,
    *,
    moving: bool,
    fallback: Callable[[str], UncachedVenv],
) -> _Attempt:
    """Steps 2-3: take the entry to build it, re-checking under the lock."""
    with _lease(lock, exclusive=True) as lease:
        if not lease.ok:
            return _Attempt(outcome=lease.outcome)

        if _is_fresh_hit(venv_loc, moving=moving):
            # Someone finished building while we waited.
            touch_last_used(venv_loc)
            hit = _regain_shared_hold(lock, venv_loc)
            if hit is None:
                return _Attempt(
                    fallback("lost the hold after downgrade"), lease.outcome
                )
            lease.keep()
            return _Attempt(hit, lease.outcome)

        # A half-built entry, or a moving one past its TTL: remove it rather
        # than build on top. Deliberately NOT threaded, unlike eviction: this
        # deletes the directory THIS coroutine's lock protects, and a
        # cancellation would release the flock while a worker was still
        # rmtree-ing, letting a peer run `uv venv` into it.
        if venv_loc.exists() and not _discard_incomplete(venv_loc):
            return _Attempt(
                fallback("could not discard an incomplete entry"), lease.outcome
            )

        # Eviction runs here and nowhere else: on the build path, after we
        # hold this entry, so it cannot select the directory we are about to
        # write into. Threaded because an age pass can touch every entry.
        await asyncio.to_thread(
            evict_stale_entries,
            venv_loc.parent,
            max_entries=get_venv_cache_max_entries(),
            max_age_sec=get_venv_cache_max_age_sec(),
        )
        STATS.builds += 1
        return _Attempt(CacheBuild(venv_loc, lease.keep()), lease.outcome)


def _serve_stale_while_in_use(
    lock: EntryLock, venv_loc: pathlib.Path
) -> Optional[CacheHit]:
    """Step 4's exception: an expired entry nobody can take EXCLUSIVE.

    Refreshing needs EXCLUSIVE, and every in-flight run holds the same entry
    SHARED for its whole life -- `latest` is the default, so essentially all
    runs share one. On a pod whose runs overlap continuously there may be no
    idle instant, and falling through to a per-run build would make the TTL
    strictly worse than not having one: every task would pay a full build
    forever. Serving the stale venv is the lesser evil; the refresh still
    happens on the first attempt that finds the entry idle.
    """
    with _lease(lock, exclusive=False) as lease:
        if not lease.ok or not is_venv_complete(venv_loc):
            return None
        touch_last_used(venv_loc)
        STATS.stale_served += 1
        logger.info(
            "venv cache entry %s is past its TTL but in use by another run, "
            "so it cannot be rebuilt right now; serving it as-is",
            venv_loc.name,
        )
        return CacheHit(venv_loc, lease.keep())


async def _acquire_cache_entry(
    cache_name: CacheName, tmp_dir: pathlib.Path
) -> CacheEntry:
    """Resolve a dynamic venv against the cache and take the lock guarding it.

    NO PATH HERE MAY BLOCK INDEFINITELY. flock() is a synchronous syscall, so
    a blocking acquire freezes the OS thread and with it the whole event loop
    -- not even an in-loop timeout could fire to rescue it. Every acquire is
    non-blocking; all waiting is an `await asyncio.sleep`.

    The protocol is the body below, one step per call:

    1. _attempt_shared_hit -- serve a fresh entry. Must never wait: a running
       task holds its entry SHARED for its whole life, so taking EXCLUSIVE
       here would serialize two recipes on the same `latest` entry behind the
       longer one.
    2. _attempt_exclusive_build -- otherwise build, which needs EXCLUSIVE.
       The shared hit is re-attempted each pass, because a peer that finishes
       its build downgrades to SHARED and keeps it: an exclusive-only retry
       could never succeed again once it lost the race.
    3. Budget exhausted: fall back to a per-run venv rather than wait, except
       for the stale-while-in-use case in _serve_stale_while_in_use.

    Every hold is taken through `_lease`, so no path here can leave the entry
    locked. That matters because this runs ABOVE setup_venv's own `try`: a
    stranded lock is never built, hit or evicted again for the life of the
    process.
    """
    venv_loc = pathlib.Path(
        venv_location(cache_name.name, str(tmp_dir), cacheable=cache_name.cacheable)
    )
    if not cache_name.cacheable:
        return UncachedVenv(venv_loc, reason="version is not cacheable")

    def fallback(reason: str) -> UncachedVenv:
        # Every fallback names itself: the cache degrades silently by design,
        # which otherwise leaves an operator unable to tell a 90% hit rate
        # from 0%.
        STATS.record_fallback(reason)
        return UncachedVenv(
            pathlib.Path(venv_location(cache_name.name, str(tmp_dir), cacheable=False)),
            reason=reason,
        )

    lock = EntryLock(entry_lock_path(venv_loc))
    stale_but_complete = False
    outcome = LockOutcome.CONTENDED

    for attempt in range(_CACHE_LOCK_ATTEMPTS):
        shared = _attempt_shared_hit(lock, venv_loc, moving=cache_name.moving)
        if shared.entry is not None:
            return shared.entry
        outcome = shared.outcome
        if outcome.ok:
            stale_but_complete = shared.stale_but_complete
        elif outcome is LockOutcome.UNAVAILABLE:
            break

        build = await _attempt_exclusive_build(
            lock, venv_loc, moving=cache_name.moving, fallback=fallback
        )
        if build.entry is not None:
            return build.entry
        outcome = build.outcome
        if outcome is LockOutcome.UNAVAILABLE:
            break

        if attempt + 1 < _CACHE_LOCK_ATTEMPTS:
            await asyncio.sleep(_CACHE_LOCK_RETRY_SEC)

    if outcome is LockOutcome.UNAVAILABLE:
        _warn_cache_unavailable_once(str(venv_loc.parent))
        return fallback("cache root unusable")

    if stale_but_complete:
        stale = _serve_stale_while_in_use(lock, venv_loc)
        if stale is not None:
            return stale

    return fallback("entry held by another build")


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
    lock: Optional[EntryLock], venv_loc: pathlib.Path
) -> Optional[EntryLock]:
    """Make a freshly built entry reusable, and keep holding it for this run.

    Returns the lock still held afterwards, or None if the hold was lost --
    the caller stores that back, so losing a hold is visible where the
    reference lives rather than hidden in a mutation from here.

    No-op for an uncached venv, which has no lock and nothing to publish.
    """
    if lock is None:
        return None

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

    # A refused downgrade has already dropped the hold (see
    # downgrade_to_shared); re-requesting SHARED is the recovery.
    if lock.downgrade_to_shared() or lock.try_acquire(exclusive=False).ok:
        return lock

    # Nothing left to hold. Reporting the entry as protected would let
    # finalize_task_output release a lock nobody has. The venv is complete
    # and this task is about to run out of it, so the honest state is
    # "usable, unprotected" -- said out loud, because an eviction landing
    # here kills the child with an ImportError on a deleted file.
    logger.warning(
        "Lost the venv cache hold on %s after building it; this run continues "
        "against an entry eviction may reclaim.",
        venv_loc,
    )
    return None


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

    def publish(
        self, lock: Optional[EntryLock], venv_loc: pathlib.Path
    ) -> Optional[EntryLock]:
        return _publish_cache_entry(lock, venv_loc)

    @staticmethod
    def resolve_existing(entry: CacheEntry, runner: "SubprocessRunner") -> bool:
        return _resolve_existing_venv(entry, runner)

    @staticmethod
    def stats() -> dict:
        return STATS.snapshot()
