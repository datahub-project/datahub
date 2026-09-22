"""Locking for the node-local venv cache.

flock is sound here because the cache is node-local: one pod is the only
writer. A shared volume would need a different mechanism -- flock on NFS/EFS is
unreliable -- so shared storage is deliberately out of scope.
"""

import errno
import fcntl
import os
import pathlib
import shutil
import tempfile
from unittest import mock

import pytest

from datahub.executor.execution import venv_cache, venv_utils
from datahub.executor.execution.venv_cache import EntryLock, evict_stale_entries


def _open_fd_count() -> int:
    """How many descriptors this process holds. Linux and macOS both expose
    this through /dev/fd; the fallback keeps the test meaningful elsewhere."""
    try:
        return len(os.listdir("/dev/fd"))
    except OSError:
        return len(os.listdir(f"/proc/{os.getpid()}/fd"))


ROOT_IGNORES_PERMISSIONS = pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason="chmod cannot deny uid 0, so the failure under test cannot be staged",
)


def test_two_locks_on_one_entry_do_not_both_get_it_exclusively(
    tmp_path: pathlib.Path,
) -> None:
    """The build path relies on this: the loser must wait, then find a
    finished venv rather than build a second one on top of the first."""
    lock_path = tmp_path / "entry.lock"
    first = EntryLock(lock_path)
    second = EntryLock(lock_path)

    assert first.acquire(exclusive=True)
    try:
        assert not second.acquire(exclusive=True)
    finally:
        first.release()

    assert second.acquire(exclusive=True)
    second.release()


def test_a_shared_holder_blocks_an_exclusive_taker(
    tmp_path: pathlib.Path,
) -> None:
    """This is what stops eviction deleting a venv out from under a running
    ingestion: the task holds shared, eviction needs exclusive."""
    lock_path = tmp_path / "entry.lock"
    user = EntryLock(lock_path)
    evictor = EntryLock(lock_path)

    assert user.acquire(exclusive=False)
    try:
        assert not evictor.acquire(exclusive=True)
    finally:
        user.release()


def test_downgrade_lets_an_evictor_be_refused_but_a_reader_in(
    tmp_path: pathlib.Path,
) -> None:
    """setup_venv builds under exclusive then downgrades for the task's life,
    so it keeps eviction out without keeping other runs out."""
    lock_path = tmp_path / "entry.lock"
    builder = EntryLock(lock_path)
    assert builder.acquire(exclusive=True)

    assert builder.downgrade_to_shared()
    reader = EntryLock(lock_path)
    try:
        assert reader.acquire(exclusive=False)
        # Released before the next assertion, otherwise the test's OWN shared
        # hold is what refuses the evictor and the assertion passes even if
        # downgrade_to_shared were replaced by a plain release() -- exactly
        # the regression the sibling test exists to catch.
        reader.release()
        assert not EntryLock(lock_path).acquire(exclusive=True)
    finally:
        reader.release()
        builder.release()


def test_a_lost_downgrade_reports_failure_instead_of_a_phantom_hold(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """flock has no atomic downgrade, so a failed conversion holds nothing.

    The kernel removes the existing lock before it looks for conflicts, so by
    the time LOCK_SH is refused the exclusive hold is already gone. Reporting
    success there would hand the caller an fd guarding nothing while eviction
    was free to delete the venv its task runs from. The race needs a
    competitor holding EXCLUSIVE while we hold it too, which cannot be staged
    in-process, so the refusal itself is injected.
    """
    lock = EntryLock(tmp_path / "entry.lock")
    assert lock.acquire(exclusive=True)

    def refuse_shared(fd: int, operation: int) -> None:
        if operation & fcntl.LOCK_SH:
            raise BlockingIOError("would block")

    monkeypatch.setattr(fcntl, "flock", refuse_shared)

    assert not lock.downgrade_to_shared()
    assert not lock.held, (
        "a lock that lost its hold must stop claiming to have one, or "
        "_keep_lock_if_child_may_be_alive and finalize_task_output both act "
        "on a lock nobody holds"
    )


def test_a_downgrade_never_blocks(tmp_path: pathlib.Path) -> None:
    """_acquire_cache_entry calls this and forbids every blocking acquire.

    flock is a plain syscall, so one blocking request freezes the OS thread
    and with it the whole event loop -- no in-loop timeout can rescue it.
    """
    lock = EntryLock(tmp_path / "entry.lock")
    assert lock.acquire(exclusive=True)

    modes: list[int] = []
    real_flock = fcntl.flock

    def record(fd: int, operation: int) -> None:
        modes.append(operation)
        real_flock(fd, operation)

    with mock.patch.object(fcntl, "flock", record):
        assert lock.downgrade_to_shared()

    assert modes == [fcntl.LOCK_SH | fcntl.LOCK_NB]
    lock.release()


def test_release_is_idempotent(tmp_path: pathlib.Path) -> None:
    """It is called from a finally that may also run after an early release."""
    lock = EntryLock(tmp_path / "entry.lock")
    assert lock.acquire(exclusive=True)
    lock.release()
    lock.release()
    assert not lock.held


@ROOT_IGNORES_PERMISSIONS
def test_an_unwritable_lock_directory_degrades_rather_than_raising(
    tmp_path: pathlib.Path,
) -> None:
    """The cache is an optimisation: a locking failure costs the cache, never
    the task.

    A path whose parents are merely MISSING is not this case -- that is the
    normal state of a fresh pod's cache root, and acquire() is expected to
    create it. The real failure is a parent that exists and cannot be written
    to: a read-only volume, or a directory owned by another user.
    """
    blocked = tmp_path / "blocked"
    blocked.mkdir()
    blocked.chmod(0o500)
    try:
        lock = EntryLock(blocked / "entry.lock")

        assert not lock.acquire(exclusive=True)
        assert not lock.held
        lock.release()
    finally:
        blocked.chmod(0o700)


def test_acquire_creates_a_missing_cache_root(tmp_path: pathlib.Path) -> None:
    """The first run on a pod finds no cache directory and must make one.

    Nothing else creates it: eviction only reads the directory. If acquire
    degraded here instead, every run would fall back to a per-run venv and the
    cache would never engage.
    """
    lock = EntryLock(tmp_path / "fresh" / "_venv_cache" / "venv-x.lock")

    assert lock.acquire(exclusive=True)
    lock.release()
    assert (tmp_path / "fresh" / "_venv_cache").is_dir()


def _surviving_entries(root: pathlib.Path) -> list[pathlib.Path]:
    """Entry directories still present.

    Explicitly is_dir() rather than glob("venv-*/"): the trailing slash only
    restricts a glob to directories from Python 3.11, and CI runs testQuick
    on 3.10 too, where it would also match the `.lock` files _remove_entry
    deliberately leaves behind.
    """
    return [p for p in root.glob(f"{venv_utils.ENTRY_PREFIX}*") if p.is_dir()]


def _entry(root: pathlib.Path, name: str, age_s: float) -> pathlib.Path:
    """A complete cache entry whose last-used marker is `age_s` seconds old."""
    venv = root / f"venv-{name}"
    (venv / "bin").mkdir(parents=True)
    (venv / "bin" / "python").touch()
    (venv / "payload").write_bytes(b"x" * 64)
    venv_utils.mark_venv_complete(venv)
    venv_utils.touch_last_used(venv)
    marker = venv / venv_utils.LAST_USED_MARKER
    stamp = marker.stat().st_mtime - age_s
    os.utime(marker, (stamp, stamp))
    return venv


_FOREVER = 10**12


def test_eviction_removes_the_least_recently_used_first(
    tmp_path: pathlib.Path,
) -> None:
    old = _entry(tmp_path, "old", age_s=10_000)
    fresh = _entry(tmp_path, "fresh", age_s=1)

    evict_stale_entries(tmp_path, max_entries=1, max_age_sec=_FOREVER)

    assert not old.exists()
    assert fresh.exists()


def test_eviction_stops_once_inside_the_bound(tmp_path: pathlib.Path) -> None:
    """It frees enough, not everything -- a cache emptied on every build is
    not a cache."""
    oldest = _entry(tmp_path, "a", age_s=300)
    middle = _entry(tmp_path, "b", age_s=200)
    newest = _entry(tmp_path, "c", age_s=100)

    evict_stale_entries(tmp_path, max_entries=2, max_age_sec=_FOREVER)

    assert not oldest.exists()
    assert middle.exists() and newest.exists()


def test_an_entry_nothing_has_used_in_too_long_goes_even_when_the_count_fits(
    tmp_path: pathlib.Path,
) -> None:
    """The count alone does not bound a pod that runs one recipe for weeks:
    it sits under the limit forever while stale venvs hold disk."""
    stale = _entry(tmp_path, "stale", age_s=60 * 60 * 24 * 30)
    active = _entry(tmp_path, "active", age_s=60)

    evict_stale_entries(tmp_path, max_entries=100, max_age_sec=60 * 60 * 24)

    assert not stale.exists()
    assert active.exists()


def test_an_in_use_entry_is_skipped_even_when_it_is_the_oldest(
    tmp_path: pathlib.Path,
) -> None:
    """The point of the shared lock: never delete a venv a running ingestion
    is executing out of."""
    in_use = _entry(tmp_path, "inuse", age_s=10_000)
    spare = _entry(tmp_path, "spare", age_s=5_000)

    holder = EntryLock(tmp_path / "venv-inuse.lock")
    assert holder.acquire(exclusive=False)
    try:
        evict_stale_entries(tmp_path, max_entries=1, max_age_sec=_FOREVER)
    finally:
        holder.release()

    assert in_use.exists(), "evicted a venv that was in use"
    assert not spare.exists(), "skipping the locked entry must not stop eviction"


def test_a_missing_cache_root_is_not_an_error(tmp_path: pathlib.Path) -> None:
    assert evict_stale_entries(tmp_path / "absent", max_entries=1, max_age_sec=1) == 0


def test_an_entry_with_no_last_used_marker_is_evicted_first(
    tmp_path: pathlib.Path,
) -> None:
    """Entries predating this feature must not be immortal."""
    legacy = tmp_path / "venv-legacy"
    (legacy / "bin").mkdir(parents=True)
    (legacy / "bin" / "python").touch()
    recent = _entry(tmp_path, "recent", age_s=1)

    evict_stale_entries(tmp_path, max_entries=1, max_age_sec=_FOREVER)

    assert not legacy.exists()
    assert recent.exists()


def test_eviction_leaves_directories_it_does_not_own(tmp_path: pathlib.Path) -> None:
    """The cache root is not necessarily ours alone.

    get_venv_cache_path returns DATAHUB_VENV_CACHE_PATH verbatim, and the docs
    advertise it as an operator knob, so the root can be an existing directory
    -- a volume mount, or /tmp. Eviction rmtree's what it selects, so it must
    select only entries this cache created.
    """
    ours = _entry(tmp_path, "ours", age_s=10_000)
    theirs = tmp_path / "important-operator-data"
    theirs.mkdir()
    (theirs / "keep.txt").write_text("not ours")
    loose = tmp_path / "notes.txt"
    loose.write_text("not ours either")

    evict_stale_entries(tmp_path, max_entries=0, max_age_sec=_FOREVER)

    assert not ours.exists(), "our own over-limit entry should have gone"
    assert (theirs / "keep.txt").read_text() == "not ours", (
        "eviction deleted a directory the cache did not create"
    )
    assert loose.exists()


def test_a_partly_removed_entry_is_not_left_looking_complete(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """rmtree raises on the FIRST failure, after deleting an arbitrary prefix.

    Traversal order is undefined, so site-packages can be gone while
    bin/python and the completion marker survive. is_venv_complete accepts
    that husk and nothing on the hit path re-validates, so every later run for
    that key reuses a venv with no packages and dies with ModuleNotFoundError
    -- permanently. Removing the marker before rmtree makes a partial removal
    self-invalidating.
    """
    entry = _entry(tmp_path, "doomed", age_s=10_000)
    assert venv_utils.is_venv_complete(entry)

    def half_delete(path, *args, **kwargs):
        # Delete something, then fail -- exactly what a mid-tree EACCES does.
        (pathlib.Path(path) / "payload").unlink()
        raise OSError(13, "Permission denied")

    monkeypatch.setattr(shutil, "rmtree", half_delete)
    evict_stale_entries(tmp_path, max_entries=0, max_age_sec=_FOREVER)

    assert entry.exists(), "the test needs a survivor to be meaningful"
    assert not venv_utils.is_venv_complete(entry), (
        "a partly-removed entry still reads as complete and will be reused"
    )


def test_concurrent_eviction_passes_do_not_each_free_the_whole_deficit(
    tmp_path: pathlib.Path,
) -> None:
    """Nothing serialises the pass itself, only the individual victims.

    DefaultExecutor gives each task its own thread and event loop, so several
    builds on DIFFERENT keys start within a second, each win EXCLUSIVE on
    their own entry and each call eviction. Every pass snapshots the same
    over-limit count and picks its own victims -- the per-entry flock only
    stops two passes choosing the SAME one -- so N passes evict N times the
    deficit, destroying warm entries well inside the limit that each cost
    minutes to rebuild.
    """
    for i in range(6):
        _entry(tmp_path, f"e{i}", age_s=1000 - i)

    first_pass_evicted = evict_stale_entries(
        tmp_path, max_entries=4, max_age_sec=_FOREVER
    )
    second_pass_evicted = evict_stale_entries(
        tmp_path, max_entries=4, max_age_sec=_FOREVER
    )

    assert first_pass_evicted == 2
    assert second_pass_evicted == 0, "a second pass re-evicted an already-fitting cache"
    assert len(_surviving_entries(tmp_path)) == 4


def test_one_eviction_pass_at_a_time_per_root(tmp_path: pathlib.Path) -> None:
    """A pass that finds another already running must decline, not duplicate it.

    Skipping is correct rather than merely convenient: the peer holding the
    pass lock is doing exactly the work this call would do, and waiting for
    it would put a blocking flock on the event-loop thread.
    """
    for i in range(6):
        _entry(tmp_path, f"e{i}", age_s=1000 - i)

    blocker = EntryLock(tmp_path / venv_cache.EVICT_LOCK_NAME)
    assert blocker.acquire(exclusive=True)
    try:
        assert evict_stale_entries(tmp_path, max_entries=1, max_age_sec=_FOREVER) == 0
    finally:
        blocker.release()

    assert len(_surviving_entries(tmp_path)) == 6, (
        "a concurrent pass evicted while another held the pass lock"
    )


def test_retaining_one_entry_twice_does_not_consume_two_descriptors() -> None:
    """Deliberately keeping a lock must cost one fd per ENTRY, not per event.

    _keep_venv_lock_unless_exited detaches the lock rather than releasing it,
    because releasing marks a venv evictable while a child is still executing
    from it. Its docstring accounts for that as "the entry becomes
    unevictable, which costs disk" -- one entry, held once.

    Simply dropping the reference does not deliver that. EntryLock owns a raw
    os.open fd and has no __del__, so every detach adds another descriptor on
    the same lock file, and test-connection reaches this on essentially every
    cancellation (the child is signalled without being waited for, so poll()
    is None). At RLIMIT_NOFILE the next os.open raises EMFILE and the cache
    switches itself off for the life of the pod.
    """
    lock_path = pathlib.Path(tempfile.mkdtemp()) / "entry.lock"
    first = EntryLock(lock_path)
    assert first.acquire(exclusive=False)
    second = EntryLock(lock_path)
    assert second.acquire(exclusive=False)

    before = _open_fd_count()
    venv_cache.retain_lock(first)
    venv_cache.retain_lock(second)
    after = _open_fd_count()

    assert after - before <= 1, (
        f"retaining the same entry twice cost {after - before} descriptors; "
        "this accumulates until EMFILE disables the cache"
    )
    # Still protected: retaining is not releasing.
    assert not EntryLock(lock_path).acquire(exclusive=True)


def test_running_out_of_descriptors_is_not_treated_as_permanent(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`unusable` means "retrying cannot fix this", and EMFILE is not that.

    Marking it unusable makes _acquire_cache_entry break out of its retry loop
    AND trip _warn_cache_unavailable_once, whose lru_cache makes the warning
    -- and the operator's picture of the cache -- permanent for the pod, over
    a condition that clears as soon as other descriptors close.
    """
    lock = EntryLock(tmp_path / "entry.lock")

    def out_of_descriptors(*args: object, **kwargs: object) -> int:
        raise OSError(errno.EMFILE, "Too many open files")

    monkeypatch.setattr(os, "open", out_of_descriptors)

    assert not lock.acquire(exclusive=True)
    assert not lock.unusable, (
        "EMFILE is transient; treating it as permanent disables the cache for "
        "the life of the pod"
    )
