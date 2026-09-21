"""Locking for the node-local venv cache.

flock is sound here because the cache is node-local: one pod is the only
writer. A shared volume would need a different mechanism -- flock on NFS/EFS is
unreliable -- which is why the spec rules shared storage out of scope.
"""

import fcntl
import logging
import os
import pathlib
import shutil
from unittest import mock

import pytest

from datahub.executor.execution import venv_cache, venv_utils
from datahub.executor.execution.venv_cache import EntryLock, evict_to_budget


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

    builder.downgrade_to_shared()
    try:
        assert EntryLock(lock_path).acquire(exclusive=False)
        assert not EntryLock(lock_path).acquire(exclusive=True)
    finally:
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


def _entry(root: pathlib.Path, name: str, size: int, age_s: float) -> pathlib.Path:
    venv = root / f"venv-{name}"
    (venv / "bin").mkdir(parents=True)
    (venv / "bin" / "python").touch()
    (venv / "payload").write_bytes(b"x" * size)
    venv_utils.mark_venv_complete(venv)
    venv_utils.touch_last_used(venv)
    marker = venv / venv_utils.LAST_USED_MARKER
    stamp = marker.stat().st_mtime - age_s
    os.utime(marker, (stamp, stamp))
    return venv


def test_eviction_removes_the_least_recently_used_first(
    tmp_path: pathlib.Path,
) -> None:
    old = _entry(tmp_path, "old", 4000, age_s=10_000)
    fresh = _entry(tmp_path, "fresh", 4000, age_s=1)

    evict_to_budget(tmp_path, max_bytes=5000)

    assert not old.exists()
    assert fresh.exists()


def test_eviction_stops_once_inside_budget(tmp_path: pathlib.Path) -> None:
    """It frees enough, not everything -- a cache emptied on every build is
    not a cache."""
    oldest = _entry(tmp_path, "a", 4000, age_s=300)
    middle = _entry(tmp_path, "b", 4000, age_s=200)
    newest = _entry(tmp_path, "c", 4000, age_s=100)

    evict_to_budget(tmp_path, max_bytes=9000)

    assert not oldest.exists()
    assert middle.exists() and newest.exists()


def test_an_in_use_entry_is_skipped_even_when_it_is_the_oldest(
    tmp_path: pathlib.Path,
) -> None:
    """The point of the shared lock: never delete a venv a running ingestion
    is executing out of."""
    in_use = _entry(tmp_path, "inuse", 4000, age_s=10_000)
    spare = _entry(tmp_path, "spare", 4000, age_s=5_000)

    holder = EntryLock(tmp_path / "venv-inuse.lock")
    assert holder.acquire(exclusive=False)
    try:
        evict_to_budget(tmp_path, max_bytes=5000)
    finally:
        holder.release()

    assert in_use.exists(), "evicted a venv that was in use"
    assert not spare.exists(), "skipping the locked entry must not stop eviction"


def test_a_missing_cache_root_is_not_an_error(tmp_path: pathlib.Path) -> None:
    assert evict_to_budget(tmp_path / "absent", max_bytes=1) == 0


def test_an_entry_with_no_last_used_marker_is_evicted_first(
    tmp_path: pathlib.Path,
) -> None:
    """Entries predating this feature must not be immortal."""
    legacy = tmp_path / "venv-legacy"
    (legacy / "bin").mkdir(parents=True)
    (legacy / "bin" / "python").touch()
    (legacy / "payload").write_bytes(b"x" * 4000)
    recent = _entry(tmp_path, "recent", 4000, age_s=1)

    evict_to_budget(tmp_path, max_bytes=5000)

    assert not legacy.exists()
    assert recent.exists()


def test_eviction_leaves_directories_it_does_not_own(tmp_path: pathlib.Path) -> None:
    """The cache root is not necessarily ours alone.

    get_venv_cache_path returns DATAHUB_VENV_CACHE_PATH verbatim, and the docs
    advertise it as an operator knob, so the root can be an existing directory
    -- a volume mount, or /tmp. Eviction rmtree's what it selects, so it must
    select only entries this cache created.
    """
    ours = _entry(tmp_path, "ours", 4000, age_s=10_000)
    theirs = tmp_path / "important-operator-data"
    theirs.mkdir()
    (theirs / "keep.txt").write_text("not ours")
    loose = tmp_path / "notes.txt"
    loose.write_text("not ours either")

    evict_to_budget(tmp_path, max_bytes=100)

    assert not ours.exists(), "our own over-budget entry should have gone"
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
    entry = _entry(tmp_path, "doomed", 4000, age_s=10_000)
    assert venv_utils.is_venv_complete(entry)

    def half_delete(path, *args, **kwargs):
        # Delete something, then fail -- exactly what a mid-tree EACCES does.
        (pathlib.Path(path) / "payload").unlink()
        raise OSError(13, "Permission denied")

    monkeypatch.setattr(shutil, "rmtree", half_delete)
    evict_to_budget(tmp_path, max_bytes=100)

    assert entry.exists(), "the test needs a survivor to be meaningful"
    assert not venv_utils.is_venv_complete(entry), (
        "a partly-removed entry still reads as complete and will be reused"
    )


def test_an_unmeasurable_entry_is_reported_not_silently_counted_as_zero(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failed measurement disables the budget, invisibly.

    Sizes come from directory reads that skip what they cannot read. If those
    fail, every entry measures 0, the total lands under budget, and eviction
    answers "nothing to do" for the life of the pod while the cache grows
    without bound. The under-count is the direction that fills a disk, so it
    has to be audible.
    """
    root = tmp_path / "cache"
    root.mkdir()
    _entry(root, "a", 4096, age_s=1)

    def unreadable(path: str) -> object:
        raise OSError("permission denied")

    monkeypatch.setattr(os, "scandir", unreadable)

    with caplog.at_level(logging.WARNING):
        evict_to_budget(root, 1)

    assert any("could not fully measure" in r.message for r in caplog.records)


def test_a_partial_measurement_is_never_remembered(tmp_path: pathlib.Path) -> None:
    """The size hint is permanent for a COMPLETE entry, so caching a partial
    reading would make one bad measurement stick for the entry's whole life."""
    root = tmp_path / "cache"
    root.mkdir()
    venv = _entry(root, "a", 4096, age_s=1)

    with mock.patch.object(
        venv_cache, "_measure_entry", return_value=venv_cache._Measurement(7, False)
    ):
        assert venv_cache._entry_size(venv).total == 7

    assert not (venv / venv_cache.SIZE_MARKER).exists()


def test_the_budget_cannot_exceed_the_filesystem_holding_it(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The 20 GB default is larger than the whole disk on a small node.

    Left unclamped it means eviction never triggers before the volume fills,
    so the cache becomes the thing that breaks the node it was meant to speed
    up. Two entries and a 1 TB budget: nothing would be evicted, but a 10 KB
    filesystem allows only 5 KB of cache, so the LRU entry goes.
    """
    root = tmp_path / "cache"
    root.mkdir()
    old = _entry(root, "old", 4096, age_s=10_000)
    new = _entry(root, "new", 4096, age_s=1)

    monkeypatch.setattr(os, "statvfs", lambda _p: mock.Mock(f_blocks=10, f_frsize=1024))
    venv_cache._warn_budget_clamped_once.cache_clear()

    assert evict_to_budget(root, 1024**4) > 0
    assert not old.exists()
    assert new.exists()
