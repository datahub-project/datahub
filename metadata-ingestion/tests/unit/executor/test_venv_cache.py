"""Locking for the node-local venv cache.

flock is sound here because the cache is node-local: one pod is the only
writer. A shared volume would need a different mechanism -- flock on NFS/EFS is
unreliable -- which is why the spec rules shared storage out of scope.
"""

import os
import pathlib

from datahub.executor.execution import venv_utils
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
        assert not second.acquire(exclusive=True, blocking=False)
    finally:
        first.release()

    assert second.acquire(exclusive=True, blocking=False)
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
        assert not evictor.acquire(exclusive=True, blocking=False)
    finally:
        user.release()


def test_two_shared_holders_coexist(tmp_path: pathlib.Path) -> None:
    """Concurrent runs share one venv. Safe: nothing writes into a venv after
    setup, and .pyc writes are atomic-rename."""
    lock_path = tmp_path / "entry.lock"
    a, b = EntryLock(lock_path), EntryLock(lock_path)

    assert a.acquire(exclusive=False)
    assert b.acquire(exclusive=False)
    a.release()
    b.release()


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
        assert EntryLock(lock_path).acquire(exclusive=False, blocking=False)
        assert not EntryLock(lock_path).acquire(exclusive=True, blocking=False)
    finally:
        builder.release()


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
