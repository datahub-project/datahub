"""Tests for the ServerStateDiskCache primitive."""

import os
import pathlib
import time

from datahub.utilities.server_state_disk_cache import (
    _TMP_PREFIX,
    ServerStateDiskCache,
)


def _cache(tmp_path: pathlib.Path) -> ServerStateDiskCache:
    cache = ServerStateDiskCache("test-ns")
    cache._dir = tmp_path
    return cache


def test_put_get_roundtrip(tmp_path: pathlib.Path) -> None:
    cache = _cache(tmp_path)
    value = {"entity_aspects": {"dataset": ["status"]}}
    cache.put("http://localhost:8080", "commit1", value)
    assert cache.get("http://localhost:8080", "commit1") == value


def test_get_missing_returns_none(tmp_path: pathlib.Path) -> None:
    assert _cache(tmp_path).get("http://localhost:8080", "nope") is None


def test_different_commit_hash_misses(tmp_path: pathlib.Path) -> None:
    cache = _cache(tmp_path)
    cache.put("http://localhost:8080", "c1", {"a": 1})
    assert cache.get("http://localhost:8080", "c2") is None


def test_symlink_is_rejected_on_load(tmp_path: pathlib.Path) -> None:
    cache = _cache(tmp_path)
    target = tmp_path / "target.json"
    target.write_text("{}")
    link = cache._path("http://localhost:8080", "commit1")
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to(target)
    assert cache.get("http://localhost:8080", "commit1") is None


def test_put_evicts_expired_entries_but_keeps_fresh_ones(
    tmp_path: pathlib.Path,
) -> None:
    cache = _cache(tmp_path)
    cache._max_age_days = 7

    # An entry written "9 days ago" — past the 7-day max age.
    cache.put("http://localhost:8080", "old", {"v": "old"})
    stale = cache._path("http://localhost:8080", "old")
    nine_days_ago = time.time() - 9 * 86400
    os.utime(stale, (nine_days_ago, nine_days_ago))

    # The next put runs cleanup, which should evict the stale entry while the
    # freshly written one survives the save -> cleanup -> load cycle.
    cache.put("http://localhost:8080", "fresh", {"v": "fresh"})

    assert cache.get("http://localhost:8080", "old") is None
    assert cache.get("http://localhost:8080", "fresh") == {"v": "fresh"}


def test_put_removes_stale_temp_files(tmp_path: pathlib.Path) -> None:
    cache = _cache(tmp_path)

    # A leftover temp file from a crashed write, older than the 1-hour cutoff.
    stale_tmp = tmp_path / f"{_TMP_PREFIX}leftover.tmp"
    stale_tmp.write_text("{}")
    two_hours_ago = time.time() - 2 * 3600
    os.utime(stale_tmp, (two_hours_ago, two_hours_ago))

    cache.put("http://localhost:8080", "commit1", {"v": 1})

    assert not stale_tmp.exists()
    assert cache.get("http://localhost:8080", "commit1") == {"v": 1}
