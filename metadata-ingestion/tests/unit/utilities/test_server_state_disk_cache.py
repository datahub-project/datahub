"""Tests for the ServerStateDiskCache primitive."""

import pathlib

from datahub.utilities.server_state_disk_cache import ServerStateDiskCache


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
