import threading
import time
from typing import Callable, List, Optional
from unittest import mock

import pytest

from datahub.ingestion.source.microstrategy.models import MicroStrategyObject
from datahub.ingestion.source.microstrategy.spill_cache import (
    SpillCache,
    object_cache_key,
)
from datahub.utilities.file_backed_collections import FileBackedDict


def _report_object(object_id: str) -> MicroStrategyObject:
    return MicroStrategyObject.model_validate(
        {
            "id": object_id,
            "name": f"Report {object_id}",
            "type": "3",
            "subtype": "768",
            "ancestors": [{"id": "f-1", "name": "Shared Reports"}],
        }
    )


def test_values_spill_to_disk_without_loss_and_none_is_a_cached_value() -> None:
    cache: SpillCache[Optional[MicroStrategyObject]] = SpillCache(1, compress=True)
    for index in range(5):
        cache.put(f"key-{index}", _report_object(f"R-{index}"))
    cache.put("failed", None)
    cache.put("key-0", _report_object("R-0"))

    with mock.patch.object(
        FileBackedDict, "__len__", side_effect=AssertionError("len() called")
    ):
        # One entry is in memory; every other read comes back from SQLite.
        for index in range(5):
            value = cache.get(f"key-{index}")
            assert value == _report_object(f"R-{index}")
        failed = cache.get_entry("failed")
        missing = cache.get_entry("missing")
        assert "failed" in cache
        # A key written twice is one entry.
        assert cache.entry_count == 6
        cache.reset()

    assert failed.found and failed.value is None
    assert not missing.found and "missing" not in cache
    assert cache.entry_count == 0
    assert cache.get("key-1") is None


def test_object_cache_key_normalizes_the_object_id() -> None:
    assert object_cache_key("project-1", " rep-1 ") == "project-1:REP-1"


def test_concurrent_get_or_load_runs_the_loader_once_per_key() -> None:
    cache: SpillCache[str] = SpillCache(1)
    calls: List[str] = []
    calls_lock = threading.Lock()
    start = threading.Barrier(8)
    results: List[str] = []

    def loader(key: str) -> str:
        with calls_lock:
            calls.append(key)
        # Long enough that the other callers arrive while this one loads.
        time.sleep(0.05)
        return f"value-{key}"

    def worker(key: str) -> None:
        start.wait()
        results.append(cache.get_or_load(key, lambda: loader(key)))

    threads = [
        threading.Thread(target=worker, args=(f"k{index % 2}",)) for index in range(8)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(calls) == ["k0", "k1"]
    assert sorted(results) == ["value-k0"] * 4 + ["value-k1"] * 4
    assert cache.entry_count == 2


def test_failed_load_caches_nothing_and_a_waiter_loads_again() -> None:
    cache: SpillCache[str] = SpillCache(1)
    loading = threading.Event()
    release = threading.Event()
    errors: List[BaseException] = []
    waiter_result: List[str] = []

    def failing_loader() -> str:
        loading.set()
        release.wait()
        raise RuntimeError("boom")

    def first() -> None:
        try:
            cache.get_or_load("k", failing_loader)
        except RuntimeError as error:
            errors.append(error)

    def second() -> None:
        waiter_result.append(cache.get_or_load("k", lambda: "recovered"))

    first_thread = threading.Thread(target=first)
    first_thread.start()
    loading.wait()
    second_thread = threading.Thread(target=second)
    second_thread.start()
    time.sleep(0.05)
    release.set()
    first_thread.join()
    second_thread.join()

    assert len(errors) == 1
    assert waiter_result == ["recovered"]
    assert cache.get("k") == "recovered"
    assert cache.entry_count == 1


def test_loader_error_propagates_and_the_key_stays_loadable() -> None:
    cache: SpillCache[int] = SpillCache(1)

    def boom() -> int:
        raise ValueError("boom")

    with pytest.raises(ValueError):
        cache.get_or_load("k", boom)

    assert "k" not in cache
    assert cache.get_or_load("k", lambda: 7) == 7
    assert cache.get_or_load("k", boom) == 7


def test_close_releases_the_sqlite_file_and_reset_reopens_lazily() -> None:
    created: List[FileBackedDict[object]] = []
    real_factory: Callable[..., FileBackedDict[object]] = FileBackedDict

    def recording_factory(**kwargs: object) -> FileBackedDict[object]:
        store = real_factory(**kwargs)
        created.append(store)
        return store

    with mock.patch(
        "datahub.ingestion.source.microstrategy.spill_cache.FileBackedDict",
        side_effect=recording_factory,
    ):
        cache: SpillCache[str] = SpillCache(2)
        assert cache.get("k") is None
        # Reads never create the file; the first write does.
        assert created == []
        cache.put("k", "v")
        cache.reset()
        cache.put("k", "v2")
        cache.close()

    assert len(created) == 2
    assert all(store.closed for store in created)
