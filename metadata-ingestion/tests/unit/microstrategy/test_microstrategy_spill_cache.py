import functools
import threading
from types import SimpleNamespace
from typing import Callable, Iterator, List, Optional
from unittest import mock

import pytest

from datahub.ingestion.source.microstrategy import spill_cache
from datahub.ingestion.source.microstrategy.models import (
    MicroStrategyObject,
    object_cache_key,
)
from datahub.ingestion.source.microstrategy.spill_cache import (
    SpillCache,
    SpillConnection,
)
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.sentinels import unset


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


@pytest.fixture
def parked() -> Iterator[threading.Semaphore]:
    """Released each time a caller starts waiting for another caller's load,
    so a test can wait for that instead of sleeping."""
    semaphore = threading.Semaphore(0)

    class _RecordingEvent(threading.Event):
        def wait(self, timeout: Optional[float] = None) -> bool:
            semaphore.release()
            return super().wait(timeout)

    fake_threading = SimpleNamespace(
        Lock=threading.Lock, RLock=threading.RLock, Event=_RecordingEvent
    )
    with mock.patch.object(spill_cache, "threading", fake_threading):
        yield semaphore


def _wait_parked(parked: threading.Semaphore, callers: int) -> None:
    for _ in range(callers):
        assert parked.acquire(timeout=10)


def _start(target: Callable[[], object]) -> threading.Thread:
    thread = threading.Thread(target=target)
    thread.start()
    return thread


def _blocking_loader(
    started: threading.Event, gate: threading.Event, value: str
) -> Callable[[], str]:
    def load() -> str:
        started.set()
        assert gate.wait(timeout=10)
        return value

    return load


def test_values_spill_to_disk_and_a_stored_none_is_not_a_miss() -> None:
    with SpillCache[Optional[MicroStrategyObject]](
        SpillConnection(), "objects", 1, compress=True
    ) as cache:
        for index in range(5):
            cache.put(f"key-{index}", _report_object(f"R-{index}"))
        cache.put("failed", None)

        with mock.patch.object(
            FileBackedDict, "__len__", side_effect=AssertionError("len() called")
        ):
            # One entry is in memory; every other read comes back from SQLite.
            for index in range(5):
                assert cache.get(f"key-{index}") == _report_object(f"R-{index}")
            assert cache.get("failed") is None
            assert cache.get("missing") is unset


def test_object_cache_key_normalizes_both_ids() -> None:
    assert object_cache_key(" project-1", " rep-1 ") == "PROJECT-1:REP-1"


def test_a_stored_none_can_be_overwritten() -> None:
    with SpillCache[Optional[str]](SpillConnection(), "entries", 2) as cache:
        cache.put("k", None)
        cache.put("k", "found")

        assert cache.get("k") == "found"


def test_a_loader_returning_none_is_cached_for_sequential_callers() -> None:
    calls: List[int] = []

    def load() -> None:
        calls.append(1)

    with SpillCache[None](SpillConnection(), "entries", 2) as cache:
        assert cache.get_or_load("k", load) is None
        assert cache.get_or_load("k", load) is None

    assert len(calls) == 1


def test_a_loader_returning_none_is_shared_with_a_waiting_caller(
    parked: threading.Semaphore,
) -> None:
    started, gate = threading.Event(), threading.Event()
    calls: List[int] = []
    results: List[Optional[str]] = []

    def load_none() -> Optional[str]:
        calls.append(1)
        started.set()
        assert gate.wait(timeout=10)
        return None

    with SpillCache[Optional[str]](SpillConnection(), "entries", 2) as cache:
        loader = _start(lambda: results.append(cache.get_or_load("k", load_none)))
        assert started.wait(timeout=10)
        waiter = _start(lambda: results.append(cache.get_or_load("k", load_none)))
        _wait_parked(parked, 1)
        gate.set()
        loader.join()
        waiter.join()

    assert results == [None, None]
    assert len(calls) == 1


def test_concurrent_get_or_load_runs_the_loader_once_per_key(
    parked: threading.Semaphore,
) -> None:
    gate = threading.Event()
    loading = threading.Semaphore(0)
    calls: List[str] = []
    calls_lock = threading.Lock()
    results: List[str] = []

    def loader(key: str) -> str:
        with calls_lock:
            calls.append(key)
        loading.release()
        assert gate.wait(timeout=10)
        return f"value-{key}"

    with SpillCache[str](SpillConnection(), "entries", 1) as cache:

        def call(key: str) -> None:
            results.append(cache.get_or_load(key, lambda: loader(key)))

        loaders = [_start(functools.partial(call, key)) for key in ("k0", "k1")]
        _wait_parked(loading, 2)
        waiters = [
            _start(functools.partial(call, f"k{index % 2}")) for index in range(6)
        ]
        _wait_parked(parked, 6)
        gate.set()
        for thread in loaders + waiters:
            thread.join()

    assert sorted(calls) == ["k0", "k1"]
    assert sorted(results) == ["value-k0"] * 4 + ["value-k1"] * 4


def test_two_waiters_on_a_failed_load_retry_it_once(
    parked: threading.Semaphore,
) -> None:
    started, gate = threading.Event(), threading.Event()
    errors: List[BaseException] = []
    results: List[str] = []
    retries: List[int] = []

    def failing_loader() -> str:
        started.set()
        assert gate.wait(timeout=10)
        raise RuntimeError("boom")

    def retry() -> str:
        retries.append(1)
        return "recovered"

    with SpillCache[str](SpillConnection(), "entries", 1) as cache:

        def first() -> None:
            try:
                cache.get_or_load("k", failing_loader)
            except RuntimeError as error:
                errors.append(error)

        loader = _start(first)
        assert started.wait(timeout=10)
        waiters = [
            _start(lambda: results.append(cache.get_or_load("k", retry)))
            for _ in range(2)
        ]
        _wait_parked(parked, 2)
        gate.set()
        for thread in [loader, *waiters]:
            thread.join()

        assert cache.get("k") == "recovered"

    assert len(errors) == 1
    assert results == ["recovered", "recovered"]
    assert len(retries) == 1


def test_a_failed_write_releases_the_key() -> None:
    with SpillCache[str](SpillConnection(), "entries", 1) as cache:
        with (
            mock.patch.object(
                FileBackedDict, "__setitem__", side_effect=OSError("disk full")
            ),
            pytest.raises(OSError),
        ):
            cache.get_or_load("k", lambda: "lost")

        assert cache.get_or_load("k", lambda: "stored") == "stored"
        assert cache.get("k") == "stored"


def test_a_load_that_finishes_after_reset_is_not_stored(
    parked: threading.Semaphore,
) -> None:
    started, gate = threading.Event(), threading.Event()
    results: List[str] = []

    with SpillCache[str](SpillConnection(), "entries", 1) as cache:
        stale = _start(
            lambda: results.append(
                cache.get_or_load("k", _blocking_loader(started, gate, "stale"))
            )
        )
        assert started.wait(timeout=10)
        waiter = _start(lambda: results.append(cache.get_or_load("k", lambda: "fresh")))
        _wait_parked(parked, 1)

        cache.reset()
        waiter.join()
        gate.set()
        stale.join()

        assert cache.get("k") == "fresh"
    assert sorted(results) == ["fresh", "stale"]


def test_close_wakes_a_waiter_and_later_use_raises(
    parked: threading.Semaphore,
) -> None:
    started, gate = threading.Event(), threading.Event()
    waiter_errors: List[BaseException] = []
    cache = SpillCache[str](SpillConnection(), "entries", 1)
    loader = _start(
        lambda: cache.get_or_load("k", _blocking_loader(started, gate, "v"))
    )
    assert started.wait(timeout=10)

    def wait_for_load() -> None:
        try:
            cache.get_or_load("k", lambda: "other")
        except RuntimeError as error:
            waiter_errors.append(error)

    waiter = _start(wait_for_load)
    _wait_parked(parked, 1)

    cache.close()
    waiter.join()
    gate.set()
    loader.join()

    assert len(waiter_errors) == 1
    with pytest.raises(RuntimeError):
        cache.get("k")
    with pytest.raises(RuntimeError):
        cache.put("k", "v")
    cache.reset()
    assert cache.get("k") is unset
    cache.close()


def test_caches_share_one_file_and_reset_drops_only_their_own_table() -> None:
    connection = SpillConnection()
    reports = SpillCache[str](connection, "reports", 1)
    metrics = SpillCache[str](connection, "metrics", 1)
    for index in range(4):
        reports.put(f"r-{index}", f"report {index}")
        metrics.put(f"m-{index}", f"metric {index}")
    database_file = connection.get().filename

    reports.reset()
    reports.put("r-0", "again")

    assert reports.get("r-1") is unset
    assert reports.get("r-0") == "again"
    assert [metrics.get(f"m-{index}") for index in range(4)] == [
        f"metric {index}" for index in range(4)
    ]

    connection.close()

    assert not database_file.exists()
    # Closing the connection empties the caches; the next write opens a new file.
    assert metrics.get("m-0") is unset
    metrics.put("m-0", "next project")
    assert metrics.get("m-0") == "next project"
    reports.close()
    metrics.close()
    connection.close()
