import queue
import threading
import time
from typing import Any, Callable, Dict, Iterator, List, Optional

import pytest

from datahub.telemetry import telemetry as t


@pytest.fixture(autouse=True)
def _isolate_telemetry_worker() -> Iterator[None]:
    # Each test gets a fresh queue and no worker, so a worker started by one test
    # can't leak its thread/atexit registration into the next.
    t._telemetry_queue = queue.Queue(maxsize=t._TELEMETRY_QUEUE_MAX_SIZE)
    t._telemetry_worker_thread = None
    yield
    t._shutdown_telemetry_worker()


def _wait(cond: Callable[[], bool], timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.01)
    return False


class FakeMixpanel:
    def __init__(self, block: Optional[threading.Event] = None) -> None:
        self.tracked: List[Any] = []
        self.people: List[Any] = []
        self._block = block
        self.called = threading.Event()

    def track(
        self, client_id: str, event_name: str, properties: Dict[str, Any]
    ) -> None:
        if self._block is not None:
            self._block.wait(timeout=10)
        self.tracked.append((client_id, event_name, properties))
        self.called.set()

    def people_set(self, client_id: str, properties: Dict[str, Any]) -> None:
        self.people.append((client_id, properties))
        self.called.set()


def _enabled_telemetry(mp: FakeMixpanel) -> t.Telemetry:
    inst = t.Telemetry.__new__(t.Telemetry)
    inst.enabled = True
    inst.sentry_enabled = False
    inst.tracking_init = False
    inst.client_id = "client-1"
    inst.mp = mp
    # Pre-populate "caller" so ping() doesn't shell out via identify_caller().
    inst.global_properties = {"datahub_version": "test", "caller": "test"}
    inst.context_properties = {}
    return inst


def test_ping_returns_immediately_even_when_send_blocks() -> None:
    block = threading.Event()
    mp = FakeMixpanel(block=block)
    inst = _enabled_telemetry(mp)
    try:
        start = time.time()
        inst.ping("test-event", {"k": "v"})
        elapsed = time.time() - start
        # A synchronous send would block on the fake's block.wait (~10s); the async
        # dispatch returns instantly. The bound is generous so a slow CI runner
        # (e.g. a rare thread-start stall) can't flake while still cleanly
        # distinguishing async from sync.
        assert elapsed < 4.0
        assert not mp.called.is_set()
    finally:
        block.set()
    assert _wait(mp.called.is_set)
    assert mp.tracked[0][1] == "test-event"


def test_ping_delivers_merged_properties() -> None:
    mp = FakeMixpanel()
    inst = _enabled_telemetry(mp)
    inst.ping("evt", {"k": "v"})
    assert _wait(mp.called.is_set)
    client_id, event_name, props = mp.tracked[0]
    assert client_id == "client-1"
    assert event_name == "evt"
    assert props["k"] == "v"
    assert props["datahub_version"] == "test"


def test_init_tracking_dispatches_people_set_once() -> None:
    mp = FakeMixpanel()
    inst = _enabled_telemetry(mp)
    inst.init_tracking()
    inst.init_tracking()  # second call must be a no-op (dedup guard)
    assert _wait(mp.called.is_set)
    assert _wait(lambda: len(mp.people) == 1)
    time.sleep(0.05)
    assert len(mp.people) == 1
    assert inst.tracking_init is True


def test_disabled_telemetry_starts_no_worker() -> None:
    mp = FakeMixpanel()
    inst = _enabled_telemetry(mp)
    inst.enabled = False
    inst.ping("evt", {"k": "v"})
    inst.init_tracking()
    assert t._telemetry_worker_thread is None
    assert not mp.tracked and not mp.people


def test_dispatch_drops_when_queue_full(monkeypatch: pytest.MonkeyPatch) -> None:
    full_q: queue.Queue = queue.Queue(maxsize=1)
    full_q.put_nowait(t._TELEMETRY_STOP)
    monkeypatch.setattr(t, "_telemetry_queue", full_q)
    # Pretend a worker is already running so no real thread is spawned.
    monkeypatch.setattr(t, "_telemetry_worker_thread", threading.current_thread())

    t._dispatch_telemetry(lambda: None)  # must not raise when full

    assert full_q.qsize() == 1  # event was dropped, not enqueued


def test_dispatch_never_raises_on_worker_start_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom() -> None:
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(t, "_ensure_telemetry_worker", boom)
    # Telemetry must not break the caller: a failed worker start is swallowed.
    t._dispatch_telemetry(lambda: None)


def test_shutdown_returns_promptly_when_idle() -> None:
    t._ensure_telemetry_worker()
    thread = t._telemetry_worker_thread
    assert thread is not None and thread.is_alive()
    start = time.time()
    t._shutdown_telemetry_worker()
    assert time.time() - start < 1.0  # idle worker stops on the sentinel at once
    thread.join(timeout=2)
    assert not thread.is_alive()


def test_shutdown_is_bounded_when_send_is_in_flight() -> None:
    # The core guarantee: if a send is wedged (e.g. a blackholed collector),
    # shutdown must still return within ~_TELEMETRY_EXIT_FLUSH_SECONDS and abandon
    # the daemon worker, rather than wait for the stuck send.
    started = threading.Event()
    release = threading.Event()

    def blocking_task() -> None:
        started.set()
        release.wait(timeout=30)

    try:
        t._ensure_telemetry_worker()
        thread = t._telemetry_worker_thread
        assert thread is not None
        t._telemetry_queue.put_nowait(blocking_task)
        assert started.wait(timeout=5)  # worker is now wedged mid-"send"

        start = time.time()
        t._shutdown_telemetry_worker()
        elapsed = time.time() - start

        assert elapsed < t._TELEMETRY_EXIT_FLUSH_SECONDS + 1.0
        assert thread.is_alive()  # abandoned as a daemon, not joined to completion
    finally:
        release.set()
