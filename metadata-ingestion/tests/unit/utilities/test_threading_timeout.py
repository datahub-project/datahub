import threading
import time

import pytest

from datahub.utilities.threading_timeout import TimeoutException, threading_timeout


def test_timeout_no_timeout():
    # Should complete without raising an exception
    with threading_timeout(1.0):
        time.sleep(0.1)


def test_timeout_raises():
    # Should raise TimeoutException
    with pytest.raises(TimeoutException), threading_timeout(0.1):
        time.sleep(0.5)


def test_timeout_early_exit():
    # Test that context manager handles other exceptions properly
    with pytest.raises(ValueError), threading_timeout(1.0):
        raise ValueError("Early exit")


def test_timeout_zero():
    # Should not raise an exception
    with threading_timeout(0.0):
        pass


def test_no_leak_after_caught_timeout():
    # A stray async exception must not surface in code that runs after a
    # caught timeout (the stopit leak-after-block bug).
    with pytest.raises(TimeoutException), threading_timeout(0.1):
        time.sleep(0.5)
    total = 0
    for i in range(1_000_000):
        total += i
    assert total > 0


def test_back_to_back_timeouts_then_clean_block():
    for _ in range(5):
        with pytest.raises(TimeoutException), threading_timeout(0.1):
            time.sleep(0.5)
    # A clean block right after must not inherit a stray exception.
    with threading_timeout(1.0):
        time.sleep(0.05)


def test_repeated_clean_exits_do_not_leak():
    # Exercises the __exit__ cancel path repeatedly; no timeout should fire.
    acc = 0
    for _ in range(20):
        with threading_timeout(1.0):
            time.sleep(0.01)
        for i in range(100_000):
            acc += i
    assert acc > 0


def test_timeout_interrupts_entering_thread_not_constructing_thread():
    # The watchdog must interrupt the thread that ENTERS the block, not the one
    # that constructed the context manager. Construction happens in the main
    # thread but the block runs in a worker, so the worker's busy loop must be
    # cut short. If the timer targeted the constructing thread instead, the
    # worker runs to completion and only __exit__ reports the timeout.
    ctx = threading_timeout(0.2)
    outcome: dict = {}

    def worker() -> None:
        start = time.monotonic()
        try:
            with ctx:
                deadline = time.monotonic() + 3.0
                while time.monotonic() < deadline:
                    pass  # bytecode boundaries let the async exception land
            outcome["result"] = "completed"
        except TimeoutException:
            outcome["result"] = "timed_out"
        outcome["elapsed"] = time.monotonic() - start

    t = threading.Thread(target=worker)
    t.start()
    try:
        t.join(10.0)
    except TimeoutException:
        # If the timer targeted this (constructing) thread, the exception is
        # mis-delivered here instead of into the worker.
        outcome.setdefault("result", "mis-delivered-to-constructing-thread")
    assert outcome.get("result") == "timed_out"
    # The block must have been interrupted early, not run its full 3s.
    assert outcome.get("elapsed", 99) < 1.5
