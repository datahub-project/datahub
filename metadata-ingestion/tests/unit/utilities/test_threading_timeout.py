import subprocess
import sys
import threading
import time

import pytest

from datahub.utilities.threading_timeout import (
    TimeoutException,
    _ThreadingTimeout,
    threading_timeout,
)


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


def test_timeout_targets_the_entering_thread_not_the_constructor():
    # Regression: the watchdog must target the thread that ENTERS the block, so
    # the target thread id has to be captured in __enter__, not __init__. A long
    # timeout keeps this deterministic — the watchdog never fires, so there is no
    # async exception whose nondeterministic delivery point would make the test
    # flaky. We construct in the main thread but enter the block in a worker.
    ctx = threading_timeout(60.0)
    seen: dict = {}

    def worker() -> None:
        with ctx:
            seen["worker_tid"] = threading.get_ident()
            seen["target_tid"] = ctx._target_tid  # type: ignore[attr-defined]

    t = threading.Thread(target=worker)
    t.start()
    t.join(5.0)
    assert seen["target_tid"] == seen["worker_tid"]
    assert seen["target_tid"] != threading.get_ident()  # not the constructing thread


def test_exit_without_enter_raises():
    # Exiting the context without entering it is a programming error.
    ctx = _ThreadingTimeout(1.0)
    with pytest.raises(RuntimeError):
        ctx.__exit__(None, None, None)


def test_on_timeout_when_target_thread_absent_does_not_mark_timed_out():
    # If the target thread is gone when the watchdog fires, the async exception
    # cannot be delivered, so the block is left to run rather than timing out.
    ctx = _ThreadingTimeout(1.0)  # _target_tid defaults to 0 (no such thread)
    ctx._on_timeout()
    assert ctx._timed_out is False


def test_armed_watchdog_does_not_block_interpreter_shutdown():
    # A watchdog still armed when the interpreter starts shutting down must not
    # delay exit. A non-daemon Timer would be joined by threading._shutdown() for
    # its full interval, and when it fires mid-shutdown it injects TimeoutException
    # into the shutdown machinery -- observed as an ~80-minute hang on py3.11 CI.
    # The watchdog must be a daemon so shutdown neither waits for it nor lets it
    # fire. Uses a subprocess because it asserts on interpreter-exit timing.
    code = (
        "from datahub.utilities.threading_timeout import _ThreadingTimeout\n"
        "ctx = _ThreadingTimeout(30.0)\n"
        "ctx.__enter__()\n"  # arm the watchdog and never disarm it
        "print('armed')\n"
    )
    start = time.monotonic()
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=20,
    )
    elapsed = time.monotonic() - start
    assert proc.stdout.strip() == "armed"
    assert elapsed < 15, (
        f"interpreter shutdown blocked {elapsed:.1f}s on an armed watchdog "
        "(the Timer must be a daemon)"
    )
