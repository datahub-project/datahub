import logging
import signal
import threading
import time
from typing import List

import pytest

from datahub.utilities import cooperative_timeout as cooperative_timeout_module
from datahub.utilities.cooperative_timeout import (
    CooperativeTimeoutError,
    cooperate,
    cooperative_timeout,
    interrupt_on_cooperative_deadline,
)


def test_cooperate_no_timeout():
    # Called outside of a timeout block, should not do anything.
    cooperate()


def test_cooperate_with_timeout():
    # Set a timeout of 0 seconds, should raise an error immediately
    with pytest.raises(CooperativeTimeoutError), cooperative_timeout(0):
        cooperate()


def test_cooperative_timeout_no_timeout():
    # No timeout set, should not raise an error
    with cooperative_timeout(timeout=None):
        for _ in range(0, 15):
            time.sleep(0.01)
            cooperate()


def test_interrupt_on_cooperative_deadline_stops_a_call_that_never_cooperates():
    started = time.perf_counter()
    with (
        pytest.raises(CooperativeTimeoutError),
        cooperative_timeout(0.4),
        interrupt_on_cooperative_deadline(),
    ):
        time.sleep(5)
    assert time.perf_counter() - started < 2


def test_interrupt_restores_a_preexisting_timer():
    if (
        not hasattr(signal, "setitimer")
        or threading.current_thread() is not threading.main_thread()
    ):
        pytest.skip("ITIMER_REAL is only available on the main thread")

    def _handler(signum: int, frame: object) -> None:
        return None

    previous_handler = signal.signal(signal.SIGALRM, _handler)
    signal.setitimer(signal.ITIMER_REAL, 30)
    try:
        with cooperative_timeout(1), interrupt_on_cooperative_deadline():
            pass
        delay, interval = signal.getitimer(signal.ITIMER_REAL)
        assert interval == 0
        assert 25 < delay <= 30
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def test_interrupt_does_not_postpone_a_preexisting_timer():
    if (
        not hasattr(signal, "setitimer")
        or threading.current_thread() is not threading.main_thread()
    ):
        pytest.skip("ITIMER_REAL is only available on the main thread")

    def _handler(signum: int, frame: object) -> None:
        return None

    previous_handler = signal.signal(signal.SIGALRM, _handler)
    signal.setitimer(signal.ITIMER_REAL, 30)
    try:
        with cooperative_timeout(5), interrupt_on_cooperative_deadline():
            time.sleep(0.5)
        delay, _ = signal.getitimer(signal.ITIMER_REAL)
        # The host's alarm keeps counting down while the block runs. Restoring
        # its original 30s would postpone it on every call.
        assert 0 < delay <= 29.6
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


class _HostTimeout(Exception):
    pass


def test_preexisting_timer_due_first_still_fires():
    if (
        not hasattr(signal, "setitimer")
        or threading.current_thread() is not threading.main_thread()
    ):
        pytest.skip("ITIMER_REAL is only available on the main thread")

    def _host_handler(signum: int, frame: object) -> None:
        raise _HostTimeout()

    previous_handler = signal.signal(signal.SIGALRM, _host_handler)
    signal.setitimer(signal.ITIMER_REAL, 0.2)
    started = time.perf_counter()
    try:
        with (
            pytest.raises(_HostTimeout),
            cooperative_timeout(5),
            interrupt_on_cooperative_deadline(),
        ):
            time.sleep(3)
        assert time.perf_counter() - started < 1
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def test_interrupt_without_deadline_does_not_fire():
    started = time.perf_counter()
    with interrupt_on_cooperative_deadline():
        time.sleep(0.05)
    assert time.perf_counter() - started < 1


def test_interrupt_unavailable_in_worker_thread_warns_once(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        cooperative_timeout_module, "_warned_interrupt_unavailable", False
    )
    errors: List[BaseException] = []

    def _run() -> None:
        try:
            for _ in range(2):
                with cooperative_timeout(5), interrupt_on_cooperative_deadline():
                    pass
        except BaseException as e:
            errors.append(e)

    with caplog.at_level(logging.WARNING, logger=cooperative_timeout_module.__name__):
        worker = threading.Thread(target=_run)
        worker.start()
        worker.join()

    assert not errors
    assert len(caplog.records) == 1


def test_interrupt_leaves_a_non_python_handler_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if (
        not hasattr(signal, "setitimer")
        or threading.current_thread() is not threading.main_thread()
    ):
        pytest.skip("ITIMER_REAL is only available on the main thread")

    # getsignal returns None for a handler installed from C, which
    # signal.signal cannot restore.
    monkeypatch.setattr(signal, "getsignal", lambda signum: None)
    previous_handler = signal.signal(signal.SIGALRM, signal.SIG_DFL)
    try:
        with cooperative_timeout(5), interrupt_on_cooperative_deadline():
            assert signal.getitimer(signal.ITIMER_REAL)[0] == 0
    finally:
        signal.signal(signal.SIGALRM, previous_handler)


def test_cooperative_timeout_with_timeout():
    # Set a timeout, and should raise an error after the timeout is hit.
    # It should, however, still run at least one iteration.
    at_least_one_iteration = False
    with pytest.raises(CooperativeTimeoutError), cooperative_timeout(0.5):
        for _ in range(0, 51):
            time.sleep(0.01)
            cooperate()
            at_least_one_iteration = True
    assert at_least_one_iteration
