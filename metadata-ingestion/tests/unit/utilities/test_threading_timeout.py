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
