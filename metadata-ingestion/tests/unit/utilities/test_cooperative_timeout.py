import time

import pytest

from datahub.utilities.cooperative_timeout import (
    CooperativeTimeoutError,
    cooperate,
    cooperative_timeout,
)


def test_cooperate_no_timeout():
    # Called outside of a timeout block, should not do anything.
    cooperate()


def test_cooperate_with_timeout():
    # Set a timeout of 0 seconds, should raise an error immediately
    with pytest.raises(CooperativeTimeoutError):
        with cooperative_timeout(0):
            cooperate()


def test_cooperative_timeout_no_timeout():
    # No timeout set, should not raise an error
    with cooperative_timeout(timeout=None):
        for _ in range(0, 15):
            time.sleep(0.01)
            cooperate()


def test_cooperative_timeout_with_timeout():
    # Set a timeout, and should raise an error after the timeout is hit.
    # It should, however, still run at least one iteration.
    at_least_one_iteration = False
    with pytest.raises(CooperativeTimeoutError):
        with cooperative_timeout(0.5):
            for _ in range(0, 51):
                time.sleep(0.01)
                cooperate()
                at_least_one_iteration = True
    assert at_least_one_iteration
