# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import time
from functools import partial

import pytest

from datahub.utilities.perf_timer import PerfTimer

approx = partial(pytest.approx, rel=2e-2)


def test_perf_timer_simple() -> None:
    with PerfTimer() as timer:
        time.sleep(0.4)
        assert approx(timer.elapsed_seconds()) == 0.4

    assert approx(timer.elapsed_seconds()) == 0.4


def test_perf_timer_paused_timer() -> None:
    with PerfTimer() as current_timer:
        time.sleep(0.5)
        assert approx(current_timer.elapsed_seconds()) == 0.5
        with current_timer.pause():
            time.sleep(0.3)
            assert approx(current_timer.elapsed_seconds()) == 0.5
        assert approx(current_timer.elapsed_seconds()) == 0.5
        time.sleep(0.2)

    assert approx(current_timer.elapsed_seconds()) == 0.7


def test_generator_with_paused_timer() -> None:
    n = 4

    def generator_function():
        with PerfTimer() as inner_timer:
            time.sleep(1)
            for i in range(n):
                time.sleep(0.2)
                with inner_timer.pause():
                    time.sleep(0.2)
                    yield i
            assert approx(inner_timer.elapsed_seconds()) == 1 + 0.2 * n

    with PerfTimer() as outer_timer:
        seq = generator_function()
        list([i for i in seq])
        assert approx(outer_timer.elapsed_seconds()) == 1 + 0.2 * n + 0.2 * n


def test_perf_timer_reuse() -> None:
    timer = PerfTimer()

    with timer:
        time.sleep(0.2)

    with timer:
        time.sleep(0.3)

    assert approx(timer.elapsed_seconds()) == 0.5
