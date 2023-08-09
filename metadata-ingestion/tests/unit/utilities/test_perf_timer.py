import time

from datahub.utilities.perf_timer import PerfTimer


def test_perf_timer_simple():
    with PerfTimer() as timer:
        time.sleep(1)
        assert round(timer.elapsed_seconds()) == 1

    assert round(timer.elapsed_seconds()) == 1


def test_perf_timer_paused_timer():
    with PerfTimer() as current_timer:
        time.sleep(1)
        assert round(current_timer.elapsed_seconds()) == 1
        with current_timer.pause_timer():
            time.sleep(2)
            assert round(current_timer.elapsed_seconds()) == 1
        assert round(current_timer.elapsed_seconds()) == 1
        time.sleep(1)

    assert round(current_timer.elapsed_seconds()) == 2


def test_generator_with_paused_timer():
    def generator_function():
        with PerfTimer() as inner_timer:
            time.sleep(1)
            for i in range(10):
                time.sleep(0.2)
                with inner_timer.pause_timer():
                    time.sleep(0.2)
                    yield i
            assert round(inner_timer.elapsed_seconds()) == 1 + 0.2 * 10

    with PerfTimer() as outer_timer:
        seq = generator_function()
        list([i for i in seq])
        assert round(outer_timer.elapsed_seconds()) == 1 + 0.2 * 10 + 0.2 * 10
