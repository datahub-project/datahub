import time

from datahub.utilities.backpressure_aware_executor import BackpressureAwareExecutor
from datahub.utilities.perf_timer import PerfTimer


def test_backpressure_aware_executor_simple():
    def task(i):
        return i

    assert {
        res.result()
        for res in BackpressureAwareExecutor.map(
            task, ((i,) for i in range(10)), max_workers=2
        )
    } == set(range(10))


def test_backpressure_aware_executor_advanced():
    task_duration = 0.5
    started = set()
    executed = set()

    def task(x, y):
        assert x + 1 == y
        started.add(x)
        time.sleep(task_duration)
        executed.add(x)
        return x

    args_list = [(i, i + 1) for i in range(10)]

    with PerfTimer() as timer:
        results = BackpressureAwareExecutor.map(
            task, args_list, max_workers=2, max_pending=4
        )
        assert timer.elapsed_seconds() < task_duration

        # No tasks should have completed yet.
        assert len(executed) == 0

        # Consume the first result.
        first_result = next(results)
        assert 0 <= first_result.result() < 4
        assert timer.elapsed_seconds() > task_duration

        # By now, the first four tasks should have started.
        time.sleep(task_duration)
        assert {0, 1, 2, 3}.issubset(started)
        assert 2 <= len(executed) <= 4

        # Finally, consume the rest of the results.
        assert {r.result() for r in results} == {
            i for i in range(10) if i != first_result.result()
        }

        # Validate that the entire process took about 5-10x the task duration.
        # That's because we have 2 workers and 10 tasks.
        assert 5 * task_duration < timer.elapsed_seconds() < 10 * task_duration
