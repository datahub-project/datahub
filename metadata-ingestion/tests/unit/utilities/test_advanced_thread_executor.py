import time
from concurrent.futures import Future

from datahub.utilities.advanced_thread_executor import (
    BackpressureAwareExecutor,
    PartitionExecutor,
)
from datahub.utilities.perf_timer import PerfTimer


def test_partitioned_executor():
    executing_tasks = set()
    done_tasks = set()

    def task(key: str, id: str) -> None:
        executing_tasks.add((key, id))
        time.sleep(0.8)
        done_tasks.add(id)
        executing_tasks.remove((key, id))

    with PartitionExecutor(max_workers=2, max_pending=10) as executor:
        # Submit tasks with the same key. They should be executed sequentially.
        executor.submit("key1", task, "key1", "task1")
        executor.submit("key1", task, "key1", "task2")
        executor.submit("key1", task, "key1", "task3")

        # Submit a task with a different key. It should be executed in parallel.
        executor.submit("key2", task, "key2", "task4")

        saw_keys_in_parallel = False
        while executing_tasks or not done_tasks:
            keys_executing = [key for key, _ in executing_tasks]
            assert list(sorted(keys_executing)) == list(
                sorted(set(keys_executing))
            ), "partitioning not working"

            if len(keys_executing) == 2:
                saw_keys_in_parallel = True

            time.sleep(0.1)

        executor.flush()
        assert saw_keys_in_parallel
        assert not executing_tasks
        assert done_tasks == {"task1", "task2", "task3", "task4"}


def test_partitioned_executor_bounding():
    task_duration = 0.5
    done_tasks = set()

    def on_done(future: Future) -> None:
        done_tasks.add(future.result())

    def task(id: str) -> str:
        time.sleep(task_duration)
        return id

    with PartitionExecutor(
        max_workers=5, max_pending=10
    ) as executor, PerfTimer() as timer:
        # The first 15 submits should be non-blocking.
        for i in range(15):
            executor.submit(f"key{i}", task, f"task{i}", done_callback=on_done)
        assert timer.elapsed_seconds() < task_duration

        # This submit should block.
        executor.submit("key-blocking", task, "task-blocking", done_callback=on_done)
        assert timer.elapsed_seconds() > task_duration

        # Wait for everything to finish.
        executor.flush()
        assert len(done_tasks) == 16


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
