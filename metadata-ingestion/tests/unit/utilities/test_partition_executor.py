import collections
import logging
import math
import threading
import time
from concurrent.futures import Future
from datetime import timedelta

import pytest

from datahub.utilities.partition_executor import (
    BatchPartitionExecutor,
    PartitionExecutor,
)
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)


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
            assert list(sorted(keys_executing)) == list(sorted(set(keys_executing))), (
                "partitioning not working"
            )

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

    with (
        PartitionExecutor(max_workers=5, max_pending=10) as executor,
        PerfTimer() as timer,
    ):
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


@pytest.mark.timeout(30)
def test_partitioned_executor_same_key_completion_race():
    # Deterministically forces interleaving #1: the same-key completion
    # callback checks its pending deque, sees it empty, and then -- BEFORE it
    # deletes the key -- a concurrent submit() for the same key appends a new
    # task to that deque. The callback then deletes the key, silently dropping
    # the queued task and leaking its semaphore permit.
    #
    # We force the exact window by instrumenting the pending deque: when the
    # callback truthiness-checks the (empty) deque, we let a submit() land its
    # append, then let the callback proceed to its `del`. Without a lock
    # guarding these transitions, task2 is lost and its permit leaks; a leaked
    # permit eventually deadlocks submit(), which the timeout catches.
    run_counts: dict = {}
    counts_lock = threading.Lock()

    task1_running = threading.Event()
    task1_may_finish = threading.Event()

    def counting_task(task_id: str, block: bool = False) -> str:
        if block:
            task1_running.set()
            # Park on the worker thread so task1's completion callback runs
            # asynchronously (on the worker), not synchronously on the caller.
            task1_may_finish.wait(timeout=10)
        with counts_lock:
            run_counts[task_id] = run_counts.get(task_id, 0) + 1
        return task_id

    callback_checked_empty = threading.Event()
    submit_appended = threading.Event()
    armed = threading.Event()

    class _InstrumentedDeque(collections.deque):
        def __len__(self):
            result = super().__len__()
            # Only trip the wire once, when task1's completion callback (on the
            # worker thread) observes the empty deque -- the check-then-act
            # window we want to exploit.
            if (
                result == 0
                and armed.is_set()
                and not callback_checked_empty.is_set()
                and threading.current_thread().name != "MainThread"
            ):
                callback_checked_empty.set()
                # Wait for the racing submit() to append before we return, so
                # the callback then proceeds to `del` a now-non-empty key.
                submit_appended.wait(timeout=5)
            return result

    with PartitionExecutor(max_workers=2, max_pending=10) as executor:
        # Force new deques created by submit()/_submit_nowait to be our
        # instrumented subclass so the callback's truthiness check is observable.
        real_deque = collections.deque
        collections.deque = _InstrumentedDeque  # type: ignore[misc]
        try:
            # Submit a blocking task1 for key1; it parks on a worker thread.
            executor.submit("key1", counting_task, "task1", block=True)
            assert task1_running.wait(timeout=5)

            # Arm the trip only now, so it fires on task1's async completion
            # callback rather than on any synchronous fast-path callback.
            armed.set()

            # Release task1. Its completion callback runs on the worker, sees
            # the empty deque, and pauses in __len__ at the check-then-act window.
            task1_may_finish.set()

            assert callback_checked_empty.wait(timeout=5), (
                "callback never reached the empty-check window"
            )

            # In the window before the callback's `del`, submit a second
            # same-key task. Pre-fix this appends to a deque the callback is
            # about to delete -> task lost + permit leaked.
            appender = threading.Thread(
                target=lambda: executor.submit("key1", counting_task, "task2")
            )
            appender.start()
            # Give the append a moment to land inside the window.
            time.sleep(0.2)
            submit_appended.set()
            appender.join(timeout=5)
        finally:
            collections.deque = real_deque  # type: ignore[misc]

        executor.flush()

        # Both tasks must have executed exactly once.
        assert run_counts == {"task1": 1, "task2": 1}, (
            f"task lost under race: {run_counts}"
        )

        # Verify no semaphore permit leaked: a leaked permit shrinks capacity
        # and would eventually deadlock submit()/flush() (caught by timeout).
        for i in range(30):
            executor.submit(f"batch{i}", counting_task, f"batch{i}")
        executor.flush()

        for i in range(30):
            assert run_counts[f"batch{i}"] == 1


@pytest.mark.timeout(60)
def test_partitioned_executor_concurrent_submit_stress():
    # Many same-key and cross-key submissions from multiple threads,
    # interleaved with fast-completing tasks. Every submitted task must run
    # exactly once and flush() must terminate. Pre-fix, the unguarded
    # check-then-act on _pending_by_key can lose tasks or raise KeyError;
    # this is probabilistic, so we run several iterations to make a failure
    # likely.
    n_threads = 6
    per_thread = 100
    n_keys = 8

    def counting_task(
        task_id: str, run_counts: dict, counts_lock: threading.Lock
    ) -> None:
        with counts_lock:
            run_counts[task_id] = run_counts.get(task_id, 0) + 1

    def submitter(
        thread_idx: int,
        executor: PartitionExecutor,
        barrier: threading.Barrier,
        run_counts: dict,
        counts_lock: threading.Lock,
    ) -> None:
        barrier.wait()
        for j in range(per_thread):
            key = f"key{(thread_idx + j) % n_keys}"
            task_id = f"t{thread_idx}-{j}"
            executor.submit(key, counting_task, task_id, run_counts, counts_lock)

    submitted = {f"t{t}-{j}" for t in range(n_threads) for j in range(per_thread)}

    for _iteration in range(5):
        run_counts: dict = {}
        counts_lock = threading.Lock()

        with PartitionExecutor(max_workers=4, max_pending=50) as executor:
            barrier = threading.Barrier(n_threads)
            threads = [
                threading.Thread(
                    target=submitter,
                    args=(t, executor, barrier, run_counts, counts_lock),
                )
                for t in range(n_threads)
            ]
            for th in threads:
                th.start()
            for th in threads:
                th.join()

            executor.flush()

        assert len(run_counts) == len(submitted)
        assert all(count == 1 for count in run_counts.values())
        assert set(run_counts) == submitted


@pytest.mark.parametrize("max_workers", [1, 2, 10])
def test_batch_partition_executor_sequential_key_execution(max_workers: int) -> None:
    executing_tasks = set()
    done_tasks = set()
    done_task_batches = set()

    def process_batch(batch):
        for key, id in batch:
            assert (key, id) not in executing_tasks, "Task is already executing"
            executing_tasks.add((key, id))

        time.sleep(0.5)  # Simulate work

        for key, id in batch:
            executing_tasks.remove((key, id))
            done_tasks.add(id)

        done_task_batches.add(tuple(id for _, id in batch))

    with BatchPartitionExecutor(
        max_workers=max_workers,
        max_pending=10,
        max_per_batch=2,
        process_batch=process_batch,
    ) as executor:
        # Submit tasks with the same key. The first two should get batched together.
        executor.submit("key1", "key1", "task1")
        executor.submit("key1", "key1", "task2")
        executor.submit("key1", "key1", "task3")

        # Submit tasks with a different key. These should get their own batch.
        executor.submit("key2", "key2", "task4")
        executor.submit("key2", "key2", "task5")

        # Test idempotency of shutdown().
        executor.shutdown()

    # Check if all tasks were executed and completed.
    assert done_tasks == {
        "task1",
        "task2",
        "task3",
        "task4",
        "task5",
    }, "Not all tasks completed"

    # Check the batching configuration.
    assert done_task_batches == {
        ("task1", "task2"),
        ("task4", "task5"),
        ("task3",),
    }


@pytest.mark.timeout(5)
def test_batch_partition_executor_max_batch_size():
    n = 5
    batches_processed = []

    def process_batch(batch):
        batches_processed.append(batch)
        time.sleep(0.1)  # Simulate batch processing time

    with BatchPartitionExecutor(
        max_workers=5,
        max_pending=10,
        process_batch=process_batch,
        max_per_batch=2,
        min_process_interval=timedelta(seconds=0.1),
        read_from_pending_interval=timedelta(seconds=0.1),
    ) as executor:
        # Submit more tasks than the max_per_batch to test batching limits.
        for i in range(n):
            executor.submit("key3", "key3", f"task{i}")

    # Check the batches.
    logger.info(f"batches_processed: {batches_processed}")
    assert len(batches_processed) == math.ceil(n / 2), "Incorrect number of batches"
    for batch in batches_processed:
        assert len(batch) <= 2, "Batch size exceeded max_per_batch limit"


@pytest.mark.timeout(10)
def test_batch_partition_executor_deadlock():
    n = 20  # Exceed max_pending to test for deadlocks when max_pending exceeded
    batch_size = 2
    batches_processed = []

    def process_batch(batch):
        batches_processed.append(batch)
        time.sleep(0.1)  # Simulate batch processing time

    with BatchPartitionExecutor(
        max_workers=5,
        max_pending=2,
        process_batch=process_batch,
        max_per_batch=batch_size,
        min_process_interval=timedelta(seconds=30),
        read_from_pending_interval=timedelta(seconds=0.01),
    ) as executor:
        # Submit more tasks than the max_per_batch to test batching limits.
        executor.submit("key3", "key3", "task0")
        executor.submit("key3", "key3", "task1")
        executor.submit("key1", "key1", "task1")  # Populates second batch
        for i in range(3, n):
            executor.submit("key3", "key3", f"task{i}")

    assert sum(len(batch) for batch in batches_processed) == n


def test_empty_batch_partition_executor():
    # We want to test that even if no submit() calls are made, cleanup works fine.
    with BatchPartitionExecutor(
        max_workers=5, max_pending=20, process_batch=lambda batch: None, max_per_batch=2
    ) as executor:
        assert executor is not None
