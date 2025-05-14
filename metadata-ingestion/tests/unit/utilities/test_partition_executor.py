import logging
import math
import time
from concurrent.futures import Future

import pytest
from pydantic.schema import timedelta

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
