import time

import pytest

from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor


def test_threaded_iterator_executor():
    def table_of(i):
        for j in range(1, 11):
            yield f"{i}x{j}={i * j}"

    assert {
        res
        for res in ThreadedIteratorExecutor.process(
            table_of, [(i,) for i in range(1, 30)], max_workers=2
        )
    } == {x for i in range(1, 30) for x in table_of(i)}


def test_worker_exception_is_propagated():
    """When a worker raises, the exception should be re-raised in the consumer."""

    def failing_worker(i):
        if i == 3:
            raise ValueError(f"Worker {i} failed")
        for j in range(5):
            yield f"{i}-{j}"

    with pytest.raises(ValueError, match="Worker 3 failed"):
        list(
            ThreadedIteratorExecutor.process(
                failing_worker, [(i,) for i in range(10)], max_workers=4
            )
        )


def test_worker_exception_does_not_deadlock():
    """A failing worker with backpressure should not hang.

    Uses a small backpressure limit and slow workers to exercise
    the drain-before-reraise path.
    """

    def slow_worker(i):
        for j in range(20):
            time.sleep(0.01)
            yield f"{i}-{j}"

    def failing_slow_worker(i):
        if i == 0:
            time.sleep(0.05)
            raise RuntimeError("boom")
        yield from slow_worker(i)

    with pytest.raises(RuntimeError, match="boom"):
        # Small backpressure to force queue blocking
        list(
            ThreadedIteratorExecutor.process(
                failing_slow_worker,
                [(i,) for i in range(8)],
                max_workers=4,
                max_backpressure=4,
            )
        )


def test_max_backpressure_validation():
    """max_backpressure < max_workers raises ValueError."""

    def noop(i):
        yield i

    with pytest.raises(ValueError, match="max_backpressure"):
        list(
            ThreadedIteratorExecutor.process(
                noop, [(1,)], max_workers=4, max_backpressure=2
            )
        )
