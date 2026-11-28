from collections import deque
from collections.abc import Generator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import AbstractContextManager
from types import TracebackType
from typing import (
    Callable,
    Deque,
    Generic,
    Iterable,
    Iterator,
    NoReturn,
    Optional,
    Type,
    TypeVar,
    Union,
)

from pydantic import BaseModel, ConfigDict

from datahub.ingestion.source.microstrategy.constants import (
    MSTR_IO_THREAD_NAME_PREFIX,
    MSTR_PENDING_TASKS_PER_WORKER,
)

_T = TypeVar("_T")
_R = TypeVar("_R")


class TaskSuccess(BaseModel, Generic[_T, _R]):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    item: _T
    value: _R

    def unwrap(self) -> _R:
        return self.value


class TaskFailure(BaseModel, Generic[_T]):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    item: _T
    error: Exception

    def unwrap(self) -> NoReturn:
        raise self.error


# Pydantic generics do not carry type parameters into a typing.Union, so the
# alias can only be subscripted in quoted annotations.
TaskOutcome = Union[TaskSuccess[_T, _R], TaskFailure[_T]]


class _PendingTask(Generic[_T, _R]):
    __slots__ = ("item", "future")

    def __init__(self, item: _T, future: "Future[_R]") -> None:
        self.item = item
        self.future = future


class OrderedParallelIterator(
    Iterator["TaskOutcome[_T, _R]"],
    AbstractContextManager["OrderedParallelIterator[_T, _R]"],
    Generic[_T, _R],
):
    """Runs fn over items on a thread pool and yields outcomes in input
    order, keeping at most max_pending items submitted ahead of the
    consumer. An exception from fn becomes a TaskFailure; one from the
    items iterator is raised after the already-submitted items are yielded."""

    def __init__(
        self,
        fn: Callable[[_T], _R],
        items: Iterable[_T],
        max_workers: int,
        max_pending: Optional[int] = None,
    ) -> None:
        if max_workers < 1:
            raise ValueError(f"max_workers must be at least 1, got {max_workers}")
        self._fn = fn
        self._iterator = iter(items)
        self._pending_limit = max(
            1,
            max_pending
            if max_pending is not None
            else max_workers * MSTR_PENDING_TASKS_PER_WORKER,
        )
        self._pending: Deque[_PendingTask[_T, _R]] = deque()
        self._exhausted = False
        self._upstream_error: Optional[Exception] = None
        self._closed = False
        self._executor: Optional[ThreadPoolExecutor] = (
            ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix=MSTR_IO_THREAD_NAME_PREFIX,
            )
            if max_workers > 1
            else None
        )

    def __enter__(self) -> "OrderedParallelIterator[_T, _R]":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def __iter__(self) -> "OrderedParallelIterator[_T, _R]":
        return self

    def __next__(self) -> "TaskOutcome[_T, _R]":
        if self._closed:
            raise StopIteration
        try:
            if self._executor is None:
                return self._run(next(self._iterator))
            self._fill(self._executor)
            if not self._pending:
                upstream_error = self._upstream_error
                self.close()
                if upstream_error is not None:
                    raise upstream_error
                raise StopIteration
            head = self._pending.popleft()
            try:
                return TaskSuccess(item=head.item, value=head.future.result())
            except Exception as error:
                return TaskFailure(item=head.item, error=error)
        except BaseException:
            self.close()
            raise

    def _run(self, item: _T) -> "TaskOutcome[_T, _R]":
        try:
            return TaskSuccess(item=item, value=self._fn(item))
        except Exception as error:
            return TaskFailure(item=item, error=error)

    def _fill(self, executor: ThreadPoolExecutor) -> None:
        while not self._exhausted and len(self._pending) < self._pending_limit:
            try:
                item = next(self._iterator)
            except StopIteration:
                self._exhausted = True
                return
            except Exception as error:
                self._exhausted = True
                self._upstream_error = error
                return
            self._pending.append(
                _PendingTask(item=item, future=executor.submit(self._fn, item))
            )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        executor, self._executor = self._executor, None
        self._pending.clear()
        try:
            if executor is not None:
                executor.shutdown(wait=True, cancel_futures=True)
        finally:
            if isinstance(self._iterator, Generator):
                self._iterator.close()


def ordered_parallel_map(
    fn: Callable[[_T], _R],
    items: Iterable[_T],
    max_workers: int,
    max_pending: Optional[int] = None,
) -> OrderedParallelIterator[_T, _R]:
    return OrderedParallelIterator(
        fn=fn,
        items=items,
        max_workers=max_workers,
        max_pending=max_pending,
    )
