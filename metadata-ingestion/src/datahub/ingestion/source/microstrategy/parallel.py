import threading
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import AbstractContextManager
from typing import (
    Callable,
    Deque,
    Dict,
    Generic,
    Iterable,
    Iterator,
    Optional,
    TypeVar,
    cast,
)

from pydantic import BaseModel, ConfigDict, Field

_T = TypeVar("_T")
_R = TypeVar("_R")
_K = TypeVar("_K")
_V = TypeVar("_V")


class TaskOutcome(BaseModel, Generic[_T, _R]):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    item: _T
    value: Optional[_R] = None
    error: Optional[BaseException] = None

    def unwrap(self) -> _R:
        if self.error is not None:
            raise self.error
        return cast(_R, self.value)


class _PendingTask(BaseModel, Generic[_T, _R]):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    item: _T
    future: Future[_R]


class OrderedParallelIterator(
    Iterator[TaskOutcome[_T, _R]],
    AbstractContextManager["OrderedParallelIterator[_T, _R]"],
    Generic[_T, _R],
):
    """Bounded FIFO parallel map that yields outcomes in strict input order."""

    def __init__(
        self,
        fn: Callable[[_T], _R],
        items: Iterable[_T],
        max_workers: int,
        max_pending: Optional[int] = None,
    ) -> None:
        self._fn = fn
        self._iterator = iter(items)
        self._max_workers = max_workers
        self._pending_limit = max(
            1, max_pending if max_pending is not None else max_workers * 2
        )
        self._pending: Deque[_PendingTask[_T, _R]] = deque()
        self._exhausted = False
        self._closed = False
        self._executor: Optional[ThreadPoolExecutor] = (
            ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="mstr-io",
            )
            if max_workers > 1
            else None
        )

    def __enter__(self) -> "OrderedParallelIterator[_T, _R]":
        return self

    def __exit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> None:
        self.close()

    def __iter__(self) -> "OrderedParallelIterator[_T, _R]":
        return self

    def __next__(self) -> TaskOutcome[_T, _R]:
        if self._closed:
            raise StopIteration

        if self._executor is None:
            next_item = next(self._iterator)
            try:
                value = self._fn(next_item)
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                return TaskOutcome.model_construct(
                    item=next_item, value=None, error=exc
                )
            return TaskOutcome.model_construct(item=next_item, value=value, error=None)

        try:
            while not self._exhausted and len(self._pending) < self._pending_limit:
                try:
                    next_item = next(self._iterator)
                except StopIteration:
                    self._exhausted = True
                    break
                self._pending.append(
                    _PendingTask.model_construct(
                        item=next_item,
                        future=self._executor.submit(self._fn, next_item),
                    )
                )
            if not self._pending:
                self.close()
                raise StopIteration

            head = self._pending.popleft()
            try:
                result = head.future.result()
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    self.close()
                    raise
                return TaskOutcome.model_construct(
                    item=head.item, value=None, error=exc
                )
            return TaskOutcome.model_construct(item=head.item, value=result, error=None)
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        executor = self._executor
        self._executor = None
        self._pending.clear()
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)


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


class _InFlightCall(BaseModel, Generic[_V]):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    event: threading.Event = Field(default_factory=threading.Event)
    value: Optional[_V] = None
    error: Optional[BaseException] = None


class SingleFlight(Generic[_K, _V]):
    """Coalesces concurrent calls for the same key so only one executes."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._calls: Dict[_K, _InFlightCall[_V]] = {}

    def do(self, key: _K, fn: Callable[[], _V]) -> _V:
        with self._lock:
            existing = self._calls.get(key)
            if existing is not None:
                leader = False
                call = existing
            else:
                leader = True
                call = _InFlightCall()
                self._calls[key] = call

        if not leader:
            call.event.wait()
            if call.error is not None:
                raise call.error
            return cast(_V, call.value)

        try:
            call.value = fn()
        except BaseException as exc:
            call.error = exc
            raise
        finally:
            with self._lock:
                self._calls.pop(key, None)
                call.event.set()
        return cast(_V, call.value)
