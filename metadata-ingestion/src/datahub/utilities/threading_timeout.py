import contextlib
import ctypes
import functools
import logging
import platform
import threading
from types import TracebackType
from typing import ContextManager, Optional, Type

logger = logging.getLogger(__name__)

__all__ = ["threading_timeout", "TimeoutException"]


class TimeoutException(Exception):
    """Raised in the calling thread when a threading_timeout block exceeds its deadline."""


@functools.lru_cache(maxsize=1)
def _is_cpython() -> bool:
    """Check if we're running on CPython."""
    return platform.python_implementation() == "CPython"


def _set_async_exc(thread_id: int, exc: Optional[Type[BaseException]]) -> int:
    # exc=None clears any pending (undelivered) async exception for the thread.
    exc_obj = ctypes.py_object(exc) if exc is not None else ctypes.py_object()
    return int(
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(thread_id), exc_obj)
    )


class _ThreadingTimeout:
    def __init__(self, seconds: float) -> None:
        self._seconds = seconds
        self._target_tid = threading.get_ident()
        self._timer: Optional[threading.Timer] = None
        self._lock = threading.Lock()
        self._timed_out = False
        self._finished = False

    def _on_timeout(self) -> None:
        with self._lock:
            if self._finished:
                return
            affected = _set_async_exc(self._target_tid, TimeoutException)
            if affected == 0:
                # Thread gone: timeout cannot be delivered; block runs unbounded.
                logger.warning(
                    "threading_timeout: target thread %s not found; timeout not enforced",
                    self._target_tid,
                )
                return
            if affected > 1:
                # Should never happen; undo to avoid poisoning other threads.
                _set_async_exc(self._target_tid, None)
                logger.error(
                    "threading_timeout: SetAsyncExc affected %d threads; cleared",
                    affected,
                )
                return
            self._timed_out = True

    def __enter__(self) -> None:
        # Reset so a reused instance starts clean.
        self._timed_out = False
        self._finished = False
        self._timer = threading.Timer(self._seconds, self._on_timeout)
        self._timer.start()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self._timer is None:
            raise RuntimeError("threading_timeout block exited without being entered")
        with self._lock:
            self._finished = True
            self._timer.cancel()
            if self._timed_out:
                # Clear a pending TimeoutException so it can't leak into later code.
                _set_async_exc(self._target_tid, None)
        self._timer.join()
        if self._timed_out and exc_type in (None, TimeoutException):
            raise TimeoutException(f"Timed out after {self._seconds}s")


def threading_timeout(timeout: float) -> ContextManager[None]:
    """A timeout context manager backed by a watchdog thread.

    Only supported on CPython: the timeout is enforced by scheduling an
    asynchronous exception in the calling thread via
    ctypes.pythonapi.PyThreadState_SetAsyncExc, which is a CPython C-API detail.
    Because the exception is only delivered at Python bytecode boundaries, a
    block spending all its time inside a single blocking C call is interrupted
    only once control returns to Python.

    Args:
        timeout: The timeout in seconds. If <= 0, no timeout is applied.

    Raises:
        RuntimeError: If the timeout is not supported on the current Python implementation.
        TimeoutException: If the timeout is exceeded.
    """

    if timeout <= 0:
        return contextlib.nullcontext()

    if not _is_cpython():
        raise RuntimeError(
            f"Timeout is only supported on CPython, not {platform.python_implementation()}"
        )

    return _ThreadingTimeout(timeout)
