import threading
import time
from contextlib import AbstractContextManager
from types import TracebackType
from typing import Optional, Type

_cooperation = threading.local()


class CooperativeTimeoutError(TimeoutError):
    """An exception raised when a cooperative timeout is exceeded."""


def cooperate() -> None:
    """Method to be called periodically to cooperate with the timeout mechanism."""

    deadline = getattr(_cooperation, "deadline", None)
    if deadline is not None and deadline < time.perf_counter_ns():
        raise CooperativeTimeoutError("CooperativeTimeout deadline exceeded")


class CooperativeTimeout(AbstractContextManager):
    """A cooperative timeout mechanism.

    Getting code to time out in Python is actually rather tricky. Common approaches include:

      - Using the signal module to set a signal handler that raises an exception
        after a certain time. Unfortunately, this approach only works on the main
        thread, and is not available on Windows.
      - Creating a separate process to run the code and then killing it if it hasn't
        finished by the deadline. This usually requires that all arguments/return
        types are pickleable so that they can be passed between processes. Overall,
        this approach is heavy-handed and can be tricky to implement correctly.
      - Using `threading` is not an option, since Python threads are not interruptible
        (unless you're willing to use some hacks https://stackoverflow.com/a/61528202).
        Attempting to forcibly terminate a thread can deadlock on the GIL.

    In cases where (1) we have control over the code that we want to time out and
    (2) we can modify it to regularly and reliably call a specific function, we can
    use a cooperative timeout mechanism instead.

    This is not reentrant and cannot be used in nested contexts. It can be used
    in multi-threaded contexts, so long as the cooperative function is called
    from the same thread that created the timeout.

    Args:
        timeout: The timeout in seconds. If None, the timeout is disabled.
    """

    def __init__(self, timeout: Optional[None] = None):
        self.timeout = timeout

    def __enter__(self) -> "CooperativeTimeout":
        if hasattr(_cooperation, "deadline"):
            raise RuntimeError("CooperativeTimeout already active")
        if self.timeout is not None:
            _cooperation.deadline = (
                time.perf_counter_ns() + self.timeout * 1_000_000_000
            )
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self.timeout is not None:
            del _cooperation.deadline
