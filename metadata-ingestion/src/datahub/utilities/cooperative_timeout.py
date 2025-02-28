import contextlib
import contextvars
import time
from typing import Iterator, Optional

# The deadline is an int from time.perf_counter_ns().
_cooperation_deadline = contextvars.ContextVar[int]("cooperation_deadline")


class CooperativeTimeoutError(TimeoutError):
    """An exception raised when a cooperative timeout is exceeded."""


def cooperate() -> None:
    """Method to be called periodically to cooperate with the timeout mechanism."""

    deadline = _cooperation_deadline.get(None)
    if deadline is not None and deadline < time.perf_counter_ns():
        raise CooperativeTimeoutError("CooperativeTimeout deadline exceeded")


@contextlib.contextmanager
def cooperative_timeout(timeout: Optional[float] = None) -> Iterator[None]:
    """A cooperative timeout mechanism.

    Useful in cases where (1) we have control over the code that we want to time out
    and (2) we can modify it to regularly and reliably call a specific function.

    This is not reentrant and cannot be used in nested contexts. It can be used
    in multi-threaded contexts, so long as the cooperative function is called
    from the same thread that created the timeout.

    Args:
        timeout: The timeout in seconds. If None, the timeout is disabled.

    Raises:
        RuntimeError: If a cooperative timeout is already active.
        CooperativeTimeoutError: If the cooperative timeout is exceeded.
    """

    # Getting code to time out in Python is actually rather tricky, and so this felt
    # like the most straightforward approach. Other approaches include:
    #   - Using the signal module to set a signal handler that raises an exception
    #     after a certain time. Unfortunately, this approach only works on the main
    #     thread, and is not available on Windows.
    #   - Creating a separate process to run the code and then killing it if it hasn't
    #     finished by the deadline. This usually requires that all arguments/return
    #     types are pickleable so that they can be passed between processes. Overall,
    #     this approach is heavy-handed and can be tricky to implement correctly.
    #   - Using `threading` is not an option, since Python threads are not interruptible
    #     (unless you're willing to use some hacks https://stackoverflow.com/a/61528202).
    #     Attempting to forcibly terminate a thread can deadlock on the GIL.

    deadline = _cooperation_deadline.get(None)
    if deadline is not None:
        raise RuntimeError("cooperative timeout already active")

    if timeout is not None:
        token = _cooperation_deadline.set(
            time.perf_counter_ns() + int(timeout * 1_000_000_000)
        )
        try:
            yield
        finally:
            _cooperation_deadline.reset(token)

    else:
        # No-op.
        yield
