import contextlib
import contextvars
import signal
import threading
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


@contextlib.contextmanager
def interrupt_on_cooperative_deadline() -> Iterator[None]:
    """Stop the current main-thread call when the active cooperative deadline passes.

    ``cooperative_timeout`` only fires when code calls ``cooperate()``. sqlglot's
    ``optimizer.optimize`` does not, so a query can sit there past the budget.
    ``SIGALRM`` interrupts that call. It is delivered to the main thread only, and
    it does not exist on Windows, so anywhere else this is a no-op and the
    cooperative checks remain the only limit.

    A host may already have an alarm armed, for example Airflow's task
    ``execution_timeout``. That alarm is never delayed: if it is due before the
    cooperative deadline it is left alone and this is a no-op, and otherwise it
    is re-armed on exit with only the time it had left.
    """
    deadline = _cooperation_deadline.get(None)
    if (
        deadline is None
        or threading.current_thread() is not threading.main_thread()
        or not hasattr(signal, "setitimer")
        or not hasattr(signal, "SIGALRM")
    ):
        yield
        return

    remaining = (deadline - time.perf_counter_ns()) / 1_000_000_000
    if remaining <= 0:
        raise CooperativeTimeoutError("CooperativeTimeout deadline exceeded")

    previous_delay, previous_interval = signal.getitimer(signal.ITIMER_REAL)
    if 0 < previous_delay <= remaining:
        # The existing alarm fires first. Taking it over would postpone it.
        yield
        return

    def _handler(signum: int, frame: object) -> None:
        raise CooperativeTimeoutError("CooperativeTimeout deadline exceeded")

    previous = signal.signal(signal.SIGALRM, _handler)
    armed_at = time.perf_counter()
    try:
        # Armed inside try so an alarm that fires immediately still runs cleanup.
        signal.setitimer(signal.ITIMER_REAL, remaining)
        yield
    finally:
        # Ignore first so a signal already queued is discarded instead of
        # running our handler during cleanup and skipping the restore.
        signal.signal(signal.SIGALRM, signal.SIG_IGN)
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
        if previous_delay > 0:
            # Subtract the time spent here, or every call would push the host's
            # alarm back and it might never fire. setitimer(0) would disarm it,
            # so a delay that is already due is armed as soon as possible.
            elapsed = time.perf_counter() - armed_at
            signal.setitimer(
                signal.ITIMER_REAL,
                max(previous_delay - elapsed, 1e-6),
                previous_interval,
            )
