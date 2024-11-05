import contextlib
import functools
import platform
from typing import ContextManager

from stopit import ThreadingTimeout as _ThreadingTimeout, TimeoutException

__all__ = ["threading_timeout", "TimeoutException"]


@functools.lru_cache(maxsize=1)
def _is_cpython() -> bool:
    """Check if we're running on CPython."""
    return platform.python_implementation() == "CPython"


def threading_timeout(timeout: float) -> ContextManager[None]:
    """A timeout context manager that uses stopit's ThreadingTimeout underneath.

    This is only supported on CPython.
    That's because stopit.ThreadingTimeout uses a CPython-internal method to raise
    an exception (the timeout error) in another thread. See stopit.threadstop.async_raise.

    Reference: https://github.com/glenfant/stopit

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

    return _ThreadingTimeout(timeout, swallow_exc=False)
