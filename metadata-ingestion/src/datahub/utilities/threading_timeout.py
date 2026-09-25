import contextlib
import functools
import platform
from typing import ContextManager, cast

from datahub.utilities._stopit import (
    ThreadingTimeout as _ThreadingTimeout,
    TimeoutException,
)

__all__ = ["threading_timeout", "TimeoutException"]


@functools.lru_cache(maxsize=1)
def _is_cpython() -> bool:
    """Check if we're running on CPython."""
    return platform.python_implementation() == "CPython"


def threading_timeout(timeout: float) -> ContextManager[None]:
    """A timeout context manager backed by a vendored copy of stopit's
    ThreadingTimeout (``datahub.utilities._stopit``).

    This is only supported on CPython.
    That's because it uses a CPython-internal method to raise an exception (the
    timeout error) in another thread. See ``datahub.utilities._stopit.async_raise``.

    Reference (upstream): https://github.com/glenfant/stopit

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

    # callers never use the entered value, so expose it as ContextManager[None].
    return cast(ContextManager[None], _ThreadingTimeout(timeout, swallow_exc=False))
