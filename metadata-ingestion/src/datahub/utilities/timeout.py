import functools
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from typing import Callable, TypeVar

from typing_extensions import ParamSpec

P = ParamSpec("P")
R = TypeVar("R")


def timeout(
    seconds: float, *, enabled: bool = True
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Add a timeout to a function, powered by threads.

    Args:
        seconds: The number of seconds to wait for the function to complete.
        enabled: If False, the timeout will be disabled.

    Returns:
        A decorator that adds a timeout to a function.

    Raises:
        TimeoutError: If the function does not complete within the specified number of seconds.
        Exception: If the function raises an exception, it will be reraised.
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        if not enabled:
            return func

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            # TODO: This creates a new ThreadPoolExecutor for each call to the
            # decorated function. We should consider creating a single
            # ThreadPoolExecutor and reusing it for all calls to the decorated
            # function.

            thread_name_prefix = f"{func.__name__}_thread"
            with ThreadPoolExecutor(
                max_workers=1, thread_name_prefix=thread_name_prefix
            ) as executor:
                future = executor.submit(func, *args, **kwargs)
                try:
                    return future.result(timeout=seconds)
                except TimeoutError:
                    # Cancel the task in case of a timeout error.
                    future.cancel()

                    raise  # Reraise timeout errors.

        return wrapper

    return decorator
