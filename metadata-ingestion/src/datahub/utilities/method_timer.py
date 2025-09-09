import functools
import time
from typing import Any, Callable, TypeVar, cast

F = TypeVar("F", bound=Callable[..., Any])


def timed_method(timer_key: str) -> Callable[[F], F]:
    """
    Decorator to time method execution and store the result in a timing map.

    The decorated class must have a 'method_timings_sec' attribute of type Dict[str, float].

    Args:
        timer_key: Key to store the timing under in the method_timings_sec dict

    Usage:
        class MyClass:
            def __init__(self):
                self.method_timings_sec: Dict[str, float] = {}

            @timed_method("compute_stats")
            def compute_stats(self):
                # method implementation
                pass
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            if not hasattr(self, "method_timings_sec"):
                raise AttributeError(
                    f"Class {self.__class__.__name__} must have a 'method_timings_sec' attribute "
                    "to use the @timed_method decorator"
                )

            start_time = time.perf_counter()
            try:
                result = func(self, *args, **kwargs)
                return result
            finally:
                end_time = time.perf_counter()
                elapsed_seconds = end_time - start_time
                if timer_key in self.method_timings_sec:
                    self.method_timings_sec[timer_key] += elapsed_seconds
                else:
                    self.method_timings_sec[timer_key] = elapsed_seconds

        return cast(F, wrapper)

    return decorator
