import threading
from typing import Callable, ParamSpec, TypeVar

__all__ = ["serialized"]

P = ParamSpec("P")
R = TypeVar("R")


def serialized(func: Callable[P, R]) -> Callable[P, R]:
    lock = threading.Lock()

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        with lock:
            return func(*args, **kwargs)

    return wrapper
