from dataclasses import dataclass
from functools import wraps
from typing import Callable, Dict, TypeVar

from typing_extensions import ParamSpec

P = ParamSpec("P")
R = TypeVar("R")
Func = Callable[P, R]


@dataclass
class SingleEntryCacher:
    """Provides the ability to cache a function's results based on its arguments.

    Only stores the most
    """

    report_dict: Dict[str, int]

    def cache(self, func: Func) -> Func:
        store = (None, None)

        @wraps
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            nonlocal store

            cached_key, cached_val = store
            if cached_key == args:
                return cached_val
            else:
                val = func(*args, **kwargs)
                store = (args, val)
                self.report_dict[func.__name__] += 1
                return val

        return wrapper
