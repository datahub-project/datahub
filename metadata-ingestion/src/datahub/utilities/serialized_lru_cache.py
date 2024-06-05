import functools
import threading
from typing import Callable, Dict, TypeVar

import cachetools
import cachetools.keys
from typing_extensions import ParamSpec

F = ParamSpec("F")
T = TypeVar("T")


def serialized_lru_cache(maxsize: int) -> Callable[[Callable[F, T]], Callable[F, T]]:
    """Similar to `lru_cache`, but ensures multiple calls with the same parameters are serialized.

    Calls with different parameters are allowed to proceed in parallel.

    Args:
        maxsize (int): Maximum number of entries to keep in the cache.

    Returns:
        Callable[[Callable[F, T]], Callable[F, T]]: Decorator for the function to be wrapped.
    """

    UNSET = object()

    def decorator(func: Callable[F, T]) -> Callable[F, T]:
        hits = 0
        misses = 0

        cache_lock = threading.Lock()
        cache = cachetools.LRUCache[str, T](maxsize=maxsize)

        key_locks_lock = threading.Lock()
        key_locks: Dict[str, threading.Lock] = {}
        key_waiters: Dict[str, int] = {}

        def wrapper(*args: F.args, **kwargs: F.kwargs) -> T:
            key = cachetools.keys.hashkey(*args, **kwargs)

            with cache_lock:
                if key in cache:
                    nonlocal hits
                    hits += 1
                    return cache[key]

            with key_locks_lock:
                if key not in key_locks:
                    key_locks[key] = threading.Lock()
                    key_waiters[key] = 0
                lock = key_locks[key]
                key_waiters[key] += 1

            try:
                with lock:
                    # Check the cache again, in case the cache was updated by another thread.
                    result = UNSET
                    with cache_lock:
                        if key in cache:
                            hits += 1
                            return cache[key]

                    nonlocal misses
                    misses += 1
                    result = func(*args, **kwargs)

                    with cache_lock:
                        cache[key] = result
                    return result

            finally:
                with key_locks_lock:
                    key_waiters[key] -= 1
                    if key_waiters[key] == 0:
                        del key_locks[key]
                        del key_waiters[key]

        def cache_info() -> functools._CacheInfo:
            return functools._CacheInfo(
                hits=hits,
                misses=misses,
                maxsize=maxsize,
                currsize=len(cache),
            )

        wrapper.cache = cache
        wrapper.cache_info = cache_info

        return functools.update_wrapper(wrapper, func)

    return decorator
