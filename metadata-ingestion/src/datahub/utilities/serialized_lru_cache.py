import functools
import threading
from typing import Callable, Dict, Hashable, Tuple, TypeVar

import cachetools
import cachetools.keys
from typing_extensions import ParamSpec

_Key = Tuple[Hashable, ...]
_F = ParamSpec("_F")
_T = TypeVar("_T")


def serialized_lru_cache(
    maxsize: int,
) -> Callable[[Callable[_F, _T]], Callable[_F, _T]]:
    """Similar to `lru_cache`, but ensures multiple calls with the same parameters are serialized.

    Calls with different parameters are allowed to proceed in parallel.

    Args:
        maxsize (int): Maximum number of entries to keep in the cache.

    Returns:
        Callable[[Callable[F, T]], Callable[F, T]]: Decorator for the function to be wrapped.
    """

    UNSET = object()

    def decorator(func: Callable[_F, _T]) -> Callable[_F, _T]:
        hits = 0
        misses = 0

        cache_lock = threading.Lock()
        cache: "cachetools.LRUCache[_Key, _T]" = cachetools.LRUCache(maxsize=maxsize)

        key_locks_lock = threading.Lock()
        key_locks: Dict[_Key, threading.Lock] = {}
        key_waiters: Dict[_Key, int] = {}

        def wrapper(*args: _F.args, **kwargs: _F.kwargs) -> _T:
            # We need a type ignore here because there's no way for us to require that
            # the args and kwargs are hashable while using ParamSpec.
            key: _Key = cachetools.keys.hashkey(*args, **{k: v for k, v in kwargs.items() if "cache_exclude" not in k})  # type: ignore

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

        # Add some extra attributes to the wrapper function. This makes it mostly compatible
        # with functools.lru_cache.
        wrapper.cache = cache  # type: ignore
        wrapper.cache_info = cache_info  # type: ignore

        return functools.update_wrapper(wrapper, func)

    return decorator
