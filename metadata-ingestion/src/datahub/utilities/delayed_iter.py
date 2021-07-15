import collections
from typing import Deque, Iterable, Optional, TypeVar

T = TypeVar("T")


def delayed_iter(iterable: Iterable[T], delay: Optional[int]) -> Iterable[T]:
    """Waits to yield the i'th element until after the (i+n)'th element has been
    materialized by the source iterator. If delay is none, wait until the full
    iterable has been materialized before yielding.
    """

    cache: Deque[T] = collections.deque([], maxlen=delay)

    for item in iterable:
        if delay is not None and len(cache) >= delay:
            yield cache.popleft()
        cache.append(item)

    while len(cache):
        yield cache.popleft()
