# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
