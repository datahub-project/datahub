# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import collections
from typing import Callable, Iterable, Tuple, TypeVar

T = TypeVar("T")
K = TypeVar("K")


def groupby_unsorted(
    iterable: Iterable[T], key: Callable[[T], K]
) -> Iterable[Tuple[K, Iterable[T]]]:
    """The default itertools.groupby() requires that the iterable is already sorted by the key.
    This method is similar to groupby() but without the pre-sorted requirement."""

    values = collections.defaultdict(list)
    for v in iterable:
        values[key(v)].append(v)
    return values.items()
