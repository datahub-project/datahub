# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import Iterable, List, Set, TypeVar

_T = TypeVar("_T")


def deduplicate_list(iterable: Iterable[_T]) -> List[_T]:
    """
    Remove duplicates from an iterable, preserving order.
    This serves as a replacement for OrderedSet, which is broken in Python 3.10.
    """

    seen: Set[_T] = set()
    result: List[_T] = []
    for item in iterable:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result
