from typing import Any, Callable, Iterable, List, Optional, TypeVar

_T = TypeVar("_T")


def deduplicate_list(
    iterable: Iterable[_T],
    key: Optional[Callable[[_T], Any]] = None,
) -> List[_T]:
    """
    Remove duplicates from an iterable, preserving order.
    This serves as a replacement for OrderedSet, which is broken in Python 3.10.
    """
    seen = set()
    result: List[_T] = []
    for item in iterable:
        k = key(item) if key is not None else item
        if k not in seen:
            seen.add(k)
            result.append(item)
    return result
