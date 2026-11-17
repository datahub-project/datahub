from typing import Any, Callable, Dict, Iterable, List, Literal, Optional, TypeVar

_T = TypeVar("_T")


def deduplicate_list(
    iterable: Iterable[_T],
    key: Optional[Callable[[_T], Any]] = None,
    keep: Literal["first", "last"] = "first",
) -> List[_T]:
    """
    Remove duplicates from an iterable, preserving order.
    This serves as a replacement for OrderedSet, which is broken in Python 3.10.

    ``keep`` selects which of two entries sharing a key survives. Either way the
    result keeps the position of the first occurrence; only the value differs.
    ``keep="last"`` gives the same result as ``{**first, **second}`` - use it when
    later entries are meant to take precedence over earlier ones.
    """
    deduped: Dict[Any, _T] = {}
    for item in iterable:
        k = key(item) if key is not None else item
        if keep == "last" or k not in deduped:
            deduped[k] = item
    return list(deduped.values())
