from typing import Any, Callable, List, Protocol, TypeVar

from datahub.errors import ItemNotFoundError


class _SupportsEq(Protocol):
    def __eq__(self, other: Any) -> bool: ...


T = TypeVar("T")
K = TypeVar("K", bound=_SupportsEq)


def add_list_unique(lst: List[T], key: Callable[[T], K], item: T) -> None:
    item_key = key(item)
    for i, existing in enumerate(lst):
        if key(existing) == item_key:
            lst[i] = item
            return
    lst.append(item)


def remove_list_unique(
    lst: List[T], key: Callable[[T], K], item: T, *, missing_ok: bool = True
) -> None:
    # Poor man's patch implementation.
    item_key = key(item)
    removed = False
    for i, existing in enumerate(lst):
        if key(existing) == item_key:
            lst.pop(i)
            removed = True
            # Tricky: no break. In case there's already duplicates, we want to remove all of them.
    if not removed and not missing_ok:
        raise ItemNotFoundError(f"Cannot remove item {item} from list: not found")
