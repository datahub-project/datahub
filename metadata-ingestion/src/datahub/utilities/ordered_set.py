from typing import Dict, Generic, Iterable, Iterator, MutableSet, Optional, TypeVar

T = TypeVar("T")


class OrderedSet(MutableSet[T], Generic[T]):
    """Ordered set implementation.

    This is a fairly naive implementation - it uses a dict to store the items, and ignores the dict values.
    """

    def __init__(self, iterable: Optional[Iterable[T]] = None) -> None:
        self._data: Dict[T, None] = {}
        if iterable:
            for item in iterable:
                self.add(item)

    def add(self, item: T) -> None:
        self._data[item] = None

    def discard(self, item: T) -> None:
        self._data.pop(item, None)

    def update(self, items: Iterable[T]) -> None:
        for item in items:
            self.add(item)

    def __contains__(self, item: object) -> bool:
        return item in self._data

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"OrderedSet({list(self._data)})"

    def __iter__(self) -> Iterator[T]:
        return iter(self._data)

    def __reversed__(self) -> Iterator[T]:
        return reversed(list(self._data))
