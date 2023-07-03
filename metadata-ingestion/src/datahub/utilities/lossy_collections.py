import random
from typing import Dict, Iterator, List, Set, TypeVar, Union

T = TypeVar("T")
_KT = TypeVar("_KT")
_VT = TypeVar("_VT")


class LossyList(List[T]):
    """A list that performs reservoir sampling of a much larger list"""

    def __init__(self, max_elements: int = 10) -> None:
        super().__init__()
        self.max_elements = max_elements
        self.total_elements = 0
        self.sampled = False

    def append(self, __object: T) -> None:
        try:
            if self.total_elements >= self.max_elements:
                i = random.choice(range(0, self.total_elements + 1))
                if i < self.max_elements:
                    self.sampled = True
                    return super().__setitem__(i, (self.total_elements, __object))  # type: ignore
                else:
                    return

            return super().append((self.total_elements, __object))  # type: ignore
        finally:
            self.total_elements += 1

    def __len__(self) -> int:
        return self.total_elements

    def __iter__(self) -> Iterator[T]:
        yield from [elem[1] for elem in sorted(super().__iter__())]  # type: ignore

    def __repr__(self) -> str:
        return repr(self.as_obj())

    def __str__(self) -> str:
        return repr(self)

    def as_obj(self) -> List[Union[T, str]]:
        base_list: List[Union[T, str]] = list(self.__iter__())
        if self.sampled:
            base_list.append(f"... sampled of {self.total_elements} total elements")
        return base_list


class LossySet(Set[T]):
    """A set that only preserves a sample of elements in a set. Currently this is a very simple greedy sampling set"""

    def __init__(self, max_elements: int = 10) -> None:
        super().__init__()
        self.max_elements = max_elements
        self.sampled = False
        self._items_removed = 0
        self._items_ignored = 0

    def add(self, __element: T) -> None:
        if (
            not super().__contains__(__element)
            and super().__len__() >= self.max_elements
        ):
            self.sampled = True
            i = random.choice(range(0, super().__len__()))
            if i < self.max_elements:
                super().remove(list(super().__iter__())[i])
                self._items_removed += 1
                return super().add(__element)
            else:
                self._items_ignored += 1
                return None
        return super().add(__element)

    def __repr__(self) -> str:
        return repr(self.as_obj())

    def __str__(self) -> str:
        return self.__repr__()

    def as_obj(self) -> List[Union[T, str]]:
        base_list: List[Union[T, str]] = list(self.__iter__())
        if self.sampled:
            base_list.append(
                f"... sampled with at most {self._items_removed} elements missing."
            )
        return base_list


class LossyDict(Dict[_KT, _VT]):
    """A structure that only preserves a sample of elements in a dictionary using reservoir sampling."""

    def __init__(self, max_elements: int = 10) -> None:
        super().__init__()
        self.max_elements = max_elements
        self.sampled = False
        self._overflow = 0
        self._items_removed = 0
        self._items_ignored = 0

    def __getitem__(self, __k: _KT) -> _VT:
        return super().__getitem__(__k)

    def __setitem__(self, __k: _KT, __v: _VT) -> None:
        if not super().__contains__(__k) and super().__len__() >= self.max_elements:
            self.sampled = True
            self._overflow += 1
            i = random.choice(range(0, super().__len__() + self._overflow))
            if i < self.max_elements:
                super().pop(list(super().__iter__())[i])
                self._items_removed += 1
                return super().__setitem__(__k, __v)
            else:
                self._items_ignored += 1
                return None
        else:
            return super().__setitem__(__k, __v)

    def __repr__(self) -> str:
        return repr(self.as_obj())

    def __str__(self) -> str:
        return self.__repr__()

    def as_obj(self) -> Dict[Union[_KT, str], Union[_VT, str]]:
        base_dict: Dict[Union[_KT, str], Union[_VT, str]] = super().copy()  # type: ignore
        if self.sampled:
            base_dict[
                "sampled"
            ] = f"{len(self.keys())} sampled of at most {self.max_elements + self._overflow} entries."
        return base_dict

    def dropped_keys_count(self) -> int:
        """Returns the number of keys that have been dropped from this dictionary."""
        return self._overflow
