import json
import pprint
import random
import sys
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterator, List, Set, TypeVar

# The sort_dicts option was added in Python 3.8.
if sys.version_info >= (3, 8):
    PPRINT_OPTIONS = {"sort_dicts": False}
else:
    PPRINT_OPTIONS: Dict = {}


@dataclass
class Report:
    @staticmethod
    def to_str(some_val: Any) -> str:
        if isinstance(some_val, Enum):
            return some_val.name
        else:
            return str(some_val)

    @staticmethod
    def to_dict(some_val: Any) -> Any:
        """A cheap way to generate a dictionary."""
        if (
            isinstance(some_val, LossyList)
            or isinstance(some_val, LossySet)
            or isinstance(some_val, LossyDict)
        ):  # we don't want these to be processed as regular objects
            return Report.to_str(some_val)
        if hasattr(some_val, "as_obj"):
            return some_val.as_obj()
        if hasattr(some_val, "dict"):
            return some_val.dict()
        elif isinstance(some_val, list):
            return [Report.to_dict(v) for v in some_val if v is not None]
        elif isinstance(some_val, dict):
            return {
                Report.to_str(k): Report.to_dict(v)
                for k, v in some_val.items()
                if v is not None
            }
        else:
            return Report.to_str(some_val)

    def compute_stats(self) -> None:
        """A hook to compute derived stats"""
        pass

    def as_obj(self) -> dict:
        self.compute_stats()
        return {
            str(key): Report.to_dict(value)
            for (key, value) in self.__dict__.items()
            if value is not None  # ignore nulls
        }

    def as_string(self) -> str:
        return pprint.pformat(self.as_obj(), width=150, **PPRINT_OPTIONS)

    def as_json(self) -> str:
        return json.dumps(self.as_obj())


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
                    super().__setitem__(i, (self.total_elements, __object))  # type: ignore
                    return
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
        return list(self.__iter__()).__repr__() + (
            f"... sampled of {self.total_elements} total elements"
            if self.sampled
            else ""
        )

    def __str__(self) -> str:
        return self.__repr__()


class LossySet(Set[T]):
    """A set that only preserves a sample of elements in a set. Currently this is a very simple greedy sampling set"""

    def __init__(self, max_elements: int = 10) -> None:
        super().__init__()
        self.max_elements = max_elements
        self.sampled = False
        self.items_removed = 0
        self.items_ignored = 0

    def add(self, __element: T) -> None:
        if (
            not super().__contains__(__element)
            and super().__len__() >= self.max_elements
        ):
            self.sampled = True
            i = random.choice(range(0, super().__len__()))
            if i < self.max_elements:
                super().remove(list(super().__iter__())[i])
                self.items_removed += 1
                return super().add(__element)
            else:
                self.items_ignored += 1
                return None
        return super().add(__element)

    def __repr__(self) -> str:
        return super().__repr__() + (
            f"... sampled with at most {self.items_removed} elements missing."
            if self.sampled
            else ""
        )

    def __str__(self) -> str:
        return self.__repr__()


class LossyDict(Dict[_KT, _VT]):
    """A structure that only preserves a sample of elements in a dictionary. Currently this is a very simple greedy sampling set"""

    def __init__(self, max_elements: int = 10) -> None:
        super().__init__()
        self.max_elements = max_elements
        self.overflow = 0

    def __getitem__(self, __k: _KT) -> _VT:
        return super().__getitem__(__k)

    def __setitem__(self, __k: _KT, __v: _VT) -> None:
        if __k not in super().keys():
            if len(super().keys()) >= self.max_elements:
                self.overflow += 1
                i = random.choice(range(0, self.max_elements + self.overflow))
                if i < self.max_elements:
                    key_to_remove = list(super().keys())[i]
                    self.pop(key_to_remove)
                else:
                    return
        return super().__setitem__(__k, __v)

    def __repr__(self) -> str:
        count = len(super().keys())
        count_str = (
            f"{count} total entries. "
            if self.overflow == 0
            else f"{count} sampled of at most {self.max_elements + self.overflow} elements. "
        )
        return count_str + super().__repr__()

    def __str__(self) -> str:
        return self.__repr__()
