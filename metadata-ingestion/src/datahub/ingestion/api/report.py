import collections
import json
import pprint
import sys
from dataclasses import dataclass
from enum import Enum
from typing import Any, Deque, Dict, Iterator, List, Optional, Set, TypeVar

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


class LossyList(List[T]):
    """A list that only preserves the head and tail of lists longer than a certain number"""

    def __init__(
        self, max_elements: int = 10, section_breaker: Optional[str] = "..."
    ) -> None:
        super().__init__()
        self.max_elements = max_elements
        self.list_head: List[T] = []
        self.list_tail: Deque[T] = collections.deque([], maxlen=int(max_elements / 2))
        self.head_full = False
        self.total_elements = 0
        self.section_breaker = section_breaker

    def __iter__(self) -> Iterator[T]:
        yield from self.list_head
        if self.section_breaker and len(self.list_tail):
            yield f"{self.section_breaker} {self.total_elements - len(self.list_head) - len(self.list_tail)} more elements"  # type: ignore
        yield from self.list_tail

    def append(self, __object: T) -> None:
        if self.head_full:
            self.list_tail.append(__object)
        else:
            self.list_head.append(__object)
            if len(self.list_head) > int(self.max_elements / 2):
                self.head_full = True
        self.total_elements += 1

    def __len__(self) -> int:
        return self.total_elements

    def __repr__(self) -> str:
        return repr(list(self.__iter__()))

    def __str__(self) -> str:
        return str(list(self.__iter__()))


class LossySet(Set[T]):
    """A set that only preserves a sample of elements in a set. Currently this is a very simple greedy sampling set"""

    def __init__(self, max_elements: int = 20) -> None:
        super().__init__()
        self.max_elements = max_elements
        self.inner_set: Set[T] = set()
        self.overflow = 0

    def __iter__(self) -> Iterator[T]:
        yield from self.inner_set

    def add(self, __element: T) -> None:
        if len(self.inner_set) <= self.max_elements:
            self.inner_set.add(__element)
        else:
            self.overflow += 1
        return None

    def __len__(self) -> int:
        return len(self.inner_set)

    def __repr__(self) -> str:
        count = len(self.inner_set)
        count_str = (
            f"{count} total. "
            if self.overflow == 0
            else f"{count} sampled of at most {self.max_elements + self.overflow} elements. "
        )
        return count_str + (repr(list(self.__iter__())))

    def __str__(self) -> str:
        return self.__repr__()
