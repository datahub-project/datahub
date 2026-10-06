import threading
from typing import Callable, Dict, Generic, Iterator, List, Optional, TypeVar, overload

K = TypeVar("K")
V = TypeVar("V")


class GuardedSet(Generic[V]):
    """Thread-safe set with atomic check-and-add.

    Wraps a plain set with an internal lock so callers never need to manage
    lock discipline themselves. The primary method is ``check_and_add`` which
    atomically tests membership and inserts in one step.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: set = set()

    def check_and_add(self, item: V) -> bool:
        """Atomically check membership and add if absent.

        Returns True if the item was newly added, False if already present.
        """
        with self._lock:
            if item in self._data:
                return False
            self._data.add(item)
            return True

    def add(self, item: V) -> None:
        with self._lock:
            self._data.add(item)

    def __contains__(self, item: object) -> bool:
        with self._lock:
            return item in self._data

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def __iter__(self) -> Iterator[V]:
        with self._lock:
            return iter(self._data.copy())


class GuardedDict(Generic[K, V]):
    """Thread-safe dict with atomic compute-if-absent.

    Wraps a plain dict with an internal lock. Individual operations like
    ``get`` and ``__setitem__`` are already atomic under CPython's GIL,
    but the lock makes compound operations (``compute_if_absent``) safe
    and keeps the API consistent regardless of runtime.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: Dict[K, V] = {}

    @overload
    def get(self, key: K) -> Optional[V]: ...

    @overload
    def get(self, key: K, default: V) -> V: ...

    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:
        with self._lock:
            return self._data.get(key, default)

    def compute_if_absent(self, key: K, factory: Callable[[K], V]) -> V:
        """Return existing value for *key*, or call *factory(key)* to create one.

        The factory is called inside the lock, so only one thread will ever
        compute a value for a given key.
        """
        with self._lock:
            if key in self._data:
                return self._data[key]
            value = factory(key)
            self._data[key] = value
            return value

    def __setitem__(self, key: K, value: V) -> None:
        with self._lock:
            self._data[key] = value

    def __getitem__(self, key: K) -> V:
        with self._lock:
            return self._data[key]

    def __contains__(self, key: object) -> bool:
        with self._lock:
            return key in self._data

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def __iter__(self) -> Iterator[K]:
        with self._lock:
            return iter(list(self._data.keys()))

    def setdefault(self, key: K, default: V) -> V:
        with self._lock:
            return self._data.setdefault(key, default)

    def values(self) -> List[V]:
        with self._lock:
            return list(self._data.values())

    def items(self) -> List:
        with self._lock:
            return list(self._data.items())
