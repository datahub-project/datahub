import collections
import sqlite3
import tempfile
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Generic,
    Iterator,
    List,
    MutableMapping,
    OrderedDict,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

# https://docs.python.org/3/library/sqlite3.html#sqlite-and-python-types
SqliteValue = Union[int, float, str, bytes, None]

_VT = TypeVar("_VT")


@dataclass
class FileBackedDict(MutableMapping[str, _VT], Generic[_VT]):
    """A dictionary that stores its data in a temporary SQLite database.

    This is useful for storing large amounts of data that don't fit in memory.

    For performance, implements an in-memory LRU cache using an OrderedDict,
    and sets a generous journal size limit.
    """

    serializer: Callable[[_VT], SqliteValue]
    deserializer: Callable[[Any], _VT]

    filename: str = field(default_factory=tempfile.mktemp)
    cache_max_size: int = field(default=2000)
    cache_eviction_batch_size: int = field(default=200)

    _conn: sqlite3.Connection = field(init=False, repr=False)
    _active_object_cache: OrderedDict[str, _VT] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._conn = sqlite3.connect(self.filename, isolation_level=None)

        # We keep a small cache in memory to avoid having to serialize/deserialize
        # data from the database too often. We use an OrderedDict to build
        # a poor-man's LRU cache.
        self._active_object_cache = collections.OrderedDict()

        # These settings are optimized for performance.
        # See https://www.sqlite.org/pragma.html for more information.
        # Because we're only using this file to offload data from memory, we don't need
        # to worry about data integrity too much.
        self._conn.execute('PRAGMA locking_mode = "EXCLUSIVE"')
        self._conn.execute('PRAGMA synchronous = "OFF"')
        self._conn.execute('PRAGMA journal_mode = "MEMORY"')
        self._conn.execute(f"PRAGMA journal_size_limit = {100 * 1024 * 1024}")  # 100MB

        # The key will automatically be indexed.
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS data (key TEXT PRIMARY KEY, value BLOB)"
        )

    def _add_to_cache(self, key: str, value: _VT) -> None:
        self._active_object_cache[key] = value

        if (
            len(self._active_object_cache)
            > self.cache_max_size + self.cache_eviction_batch_size
        ):
            num_items_to_prune = len(self._active_object_cache) - self.cache_max_size
            self._prune_cache(num_items_to_prune)

    def _prune_cache(self, num_items_to_prune: int) -> None:
        items_to_write: List[Tuple[str, SqliteValue]] = []
        for _ in range(num_items_to_prune):
            key, value = self._active_object_cache.popitem(last=False)
            items_to_write.append((key, self.serializer(value)))

        self._conn.executemany(
            "INSERT OR REPLACE INTO data (key, value) VALUES (?, ?)", items_to_write
        )

    def flush(self) -> None:
        self._prune_cache(len(self._active_object_cache))

    def __getitem__(self, key: str) -> _VT:
        if key in self._active_object_cache:
            self._active_object_cache.move_to_end(key)
            return self._active_object_cache[key]

        cursor = self._conn.execute("SELECT value FROM data WHERE key = ?", (key,))
        result: Sequence[SqliteValue] = cursor.fetchone()
        if result is None:
            raise KeyError(key)

        deserialized_result = self.deserializer(result[0])
        self._add_to_cache(key, deserialized_result)
        return deserialized_result

    def __setitem__(self, key: str, value: _VT) -> None:
        self._add_to_cache(key, value)

    def __delitem__(self, key: str) -> None:
        in_cache = False
        if key in self._active_object_cache:
            del self._active_object_cache[key]
            in_cache = True

        n_deleted = self._conn.execute(
            "DELETE FROM data WHERE key = ?", (key,)
        ).rowcount
        if not in_cache and not n_deleted:
            raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        cursor = self._conn.execute("SELECT key FROM data")
        for row in cursor:
            if row[0] in self._active_object_cache:
                # If the key is in the active object cache, then SQL isn't the source of truth.
                continue

            yield row[0]

        for key in self._active_object_cache:
            yield key

    def __len__(self) -> int:
        cursor = self._conn.execute(
            # Binding a list of values in SQLite: https://stackoverflow.com/a/1310001/5004662.
            f"SELECT COUNT(*) FROM data WHERE key NOT IN ({','.join('?' * len(self._active_object_cache))})",
            (*self._active_object_cache.keys(),),
        )
        row = cursor.fetchone()

        return row[0] + len(self._active_object_cache)

    def __repr__(self) -> str:
        return f"FileBackedDict({self.filename})"

    def close(self) -> None:
        self._conn.close()

    def __del__(self) -> None:
        self.close()
