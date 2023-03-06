import collections
import pathlib
import sqlite3
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
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
class _SqliteConnectionCache:
    """
    If you pass the same filename to multiple FileBacked* objects, they will
    share the same underlying database connection. This also does ref counting
    to drop the connection when appropriate.
    """

    _ref_count: Dict[pathlib.Path, int] = field(default_factory=dict)
    _sqlite_connection_cache: Dict[pathlib.Path, sqlite3.Connection] = field(
        default_factory=dict
    )

    def get_connection(self, filename: pathlib.Path) -> sqlite3.Connection:
        if filename not in self._ref_count:
            conn = sqlite3.connect(filename, isolation_level=None)

            # These settings are optimized for performance.
            # See https://www.sqlite.org/pragma.html for more information.
            # Because we're only using these dbs to offload data from memory, we don't need
            # to worry about data integrity too much.
            conn.execute('PRAGMA locking_mode = "EXCLUSIVE"')
            conn.execute('PRAGMA synchronous = "OFF"')
            conn.execute('PRAGMA journal_mode = "MEMORY"')
            conn.execute(f"PRAGMA journal_size_limit = {100 * 1024 * 1024}")  # 100MB

            self._ref_count[filename] = 0
            self._sqlite_connection_cache[filename] = conn

        self._ref_count[filename] += 1
        return self._sqlite_connection_cache[filename]

    def drop_connection(self, filename: pathlib.Path) -> None:
        self._ref_count[filename] -= 1

        if self._ref_count[filename] == 0:
            # Cleanup the connection object.
            self._sqlite_connection_cache[filename].close()
            del self._sqlite_connection_cache[filename]
            del self._ref_count[filename]


_sqlite_connection_cache = _SqliteConnectionCache()


@dataclass(eq=False)
class FileBackedDict(MutableMapping[str, _VT], Generic[_VT]):
    """A dictionary that stores its data in a temporary SQLite database.

    This is useful for storing large amounts of data that don't fit in memory.

    For performance, implements an in-memory LRU cache using an OrderedDict,
    and sets a generous journal size limit.
    """

    filename: pathlib.Path

    serializer: Callable[[_VT], SqliteValue]
    deserializer: Callable[[Any], _VT]

    tablename: str = field(default="data")
    cache_max_size: int = field(default=2000)
    cache_eviction_batch_size: int = field(default=200)

    _conn: sqlite3.Connection = field(init=False, repr=False)
    _active_object_cache: OrderedDict[str, _VT] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._conn = _sqlite_connection_cache.get_connection(self.filename)

        # We keep a small cache in memory to avoid having to serialize/deserialize
        # data from the database too often. We use an OrderedDict to build
        # a poor-man's LRU cache.
        self._active_object_cache = collections.OrderedDict()

        # The key column will automatically be indexed.
        self._conn.execute(
            f"CREATE TABLE {self.tablename} (key TEXT PRIMARY KEY, value BLOB)"
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
            f"INSERT OR REPLACE INTO {self.tablename} (key, value) VALUES (?, ?)",
            items_to_write,
        )

    def flush(self) -> None:
        self._prune_cache(len(self._active_object_cache))

    def __getitem__(self, key: str) -> _VT:
        if key in self._active_object_cache:
            self._active_object_cache.move_to_end(key)
            return self._active_object_cache[key]

        cursor = self._conn.execute(
            f"SELECT value FROM {self.tablename} WHERE key = ?", (key,)
        )
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
            f"DELETE FROM {self.tablename} WHERE key = ?", (key,)
        ).rowcount
        if not in_cache and not n_deleted:
            raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        cursor = self._conn.execute(f"SELECT key FROM {self.tablename}")
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
            f"SELECT COUNT(*) FROM {self.tablename} WHERE key NOT IN ({','.join('?' * len(self._active_object_cache))})",
            (*self._active_object_cache.keys(),),
        )
        row = cursor.fetchone()

        return row[0] + len(self._active_object_cache)

    def close(self) -> None:
        _sqlite_connection_cache.drop_connection(self.filename)

    def __del__(self) -> None:
        self.close()
