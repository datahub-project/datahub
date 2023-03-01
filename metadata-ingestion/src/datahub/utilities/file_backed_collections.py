import collections
import sqlite3
import tempfile
from typing import Generic, Iterator, MutableMapping, Optional, OrderedDict, TypeVar

_VT = TypeVar("_VT")

_CACHE_EVICTION_ALLOWED_OVERAGE = 10


class FileBackedDict(MutableMapping[str, _VT], Generic[_VT]):
    """A dictionary that stores its data in a temporary SQLite database.

    This is useful for storing large amounts of data that don't fit in memory.
    """

    _cache_max_size: int
    _filename: str
    _conn: sqlite3.Connection

    _active_object_cache: OrderedDict[str, _VT]

    def __init__(self, filename: Optional[str] = None, cache_max_size: int = 2000):
        self._cache_max_size = cache_max_size
        self._filename = filename or tempfile.mktemp()

        self._conn = sqlite3.connect(self._filename, isolation_level=None)

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
            > self._cache_max_size + _CACHE_EVICTION_ALLOWED_OVERAGE
        ):
            num_items_to_prune = len(self._active_object_cache) - self._cache_max_size
            self._prune_cache(num_items_to_prune)

    def _prune_cache(self, num_items_to_prune: int) -> None:
        items_to_write = []
        for _ in range(num_items_to_prune):
            key, value = self._active_object_cache.popitem(last=False)
            items_to_write.append((key, value))

        self._conn.executemany(
            "INSERT OR REPLACE INTO data (key, value) VALUES (?, ?)", items_to_write
        )

    def __getitem__(self, key: str) -> _VT:
        if key in self._active_object_cache:
            self._active_object_cache.move_to_end(key)
            return self._active_object_cache[key]

        cursor = self._conn.execute("SELECT value FROM data WHERE key = ?", (key,))
        result = cursor.fetchone()
        if result is None:
            raise KeyError(key)

        self._add_to_cache(key, result[0])
        return result[0]

    def __setitem__(self, key: str, value: _VT) -> None:
        self._add_to_cache(key, value)

    def __delitem__(self, key: str) -> None:
        self[key]  # raise KeyError if key doesn't exist

        if key in self._active_object_cache:
            del self._active_object_cache[key]

        self._conn.execute("DELETE FROM data WHERE key = ?", (key,))

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
        return f"FileBackedDict({self._filename})"

    def close(self) -> None:
        self._conn.close()
