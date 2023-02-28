import sqlite3
import tempfile
from typing import Generic, Iterator, MutableMapping, Optional, TypeVar

_VT = TypeVar("_VT")


class FileBackedDict(MutableMapping[str, _VT], Generic[_VT]):
    """A dictionary that stores its data in a temporary SQLite database.

    This is useful for storing large amounts of data that don't fit in memory.
    """

    def __init__(self, filename: Optional[str] = None):
        self._filename = filename or tempfile.mktemp()

        self._conn = sqlite3.connect(self._filename, isolation_level=None)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS data (key TEXT PRIMARY KEY, value BLOB)"
        )
        self._conn.execute('PRAGMA synchronous = "OFF"')

    def __getitem__(self, key: str) -> _VT:
        cursor = self._conn.execute("SELECT value FROM data WHERE key = ?", (key,))
        result = cursor.fetchone()
        if result is None:
            raise KeyError(key)
        return result[0]

    def __setitem__(self, key: str, value: _VT) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO data (key, value) VALUES (?, ?)", (key, value)
        )
        self._conn.commit()

    def __delitem__(self, key: str) -> None:
        self._conn.execute("DELETE FROM data WHERE key = ?", (key,))
        self._conn.commit()

    def __iter__(self) -> Iterator[str]:
        cursor = self._conn.execute("SELECT key FROM data")
        for row in cursor:
            yield row[0]

    def __len__(self) -> int:
        cursor = self._conn.execute("SELECT COUNT(*) FROM data")
        return cursor.fetchone()[0]

    def __repr__(self) -> str:
        return f"FileBackedDict({self._filename})"

    def close(self) -> None:
        self._conn.close()
