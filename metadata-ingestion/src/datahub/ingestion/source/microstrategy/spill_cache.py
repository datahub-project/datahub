import threading
from typing import Callable, Dict, Generic, Optional, TypeVar, Union

from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.source.microstrategy.constants import (
    MSTR_EVICTION_BATCH_DIVISOR,
    MSTR_MAX_EVICTION_BATCH,
)
from datahub.utilities.file_backed_collections import ConnectionWrapper, FileBackedDict
from datahub.utilities.sentinels import Unset, unset

_V = TypeVar("_V")


class SpillConnection(Closeable):
    """One temporary SQLite file shared by a project's spill caches, each in
    its own table. Opened on first write and removed by close()."""

    def __init__(self) -> None:
        # Shared with every cache on this connection: ConnectionWrapper only
        # locks execute(), and FileBackedDict reads the cursor after it returns.
        self.lock = threading.RLock()
        self._wrapper: Optional[ConnectionWrapper] = None

    def get(self) -> ConnectionWrapper:
        with self.lock:
            if self._wrapper is None:
                self._wrapper = ConnectionWrapper()
            return self._wrapper

    def drop_table(self, tablename: str) -> None:
        with self.lock:
            if self._wrapper is not None:
                self._wrapper.execute(f"DROP TABLE IF EXISTS {tablename}")

    def close(self) -> None:
        with self.lock:
            wrapper, self._wrapper = self._wrapper, None
        if wrapper is not None:
            wrapper.close()


class SpillCache(Closeable, Generic[_V]):
    # FileBackedDict is not thread-safe, so every access holds the connection's
    # lock. A loader runs outside it; concurrent callers of the same key wait.
    def __init__(
        self,
        connection: SpillConnection,
        tablename: str,
        memory_entries: int,
        compress: bool = False,
    ) -> None:
        self._connection = connection
        self._tablename = tablename
        self._memory_entries = max(1, memory_entries)
        self._compress = compress
        self._lock = connection.lock
        self._store: Optional[FileBackedDict[_V]] = None
        self._loading: Dict[str, threading.Event] = {}
        self._closed = False

    def get(self, key: str) -> Union[_V, Unset]:
        with self._lock:
            return self._lookup(key)

    def put(self, key: str, value: _V) -> None:
        with self._lock:
            self._open()[key] = value

    def get_or_load(self, key: str, loader: Callable[[], _V]) -> _V:
        # A failed load caches nothing, so a waiter runs its own load.
        while True:
            with self._lock:
                value = self._lookup(key)
                if not isinstance(value, Unset):
                    return value
                pending = self._loading.get(key)
                if pending is None:
                    pending = threading.Event()
                    self._loading[key] = pending
                    break
            pending.wait()
        try:
            loaded = loader()
            with self._lock:
                # A reset or close during the load released the key; the value
                # must not land in the store opened after it.
                if self._loading.get(key) is pending:
                    self._open()[key] = loaded
            return loaded
        finally:
            with self._lock:
                if self._loading.get(key) is pending:
                    del self._loading[key]
            pending.set()

    def reset(self) -> None:
        with self._lock:
            self._closed = False
            self._release()

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._release()

    def _release(self) -> None:
        loading, self._loading = self._loading, {}
        for pending in loading.values():
            pending.set()
        store, self._store = self._store, None
        if store is None or store.closed:
            return
        try:
            store.close()
        finally:
            self._connection.drop_table(self._tablename)

    def _live_store(self) -> Optional[FileBackedDict[_V]]:
        if self._closed:
            raise RuntimeError(f"Spill cache {self._tablename} is closed")
        # Closing the shared connection closes every store opened on it.
        if self._store is not None and self._store.closed:
            self._store = None
        return self._store

    def _lookup(self, key: str) -> Union[_V, Unset]:
        store = self._live_store()
        if store is None:
            return unset
        try:
            return store[key]
        except KeyError:
            return unset

    def _open(self) -> FileBackedDict[_V]:
        store = self._live_store()
        if store is None:
            store = FileBackedDict(
                shared_connection=self._connection.get(),
                tablename=self._tablename,
                cache_max_size=self._memory_entries,
                cache_eviction_batch_size=max(
                    1,
                    min(
                        MSTR_MAX_EVICTION_BATCH,
                        self._memory_entries // MSTR_EVICTION_BATCH_DIVISOR,
                    ),
                ),
                should_compress_value=self._compress,
            )
            self._store = store
        return store
