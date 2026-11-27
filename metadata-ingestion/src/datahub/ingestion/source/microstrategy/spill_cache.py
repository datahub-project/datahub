import threading
from typing import Callable, Dict, Generic, Optional, TypeVar, Union

from pydantic import BaseModel, ConfigDict

from datahub.ingestion.source.microstrategy.models import normalize_object_id
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.utilities.file_backed_collections import FileBackedDict

_V = TypeVar("_V")


class _Missing:
    pass


_MISSING = _Missing()


class CacheLookup(BaseModel, Generic[_V]):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    found: bool
    value: Optional[_V] = None


def object_cache_key(project_id: str, object_id: str) -> str:
    return f"{project_id}:{normalize_object_id(object_id)}"


class SpillCache(Generic[_V]):
    # __len__ binds one SQL variable per key, and the dict is not thread-safe.
    def __init__(self, memory_entries: int, compress: bool = False) -> None:
        self._memory_entries = max(1, memory_entries)
        self._compress = compress
        self._lock = threading.RLock()
        self._store: Optional[FileBackedDict[_V]] = None
        self._entry_count = 0
        self._loading: Dict[str, threading.Event] = {}

    @property
    def entry_count(self) -> int:
        return self._entry_count

    def get(self, key: str) -> Optional[_V]:
        with self._lock:
            value = self._lookup(key)
        return None if isinstance(value, _Missing) else value

    def get_entry(self, key: str) -> CacheLookup[_V]:
        with self._lock:
            value = self._lookup(key)
        if isinstance(value, _Missing):
            return CacheLookup(found=False)
        return CacheLookup(found=True, value=value)

    def contains(self, key: str) -> bool:
        with self._lock:
            return not isinstance(self._lookup(key), _Missing)

    def __contains__(self, key: str) -> bool:
        return self.contains(key)

    def put(self, key: str, value: _V) -> None:
        with self._lock:
            if isinstance(self._lookup(key), _Missing):
                self._entry_count += 1
            self._open()[key] = value

    def put_if_absent(self, key: str, value: _V) -> bool:
        with self._lock:
            if not isinstance(self._lookup(key), _Missing):
                return False
            self._entry_count += 1
            self._open()[key] = value
            return True

    def get_or_load(self, key: str, loader: Callable[[], _V]) -> _V:
        # A loader that raises caches nothing, so a waiter runs its own load.
        while True:
            with self._lock:
                value = self._lookup(key)
                if not isinstance(value, _Missing):
                    return value
                pending = self._loading.get(key)
                if pending is None:
                    pending = threading.Event()
                    self._loading[key] = pending
                    break
            pending.wait()
        try:
            loaded = loader()
        except BaseException:
            with self._lock:
                del self._loading[key]
            pending.set()
            raise
        with self._lock:
            if isinstance(self._lookup(key), _Missing):
                self._entry_count += 1
            self._open()[key] = loaded
            del self._loading[key]
        pending.set()
        return loaded

    def reset(
        self,
        report: Optional[MicroStrategyReport] = None,
        metric_name: Optional[str] = None,
    ) -> None:
        with self._lock:
            if report is not None and metric_name is not None:
                report.report_cache_entries(metric_name, self._entry_count)
            self._close_store()
            self._entry_count = 0

    def close(self) -> None:
        with self._lock:
            self._close_store()

    def _lookup(self, key: str) -> Union[_V, _Missing]:
        if self._store is None:
            return _MISSING
        try:
            return self._store[key]
        except KeyError:
            return _MISSING

    def _open(self) -> FileBackedDict[_V]:
        if self._store is None:
            self._store = FileBackedDict(
                tablename="cache",
                cache_max_size=self._memory_entries,
                cache_eviction_batch_size=max(1, min(150, self._memory_entries // 4)),
                should_compress_value=self._compress,
            )
        return self._store

    def _close_store(self) -> None:
        if self._store is not None:
            self._store.close()
            self._store = None
