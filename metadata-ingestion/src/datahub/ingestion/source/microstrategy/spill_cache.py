from typing import Generic, Optional, TypeVar

from datahub.utilities.file_backed_collections import FileBackedDict

_VT = TypeVar("_VT")

_MAX_EVICTION_BATCH = 150
_EVICTION_BATCH_DIVISOR = 4


class SpillCache(Generic[_VT]):
    def __init__(self, memory_entries: int, compress: bool = False) -> None:
        self._memory_entries = memory_entries
        self._compress = compress
        self._store = self._new_store()

    def _new_store(self) -> FileBackedDict[_VT]:
        return FileBackedDict(
            cache_max_size=self._memory_entries,
            cache_eviction_batch_size=max(
                1,
                min(
                    _MAX_EVICTION_BATCH, self._memory_entries // _EVICTION_BATCH_DIVISOR
                ),
            ),
            should_compress_value=self._compress,
        )

    def __contains__(self, key: str) -> bool:
        return key in self._store

    def get(self, key: str) -> Optional[_VT]:
        return self._store.get(key)

    def put(self, key: str, value: _VT) -> None:
        self._store[key] = value

    def reset(self) -> None:
        self._store.close()
        self._store = self._new_store()

    def close(self) -> None:
        self._store.close()
