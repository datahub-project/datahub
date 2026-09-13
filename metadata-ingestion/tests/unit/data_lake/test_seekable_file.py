import pytest

from datahub.ingestion.source.data_lake_common.seekable_file import SeekableRangeFile


class _FakeRangeFile(SeekableRangeFile):
    def __init__(self, data: bytes) -> None:
        super().__init__()
        self._data = data
        self._size = len(data)

    def _fetch_range(self, start: int, length: int) -> bytes:
        return self._data[start : start + length]


def test_seek_and_read_ranges() -> None:
    f = _FakeRangeFile(b"hello world")
    assert f.seek(6) == 6
    assert f.read(5) == b"world"
    assert f.seek(-5, 2) == 6
    assert f.read() == b"world"


def test_seek_rejects_unsupported_whence() -> None:
    f = _FakeRangeFile(b"hello world")
    with pytest.raises(ValueError):
        f.seek(0, 5)


def test_operations_after_close_raise() -> None:
    f = _FakeRangeFile(b"hello world")
    f.close()
    with pytest.raises(ValueError):
        f.read(1)
    with pytest.raises(ValueError):
        f.seek(0)
    with pytest.raises(ValueError):
        f.tell()
