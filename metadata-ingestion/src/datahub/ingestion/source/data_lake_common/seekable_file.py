import io
from abc import abstractmethod


class SeekableRangeFile(io.RawIOBase):
    """Seekable, read-only file-like wrapper backed by HTTP range requests.

    ``zipfile.ZipFile`` requires random access (seek + tell) because the central
    directory lives at the end of the archive. Subclasses fetch only the bytes
    actually requested by implementing ``_fetch_range``; the seek/tell/read
    bookkeeping is shared so the S3 and ABS wrappers cannot diverge.

    Subclasses must set ``self._size`` (total object size in bytes) in their
    ``__init__`` before the wrapper is used.
    """

    _size: int

    def __init__(self) -> None:
        super().__init__()
        self._pos = 0

    @abstractmethod
    def _fetch_range(self, start: int, length: int) -> bytes:
        """Return ``length`` bytes starting at ``start`` (``length`` >= 1)."""

    def read(self, size: int = -1) -> bytes:
        self._checkClosed()
        if size == 0 or self._pos >= self._size:
            return b""
        length = (
            (self._size - self._pos) if size < 0 else min(size, self._size - self._pos)
        )
        data = self._fetch_range(self._pos, length)
        self._pos += len(data)
        return data

    def seek(self, offset: int, whence: int = 0) -> int:
        self._checkClosed()
        if whence == 0:
            self._pos = offset
        elif whence == 1:
            self._pos += offset
        elif whence == 2:
            self._pos = self._size + offset
        else:
            # Reject unsupported whence rather than silently clamping, so an
            # invalid seek surfaces instead of returning a bogus position.
            raise ValueError(f"unsupported whence value: {whence!r}")
        self._pos = max(0, min(self._pos, self._size))
        return self._pos

    def tell(self) -> int:
        self._checkClosed()
        return self._pos

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True
