from typing import Any, TextIO


class TeeIO:
    """
    A file-like object that writes to multiple streams, similar to `tee`.
    It mirrors the attributes of the first stream for encoding/tty/etc.
    """

    def __init__(self, *args: TextIO) -> None:
        assert args
        self._streams = args

    def write(self, line: str) -> None:
        for stream in self._streams:
            stream.write(line)

    def flush(self) -> None:
        for stream in self._streams:
            stream.flush()

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._streams[0], attr)
