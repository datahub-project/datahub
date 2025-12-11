# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
