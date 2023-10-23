import logging
import pathlib
from typing import Callable, Optional, Union

import filelock

from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.file import read_metadata_file
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)

logger = logging.getLogger(__name__)


class SynchronizedFileEmitter(Closeable, Emitter):
    """
    A multiprocessing-safe emitter that writes to a file.

    This emitter is intended for testing purposes only. It is not performant
    because it reads and writes the full file on every emit call to ensure
    that the file is always valid JSON.
    """

    def __init__(self, filename: str) -> None:
        self._filename = pathlib.Path(filename)
        self._lock = filelock.FileLock(self._filename.with_suffix(".lock"))

    def emit(
        self,
        item: Union[
            MetadataChangeEvent, MetadataChangeProposal, MetadataChangeProposalWrapper
        ],
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        with self._lock:
            if self._filename.exists():
                metadata = list(read_metadata_file(self._filename))
            else:
                metadata = []

            logger.debug("Emitting metadata: %s", item)
            metadata.append(item)

            write_metadata_file(self._filename, metadata)

    def __repr__(self) -> str:
        return f"SynchronizedFileEmitter('{self._filename}')"

    def flush(self) -> None:
        # No-op.
        pass

    def close(self) -> None:
        # No-op.
        pass
