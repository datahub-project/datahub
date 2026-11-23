import logging
import pathlib
from typing import Callable, List, Optional, Union

import filelock

from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.file import read_metadata_file
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.utilities.deprecation import warn_deprecated

logger = logging.getLogger(__name__)

_MCE_DEPRECATION_MESSAGE = (
    "Passing a MetadataChangeEvent to SynchronizedFileEmitter.emit is deprecated and will be "
    "removed in a future release. Use MetadataChangeProposalWrapper instead."
)


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
        new_items: List[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ] = [item]
        if isinstance(item, MetadataChangeEvent):
            # No caller check: datahub never passes an MCE here on its own, and
            # CompositeEmitter forwards the user's MCE as it is.
            warn_deprecated(_MCE_DEPRECATION_MESSAGE)
            # An MCE without aspects has no MCPs; keep it so it is not silently lost.
            new_items = list(mcps_from_mce(item)) or [item]

        with self._lock:
            if self._filename.exists():
                metadata = list(read_metadata_file(self._filename))
            else:
                metadata = []

            logger.debug("Emitting metadata: %s", item)
            metadata.extend(new_items)

            write_metadata_file(self._filename, metadata)

    def __repr__(self) -> str:
        return f"SynchronizedFileEmitter('{self._filename}')"

    def flush(self) -> None:
        # No-op.
        pass

    def close(self) -> None:
        # No-op.
        pass
