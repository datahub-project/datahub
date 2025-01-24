from typing import Callable, Optional, Sequence, Union

from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)


class CompositeEmitter(Emitter):
    def __init__(self, emitters: Sequence[Emitter]) -> None:
        self.emitters = emitters

    def emit(
            self,
            item: Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ],
            callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        callback_called = False
        for emitter in self.emitters:
            if not callback_called:
                emitter.emit(item, callback)
                callback_called = True
            else:
                emitter.emit(item)

    def flush(self) -> None:
        for emitter in self.emitters:
            emitter.flush()
