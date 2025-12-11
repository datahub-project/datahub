# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import Callable, List, Optional, Union

from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)


# Experimental composite emitter that allows multiple emitters to be used in a single ingestion job
class CompositeEmitter(Emitter):
    def __init__(self, emitters: List[Emitter]) -> None:
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
                # We want to ensure that the callback is only called once and we tie it to the first emitter
                emitter.emit(item, callback)
                callback_called = True
            else:
                emitter.emit(item)

    def flush(self) -> None:
        for emitter in self.emitters:
            emitter.flush()
