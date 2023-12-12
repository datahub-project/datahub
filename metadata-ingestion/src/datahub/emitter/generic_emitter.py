from typing import Callable, Optional, Union

from typing_extensions import Protocol

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)


class Emitter(Protocol):
    def emit(
        self,
        item: Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ],
        # NOTE: This signature should have the exception be optional rather than
        #      required. However, this would be a breaking change that may need
        #      more careful consideration.
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        raise NotImplementedError

    def flush(self) -> None:
        pass
