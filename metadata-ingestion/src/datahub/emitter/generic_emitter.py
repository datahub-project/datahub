import functools
import logging
from typing import Callable, Optional, Union

from typing_extensions import Protocol

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)

logger = logging.getLogger(__name__)

MCE_DEPRECATION_MESSAGE = (
    "Emitting MetadataChangeEvents is deprecated and will be removed in a "
    "future release. They are converted to MCPs; emit "
    "MetadataChangeProposalWrapper instead."
)


# Shared by every emitter and cached so it prints once per process; the Airflow hook builds a new emitter per call.
@functools.lru_cache(maxsize=None)
def warn_mce_deprecated() -> None:
    logger.warning(MCE_DEPRECATION_MESSAGE)


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
