# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
