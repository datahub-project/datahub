# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import Iterable, Protocol, Union, runtime_checkable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.sdk.entity import Entity

# Type alias for metadata work units - Python 3.9 compatible
MetadataWorkUnitIterable = Iterable[
    Union[MetadataWorkUnit, MetadataChangeProposalWrapper, Entity]
]


@runtime_checkable
class ProfilingCapable(Protocol):
    """Protocol for sources that support profiling functionality."""

    def is_profiling_enabled_internal(self) -> bool:
        """Check if profiling is enabled for this source."""
        ...

    def get_profiling_internal(self) -> MetadataWorkUnitIterable:
        """Generate profiling work units."""
        ...
