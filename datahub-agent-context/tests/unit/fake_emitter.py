"""A minimal in-memory emitter for asserting on emitted MCPs."""

from __future__ import annotations

from typing import Callable, List, Optional, Set, Union

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)

_Item = Union[
    MetadataChangeEvent, MetadataChangeProposal, MetadataChangeProposalWrapper
]


class FakeEmitter:
    """Captures every emitted :class:`MetadataChangeProposalWrapper`."""

    def __init__(self) -> None:
        self.mcps: List[MetadataChangeProposalWrapper] = []

    def emit(
        self,
        item: _Item,
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        assert isinstance(item, MetadataChangeProposalWrapper)
        self.mcps.append(item)

    def flush(self) -> None:
        pass

    def urns(self) -> Set[str]:
        return {mcp.entityUrn for mcp in self.mcps if mcp.entityUrn is not None}

    def aspects_for(self, urn: str) -> List[object]:
        return [mcp.aspect for mcp in self.mcps if mcp.entityUrn == urn]
