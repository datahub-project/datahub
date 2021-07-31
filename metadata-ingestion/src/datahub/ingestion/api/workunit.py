from dataclasses import dataclass
from typing import Union, overload

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source import WorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.schema_classes import UsageAggregationClass


@dataclass
class MetadataWorkUnit(WorkUnit):
    metadata: Union[
        MetadataChangeEvent, MetadataChangeProposal, MetadataChangeProposalWrapper
    ]

    @overload
    def __init__(self, id: str, mce: MetadataChangeEvent):
        # TODO: Force `mce` to be a keyword-only argument.
        ...

    @overload
    def __init__(self, id: str, *, mcp_raw: MetadataChangeProposal):
        # We force `mcp_raw` to be a keyword-only argument.
        ...

    @overload
    def __init__(self, id: str, *, mcp: MetadataChangeProposalWrapper):
        # We force `mcp` to be a keyword-only argument.
        ...

    def __init__(
        self,
        id: str,
        mce: MetadataChangeEvent = None,
        mcp: MetadataChangeProposalWrapper = None,
        mcp_raw: MetadataChangeProposal = None,
    ):
        super().__init__(id)

        if sum(1 if v else 0 for v in [mce, mcp, mcp_raw]) != 1:
            raise ValueError("exactly one of mce, mcp, or mcp_raw must be provided")

        if mcp_raw:
            assert not (mce or mcp)
            self.metadata = mcp_raw
        elif mce and not mcp:
            self.metadata = mce
        elif mcp and not mce:
            self.metadata = mcp

    def get_metadata(self) -> dict:
        return {"metadata": self.metadata}


@dataclass
class UsageStatsWorkUnit(WorkUnit):
    usageStats: UsageAggregationClass

    def get_metadata(self) -> dict:
        return {"usage": self.usageStats}
