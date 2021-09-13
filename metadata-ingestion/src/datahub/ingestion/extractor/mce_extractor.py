from typing import Iterable, Union

from datahub.emitter.mce_builder import get_sys_time
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api import RecordEnvelope
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Extractor, WorkUnit
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
    SystemMetadata,
)
from datahub.metadata.schema_classes import UsageAggregationClass


class WorkUnitRecordExtractor(Extractor):
    """An extractor that simply returns the data inside workunits back as records."""

    ctx: PipelineContext

    def configure(self, config_dict: dict, ctx: PipelineContext) -> None:
        self.ctx = ctx

    def get_records(
        self, workunit: WorkUnit
    ) -> Iterable[
        RecordEnvelope[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
                UsageAggregationClass,
            ]
        ]
    ]:
        if isinstance(workunit, MetadataWorkUnit):
            if isinstance(workunit.metadata, MetadataChangeEvent):
                mce = workunit.metadata
                mce.systemMetadata = SystemMetadata(
                    lastObserved=get_sys_time(), runId=self.ctx.run_id
                )
                if len(mce.proposedSnapshot.aspects) == 0:
                    raise AttributeError("every mce must have at least one aspect")
            if not workunit.metadata.validate():
                raise ValueError(
                    f"source produced an invalid metadata work unit: {workunit.metadata}"
                )
            yield RecordEnvelope(
                workunit.metadata,
                {
                    "workunit_id": workunit.id,
                },
            )
        elif isinstance(workunit, UsageStatsWorkUnit):
            if not workunit.usageStats.validate():
                raise ValueError(
                    f"source produced an invalid usage stat: {workunit.usageStats}"
                )
            yield RecordEnvelope(
                workunit.usageStats,
                {
                    "workunit_id": workunit.id,
                },
            )
        else:
            raise ValueError(f"unknown WorkUnit type {type(workunit)}")

    def close(self):
        pass
