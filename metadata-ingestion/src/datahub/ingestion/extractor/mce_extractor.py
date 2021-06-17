from typing import Iterable

from datahub.ingestion.api import RecordEnvelope
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Extractor, WorkUnit
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent


class WorkUnitRecordExtractor(Extractor):
    """An extractor that simply returns the data inside workunits back as records."""

    def configure(self, config_dict: dict, ctx: PipelineContext) -> None:
        pass

    def get_records(
        self, workunit: WorkUnit
    ) -> Iterable[RecordEnvelope[MetadataChangeEvent]]:
        if isinstance(workunit, MetadataWorkUnit):
            if len(workunit.mce.proposedSnapshot.aspects) == 0:
                raise AttributeError("every mce must have at least one aspect")
            if not workunit.mce.validate():
                raise ValueError(f"source produced an invalid MCE: {workunit.mce}")
            yield RecordEnvelope(
                workunit.mce,
                {
                    "workunit_id": workunit.id,
                },
            )
        elif isinstance(workunit, UsageStatsWorkUnit):
            # TODO this
            pass
        else:
            raise ValueError(f"unknown WorkUnit type {type(workunit)}")

    def close(self):
        pass
