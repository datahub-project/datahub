from typing import Iterable, cast

from datahub.ingestion.api import RecordEnvelope
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Extractor, WorkUnit
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent


class WorkUnitMCEExtractor(Extractor):
    """An extractor that simply returns MCE-s inside workunits back as records"""

    def configure(self, config_dict: dict, ctx: PipelineContext) -> None:
        pass

    def get_records(
        self, workunit: WorkUnit
    ) -> Iterable[RecordEnvelope[MetadataChangeEvent]]:
        workunit = cast(MetadataWorkUnit, workunit)
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

    def close(self):
        pass
