from typing import Iterable

from datahub.ingestion.api import RecordEnvelope
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Extractor
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent


class WorkUnitMCEExtractor(Extractor):
    """An extractor that simply returns MCE-s inside workunits back as records"""

    def configure(self, config_dict: dict, ctx: PipelineContext):
        pass

    def get_records(self, workunit) -> Iterable[RecordEnvelope[MetadataChangeEvent]]:
        if len(workunit.mce.proposedSnapshot.aspects) == 0:
            raise AttributeError("every mce must have at least one aspect")
        if not workunit.mce.validate():
            raise ValueError(f"source produced an invalid MCE: {workunit.mce}")
        yield RecordEnvelope(workunit.mce, {})

    def close(self):
        pass
