from typing import Iterable
from gometa.ingestion.api.source import Extractor, WorkUnit
from gometa.ingestion.api import RecordEnvelope
from gometa.ingestion.api.common import PipelineContext
from gometa.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

class WorkUnitMCEExtractor(Extractor):
    """An extractor that simply returns MCE-s inside workunits back as records"""

    def configure(self, config_dict: dict, ctx: PipelineContext):
        pass

    def get_records(self, workunit) -> Iterable[RecordEnvelope[MetadataChangeEvent]]:
        yield RecordEnvelope(workunit.mce, {})

    def close(self):
        pass

