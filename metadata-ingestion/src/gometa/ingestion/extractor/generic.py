from gometa.ingestion.api.source import Extractor, WorkUnit
from gometa.ingestion.api import RecordEnvelope

class WorkUnitMCEExtractor(Extractor):
    """An extractor that simply returns MCE-s inside workunits back as records"""

    def configure(self, workunit: WorkUnit):
        self.workunit = workunit

    def get_records(self) -> RecordEnvelope:
        yield RecordEnvelope(self.workunit.mce, {})

    def close(self):
        pass

