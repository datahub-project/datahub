from dataclasses import dataclass
from gometa.ingestion.api.source import WorkUnit
from gometa.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

@dataclass
class MetadataWorkUnit(WorkUnit):
    mce: MetadataChangeEvent 
    
    def get_metadata(self):
        return {'mce': self.mce}