from dataclasses import dataclass

from datahub.ingestion.api.source import WorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent


@dataclass
class MetadataWorkUnit(WorkUnit):
    mce: MetadataChangeEvent

    def get_metadata(self):
        return {"mce": self.mce}
