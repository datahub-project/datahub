from dataclasses import dataclass

from datahub.ingestion.api.source import WorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import UsageAggregationClass


@dataclass
class MetadataWorkUnit(WorkUnit):
    mce: MetadataChangeEvent

    def get_metadata(self):
        return {"mce": self.mce}


@dataclass
class UsageStatsWorkUnit(WorkUnit):
    usageStats: UsageAggregationClass

    def get_metadata(self):
        return {"usage": self.usageStats}
