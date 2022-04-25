from dataclasses import dataclass, field
from typing import Iterable, Union

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from src.datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    DatasetSnapshot,
)


class DataCatalogSourceConfig(ConfigModel):
    orientdbAddress: str


@dataclass
class DataCatalogSource(Source):
    config: DataCatalogSourceConfig
    report: SourceReport = field(default_factory=SourceReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = DataCatalogSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        dataset_snapshot = DatasetSnapshot(
            urn="urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)",
            aspects=[],
        )

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return [MetadataWorkUnit("dc", mce=mce)]

    def get_report(self):
        return self.report

    def close(self):
        pass
