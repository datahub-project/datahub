from datahub.configuration import ConfigModel
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
import boto3
from dataclasses import dataclass, field
from datahub.ingestion.api.source import Source, SourceReport
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from typing import Iterable, List
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.ingestion.api.common import PipelineContext
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp, Status


@dataclass
class GlueSourceReport(SourceReport):
    topics_scanned = 0
    filtered: List[str] = field(default_factory=list)

    def report_topic_scanned(self, topic: str) -> None:
        self.topics_scanned += 1

    def report_dropped(self, topic: str) -> None:
        self.filtered.append(topic)



class GlueSourceConfig(ConfigModel):
    # add this back later: database: str
    # swap this to dataset?: _cluster = conf.get_string(GlueExtractor.CLUSTER_KEY)
    # don't need these to start with: ._filters = conf.get(GlueExtractor.FILTER_KEY)
    _glue = boto3.client('glue')


@dataclass
class GlueSource(Source):
    source_config: GlueSourceConfig
    report = GlueSourceReport()



    def _extract_record(self, topic: str) -> MetadataChangeEvent:
        metadata_record = MetadataChangeEvent()
        dataset_snapshot = DatasetSnapshot(
            urn=f"glueeeeeeee",
            aspects=[],  # we append to this list later on
        )

        metadata_record.proposedSnapshot = dataset_snapshot
        return metadata_record

    @classmethod
    def create(cls, config_dict, ctx):
        config = GlueSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_report(self):
        return self.report

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        #for x in self.source_config._glue.search_tables():
        snapshot = DatasetSnapshot(urn="urn:li:dataset:(urn:li:dataPlatform:platform,dataset_name,PROD)", aspects=[Status(removed=False)])
        change = MetadataChangeEvent()
        change.proposedSnapshot = snapshot
        yield MetadataWorkUnit('first workunit', change)




def main():
    glueSource = GlueSource(PipelineContext('the first'), GlueSourceConfig())


if __name__ == '__main__':
    main()
