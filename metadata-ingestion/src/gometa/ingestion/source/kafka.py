import logging
from gometa.configuration import ConfigModel, KafkaConnectionConfig
from gometa.ingestion.api.source import Source, Extractor, SourceReport
from gometa.ingestion.api.source import WorkUnit
from typing import Optional, Iterable, List
from dataclasses import dataclass, field
import confluent_kafka
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
import re
from gometa.ingestion.api.closeable import Closeable
from gometa.ingestion.source.metadata_common import MetadataWorkUnit

import time
from gometa.ingestion.api.common import PipelineContext
import gometa.ingestion.extractor.schema_util as schema_util

from gometa.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from gometa.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from gometa.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata, KafkaSchema, SchemaField
from gometa.metadata.com.linkedin.pegasus2avro.common import AuditStamp

logger = logging.getLogger(__name__)


class KafkaSourceConfig(ConfigModel):
    connection: KafkaConnectionConfig = KafkaConnectionConfig()
    topic: str = ".*" # default is wildcard subscription


@dataclass
class KafkaSourceReport(SourceReport):
    topics_scanned = 0
    incomplete_schemas: List[str] = field(default_factory=list)
    filtered: List[str] = field(default_factory=list)

    def report_topic_scanned(self, topic: str) -> None:
        self.topics_scanned += 1

    def report_schema_incomplete(self, topic: str) -> None:
        self.incomplete_schemas.append(topic)
    
    def report_dropped(self, topic: str) -> None:
        self.filtered.append(topic)

@dataclass
class KafkaSource(Source):
    source_config: KafkaSourceConfig
    topic_pattern: re.Pattern
    consumer: confluent_kafka.Consumer
    report: KafkaSourceReport

    def __init__(self, config: KafkaSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.topic_pattern = re.compile(self.source_config.topic)
        self.consumer = confluent_kafka.Consumer({'group.id':'test', 'bootstrap.servers':self.source_config.connection.bootstrap})
        self.schema_registry_client = SchemaRegistryClient(
            {"url": self.source_config.connection.schema_registry_url}
        )
        self.report = KafkaSourceReport()

    @classmethod
    def create(cls, config_dict, ctx):
        config = KafkaSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        topics = self.consumer.list_topics().topics
        for t in topics:
            self.report.report_topic_scanned(t)

            # TODO: topics config should support allow and deny patterns
            if re.fullmatch(self.topic_pattern, t) and not t.startswith("_"): 
                mce = self._extract_record(t)
                wu = MetadataWorkUnit(id=f'kafka-{t}', mce=mce)
                self.report.report_workunit(wu)
                yield wu
            else:
                self.report.report_dropped(t)
    
    def _extract_record(self, topic: str) -> MetadataChangeEvent:
        logger.debug(f"topic = {topic}")
        platform = "kafka"
        dataset_name = topic
        env = "PROD"  # TODO: configure!
        actor, sys_time = "urn:li:corpuser:etl", int(time.time()) * 1000
        metadata_record = MetadataChangeEvent()
        dataset_snapshot = DatasetSnapshot()
        dataset_snapshot.urn = (
            f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},{env})"
        )

        schema = None
        try:
            registered_schema = self.schema_registry_client.get_latest_version(
                topic + "-value"
            )
            schema = registered_schema.schema
        except Exception as e:
            logger.debug(f"failed to get schema for {topic} with {e}")

        fields: Optional[List[SchemaField]] = None
        if schema and schema.schema_type == 'AVRO':
            fields = schema_util.avro_schema_to_mce_fields(schema.schema_str)
        elif schema:
            logger.debug(f"unable to parse kafka schema type {schema.schema_type}")

        is_incomplete = True
        if schema:
            if not fields:
                fields = []
            else:
                is_incomplete = False

            schema_metadata = SchemaMetadata(
                schemaName=topic,
                version=0,
                hash=str(schema._hash),
                platform=f"urn:li:dataPlatform:{platform}",
                platformSchema = KafkaSchema(
                    documentSchema=schema.schema_str
                ),
                fields=(fields if fields is not None else []),
                created=AuditStamp(time=sys_time, actor=actor),
                lastModified=AuditStamp(time=sys_time, actor=actor),
            )

            dataset_snapshot.aspects.append(schema_metadata)

        if is_incomplete:
            self.report.report_schema_incomplete(topic)

        metadata_record.proposedSnapshot = dataset_snapshot
        return metadata_record

    def get_report(self):
        return self.report
        
    def close(self):
        if self.consumer:
            self.consumer.close()

