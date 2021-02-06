import logging
from gometa.configuration import ConfigModel, KafkaConnectionConfig
from gometa.ingestion.api.source import Source, Extractor
from gometa.ingestion.api import RecordEnvelope
from gometa.ingestion.api.source import WorkUnit
from typing import Optional, Iterable
from dataclasses import dataclass
import confluent_kafka
import re
from gometa.ingestion.api.closeable import Closeable
from gometa.ingestion.source.kafka import KafkaWorkUnit
import gometa.ingestion.extractor.schema_util as schema_util
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
import time
from gometa.ingestion.api.common import PipelineContext

from gometa.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from gometa.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from gometa.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata, KafkaSchema
from gometa.metadata.com.linkedin.pegasus2avro.common import AuditStamp

logger = logging.getLogger(__name__)


class KafkaMetadataExtractor(Extractor):
    def configure(self, config_dict: dict, ctx: PipelineContext):
        # TODO: get the source config from here
        pass

    def get_records(
        self, workunit: KafkaWorkUnit
    ) -> Iterable[RecordEnvelope[MetadataChangeEvent]]:
        self.schema_registry_client = SchemaRegistryClient(
            {"url": workunit.config.connection.schema_registry_url}
        )
        topic = workunit.config.topic

        logger.debug(f"topic = {topic}")
        schema = None
        platform = "kafka"
        dataset_name = topic
        env = "PROD"  # TODO: configure!
        actor, sys_time = "urn:li:corpuser:etl", int(time.time()) * 1000
        metadata_record = MetadataChangeEvent()
        dataset_snapshot = DatasetSnapshot()
        dataset_snapshot.urn = (
            f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},{env})"
        )

        try:
            registered_schema = self.schema_registry_client.get_latest_version(
                topic + "-value"
            )
            schema = registered_schema.schema
        except Exception as e:
            logger.info(f"failed to get schema for {topic} with {e}")

        if schema:
            # TODO: add schema parsing capabilities
            # canonical_schema = []
            # if schema.schema_type == "AVRO":
            #     canonical_schema = schema_util.avro_schema_to_mce_fields(schema.schema_str)

            schema_metadata = SchemaMetadata(
                schemaName=topic,
                version=0,
                hash=str(schema._hash),
                platform=f"urn:li:dataPlatform:{platform}",
                platformSchema = KafkaSchema(
                    # TODO: keySchema
                    documentSchema=schema.schema_str
                ),
                fields=[],
                created=AuditStamp(time=sys_time, actor=actor),
                lastModified=AuditStamp(time=sys_time, actor=actor),
            )

            dataset_snapshot.aspects.append(schema_metadata)

        metadata_record.proposedSnapshot = dataset_snapshot
        yield RecordEnvelope(metadata_record, {})

    def close(self):
        pass
