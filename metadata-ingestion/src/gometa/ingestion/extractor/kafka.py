import logging
from gometa.configuration import ConfigModel, KafkaConnectionConfig
from gometa.ingestion.api.source import Source, Extractor
from gometa.ingestion.api import RecordEnvelope
from gometa.ingestion.api.source import WorkUnit
from typing import Optional
from dataclasses import dataclass
import confluent_kafka
import re
from gometa.ingestion.api.closeable import Closeable
from gometa.ingestion.source.kafka import KafkaWorkUnit
import gometa.ingestion.extractor.schema_util as schema_util
from gometa.metadata.model import *
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
import time

logger = logging.getLogger(__name__)

class KafkaMetadataExtractor(Extractor):
    
    def configure(self, workunit: KafkaWorkUnit):
        self.workunit = workunit
        self.schema_registry_client = SchemaRegistryClient({'url':self.workunit.config.connection.schema_registry_url})

        return self

    def get_records(self) -> RecordEnvelope:
        topic = self.workunit.config.topic
        logger.debug(f"topic = {topic}")
        schema = None
        platform="kafka"
        dataset_name=topic
        env="PROD" #TODO: configure!
        actor, sys_time = "urn:li:corpuser:etl", int(time.time()) * 1000
        metadata_record = MetadataChangeEvent()
        dataset_metadata = DatasetMetadataSnapshot(platform=platform, dataset_name=dataset_name, env="PROD")
     
        try:
            registered_schema = self.schema_registry_client.get_latest_version(topic+"-value")
            schema = registered_schema.schema
            canonical_schema = None
            if schema.schema_type == "AVRO":
                canonical_schema = schema_util.avro_schema_to_mce_fields(schema.schema_str)

            schema_metadata = SchemaMetadata(schemaName=topic,
            platform=f"urn:li:dataPlatform:{platform}",
            version=0,
            hash=str(schema._hash),
            platformSchema=KafkaSchema(documentSchema = schema.schema_str), #TODO: keySchema
            fields = canonical_schema,
            created = { "time": sys_time, "actor": actor },
            lastModified = { "time":sys_time, "actor": actor },
            )
            dataset_metadata.with_aspect(schema_metadata)
        except Exception as e:
            logger.warn(f"failed to get schema for {topic} with {e}")
        
        metadata_record.with_snapshot(dataset_metadata)
        yield RecordEnvelope(metadata_record, {})


    def close(self):
        pass
