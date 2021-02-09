from dataclasses import dataclass
import json
from typing import Optional, TypeVar, Type
from pydantic import BaseModel, Field, ValidationError, validator
from gometa.ingestion.api.sink import Sink, WriteCallback
from gometa.ingestion.api.common import RecordEnvelope, WorkUnit, PipelineContext

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from gometa.metadata import json_converter
from gometa.metadata.schema_classes import SCHEMA_JSON_STR
from gometa.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

class KafkaConnectionConfig(BaseModel):
    """Configuration class for holding connectivity information for Kafka"""
    
    # bootstrap servers
    bootstrap: str = "localhost:9092"

    # schema registry location
    schema_registry_url: str = "http://localhost:8081"

    @validator('bootstrap')
    def bootstrap_host_colon_port_comma(cls, val):
        for entry in val.split(","):
            assert ":" in entry, f'entry must be of the form host:port, found {entry}'
            (host,port) = entry.split(":")
            assert host.isalnum(), f'host must be alphanumeric, found {host}'
            assert port.isdigit(), f'port must be all digits, found {port}'

DEFAULT_KAFKA_TOPIC="MetadataChangeEvent_v4"

class KafkaSinkConfig(BaseModel):
    """TODO: Write a post_init method to populate producer_config from the modeled config"""
    connection: KafkaConnectionConfig = KafkaConnectionConfig()
    topic: str = DEFAULT_KAFKA_TOPIC
    producer_config: dict = {}

@dataclass
class KafkaCallback:
    record_envelope: RecordEnvelope
    write_callback: WriteCallback

    def kafka_callback(self, err, msg):
        if err is not None:
            if self.write_callback:
                self.write_callback.on_failure(self.record_envelope, None, {"error": err})
        else:
            if self.write_callback:
                self.write_callback.on_success(self.record_envelope, {"msg": msg}) 
    
@dataclass
class DatahubKafkaSink(Sink):
    config: KafkaSinkConfig

    def __init__(self, config: KafkaSinkConfig, ctx):
        super().__init__(ctx)
        self.config = config

        mce_schema = MetadataChangeEvent.RECORD_SCHEMA
        
        producer_config = {
            "bootstrap.servers": self.config.connection.bootstrap,
            "schema.registry.url": self.config.connection.schema_registry_url,
            **self.config.producer_config,
        }

        schema_registry_conf = {'url': self.config.connection.schema_registry_url}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        def convert_mce_to_dict(mce, ctx):
            tuple_encoding = json_converter.with_tuple_union().to_json_object(mce)
            return tuple_encoding
        avro_serializer = AvroSerializer(SCHEMA_JSON_STR, schema_registry_client, to_dict=convert_mce_to_dict)

        producer_conf = {
            "bootstrap.servers": self.config.connection.bootstrap,
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': avro_serializer,
        }

        self.producer = SerializingProducer(producer_conf)

    @classmethod
    def create(cls, config_dict, ctx: PipelineContext):
        config = KafkaSinkConfig.parse_obj(config_dict)
        return cls(config, ctx)
 
    def handle_work_unit_start(self, workunit: WorkUnit) -> None:
        pass

    def handle_work_unit_end(self, workunit: WorkUnit) -> None:
        self.producer.flush()

    def write_record_async(self, record_envelope: RecordEnvelope[MetadataChangeEvent], write_callback: WriteCallback):
        # call poll to trigger any callbacks on success / failure of previous writes
        self.producer.poll(0)
        mce = record_envelope.record
        self.producer.produce(topic=self.config.topic, value=mce,
            on_delivery=KafkaCallback(record_envelope, write_callback).kafka_callback)
        
    def close(self):
        self.producer.flush()
        # self.producer.close()
        
