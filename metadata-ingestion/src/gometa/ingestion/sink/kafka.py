from dataclasses import dataclass
from typing import Optional, TypeVar, Type
from pydantic import BaseModel, Field, ValidationError, validator
from gometa.ingestion.api.sink import Sink, WriteCallback
from gometa.ingestion.api.common import RecordEnvelope
from confluent_kafka import Producer
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

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
    

    
class KafkaSink(Sink):
    """TODO: Add support for Avro / Protobuf serialization etc."""
    
    def __init__(self):
        self.config: Optional[KafkaSinkConfig] = None

    def configure(self, config_dict={}):
        self.config = KafkaSinkConfig.parse_obj(config_dict)

        mce_schema = avro.load('../datahub/metadata-models/src/mainGeneratedAvroSchema/avro/com/linkedin/mxe/MetadataChangeEvent.avsc')
        
        producer_config = {
            "bootstrap.servers": self.config.connection.bootstrap,
            "schema.registry.url": self.config.connection.schema_registry_url,
            **self.config.producer_config,
        }

        self.producer = AvroProducer(producer_config, default_value_schema=mce_schema)
        return self

 
    def write_record_async(self, record_envelope: RecordEnvelope, write_callback: WriteCallback):
        # call poll to trigger any callbacks on success / failure of previous writes
        self.producer.poll(0)
        breakpoint()
        self.producer.produce(topic=self.config.topic, value=record_envelope.record, 
            callback= KafkaCallback(record_envelope, write_callback).kafka_callback)
        
    def close(self):
        self.producer.flush()
        self.producer.close()
        
