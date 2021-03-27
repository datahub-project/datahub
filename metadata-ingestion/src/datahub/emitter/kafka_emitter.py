from typing import Callable

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from pydantic import Field

from datahub.configuration.common import ConfigModel
from datahub.configuration.kafka import KafkaProducerConnectionConfig
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import SCHEMA_JSON_STR

DEFAULT_KAFKA_TOPIC = "MetadataChangeEvent_v4"


class KafkaEmitterConfig(ConfigModel):
    connection: KafkaProducerConnectionConfig = Field(
        default_factory=KafkaProducerConnectionConfig
    )
    topic: str = DEFAULT_KAFKA_TOPIC


class DatahubKafkaEmitter:
    def __init__(self, config: KafkaEmitterConfig):
        self.config = config

        schema_registry_conf = {
            "url": self.config.connection.schema_registry_url,
            **self.config.connection.schema_registry_config,
        }
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        def convert_mce_to_dict(mce: MetadataChangeEvent, ctx):
            tuple_encoding = mce.to_obj(tuples=True)
            return tuple_encoding

        avro_serializer = AvroSerializer(
            schema_str=SCHEMA_JSON_STR,
            schema_registry_client=schema_registry_client,
            to_dict=convert_mce_to_dict,
        )

        producer_config = {
            "bootstrap.servers": self.config.connection.bootstrap,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
            **self.config.connection.producer_config,
        }

        self.producer = SerializingProducer(producer_config)

    def emit_mce_async(
        self,
        mce: MetadataChangeEvent,
        callback: Callable[[Exception, str], None],
    ):
        # Call poll to trigger any callbacks on success / failure of previous writes
        self.producer.poll(0)
        self.producer.produce(
            topic=self.config.topic,
            value=mce,
            on_delivery=callback,
        )

    def flush(self) -> None:
        self.producer.flush()
