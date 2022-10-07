import logging
from typing import Callable, Dict, Union

import pydantic
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, StringSerializer

from datahub.configuration.common import ConfigModel
from datahub.configuration.kafka import KafkaProducerConnectionConfig
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass as MetadataChangeEvent,
    MetadataChangeProposalClass as MetadataChangeProposal,
)
from datahub.metadata.schemas import (
    getMetadataChangeEventSchema,
    getMetadataChangeProposalSchema,
)

logger = logging.getLogger(__name__)


DEFAULT_MCE_KAFKA_TOPIC = "MetadataChangeEvent_v4"
DEFAULT_MCP_KAFKA_TOPIC = "MetadataChangeProposal_v1"
MCE_KEY = "MetadataChangeEvent"
MCP_KEY = "MetadataChangeProposal"


class KafkaEmitterConfig(ConfigModel):
    connection: KafkaProducerConnectionConfig = pydantic.Field(
        default_factory=KafkaProducerConnectionConfig
    )
    topic_routes: Dict[str, str] = {
        MCE_KEY: DEFAULT_MCE_KAFKA_TOPIC,
        MCP_KEY: DEFAULT_MCP_KAFKA_TOPIC,
    }

    _topic_field_compat = pydantic_renamed_field(
        "topic",
        "topic_routes",
        transform=lambda x: {
            MCE_KEY: x,
            MCP_KEY: DEFAULT_MCP_KAFKA_TOPIC,
        },
    )

    @pydantic.validator("topic_routes")
    def validate_topic_routes(cls, v: Dict[str, str]) -> Dict[str, str]:
        assert MCE_KEY in v, f"topic_routes must contain a route for {MCE_KEY}"
        assert MCP_KEY in v, f"topic_routes must contain a route for {MCP_KEY}"
        return v


class DatahubKafkaEmitter:
    def __init__(self, config: KafkaEmitterConfig):
        self.config = config
        schema_registry_conf = {
            "url": self.config.connection.schema_registry_url,
            **self.config.connection.schema_registry_config,
        }
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        def convert_mce_to_dict(
            mce: MetadataChangeEvent, ctx: SerializationContext
        ) -> dict:
            return mce.to_obj(tuples=True)

        mce_avro_serializer = AvroSerializer(
            schema_str=getMetadataChangeEventSchema(),
            schema_registry_client=schema_registry_client,
            to_dict=convert_mce_to_dict,
        )

        def convert_mcp_to_dict(
            mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
            ctx: SerializationContext,
        ) -> dict:
            return mcp.to_obj(tuples=True)

        mcp_avro_serializer = AvroSerializer(
            schema_str=getMetadataChangeProposalSchema(),
            schema_registry_client=schema_registry_client,
            to_dict=convert_mcp_to_dict,
        )

        # We maintain a map of producers for each kind of event
        producers_config = {
            MCE_KEY: {
                "bootstrap.servers": self.config.connection.bootstrap,
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": mce_avro_serializer,
                **self.config.connection.producer_config,
            },
            MCP_KEY: {
                "bootstrap.servers": self.config.connection.bootstrap,
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": mcp_avro_serializer,
                **self.config.connection.producer_config,
            },
        }

        self.producers = {
            key: SerializingProducer(value) for (key, value) in producers_config.items()
        }

    def emit(
        self,
        item: Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ],
        callback: Callable[[Exception, str], None],
    ) -> None:
        if isinstance(item, (MetadataChangeProposal, MetadataChangeProposalWrapper)):
            return self.emit_mcp_async(item, callback)
        else:
            return self.emit_mce_async(item, callback)

    def emit_mce_async(
        self,
        mce: MetadataChangeEvent,
        callback: Callable[[Exception, str], None],
    ) -> None:
        # Call poll to trigger any callbacks on success / failure of previous writes
        producer: SerializingProducer = self.producers[MCE_KEY]
        producer.poll(0)
        producer.produce(
            topic=self.config.topic_routes[MCE_KEY],
            key=mce.proposedSnapshot.urn,
            value=mce,
            on_delivery=callback,
        )

    def emit_mcp_async(
        self,
        mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
        callback: Callable[[Exception, str], None],
    ) -> None:
        # Call poll to trigger any callbacks on success / failure of previous writes
        producer: SerializingProducer = self.producers[MCP_KEY]
        producer.poll(0)
        producer.produce(
            topic=self.config.topic_routes[MCP_KEY],
            key=mcp.entityUrn,
            value=mcp,
            on_delivery=callback,
        )

    def flush(self) -> None:
        for producer in self.producers.values():
            producer.flush()
