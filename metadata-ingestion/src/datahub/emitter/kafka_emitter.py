import logging
from typing import Callable, Dict, Optional, Union

import pydantic
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, StringSerializer
from pydantic import field_validator

from datahub.configuration.common import ConfigModel
from datahub.configuration.kafka import KafkaProducerConnectionConfig
from datahub.configuration.kafka_consumer_config import KafkaOAuthCallbackResolver
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.closeable import Closeable
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

    @field_validator("topic_routes", mode="after")
    @classmethod
    def validate_topic_routes(cls, v: Dict[str, str]) -> Dict[str, str]:
        assert MCP_KEY in v, f"topic_routes must contain a route for {MCP_KEY}"
        if MCE_KEY not in v:
            logger.warning(
                f"MCE topic not configured in topic_routes. MCE emissions will fail. "
                f"To enable MCE emission, add '{MCE_KEY}' to topic_routes."
            )
        return v


class DatahubKafkaEmitter(Closeable, Emitter):
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
            key: SerializingProducer(value)
            for (key, value) in producers_config.items()
            if key in self.config.topic_routes
        }

        # If OAuth callback is configured, call poll() to trigger OAuth callback execution
        # This is required for OAuth authentication mechanisms like AWS MSK IAM
        # Note: poll(0) is non-blocking - just triggers the OAuth callback without waiting
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration
        if KafkaOAuthCallbackResolver.is_callable_config(
            self.config.connection.producer_config
        ):
            logger.debug(
                "OAuth callback detected, triggering OAuth callbacks for Kafka producers"
            )
            for producer in self.producers.values():
                producer.poll(0)  # Non-blocking - just triggers OAuth callback
            logger.debug("OAuth callbacks triggered for Kafka producers")

    def emit(
        self,
        item: Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ],
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        if isinstance(item, (MetadataChangeProposal, MetadataChangeProposalWrapper)):
            return self.emit_mcp_async(item, callback or _error_reporting_callback)
        else:
            return self.emit_mce_async(item, callback or _error_reporting_callback)

    def emit_mce_async(
        self,
        mce: MetadataChangeEvent,
        callback: Callable[[Exception, str], None],
    ) -> None:
        # Report error via callback if MCE_KEY is not configured
        if MCE_KEY not in self.config.topic_routes:
            error = Exception(
                f"Cannot emit MetadataChangeEvent: {MCE_KEY} topic not configured in topic_routes"
            )
            callback(error, "MCE emission failed - topic not configured")
            return
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

    def close(self) -> None:
        self.flush()


def _error_reporting_callback(err: Exception, msg: str) -> None:
    if err:
        logger.error(f"Failed to emit to kafka: {err} {msg}")
