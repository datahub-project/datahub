import logging
import time
from typing import Any, Callable, Dict, Optional, Union

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

# When the local producer queue is full, poll for up to this long to drain
# delivery callbacks before retrying produce(). Acts as backpressure.
_QUEUE_FULL_POLL_SECONDS = 1.0

# Default time to block on a full queue before surfacing the error, so a
# permanently unreachable broker fails the run instead of hanging forever.
# Overridable per-deployment via KafkaEmitterConfig.max_queue_full_block_seconds.
_MAX_QUEUE_FULL_BLOCK_SECONDS = 300.0

# Re-emit the "queue full" warning every N polls while blocked, so a persistent
# stall stays visible to operators instead of going silent after the first log.
_QUEUE_FULL_WARN_EVERY_N_POLLS = 30


class KafkaEmitterConfig(ConfigModel):
    connection: KafkaProducerConnectionConfig = pydantic.Field(
        default_factory=KafkaProducerConnectionConfig
    )
    topic_routes: Dict[str, str] = {
        MCE_KEY: DEFAULT_MCE_KAFKA_TOPIC,
        MCP_KEY: DEFAULT_MCP_KAFKA_TOPIC,
    }

    max_queue_full_block_seconds: float = pydantic.Field(
        default=_MAX_QUEUE_FULL_BLOCK_SECONDS,
        description=(
            "Max time to block on a full local producer queue (backpressure) "
            "before failing the run, so an unreachable broker does not hang "
            "ingestion indefinitely."
        ),
    )

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

    @staticmethod
    def _produce_with_backpressure(
        producer: SerializingProducer,
        poll_timeout_seconds: float = _QUEUE_FULL_POLL_SECONDS,
        max_block_seconds: float = _MAX_QUEUE_FULL_BLOCK_SECONDS,
        **produce_kwargs: Any,
    ) -> None:
        """Produce to Kafka, blocking on a full local queue instead of failing.

        confluent-kafka's produce() raises BufferError when the producer's
        local queue is full -- i.e. the source is emitting faster than the
        broker (and, downstream, the MCP consumer) can drain. Rather than
        crashing the ingestion run, block by polling to drive delivery
        callbacks and free queue space, then retry. This is the sink's
        backpressure: it throttles the source to Kafka's sustainable rate.
        Tune the queue bounds via producer_config (queue.buffering.max.messages,
        queue.buffering.max.kbytes).

        If the queue stays full for max_block_seconds -- e.g. the broker is
        unreachable and nothing drains -- give up and re-raise BufferError so
        the caller reports a failure instead of hanging indefinitely.
        """
        deadline = time.monotonic() + max_block_seconds
        polls = 0
        while True:
            try:
                producer.produce(**produce_kwargs)
                return
            except BufferError:
                if polls % _QUEUE_FULL_WARN_EVERY_N_POLLS == 0:
                    logger.warning(
                        "Kafka producer queue full; blocking to apply backpressure. "
                        "If this persists the broker may be unreachable."
                    )
                producer.poll(poll_timeout_seconds)
                polls += 1
                # Bail out once total blocked time exceeds the deadline, so an
                # unreachable broker fails the run instead of blocking forever.
                if time.monotonic() >= deadline:
                    logger.error(
                        "Kafka producer queue still full after %.0fs; giving up "
                        "(broker unreachable?).",
                        max_block_seconds,
                    )
                    raise

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
        self._produce_with_backpressure(
            producer,
            max_block_seconds=self.config.max_queue_full_block_seconds,
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
        self._produce_with_backpressure(
            producer,
            max_block_seconds=self.config.max_queue_full_block_seconds,
            topic=self.config.topic_routes[MCP_KEY],
            key=mcp.entityUrn,
            value=mcp,
            on_delivery=callback,
        )

    def flush(self) -> None:
        # Emitter interface returns None; callers needing the undelivered count
        # (the sink) use flush_with_undelivered_count().
        self.flush_with_undelivered_count()

    def flush_with_undelivered_count(self) -> int:
        """Flush all producers, bounded by max_queue_full_block_seconds so an
        unreachable broker cannot hang the run indefinitely at flush time (the
        backpressure loop only bounds produce()).

        Returns the number of messages still undelivered after the timeout
        (0 = fully drained).
        """
        undelivered = 0
        for producer in self.producers.values():
            undelivered += producer.flush(self.config.max_queue_full_block_seconds)
        if undelivered:
            logger.error(
                "Kafka flush timed out with %d message(s) still queued "
                "(broker unreachable?).",
                undelivered,
            )
        return undelivered

    def close(self) -> None:
        self.flush()


def _error_reporting_callback(err: Exception, msg: str) -> None:
    if err:
        logger.error(f"Failed to emit to kafka: {err} {msg}")
