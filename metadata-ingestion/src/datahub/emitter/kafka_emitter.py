import functools
import logging
import threading
import time
import warnings
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Union

import pydantic
from confluent_kafka import KafkaError, KafkaException, SerializingProducer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import SerializationContext, StringSerializer
from pydantic import field_validator
from typing_extensions import deprecated

from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.configuration.kafka import KafkaProducerConnectionConfig
from datahub.configuration.kafka_consumer_config import KafkaOAuthCallbackResolver
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import DataHubDeprecationWarning
from datahub.ingestion.api.closeable import Closeable
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass as MetadataChangeEvent,
    MetadataChangeProposalClass as MetadataChangeProposal,
)
from datahub.metadata.schemas import getMetadataChangeProposalSchema

logger = logging.getLogger(__name__)

# confluent-kafka>=2.13 types produce(on_delivery=) as Callable[[Any | None, Any], None].
KafkaOnDelivery = Callable[[Any, Any], None]


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

_MCE_DEPRECATION_MESSAGE = (
    "Emitting MetadataChangeEvents is deprecated and will be removed in a "
    "future release. They are converted to MCPs; emit "
    "MetadataChangeProposalWrapper instead."
)


class MessageTooLargeError(Exception):
    """A serialized Kafka message exceeded producer ``message.max.bytes``.

    Distinct from ``BufferError`` (full local queue, retried via backpressure):
    an oversize message can never succeed at the current cap, so it is not
    retried. The sink catches this to degrade the record to its REST fallback
    (REST accepts ~16 MiB vs the Kafka topic's ~5 MiB cap) rather than fail the
    run purely because an aspect REST would accept was routed to the async sink.
    """


class KafkaEmitterConfig(ConfigModel):
    connection: KafkaProducerConnectionConfig = pydantic.Field(
        default_factory=KafkaProducerConnectionConfig
    )
    topic_routes: Dict[str, str] = {MCP_KEY: DEFAULT_MCP_KAFKA_TOPIC}

    max_queue_full_block_seconds: float = pydantic.Field(
        default=_MAX_QUEUE_FULL_BLOCK_SECONDS,
        ge=0,
        description=(
            "Max time to block on a full local producer queue (backpressure) "
            "before failing the run, so an unreachable broker does not hang "
            "ingestion indefinitely."
        ),
    )

    _topic_removed = pydantic_removed_field("topic", month="November", year=2026)

    @field_validator("topic_routes", mode="after")
    @classmethod
    def validate_topic_routes(cls, v: Dict[str, str]) -> Dict[str, str]:
        assert MCP_KEY in v, f"topic_routes must contain a route for {MCP_KEY}"
        if MCE_KEY in v:
            logger.warning(
                f"{MCE_KEY} route is ignored; MCEs are published to the MCP topic {v[MCP_KEY]!r}."
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

        avro_serializer_conf: Dict[str, Any] = {}
        if not self.config.connection.auto_register_schemas:
            avro_serializer_conf["auto.register.schemas"] = False
            logger.warning(
                "Schema auto-registration is disabled; the MCP schema must already "
                f"be registered in the Schema Registry at {self.config.connection.schema_registry_url}."
            )
            self._verify_schemas_registered(schema_registry_client)

        def convert_mcp_to_dict(
            mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
            ctx: SerializationContext,
        ) -> dict:
            return mcp.to_obj(tuples=True)

        mcp_avro_serializer = AvroSerializer(
            schema_str=getMetadataChangeProposalSchema(),
            schema_registry_client=schema_registry_client,
            to_dict=convert_mcp_to_dict,
            conf=dict(avro_serializer_conf),
        )

        producers_config = {
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

        self.backpressure_engagements = 0
        self.backpressure_blocked_seconds = 0.0

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

    def _verify_schemas_registered(
        self, schema_registry_client: SchemaRegistryClient
    ) -> None:
        """Fail fast if the MCP topic's schema is not pre-registered.

        With auto-registration off, AvroSerializer looks up this exact schema
        under the topic's "<topic>-value" subject (TopicNameStrategy) on the
        first produce. Doing the same lookup here surfaces a missing or
        outdated subject at startup instead of as a serialization error on
        every record.

        MetadataChangeEvent routes are ignored: MCEs are converted to MCPs and
        published on the MCP topic, so only that subject is checked.
        """
        schemas = {
            MCP_KEY: getMetadataChangeProposalSchema(),
        }
        for key, schema_str in schemas.items():
            topic = self.config.topic_routes.get(key)
            if topic is None:
                continue
            subject = f"{topic}-value"
            try:
                schema_registry_client.lookup_schema(
                    subject, Schema(schema_str, schema_type="AVRO")
                )
            except SchemaRegistryError as e:
                if e.http_status_code == 404:
                    raise ConfigurationError(
                        f"auto_register_schemas is disabled, but the {key} schema is "
                        f"not registered under subject {subject!r} in the Schema "
                        f"Registry at {self.config.connection.schema_registry_url}. "
                        "Register the schema bundled with this DataHub version under "
                        "that subject, or check that topic_routes matches your topics."
                    ) from e
                raise ConfigurationError(
                    f"auto_register_schemas is disabled, but checking subject "
                    f"{subject!r} in the Schema Registry failed: {e}"
                ) from e

    def emit(  # type: ignore[override]
        self,
        item: Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ],
        callback: Optional[KafkaOnDelivery] = None,
    ) -> None:
        if isinstance(item, (MetadataChangeProposal, MetadataChangeProposalWrapper)):
            return self.emit_mcp_async(item, callback or _error_reporting_callback)
        else:
            warnings.warn(
                _MCE_DEPRECATION_MESSAGE, DataHubDeprecationWarning, stacklevel=2
            )
            return self._emit_mce(item, callback or _error_reporting_callback)

    @staticmethod
    def _produce_with_backpressure(
        producer: SerializingProducer,
        *,
        topic: str,
        key: Optional[str],
        value: Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ],
        on_delivery: KafkaOnDelivery,
        poll_timeout_seconds: float = _QUEUE_FULL_POLL_SECONDS,
        max_block_seconds: float = _MAX_QUEUE_FULL_BLOCK_SECONDS,
    ) -> float:
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

        Returns seconds spent blocked on a full queue (0.0 if the first produce
        succeeded), so the caller can surface backpressure in its report.
        """
        deadline = time.monotonic() + max_block_seconds
        blocked_start: Optional[float] = None
        polls = 0
        while True:
            try:
                producer.produce(
                    topic=topic, key=key, value=value, on_delivery=on_delivery
                )
                return (
                    0.0 if blocked_start is None else time.monotonic() - blocked_start
                )
            except BufferError:
                if blocked_start is None:
                    blocked_start = time.monotonic()
                polls += 1
                if polls == 1 or polls % _QUEUE_FULL_WARN_EVERY_N_POLLS == 0:
                    logger.warning(
                        "Kafka producer queue full; blocking to apply backpressure. "
                        "If this persists the broker may be unreachable."
                    )
                producer.poll(poll_timeout_seconds)
                if time.monotonic() >= deadline:
                    logger.error(
                        "Kafka producer queue still full after %.0fs; giving up "
                        "(broker unreachable?).",
                        max_block_seconds,
                    )
                    raise
            except KafkaException as ke:
                # MSG_SIZE_TOO_LARGE is raised synchronously from produce() (the
                # size check runs before queue insertion), so the sink catches it
                # around emit() rather than via the async delivery callback -- if
                # that ever changes, the oversize->REST fallback stops firing. Map
                # it to MessageTooLargeError (see its docstring); other
                # KafkaExceptions propagate and are reported as failures.
                kerr = ke.args[0] if ke.args else None
                if kerr is not None and kerr.code() == KafkaError.MSG_SIZE_TOO_LARGE:
                    raise MessageTooLargeError(
                        f"Kafka message for key {key!r} on topic {topic!r} "
                        f"exceeds producer message.max.bytes: {kerr.str()}"
                    ) from ke
                raise

    def _record_backpressure(self, blocked_seconds: float) -> None:
        if blocked_seconds > 0:
            self.backpressure_engagements += 1
            self.backpressure_blocked_seconds += blocked_seconds

    @deprecated(_MCE_DEPRECATION_MESSAGE, category=DataHubDeprecationWarning)
    def emit_mce_async(
        self,
        mce: MetadataChangeEvent,
        callback: KafkaOnDelivery,
    ) -> None:
        """Emit an MCE as one MCP per aspect to the MCP topic; ``callback`` fires at most once per MCE."""
        self._emit_mce(mce, callback)

    def _emit_mce(self, mce: MetadataChangeEvent, callback: KafkaOnDelivery) -> None:
        """Not atomic: if producing aspect k raises, aspects before it stay queued.

        ``callback`` never fires after the raise, but may already have fired with an earlier aspect's error.
        """
        # Deferred: mcp_builder pulls in ingestion sources, which pgqueue must not import.
        from datahub.emitter.mcp_builder import mcps_from_mce

        _warn_mce_deprecated()
        # The MCP topic is consumed by every GMS that consumes the MCE topic.
        mcps = list(mcps_from_mce(mce))
        if not mcps:
            logger.warning(f"MCE for {mce.proposedSnapshot.urn} produced zero MCPs")
            callback(None, "MCE had zero aspects")
            return
        agg_callback = _AggregatingKafkaCallback(total=len(mcps), inner=callback)
        try:
            for mcp in mcps:
                self.emit_mcp_async(mcp, agg_callback.kafka_callback)
        except BaseException:
            agg_callback.suppress()
            raise

    def emit_mcp_async(
        self,
        mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
        callback: KafkaOnDelivery,
    ) -> None:
        # Call poll to trigger any callbacks on success / failure of previous writes
        producer: SerializingProducer = self.producers[MCP_KEY]
        producer.poll(0)
        blocked = self._produce_with_backpressure(
            producer,
            max_block_seconds=self.config.max_queue_full_block_seconds,
            topic=self.config.topic_routes[MCP_KEY],
            key=mcp.entityUrn,
            value=mcp,
            on_delivery=callback,
        )
        self._record_backpressure(blocked)

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


@dataclass
class _AggregatingKafkaCallback:
    """Fires ``inner`` once for the N MCPs of one MCE: on the first error, or after all N succeed.

    ``suppress()`` silences it when the failure was already raised to the caller.
    Locked because delivery callbacks run in whichever thread calls ``poll()``/``flush()``,
    and the sinks poll from more than one thread.
    """

    total: int
    inner: KafkaOnDelivery
    _remaining: int = field(init=False)
    _failed: bool = field(init=False, default=False)
    _suppressed: bool = field(init=False, default=False)
    _lock: threading.Lock = field(init=False, default_factory=threading.Lock)

    def __post_init__(self) -> None:
        self._remaining = self.total

    def kafka_callback(self, err: Any, msg: Any) -> None:
        # Decide under the lock, call ``inner`` outside it: ``inner`` may flush/emit,
        # and the resulting poll() can re-enter this callback on the same thread.
        with self._lock:
            self._remaining -= 1
            if err is not None and self._suppressed:
                fire = False
                # The caller already saw an exception for this MCE; keep the broker error visible.
                logger.error(f"Kafka delivery failed after emit raised: {err} {msg}")
            elif err is not None and not self._failed:
                self._failed = True
                fire = True
            elif self._remaining == 0 and not self._failed:
                fire = True
            else:
                fire = False
        if fire:
            self.inner(err, msg)

    def suppress(self) -> bool:
        """Never fire ``inner``; for when the failure was already raised to the caller.

        Returns whether ``inner`` had already fired with an earlier error.
        """
        with self._lock:
            already_failed = self._failed
            self._failed = True
            self._suppressed = True
            return already_failed


# Cached so the warning prints once per process: the Airflow hook builds a new emitter per call.
@functools.lru_cache(maxsize=None)
def _warn_mce_deprecated() -> None:
    logger.warning(_MCE_DEPRECATION_MESSAGE)


def _error_reporting_callback(err: Any, msg: Any) -> None:
    if err:
        logger.error(f"Failed to emit to kafka: {err} {msg}")
