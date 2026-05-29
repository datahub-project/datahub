# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional

# Confluent important
import confluent_kafka
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from prometheus_client import Counter, Gauge
from pydantic import Field

from datahub.configuration import ConfigModel
from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.emitter.serialization_helper import post_json_transform

# DataHub imports.
from datahub.metadata.schema_classes import GenericPayloadClass, MetadataChangeLogClass
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    ENTITY_CHANGE_EVENT_V1_TYPE,
    METADATA_CHANGE_LOG_EVENT_V1_TYPE,
    RELATIONSHIP_CHANGE_EVENT_V1_TYPE,
    EntityChangeEvent,
    MetadataChangeLogEvent,
    RelationshipChangeEvent,
)
from datahub_actions.filter.filter import Filter

# May or may not need these.
from datahub_actions.observability.kafka_lag_monitor import KafkaLagMonitor
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.kafka.utils import with_retry
from datahub_actions.source.event_source import EventSource

logger = logging.getLogger(__name__)


ENTITY_CHANGE_EVENT_NAME = "entityChangeEvent"
RELATIONSHIP_CHANGE_EVENT_NAME = "relationshipChangeEvent"
DEFAULT_TOPIC_ROUTES = {
    "mcl": "MetadataChangeLog_Versioned_v1",
    "mcl_timeseries": "MetadataChangeLog_Timeseries_v1",
    "pe": "PlatformEvent_v1",
}

# Top-level scalar fields in the MetadataChangeLog Avro schema that are
# accessible on the raw Kafka message dict before avrogen deserialization.
# Only these fields are candidates for pre-deserialization filtering.
_MCL_EARLY_FILTER_FIELDS = frozenset(
    {"entityType", "aspectName", "entityUrn", "changeType"}
)

OFFSET_METRIC = Gauge(
    name="kafka_offset",
    documentation="Kafka offsets per topic, partition",
    labelnames=["topic", "partition", "pipeline_name"],
)

MESSAGE_COUNTER_METRIC = Counter(
    name="kafka_messages",
    documentation="Number of kafka messages",
    labelnames=["pipeline_name", "error"],
)

MCL_EARLY_FILTER_METRIC = Counter(
    name="kafka_mcl_early_filter",
    documentation="MCL events handled by KafkaSource pre-deserialization filter",
    labelnames=["pipeline_name", "result"],  # result: rejected | passed
)


# Converts a Kafka Message to a Kafka Metadata Dictionary.
def build_kafka_meta(msg: Any) -> dict:
    return {
        "kafka": {
            "topic": msg.topic(),
            "offset": msg.offset(),
            "partition": msg.partition(),
        }
    }


# Converts a Kafka Message to a MetadataChangeLogEvent
def build_metadata_change_log_event(msg: Any) -> MetadataChangeLogEvent:
    value: dict = msg.value()
    return MetadataChangeLogEvent.from_class(
        MetadataChangeLogClass.from_obj(value, True)
    )


# Converts a Kafka Message to an EntityChangeEvent.
def build_entity_change_event(payload: GenericPayloadClass) -> EntityChangeEvent:
    return EntityChangeEvent.from_json(payload.get("value"))


class KafkaEventSourceConfig(ConfigModel):
    connection: KafkaConsumerConnectionConfig = KafkaConsumerConnectionConfig()
    topic_routes: Optional[Dict[str, str]] = Field(default=None)
    async_commit_enabled: bool = True
    async_commit_interval: int = 10000
    commit_retry_count: int = 5
    commit_retry_backoff: float = 10.0
    enable_mcl_pre_deserialization_filter: bool = Field(
        default=True,
        description=(
            "When True, use the pipeline's EventTypeFilter criteria to drop "
            "MetadataChangeLog (MCL) messages before the expensive avrogen "
            "MetadataChangeLogClass.from_obj() deserialization call. "
            "Scope is intentionally limited to MCL events: MCL has entityType, "
            "aspectName, entityUrn, and changeType as top-level Avro fields that "
            "are accessible on the raw Kafka message without any deserialization. "
            "EntityChangeEvent (ECE) events arrive inside a PlatformEvent envelope "
            "with a JSON-encoded payload — their fields (category, operation, etc.) "
            "can only be read after deserializing the envelope, so pre-deserialization "
            "filtering of ECE events is not supported and ECE delivery is never affected "
            "by this flag. Requires at least one EventTypeFilter in the pipeline's "
            "'filters' section that does NOT include MetadataChangeLogEvent_v1 (or "
            "includes it with predicate fields restricted to entityType, aspectName, "
            "entityUrn, or changeType)."
        ),
    )


def kafka_messages_observer(pipeline_name: str) -> Callable:
    def _observe(message):
        if message is not None:
            topic = message.topic()
            partition = message.partition()
            offset = message.offset()
            logger.debug(f"Kafka msg received: {topic}, {partition}, {offset}")
            OFFSET_METRIC.labels(
                topic=topic, partition=partition, pipeline_name=pipeline_name
            ).set(offset)
            MESSAGE_COUNTER_METRIC.labels(
                error=message.error() is not None, pipeline_name=pipeline_name
            ).inc()

    return _observe


# This is the default Kafka-based Event Source.
@dataclass
class KafkaEventSource(EventSource):
    running = False
    source_config: KafkaEventSourceConfig
    _lag_monitor: Optional[KafkaLagMonitor] = None

    # Pre-deserialization filter state (populated by set_filters when enabled)
    _skip_mcl_entirely: bool = field(default=False, init=False)
    _early_mcl_criteria_list: List[Dict[str, Any]] = field(
        default_factory=list, init=False
    )
    _pipeline_name: str = field(default="", init=False)

    def __init__(self, config: KafkaEventSourceConfig, ctx: PipelineContext):
        self.source_config = config
        schema_client_config = config.connection.schema_registry_config.copy()
        schema_client_config["url"] = self.source_config.connection.schema_registry_url
        self.schema_registry_client = SchemaRegistryClient(schema_client_config)

        async_commit_config: Dict[str, Any] = {}
        if self.source_config.async_commit_enabled:
            # See for details: https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#auto-offset-commit
            async_commit_config["enable.auto.offset.store"] = False
            async_commit_config["enable.auto.commit"] = True
            async_commit_config["auto.commit.interval.ms"] = (
                self.source_config.async_commit_interval
            )

        self.consumer: confluent_kafka.Consumer = confluent_kafka.DeserializingConsumer(
            {
                # Provide a custom group id to subscribe to multiple partitions via separate actions pods.
                "group.id": ctx.pipeline_name,
                "bootstrap.servers": self.source_config.connection.bootstrap,
                "enable.auto.commit": False,  # We manually commit offsets.
                "auto.offset.reset": "latest",  # Latest by default, unless overwritten.
                "value.deserializer": AvroDeserializer(
                    schema_registry_client=self.schema_registry_client,
                    return_record_name=True,
                ),
                "session.timeout.ms": "10000",  # 10s timeout.
                "max.poll.interval.ms": "10000",  # 10s poll max.
                **self.source_config.connection.consumer_config,
                **async_commit_config,
            }
        )
        self._observe_message: Callable = kafka_messages_observer(ctx.pipeline_name)
        self._skip_mcl_entirely = False
        self._early_mcl_criteria_list: List[Dict[str, Any]] = []
        self._pipeline_name = ctx.pipeline_name

        # Initialize lag monitoring (if enabled)
        if self._is_lag_monitoring_enabled():
            lag_interval = float(
                os.environ.get("DATAHUB_ACTIONS_KAFKA_LAG_INTERVAL_SECONDS", "30")
            )
            lag_timeout = float(
                os.environ.get("DATAHUB_ACTIONS_KAFKA_LAG_TIMEOUT_SECONDS", "5")
            )
            self._lag_monitor = KafkaLagMonitor(
                consumer=self.consumer,
                pipeline_name=ctx.pipeline_name,
                interval_seconds=lag_interval,
                timeout_seconds=lag_timeout,
            )
            logger.info(
                f"Kafka lag monitoring enabled for '{ctx.pipeline_name}' "
                f"(interval={lag_interval}s, timeout={lag_timeout}s)"
            )
        else:
            logger.debug(
                f"Kafka lag monitoring disabled for pipeline '{ctx.pipeline_name}'"
            )

    @staticmethod
    def _is_lag_monitoring_enabled() -> bool:
        """Check if Kafka lag monitoring should be enabled.

        Lag monitoring is enabled if:
        1. DATAHUB_ACTIONS_KAFKA_LAG_ENABLED=true (case-insensitive)

        Default: False (conservative default for OSS rollout)
        """
        enabled_str = os.environ.get("DATAHUB_ACTIONS_KAFKA_LAG_ENABLED", "false")
        return enabled_str.lower() in ("true", "1", "yes")

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "EventSource":
        config = KafkaEventSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def set_filters(self, filters: List[Filter]) -> None:
        """
        Configure pre-deserialization filtering for MCL events (optimization).

        CONSERVATIVE BY DESIGN: This optimization extracts what it CAN check before
        deserialization and uses that for early rejection. Events that pass the early
        filter will still be fully evaluated by the pipeline filter after deserialization.

        FILTER SEMANTICS:
        - Predicates are evaluated with OR logic: pass if ANY predicate matches
        - Within a predicate, constraints are AND: must match ALL constraints
        - List values use IN logic: field value must be in the list

        Example:
          predicates = [
            {
              entityType: "dataset",
              aspectName: ["documentation", "ownership"]  # matches if aspectName IN this list
            },  # predicate 1
            {
              entityType: "dataHubExecutionRequest",
              changeType: "UPSERT",
              aspectName: ["dataHubExecutionRequestInput", "dataHubExecutionRequestSignal"],
              aspect: {value: {executorId: "default"}}  # NOT extractable - requires deserialization
            },  # predicate 2
            {
              aspect: {value: {executorId: "default"}}  # NO extractable fields
            }   # predicate 3
          ]

          Filter logic:
            (entityType="dataset" AND aspectName IN ["documentation", "ownership"])
            OR
            (entityType="dataHubExecutionRequest" AND changeType="UPSERT"
             AND aspectName IN ["dataHubExecutionRequestInput", "dataHubExecutionRequestSignal"]
             AND aspect.value.executorId="default")
            OR
            (aspect.value.executorId="default")

        OPTIMIZATION REQUIREMENT:
        Optimization is ONLY enabled when ALL predicates have at least one extractable
        field from _MCL_EARLY_FILTER_FIELDS (entityType, aspectName, entityUrn, changeType).

        WHY: If ANY predicate has zero extractable fields (like predicate 3 above),
        we cannot check that predicate early. Since predicates use OR semantics,
        any message MIGHT match the unoptimizable predicate, so we cannot safely
        reject anything. The optimization becomes useless and must be disabled.

        When enabled, we extract partial criteria from each predicate (ignoring
        non-extractable fields like aspect.value.executorId in predicate 2) and
        apply OR semantics for early rejection. Messages passing the early filter
        proceed to full deserialization and complete evaluation.
        """
        if not self.source_config.enable_mcl_pre_deserialization_filter:
            logger.debug(
                f"KafkaEventSource [{self._pipeline_name}]: MCL pre-deserialization filter disabled. "
                "Set enable_mcl_pre_deserialization_filter: true on the Kafka source to reduce "
                "deserialization overhead when the pipeline filters on MCL fields."
            )
            return

        # Import here to avoid a circular import at module load time.
        from datahub_actions.plugin.filter.event_type_filter import EventTypeFilter

        mcl_predicates: List[Dict[str, Any]] = []
        mcl_seen = False
        has_event_type_filter = False

        for f in filters:
            if not isinstance(f, EventTypeFilter):
                continue
            has_event_type_filter = True
            spec = f.config.filter.get(METADATA_CHANGE_LOG_EVENT_V1_TYPE)
            if spec is not None:
                mcl_seen = True
                if spec.event is not None:
                    mcl_predicates.extend(spec.event)

        if not has_event_type_filter:
            # No EventTypeFilter configured — nothing to optimize; pass MCL through normally.
            logger.debug(
                f"KafkaEventSource [{self._pipeline_name}]: enable_mcl_pre_deserialization_filter is set "
                "but no EventTypeFilter was found in the pipeline's filters. MCL events will be passed through."
            )
            return

        if not mcl_seen:
            # An EventTypeFilter exists but does not include MCL at all.
            # The pipeline filter will drop all MCL events regardless, so we can
            # skip avrogen deserialization entirely for every MCL message.
            self._skip_mcl_entirely = True
            logger.info(
                f"KafkaEventSource [{self._pipeline_name}]: pre-deserialization filter active — "
                "all MCL messages will be dropped before avrogen deserialization "
                "(MetadataChangeLogEvent_v1 is not present in the EventTypeFilter)"
            )
            return

        # Extract early-checkable criteria from each predicate.
        # CONSERVATIVE: We extract only what we CAN check (scalar/list fields from
        # _MCL_EARLY_FILTER_FIELDS) and ignore fields that require deserialization.
        # This creates a "partial view" of each predicate that's safe for early rejection.
        criteria_list: List[Dict[str, Any]] = []

        for predicate in mcl_predicates:
            # Extract ONLY the fields we can check pre-deserialization
            predicate_criteria: Dict[str, Any] = {}

            for key, val in predicate.items():
                if key in _MCL_EARLY_FILTER_FIELDS and not isinstance(val, dict):
                    # Scalar or list value that's accessible on raw Kafka message
                    predicate_criteria[key] = val
                # Ignore: key not in _MCL_EARLY_FILTER_FIELDS or dict-valued (requires deserialization)

            if not predicate_criteria:
                # This predicate has NO extractable fields - cannot optimize.
                # If we can't check this predicate early, any message might match it (OR semantics),
                # so we cannot safely reject anything. Disable optimization.
                logger.warning(
                    f"KafkaEventSource [{self._pipeline_name}]: MCL pre-deserialization "
                    f"optimization skipped - at least one predicate has no extractable fields. "
                    f"All fields in that predicate require deserialization. "
                    f"Optimization only supports scalar/list values for: {sorted(_MCL_EARLY_FILTER_FIELDS)}. "
                    f"All MCL events will proceed to full deserialization and filter evaluation."
                )
                return

            criteria_list.append(predicate_criteria)

        self._early_mcl_criteria_list = criteria_list
        logger.info(
            f"KafkaEventSource [{self._pipeline_name}]: pre-deserialization MCL optimization active. "
            f"Criteria (OR semantics - pass if ANY match, conservative by design): {criteria_list}"
        )

    def events(self) -> Iterable[EventEnvelope]:
        topic_routes = self.source_config.topic_routes or DEFAULT_TOPIC_ROUTES
        topics_to_subscribe = list(topic_routes.values())
        logger.debug(f"Subscribing to the following topics: {topics_to_subscribe}")
        self.consumer.subscribe(topics_to_subscribe)

        # Start lag monitoring after subscription
        if self._lag_monitor is not None:
            self._lag_monitor.start()

        self.running = True
        while self.running:
            try:
                msg = self.consumer.poll(timeout=2.0)
            except confluent_kafka.error.ConsumeError as e:
                logger.exception(f"Kafka consume error: {e}")
                continue

            if msg is None:
                continue

            self._observe_message(msg)
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.debug(
                        "%% %s [%d] reached end at offset %d\n"
                        % (msg.topic(), msg.partition(), msg.offset())
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                if "mcl" in topic_routes and msg.topic() == topic_routes["mcl"]:
                    yield from self.handle_mcl(msg)
                if (
                    "mcl_timeseries" in topic_routes
                    and msg.topic() == topic_routes["mcl_timeseries"]
                ):
                    yield from self.handle_mcl(
                        msg
                    )  # Handle timeseries in the same way as usual MCL.
                elif "pe" in topic_routes and msg.topic() == topic_routes["pe"]:
                    yield from self.handle_pe(msg)

        logger.info("Kafka consumer exiting main loop")

    def handle_mcl(self, msg: Any) -> Iterable[EventEnvelope]:
        """
        Handle MCL message with optional pre-deserialization filtering.

        CONSERVATIVE BY DESIGN: If early criteria are configured, we check if the
        message matches ANY criteria (OR semantics). If no match, reject early.
        If match, proceed to deserialization and full filter evaluation.
        """
        if self._skip_mcl_entirely:
            MCL_EARLY_FILTER_METRIC.labels(
                pipeline_name=self._pipeline_name, result="entirely_rejected"
            ).inc()
            return

        if self._early_mcl_criteria_list:
            raw: Dict[str, Any] = msg.value()

            # OR semantics: pass if ANY criteria matches
            matched = False
            for criteria in self._early_mcl_criteria_list:
                # AND within a single criteria: all keys must match
                all_match = True
                for key, val in criteria.items():
                    raw_val = raw.get(key)
                    match = raw_val in val if isinstance(val, list) else raw_val == val
                    if not match:
                        all_match = False
                        break

                if all_match:
                    matched = True
                    break

            if not matched:
                # No criteria matched - early reject (conservative: definite non-match)
                MCL_EARLY_FILTER_METRIC.labels(
                    pipeline_name=self._pipeline_name, result="rejected"
                ).inc()
                return

            # At least one criteria matched - pass through (conservative: might match after full eval)
            MCL_EARLY_FILTER_METRIC.labels(
                pipeline_name=self._pipeline_name, result="passed"
            ).inc()

        metadata_change_log_event = build_metadata_change_log_event(msg)
        kafka_meta = build_kafka_meta(msg)
        yield EventEnvelope(
            METADATA_CHANGE_LOG_EVENT_V1_TYPE, metadata_change_log_event, kafka_meta
        )

    @staticmethod
    def handle_pe(msg: Any) -> Iterable[EventEnvelope]:
        value: dict = msg.value()
        payload: GenericPayloadClass = GenericPayloadClass.from_obj(
            post_json_transform(value["payload"])
        )
        if ENTITY_CHANGE_EVENT_NAME == value["name"]:
            ece = build_entity_change_event(payload)
            kafka_meta = build_kafka_meta(msg)
            yield EventEnvelope(ENTITY_CHANGE_EVENT_V1_TYPE, ece, kafka_meta)
        elif RELATIONSHIP_CHANGE_EVENT_NAME == value["name"]:
            rce = RelationshipChangeEvent.from_json(payload.get("value"))
            kafka_meta = build_kafka_meta(msg)
            yield EventEnvelope(RELATIONSHIP_CHANGE_EVENT_V1_TYPE, rce, kafka_meta)

    def close(self) -> None:
        # Stop lag monitoring first
        if self._lag_monitor is not None:
            self._lag_monitor.stop()

        # Then close consumer
        if self.consumer:
            self.running = False
            self.consumer.close()

    def _commit_offsets(self, event: EventEnvelope) -> None:
        retval = self.consumer.commit(
            asynchronous=False,
            offsets=[
                TopicPartition(
                    event.meta["kafka"]["topic"],
                    event.meta["kafka"]["partition"],
                    event.meta["kafka"]["offset"] + 1,
                )
            ],
        )
        if retval is None:
            logger.exception(
                f"Unexpected response when committing offset to kafka: topic: {event.meta['kafka']['topic']}, partition: {event.meta['kafka']['partition']}, offset: {event.meta['kafka']['offset']}"
            )
            return
        for partition in retval:
            if partition.error is not None:
                raise KafkaException(
                    f"Failed to commit offset for topic: {partition.topic}, partition: {partition.partition}, offset: {partition.offset}: {partition.error.str()}"
                )
        logger.debug(
            f"Successfully committed offsets at message: topic: {event.meta['kafka']['topic']}, partition: {event.meta['kafka']['partition']}, offset: {event.meta['kafka']['offset']}"
        )

    def _store_offsets(self, event: EventEnvelope) -> None:
        self.consumer.store_offsets(
            offsets=[
                TopicPartition(
                    event.meta["kafka"]["topic"],
                    event.meta["kafka"]["partition"],
                    event.meta["kafka"]["offset"] + 1,
                )
            ],
        )

    def ack(self, event: EventEnvelope, processed: bool = True) -> None:
        if not processed:  # No action if event not processed successfully
            return

        # See for details: https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#auto-offset-commit
        if self.source_config.async_commit_enabled:
            self._store_offsets(event)
        else:
            with_retry(
                self.source_config.commit_retry_count,
                self.source_config.commit_retry_backoff,
                self._commit_offsets,
                event,
            )
