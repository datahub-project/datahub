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
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional

# Confluent important
import confluent_kafka
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from prometheus_client import Counter, Gauge

from datahub.configuration import ConfigModel
from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.emitter.serialization_helper import post_json_transform

# DataHub imports.
from datahub.metadata.schema_classes import GenericPayloadClass, MetadataChangeLogClass
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    ENTITY_CHANGE_EVENT_V1_TYPE,
    METADATA_CHANGE_LOG_EVENT_V1_TYPE,
    EntityChangeEvent,
    MetadataChangeLogEvent,
)

# May or may not need these.
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.kafka.utils import with_retry
from datahub_actions.source.event_source import EventSource

logger = logging.getLogger(__name__)


ENTITY_CHANGE_EVENT_NAME = "entityChangeEvent"
DEFAULT_TOPIC_ROUTES = {
    "mcl": "MetadataChangeLog_Versioned_v1",
    "mcl_timeseries": "MetadataChangeLog_Timeseries_v1",
    "pe": "PlatformEvent_v1",
}

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
    topic_routes: Optional[Dict[str, str]]
    async_commit_enabled: bool = False
    async_commit_interval: int = 10000
    commit_retry_count: int = 5
    commit_retry_backoff: float = 10.0


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
                # Provide a custom group id to subcribe to multiple partitions via separate actions pods.
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

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "EventSource":
        config = KafkaEventSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def events(self) -> Iterable[EventEnvelope]:
        topic_routes = self.source_config.topic_routes or DEFAULT_TOPIC_ROUTES
        topics_to_subscribe = list(topic_routes.values())
        logger.debug(f"Subscribing to the following topics: {topics_to_subscribe}")
        self.consumer.subscribe(topics_to_subscribe)
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

    @staticmethod
    def handle_mcl(msg: Any) -> Iterable[EventEnvelope]:
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
            event = build_entity_change_event(payload)
            kafka_meta = build_kafka_meta(msg)
            yield EventEnvelope(ENTITY_CHANGE_EVENT_V1_TYPE, event, kafka_meta)

    def close(self) -> None:
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
                f"Unexpected response when commiting offset to kafka: topic: {event.meta['kafka']['topic']}, partition: {event.meta['kafka']['partition']}, offset: {event.meta['kafka']['offset']}"
            )
            return
        for partition in retval:
            if partition.error is not None:
                raise KafkaException(
                    f"Failed to commit offest for topic: {partition.topic}, partition: {partition.partition}, offset: {partition.offset}: {partition.error.str()}"
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
        # See for details: https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#auto-offset-commit

        if processed or not self.source_config.async_commit_enabled:
            # Immediately commit if the message was processed by the upstream,
            # or delayed commit is disabled
            with_retry(
                self.source_config.commit_retry_count,
                self.source_config.commit_retry_backoff,
                self._commit_offsets,
                event,
            )
        else:
            # Otherwise store offset for periodic autocommit
            self._store_offsets(event)
