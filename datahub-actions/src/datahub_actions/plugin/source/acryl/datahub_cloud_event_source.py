import json
import logging
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Union, cast

from pydantic import Field

from datahub.configuration import ConfigModel
from datahub.emitter.serialization_helper import post_json_transform
from datahub.ingestion.graph.client import DataHubGraph

# DataHub imports.
from datahub.metadata.schema_classes import GenericPayloadClass
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    ENTITY_CHANGE_EVENT_V1_TYPE,
    METADATA_CHANGE_LOG_EVENT_V1_TYPE,
    RELATIONSHIP_CHANGE_EVENT_V1_TYPE,
    EntityChangeEvent,
    MetadataChangeLogEvent,
    RelationshipChangeEvent,
)

# May or may not need these.
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.acryl.constants import (
    ENTITY_CHANGE_EVENT_NAME,
    METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME,
    METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME,
    PLATFORM_EVENT_TOPIC_NAME,
    RELATIONSHIP_CHANGE_EVENT_NAME,
)
from datahub_actions.plugin.source.acryl.datahub_cloud_events_ack_manager import (
    AckManager,
)
from datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer import (
    DataHubEventsConsumer,
    ExternalEvent,
)
from datahub_actions.source.event_source import EventSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Converts a DataHub Events Message to an EntityChangeEvent.
def build_entity_change_event(payload: GenericPayloadClass) -> EntityChangeEvent:
    try:
        return EntityChangeEvent.from_json(payload.get("value"))
    except Exception as e:
        raise ValueError("Failed to parse into EntityChangeEvent") from e


def build_metadata_change_log_event(msg: ExternalEvent) -> MetadataChangeLogEvent:
    try:
        return cast(MetadataChangeLogEvent, MetadataChangeLogEvent.from_json(msg.value))
    except Exception as e:
        raise ValueError("Failed to parse into MetadataChangeLogEvent") from e


class DataHubEventsSourceConfig(ConfigModel):
    topics: Union[str, List[str]] = PLATFORM_EVENT_TOPIC_NAME
    consumer_id: Optional[str] = Field(
        default=None, description="Used to store offset for the consumer."
    )
    lookback_days: Optional[int] = Field(default=None)
    reset_offsets: Optional[bool] = False
    infinite_retry: Optional[bool] = False

    # Time and Exit Conditions.
    kill_after_idle_timeout: bool = False
    idle_timeout_duration_seconds: int = 30
    event_processing_time_max_duration_seconds: int = 60


# This is the custom DataHub-based Event Source.
@dataclass
class DataHubEventSource(EventSource):
    running = False
    source_config: DataHubEventsSourceConfig
    ctx: PipelineContext

    @staticmethod
    def _get_pipeline_urn(pipeline_name: str) -> str:
        if pipeline_name.startswith("urn:li:dataHubAction:"):
            return pipeline_name
        else:
            return f"urn:li:dataHubAction:{pipeline_name}"

    def __init__(self, config: DataHubEventsSourceConfig, ctx: PipelineContext):
        self.ctx = ctx
        self.source_config = config
        self.base_consumer_id = DataHubEventSource._get_pipeline_urn(
            self.ctx.pipeline_name
        )

        # Convert topics to a list for consistent handling
        if isinstance(self.source_config.topics, str):
            self.topics_list = [self.source_config.topics]
        else:
            self.topics_list = self.source_config.topics

        # Ensure a Graph Instance was provided.
        assert self.ctx.graph is not None

        # Initialize topic consumers
        self.topic_consumers = self._initialize_topic_consumers(
            topics_list=self.topics_list,
            base_consumer_id=self.base_consumer_id,
            graph=self.ctx.graph.graph,
            lookback_days=self.source_config.lookback_days,
            reset_offsets=self.source_config.reset_offsets,
            infinite_retry=self.source_config.infinite_retry,
        )

        self.ack_manager = AckManager()
        self.safe_to_ack_offsets: Dict[str, Optional[str]] = {
            topic: None for topic in self.topics_list
        }

    def _initialize_topic_consumers(
        self,
        topics_list: List[str],
        base_consumer_id: str,
        graph: DataHubGraph,
        lookback_days: Optional[int],
        reset_offsets: Optional[bool],
        infinite_retry: Optional[bool],
    ) -> Dict[str, DataHubEventsConsumer]:
        """
        Initialize DataHub consumers for each topic with appropriate consumer IDs.

        Maintains backward compatibility by using the legacy consumer ID format
        for single PlatformEvent_v1 deployments, and topic-suffixed IDs for
        multi-topic or other single-topic deployments.

        Args:
            topics_list: List of topic names to create consumers for
            base_consumer_id: Base consumer ID for the pipeline
            graph: DataHub graph instance
            lookback_days: Number of days to look back for events
            reset_offsets: Whether to reset consumer offsets

        Returns:
            Dictionary mapping topic names to their corresponding consumers
        """
        topic_consumers: Dict[str, DataHubEventsConsumer] = {}

        for topic in topics_list:
            # Backward compatibility: if only PlatformEvent_v1, use legacy consumer ID format
            if len(topics_list) == 1 and topic == PLATFORM_EVENT_TOPIC_NAME:
                topic_consumer_id = (
                    base_consumer_id  # Legacy format for existing deployments
                )
            else:
                topic_consumer_id = (
                    f"{base_consumer_id}-{topic}"  # New format for multi-topic
                )

            topic_consumers[topic] = DataHubEventsConsumer(
                graph=graph,
                consumer_id=topic_consumer_id,
                lookback_days=lookback_days,
                reset_offsets=reset_offsets,
                infinite_retry=infinite_retry,
            )

        return topic_consumers

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "EventSource":
        config = DataHubEventsSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def events(self) -> Iterable[EventEnvelope]:
        logger.info("Starting DataHub Cloud events source...")
        logger.info(f"Subscribing to the following topics: {self.topics_list}")
        self.running = True
        yield from self._poll_and_process_events()

    def _poll_and_process_events(self) -> Iterable[EventEnvelope]:
        """Poll and process events in the main loop."""
        last_idle_response_timestamp = 0
        while self.running:
            try:
                sleeps_to_go = (
                    self.source_config.event_processing_time_max_duration_seconds
                )

                while self.ack_manager.outstanding_acks():
                    time.sleep(1)
                    sleeps_to_go -= 1
                    logger.debug(f"Sleeps to go: {sleeps_to_go}")

                    if sleeps_to_go == 0:
                        self.running = False
                        raise Exception(
                            f"Failed to process all events successfully after specified time {self.source_config.event_processing_time_max_duration_seconds}! If more time is required, please increase the timeout using this config. {self.ack_manager.acks.values()}",
                        )
                # Update safe-to-ack offsets for each topic
                for topic in self.topics_list:
                    consumer = self.topic_consumers[topic]
                    self.safe_to_ack_offsets[topic] = consumer.offset_id
                    logger.debug(
                        f"Safe to ack offset for {topic}: {self.safe_to_ack_offsets[topic]}"
                    )

                # Poll events from all topics using their respective consumers
                all_events = []
                total_events = 0

                for topic in self.topics_list:
                    consumer = self.topic_consumers[topic]
                    events_response = consumer.poll_events(
                        topic=topic, poll_timeout_seconds=2
                    )
                    total_events += len(events_response.events)

                    # Process events from this topic
                    for msg in events_response.events:
                        all_events.append((topic, msg))

                # Handle Idle Timeout
                if total_events == 0:
                    if last_idle_response_timestamp == 0:
                        last_idle_response_timestamp = (
                            self._get_current_timestamp_seconds()
                        )
                    if self._should_idle_timeout(
                        total_events, last_idle_response_timestamp
                    ):
                        logger.info("Exiting main loop due to idle timeout")
                        return
                else:
                    self.ack_manager.new_batch()
                    last_idle_response_timestamp = 0  # Reset the idle timeout

                event_envelopes: List[EventEnvelope] = []
                for topic, msg in all_events:
                    # Route events based on topic type
                    for event_envelope in self._route_event_by_topic(topic, msg):
                        event_envelope.meta = self.ack_manager.get_meta(event_envelope)
                        event_envelopes.append(event_envelope)

                yield from event_envelopes

            except Exception as e:
                logger.exception(f"DataHub Events consumer error: {e}")
                self.running = False

        logger.info("DataHub Events consumer exiting main loop")

    def _route_event_by_topic(
        self, topic: str, msg: ExternalEvent
    ) -> Iterable[EventEnvelope]:
        """Route events to appropriate handlers based on topic type."""
        if topic == PLATFORM_EVENT_TOPIC_NAME:
            yield from self.handle_pe(msg)
        elif topic in [
            METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME,
            METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME,
        ]:
            yield from self.handle_mcl(msg)
        else:
            logger.warning(f"Unknown topic: {topic}, skipping event")

    @staticmethod
    def handle_pe(msg: ExternalEvent) -> Iterable[EventEnvelope]:
        value: dict = json.loads(msg.value)
        payload: GenericPayloadClass = GenericPayloadClass.from_obj(
            post_json_transform(value["payload"])
        )
        if ENTITY_CHANGE_EVENT_NAME == value["name"]:
            ece = build_entity_change_event(payload)
            yield EventEnvelope(ENTITY_CHANGE_EVENT_V1_TYPE, ece, {})
        elif RELATIONSHIP_CHANGE_EVENT_NAME == value["name"]:
            rce = RelationshipChangeEvent.from_json(payload.get("value"))
            yield EventEnvelope(RELATIONSHIP_CHANGE_EVENT_V1_TYPE, rce, {})

    @staticmethod
    def handle_mcl(msg: ExternalEvent) -> Iterable[EventEnvelope]:
        event = build_metadata_change_log_event(msg)
        yield EventEnvelope(METADATA_CHANGE_LOG_EVENT_V1_TYPE, event, {})

    def close(self) -> None:
        self.running = False
        # Close and commit offsets for each topic consumer
        for topic, consumer in self.topic_consumers.items():
            safe_offset = self.safe_to_ack_offsets.get(topic)
            if safe_offset:
                consumer.commit_offsets(offset_id=safe_offset)
            consumer.close()

    def ack(self, event: EventEnvelope, processed: bool = True) -> None:
        self.ack_manager.ack(event.meta, processed=processed)
        logger.debug(f"Actions acked event {event} as processed {processed}")

    def _should_idle_timeout(
        self, num_events: int, last_idle_response_timestamp: int
    ) -> bool:
        """Handle idle timeout logic and decide if the loop should exit."""
        if num_events > 0:
            return False  # Continue processing

        current_timestamp_seconds = self._get_current_timestamp_seconds()

        if (
            self.source_config.kill_after_idle_timeout
            and current_timestamp_seconds - last_idle_response_timestamp
            > self.source_config.idle_timeout_duration_seconds
        ):
            logger.info(
                f"Shutting down due to idle timeout of {self.source_config.idle_timeout_duration_seconds} seconds"
            )
            self.running = False
            return True  # Signal that we should exit
        return False  # Continue processing

    def _get_current_timestamp_seconds(self) -> int:
        return int(time.time())
