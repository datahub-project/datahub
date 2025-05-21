import json
import logging
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional

from datahub.configuration import ConfigModel
from datahub.emitter.serialization_helper import post_json_transform

# DataHub imports.
from datahub.metadata.schema_classes import GenericPayloadClass
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    ENTITY_CHANGE_EVENT_V1_TYPE,
    EntityChangeEvent,
)

# May or may not need these.
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.acryl.constants import (
    ENTITY_CHANGE_EVENT_NAME,
    PLATFORM_EVENT_TOPIC_NAME,
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


class DataHubEventsSourceConfig(ConfigModel):
    topic: str = PLATFORM_EVENT_TOPIC_NAME
    consumer_id: Optional[str] = None  # Used to store offset for the consumer.
    lookback_days: Optional[int] = None
    reset_offsets: Optional[bool] = False

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
        self.consumer_id = DataHubEventSource._get_pipeline_urn(self.ctx.pipeline_name)

        # Ensure a Graph Instance was provided.
        assert self.ctx.graph is not None

        self.datahub_events_consumer: DataHubEventsConsumer = DataHubEventsConsumer(
            # TODO: This PipelineContext provides an Acryl Graph Instance
            graph=self.ctx.graph.graph,
            consumer_id=self.consumer_id,
            lookback_days=self.source_config.lookback_days,
            reset_offsets=self.source_config.reset_offsets,
        )
        self.ack_manager = AckManager()
        self.safe_to_ack_offset: Optional[str] = None

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "EventSource":
        config = DataHubEventsSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def events(self) -> Iterable[EventEnvelope]:
        logger.info("Starting DataHub Cloud events source...")
        logger.info(f"Subscribing to the following topic: {self.source_config.topic}")
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
                logger.debug(
                    f"Successfully processed events up to offset id {self.safe_to_ack_offset}"
                )
                self.safe_to_ack_offset = self.datahub_events_consumer.offset_id
                logger.debug(f"Safe to ack offset: {self.safe_to_ack_offset}")

                events_response = self.datahub_events_consumer.poll_events(
                    topic=self.source_config.topic, poll_timeout_seconds=2
                )

                # Handle Idle Timeout
                num_events = len(events_response.events)

                if num_events == 0:
                    if last_idle_response_timestamp == 0:
                        last_idle_response_timestamp = (
                            self._get_current_timestamp_seconds()
                        )
                    if self._should_idle_timeout(
                        num_events, last_idle_response_timestamp
                    ):
                        logger.info("Exiting main loop due to idle timeout")
                        return
                else:
                    self.ack_manager.new_batch()
                    last_idle_response_timestamp = 0  # Reset the idle timeout

                event_envelopes: List[EventEnvelope] = []
                for msg in events_response.events:
                    for event_envelope in self.handle_pe(msg):
                        event_envelope.meta = self.ack_manager.get_meta(event_envelope)
                        event_envelopes.append(event_envelope)

                yield from event_envelopes

            except Exception as e:
                logger.exception(f"DataHub Events consumer error: {e}")
                self.running = False

        logger.info("DataHub Events consumer exiting main loop")

    @staticmethod
    def handle_pe(msg: ExternalEvent) -> Iterable[EventEnvelope]:
        value: dict = json.loads(msg.value)
        payload: GenericPayloadClass = GenericPayloadClass.from_obj(
            post_json_transform(value["payload"])
        )
        if ENTITY_CHANGE_EVENT_NAME == value["name"]:
            event = build_entity_change_event(payload)
            yield EventEnvelope(ENTITY_CHANGE_EVENT_V1_TYPE, event, {})

    def close(self) -> None:
        if self.datahub_events_consumer:
            self.running = False
            if self.safe_to_ack_offset:
                self.datahub_events_consumer.commit_offsets(
                    offset_id=self.safe_to_ack_offset
                )
            self.datahub_events_consumer.close()

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
