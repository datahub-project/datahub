import json
import logging
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional

from datahub.configuration import ConfigModel
from datahub.emitter.serialization_helper import post_json_transform

# DataHub imports.
from datahub.metadata.schema_classes import (
    GenericPayloadClass,
)
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    ENTITY_CHANGE_EVENT_V1_TYPE,
    RELATIONSHIP_CHANGE_EVENT_V1_TYPE,
    EntityChangeEvent,
    RelationshipChangeEvent,
)

# May or may not need these.
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.source.event_source import EventSource

from datahub_integrations.sources.remote_actions.events.datahub_events_ack_manager import (
    AckManager,
)
from datahub_integrations.sources.remote_actions.events.datahub_events_consumer import (
    DataHubEventsConsumer,
    ExternalEvent,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ENTITY_CHANGE_EVENT_NAME = "entityChangeEvent"
RELATIONSHIP_CHANGE_EVENT_NAME = "relationshipChangeEvent"


# Converts a DataHub Events Message to an EntityChangeEvent.
def build_entity_change_event(payload: GenericPayloadClass) -> EntityChangeEvent:
    return EntityChangeEvent.from_json(payload.get("value"))


class DataHubEventsSourceConfig(ConfigModel):
    consumer_id: str  # Used to store offset for the consumer.
    token: str = ""
    topic: str = "PlatformEvent_v1"
    lookback_days: Optional[int] = None
    force_full_refresh: Optional[bool] = False

    # Time and Exit Conditions.
    kill_after_idle_timeout: bool = True
    idle_timeout_duration_seconds: int = 30
    event_processing_time_max_duration_seconds: int = 30


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
        self.my_urn = DataHubEventSource._get_pipeline_urn(self.ctx.pipeline_name)

        # Ensure a Graph Instance was provided.
        assert self.ctx.graph is not None

        self.datahub_events_consumer: DataHubEventsConsumer = DataHubEventsConsumer(
            # TODO: This PipelineContext provides an Acryl Graph Instance
            graph=self.ctx.graph.graph,
            consumer_id=self.my_urn,
            lookback_days=self.source_config.lookback_days,
            force_full_refresh=self.source_config.force_full_refresh,
        )
        self.ack_manager = AckManager()
        self.safe_to_ack_offset: Optional[str] = None

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "EventSource":
        config = DataHubEventsSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def events(self) -> Iterable[EventEnvelope]:
        logger.debug(f"Subscribing to the following topic: {self.source_config.topic}")
        self.running = True
        yield from self._poll_and_process_events()

    def _poll_and_process_events(self) -> Iterable[EventEnvelope]:
        """Poll and process events in the main loop."""
        last_idle_response_timestamp = 0
        while self.running:
            logger.info("DataHub Events consumer main loop")
            try:
                sleeps_to_go = (
                    self.source_config.event_processing_time_max_duration_seconds
                )

                while self.ack_manager.outstanding_acks():
                    time.sleep(1)
                    sleeps_to_go -= 1
                    logger.info(f"Sleeps to go: {sleeps_to_go}")

                if sleeps_to_go == 0:
                    self.running = False
                    raise Exception(
                        f"Failed to process all events successfully within specified time \n{self.ack_manager.acks.values()}",
                    )

                logger.info(
                    f"Successfully processed events up to offset id {self.safe_to_ack_offset}"
                )
                self.safe_to_ack_offset = self.datahub_events_consumer.offset_id
                logger.info(f"Safe to ack offset: {self.safe_to_ack_offset}")

                events_response = self.datahub_events_consumer.poll_events(
                    topic=self.source_config.topic, poll_timeout_seconds=2
                )

                # Handle Idle Timeout
                num_events = len(events_response.events)
                # breakpoint()
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
                continue

        logger.info("DataHub Events consumer exiting main loop")

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

    def close(self) -> None:
        if self.datahub_events_consumer:
            self.running = False
            if self.safe_to_ack_offset:
                self.datahub_events_consumer.commit_offsets(
                    offset_id=self.safe_to_ack_offset
                )
            self.datahub_events_consumer.close()

    def ack(self, event: EventEnvelope, processed: bool = True) -> None:
        # TODO: Either save to ingestion source state or to automation state (maybe automation state is preferable).
        # We should be able to do this.
        # Use self.ctx.graph
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
