import json
from datetime import datetime, timezone
from typing import Optional

from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    ENTITY_CHANGE_EVENT_V1_TYPE,
    METADATA_CHANGE_LOG_EVENT_V1_TYPE,
    EntityChangeEvent,
    MetadataChangeLogEvent,
)
from pydantic import BaseModel


class EventProcessingStats(BaseModel):
    """
    A class to represent the processing stats for a pipeline.
    """

    last_seen_event_time: Optional[str]  # The event time of the last event we processed
    last_event_processed_time: Optional[
        str
    ]  # The time at which we processed the last event
    last_seen_event_time_success: Optional[
        str
    ]  # The event time of the last event we processed successfully
    last_event_processed_time_success: Optional[
        str
    ]  # The time at which we processed the last event successfully
    last_seen_event_time_failure: Optional[
        str
    ]  # The event time of the last event we processed unsuccessfully
    last_event_processed_time_failure: Optional[
        str
    ]  # The time at which we processed the last event unsuccessfully

    def __get_event_time(self, event: EventEnvelope) -> Optional[str]:
        """
        Get the event time from the event.
        """
        if event.event_type == ENTITY_CHANGE_EVENT_V1_TYPE:
            if isinstance(event.event, EntityChangeEvent):
                return (
                    datetime.fromtimestamp(
                        event.event.auditStamp.time / 1000.0, tz=timezone.utc
                    ).isoformat()
                    if event.event.auditStamp
                    else None
                )
        elif event.event_type == METADATA_CHANGE_LOG_EVENT_V1_TYPE:
            if isinstance(event.event, MetadataChangeLogEvent):
                return (
                    datetime.fromtimestamp(
                        event.event.auditHeader.time / 1000.0, tz=timezone.utc
                    ).isoformat()
                    if event.event.auditHeader
                    else None
                )
        return None

    def start(self, event: EventEnvelope) -> None:
        """
        Update the stats based on the event.
        """
        self.last_event_processed_time = datetime.now(tz=timezone.utc).isoformat()
        self.last_seen_event_time = self.__get_event_time(event)

    def end(self, event: EventEnvelope, success: bool) -> None:
        """
        Update the stats based on the event.
        """

        if success:
            self.last_seen_event_time_success = (
                self.__get_event_time(event) or self.last_seen_event_time_success
            )
            self.last_event_processed_time_success = datetime.now(
                timezone.utc
            ).isoformat()
        else:
            self.last_seen_event_time_failure = (
                self.__get_event_time(event) or self.last_seen_event_time_failure
            )
            self.last_event_processed_time_failure = datetime.now(
                timezone.utc
            ).isoformat()

    def __str__(self) -> str:
        return json.dumps(self.dict(), indent=2)
