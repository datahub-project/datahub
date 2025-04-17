import logging
import time
from typing import List, Optional

import requests
from pydantic import BaseModel, Field
from requests.exceptions import ConnectionError, HTTPError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer_offsets_store import (
    DataHubEventsConsumerPlatformResourceOffsetsStore,
)

logger = logging.getLogger(__name__)


class ExternalEvent(BaseModel):
    contentType: str = Field(..., description="The encoding type of the event")
    value: str = Field(..., description="The raw serialized event itself")


class ExternalEventsResponse(BaseModel):
    offsetId: str = Field(..., description="Offset id the stream for scrolling")
    count: int = Field(..., description="Count of the events")
    events: List[ExternalEvent] = Field(..., description="The raw events")


class DataHubEventsConsumer:
    """
    A simple consumer of DataHub Events API.
    Note that this class is not thread safe.
    """

    def __init__(
        self,
        graph: DataHubGraph,
        consumer_id: Optional[str] = None,
        offset_id: Optional[str] = None,
        lookback_days: Optional[int] = None,
        reset_offsets: Optional[bool] = False,
    ):
        # 1) Always set self.consumer_id, even if None, so tests can assert it safely.
        self.consumer_id: Optional[str] = consumer_id

        # Base properties
        self.graph: DataHubGraph = graph
        self.offset_id: Optional[str] = offset_id
        self.default_lookback_days: Optional[int] = lookback_days
        self.offsets_store: Optional[
            DataHubEventsConsumerPlatformResourceOffsetsStore
        ] = None

        # Build the base URL from the graph config
        self.base_url = f"{graph.config.server}/openapi"

        # 2) Create the offsets store only if we have a consumer_id
        if self.consumer_id is not None:
            self.offsets_store = DataHubEventsConsumerPlatformResourceOffsetsStore(
                graph=self.graph,
                consumer_id=self.consumer_id,
            )
            # 3) If you've chosen to reset consumer offsets, we simply do not load the previous.
            # Otherwise, load the offsets.
            if not reset_offsets:
                loaded_offset = self.offsets_store.load_offset_id()
                if loaded_offset is not None:
                    self.offset_id = loaded_offset

            logger.debug(
                f"Starting DataHub Events Consumer with id {self.consumer_id} at offset id {self.offset_id}"
            )
        else:
            logger.debug("Starting DataHub Events Consumer with no consumer ID.")

    @retry(
        retry=retry_if_exception_type((HTTPError, ConnectionError)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def poll_events(
        self,
        topic: str,
        offset_id: Optional[str] = None,
        limit: Optional[int] = None,
        poll_timeout_seconds: Optional[int] = None,
    ) -> ExternalEventsResponse:
        """
        Fetch events for a specific topic.
        """
        endpoint = f"{self.base_url}/v1/events/poll"

        # If the caller provided an offset_id, use it; otherwise fall back to self.offset_id.
        resolved_offset_id = offset_id or self.offset_id

        params = {
            "topic": topic,
            "offsetId": resolved_offset_id,
            "limit": limit,
            "pollTimeoutSeconds": poll_timeout_seconds,
            "lookbackWindowDays": self.default_lookback_days,
        }
        # Remove keys where the value is None
        params = {k: v for k, v in params.items() if v is not None}

        # Pass along the session headers from the graph for authentication.
        headers = dict(self.graph._session.headers)

        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()

        external_events_response = ExternalEventsResponse.parse_obj(response.json())

        # Update our internal offset_id to the newly returned offset
        self.offset_id = external_events_response.offsetId
        return external_events_response

    def get_events(self, response: ExternalEventsResponse) -> List[ExternalEvent]:
        """
        Extract events from the response data.
        """
        return response.events

    def commit_offsets(self, offset_id: Optional[str] = None) -> None:
        """
        Commit the current offset id or a passed-in offset_id.
        """
        if self.offsets_store is not None:
            store_offset_id = offset_id or self.offset_id
            if store_offset_id is not None:
                self.offsets_store.store_offset_id(store_offset_id)

    def close(self) -> None:
        """
        Optional cleanup (currently no-op).
        """
        pass


if __name__ == "__main__":
    with get_default_graph() as graph:
        client = DataHubEventsConsumer(graph=graph, consumer_id="events_consumer_cli")
        if client.offset_id is not None:
            print(f"Starting offset id: {client.offset_id}")

        while True:
            # Example: Poll events for the PlatformEvent_v1 topic
            response = client.poll_events(
                topic="PlatformEvent_v1", limit=10, poll_timeout_seconds=5
            )

            print(f"Offset ID: {response.offsetId}")
            print(f"Event count: {response.count}")

            events = client.get_events(response)
            if len(events) == 0:
                print("No events to process.")
            else:
                for event in events:
                    print(f"Content Type: {event.contentType}")
                    print(f"Value: {event.value}")
                    print("---")
                client.commit_offsets()

            time.sleep(1)
