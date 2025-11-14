import logging
import time
from typing import List, Optional

import requests
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from pydantic import BaseModel, Field
from requests.exceptions import ConnectionError, HTTPError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from datahub_integrations.sources.remote_actions.events.resource_offsets_store import (
    ResourceBasedOffsetsStore,
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
        force_full_refresh: Optional[bool] = False,
    ):
        self.base_url = f"{graph.config.server}/openapi"
        self.default_lookback_days = lookback_days
        self.offset_id = offset_id
        self.offsets_store: Optional[ResourceBasedOffsetsStore] = None
        self.graph = graph
        if consumer_id is not None:
            # Case 1: Provided a consumer id to load offsets for
            self.consumer_id = consumer_id
            self.offsets_store = ResourceBasedOffsetsStore(
                graph=graph, consumer_id=consumer_id
            )
            if not force_full_refresh:
                self.offset_id = self.offsets_store.load_offset_id()
            logger.info(
                f"Starting DataHub Events Consumer with id {consumer_id} at offset id {self.offset_id}"
            )

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

        Args:
            topic (str): The topic to read events for.
            offset_id (str, optional): The offset to start reading the topic
            from. If not provided, and the consumer has a previous offset
            stored, it will use it
            limit (int, optional): The max number of events to read. Defaults to 100 events.
            poll_timeout_seconds (int, optional): The maximum time to wait for new events. Defaults to 10 seconds.

        Returns:
            ExternalEventsResponse: A Pydantic model containing the response data.
        """

        # TODO: Push the following into AcrylDataHubGraph, or DataHubGraph if migrated back to DataHub.
        endpoint = f"{self.base_url}/v1/events/poll"
        resolved_offset_id = offset_id or self.offset_id
        params = {
            "topic": topic,
            "offsetId": resolved_offset_id,
            "limit": limit,
            "pollTimeoutSeconds": poll_timeout_seconds,
            "lookbackWindowDays": self.default_lookback_days,
        }

        # Important: We need to add headers for authentication from the base graph object.
        headers = {**self.graph._session.headers}

        # Remove None values from params
        params = {k: v for k, v in params.items() if v is not None}

        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        external_events_response = ExternalEventsResponse.model_validate(
            response.json()
        )
        self.offset_id = external_events_response.offsetId
        return external_events_response

    def get_events(self, response: ExternalEventsResponse) -> List[ExternalEvent]:
        """
        Extract events from the response data.

        Args:
            response (ExternalEventsResponse): The response data from poll_events.

        Returns:
            List[ExternalEvent]: A list of events.
        """
        return response.events

    def commit_offsets(self, offset_id: Optional[str] = None) -> None:
        """
        Commit the current offset id or a passed in offset_id.

        Args:
            offset_id (str, optional): The offset id to commit. If not provided, the consumer's current offset id will be used.
        """
        if self.offsets_store is not None:
            store_offset_id = offset_id or self.offset_id
            if store_offset_id is not None:
                self.offsets_store.store_offset_id(store_offset_id)

    def close(self) -> None:
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
