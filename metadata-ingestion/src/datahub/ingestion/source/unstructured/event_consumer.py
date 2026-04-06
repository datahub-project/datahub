"""Event consumer for document chunking using DataHub Events API."""

import json
import logging
import time
from typing import TYPE_CHECKING, Any, Iterator, Optional

from datahub.ingestion.graph.client import DataHubGraph

if TYPE_CHECKING:
    from datahub.ingestion.source.datahub_documents.document_chunking_state_handler import (
        DocumentChunkingStateHandler,
    )

logger = logging.getLogger(__name__)


class DocumentEventConsumer:
    """Consumer for Document MCL events from DataHub Events API.

    This consumer:
    1. Polls events from DataHub's /openapi/v1/events/poll endpoint
    2. Filters for Document entity MCL events
    3. Tracks offsets via stateful ingestion (if state_handler provided) or Platform Resources (fallback)
    4. Exits after idle timeout (incremental batch mode)
    """

    def __init__(
        self,
        graph: DataHubGraph,
        consumer_id: str,
        topics: list[str],
        lookback_days: Optional[int] = None,
        reset_offsets: bool = False,
        idle_timeout_seconds: int = 30,
        poll_timeout_seconds: int = 2,
        poll_limit: int = 100,
        state_handler: Optional["DocumentChunkingStateHandler"] = None,
    ):
        self.graph = graph
        self.consumer_id = consumer_id
        self.topics = topics
        self.lookback_days = lookback_days
        self.reset_offsets = reset_offsets
        self.idle_timeout_seconds = idle_timeout_seconds
        self.poll_timeout_seconds = poll_timeout_seconds
        self.poll_limit = poll_limit
        self.state_handler = state_handler

        # Base URL for events API
        self.base_url = f"{graph.config.server}/openapi"

        # Track offsets per topic
        self.offset_ids: dict[str, Optional[str]] = {}
        for topic in topics:
            self.offset_ids[topic] = self._load_offset(topic)

        logger.info(
            f"Initialized DocumentEventConsumer {consumer_id} with offsets: {self.offset_ids}"
        )

    def _get_offset_urn(self, topic: str) -> str:
        """Get the Platform Resource URN for storing this topic's offset."""
        # Use Platform Resources to store offsets, similar to datahub-actions
        resource_id = f"{self.consumer_id}-{topic}"
        return f"urn:li:dataHubPlatformResource:{resource_id}"

    def _load_offset(self, topic: str) -> Optional[str]:
        """Load the last committed offset for a topic from stateful ingestion or Platform Resources."""
        if self.reset_offsets:
            logger.info(f"Resetting offsets for topic {topic}")
            return None

        # Try stateful ingestion first (preferred method)
        if self.state_handler:
            try:
                offset_id = self.state_handler.get_event_offset(topic)
                if offset_id:
                    logger.info(
                        f"Loaded offset for topic {topic} from stateful ingestion: {offset_id}"
                    )
                    return offset_id
            except Exception as e:
                logger.debug(
                    f"Failed to load offset from stateful ingestion for topic {topic}: {e}"
                )

        # Fallback to Platform Resources (legacy method)
        try:
            urn = self._get_offset_urn(topic)
            # Use GraphQL to query custom properties
            query = """
            query getPlatformResource($urn: String!) {
                entity(urn: $urn) {
                    ... on DataHubPlatformResource {
                        properties {
                            customProperties {
                                key
                                value
                            }
                        }
                    }
                }
            }
            """
            response = self.graph.execute_graphql(query, {"urn": urn})

            entity = response.get("entity")
            if entity and "properties" in entity:
                custom_props = entity["properties"].get("customProperties", [])
                for prop in custom_props:
                    if prop["key"] == "offset_id":
                        offset_id = prop["value"]
                        logger.info(
                            f"Loaded offset for topic {topic} from Platform Resources: {offset_id}"
                        )
                        return offset_id
        except Exception as e:
            logger.debug(f"No existing offset found for topic {topic}: {e}")

        return None

    def _save_offset(self, topic: str, offset_id: str) -> None:
        """Save the current offset for a topic via stateful ingestion or Platform Resources."""
        # Use stateful ingestion if available (preferred method)
        if self.state_handler:
            try:
                self.state_handler.update_event_offset(topic, offset_id)
                logger.debug(
                    f"Saved offset for topic {topic} via stateful ingestion: {offset_id}"
                )
                return
            except Exception as e:
                logger.warning(
                    f"Failed to save offset via stateful ingestion for topic {topic}: {e}"
                )
                # Fall through to Platform Resources fallback

        # Fallback to Platform Resources (legacy method - not fully implemented)
        try:
            logger.debug(
                f"Offset tracking: topic={topic}, offset_id={offset_id} (Platform Resources not implemented, using stateful ingestion)"
            )
        except Exception as e:
            logger.error(f"Failed to save offset for topic {topic}: {e}")

    def poll_events(self, topic: str) -> list[dict[str, Any]]:
        """Poll events for a specific topic."""
        endpoint = f"{self.base_url}/v1/events/poll"

        params: dict[str, Any] = {
            "topic": topic,
            "limit": self.poll_limit,
            "pollTimeoutSeconds": self.poll_timeout_seconds,
        }

        # Add offset if we have one (only if non-empty)
        offset_id = self.offset_ids.get(topic)
        if offset_id:
            params["offsetId"] = offset_id

        # Add lookback window if specified
        if self.lookback_days:
            params["lookbackWindowDays"] = self.lookback_days

        # Remove None and empty string values to avoid sending empty offsetId
        params = {k: v for k, v in params.items() if v is not None and v != ""}

        try:
            import requests

            headers = dict(self.graph._session.headers)
            response = requests.get(
                endpoint, params=params, headers=headers, timeout=30
            )
            response.raise_for_status()

            data = response.json()
            events = data.get("events", [])
            new_offset_id = data.get("offsetId")

            # Update our tracked offset
            if new_offset_id:
                self.offset_ids[topic] = new_offset_id

            logger.debug(
                f"Polled {len(events)} events from topic {topic}, new offset: {new_offset_id}"
            )

            return events
        except Exception as e:
            # Log the error but re-raise it so the source can detect and fall back to batch mode
            logger.error(
                f"Failed to poll events from topic {topic}: {e}", exc_info=True
            )
            # Re-raise to allow source to handle fallback
            raise

    def get_current_offset(self, topic: str) -> Optional[str]:
        """Get the current offset for a topic by polling with no offset (seeks to end).

        This is used for bootstrapping: BEFORE batch mode processes all existing documents,
        we capture the current offset to prevent race conditions. Batch mode processes
        everything up to this offset, and subsequent runs continue from this offset.

        The Events API will seek to end when no offsetId and no lookbackWindowDays are provided,
        and return the current position as offsetId in the response.

        Args:
            topic: Topic to get current offset for

        Returns:
            Current offset ID as string, or None if polling fails
        """
        endpoint = f"{self.base_url}/v1/events/poll"

        # Poll with NO offsetId and NO lookbackWindowDays
        # This causes the Events API to seek to end and return current position
        params: dict[str, Any] = {
            "topic": topic,
            "limit": 1,  # Minimal limit - we just want the offset
            "pollTimeoutSeconds": self.poll_timeout_seconds,
            # Explicitly NOT including offsetId
            # Explicitly NOT including lookbackWindowDays
        }

        try:
            import requests

            headers = dict(self.graph._session.headers)
            response = requests.get(
                endpoint, params=params, headers=headers, timeout=30
            )
            response.raise_for_status()

            data = response.json()
            offset_id = data.get("offsetId")

            if offset_id:
                logger.info(f"Retrieved current offset for topic {topic}: {offset_id}")
                return offset_id
            else:
                logger.warning(
                    f"No offsetId in response when polling topic {topic} for current offset"
                )
                return None

        except Exception as e:
            logger.warning(
                f"Failed to get current offset for topic {topic}: {e}",
                exc_info=True,
            )
            return None

    def consume_events(self) -> Iterator[dict[str, Any]]:
        """Consume events in incremental batch mode.

        Yields MCL events for Document entities with unstructured_elements.
        Exits after idle_timeout_seconds with no new events.
        """
        last_event_time = time.time()
        total_events_processed = 0

        while True:
            # Poll all topics
            batch_events = []
            try:
                for topic in self.topics:
                    events = self.poll_events(topic)
                    for event in events:
                        batch_events.append((topic, event))
            except Exception as e:
                # Re-raise polling errors so source can detect and fall back to batch mode
                logger.error(f"Failed to poll events from topics: {e}", exc_info=True)
                raise

            if not batch_events:
                # Check if we should exit due to idle timeout
                idle_duration = time.time() - last_event_time
                if idle_duration > self.idle_timeout_seconds:
                    logger.info(
                        f"Exiting after {idle_duration:.1f}s idle time (processed {total_events_processed} events)"
                    )
                    break

                # Sleep briefly before next poll
                time.sleep(1)
                continue

            # Process and yield events
            # Only reset idle timer when we actually yield document events
            for _topic, event in batch_events:
                # Parse the event
                try:
                    content_type = event.get("contentType", "application/json")
                    value_str = event.get("value", "{}")

                    if content_type == "application/json":
                        mcl_dict = json.loads(value_str)
                    else:
                        logger.warning(f"Unsupported content type: {content_type}")
                        continue

                    # Filter for Document entities
                    entity_type = mcl_dict.get("entityType")
                    if entity_type != "document":
                        continue

                    # Check if it's a documentInfo aspect change
                    # aspectName comes wrapped in a dict: {"string": "documentInfo"}
                    aspect_name_raw = mcl_dict.get("aspectName")
                    if not aspect_name_raw:
                        continue
                    aspect_name = (
                        aspect_name_raw.get("string")
                        if isinstance(aspect_name_raw, dict)
                        else aspect_name_raw
                    )
                    if aspect_name != "documentInfo":
                        continue

                    # Reset idle timer only when yielding a document event
                    last_event_time = time.time()

                    # Log the MCL structure for debugging
                    entity_urn = mcl_dict.get("entityUrn", "unknown")
                    aspect_keys = (
                        list(mcl_dict.get("aspect", {}).keys())
                        if isinstance(mcl_dict.get("aspect"), dict)
                        else "not-a-dict"
                    )
                    logger.info(
                        f"Yielding document MCL event: urn={entity_urn}, aspect type={type(mcl_dict.get('aspect'))}, aspect_keys={aspect_keys}"
                    )

                    # Yield the MCL event
                    yield mcl_dict
                    total_events_processed += 1

                except Exception as e:
                    logger.error(f"Failed to process event: {e}", exc_info=True)
                    continue

        # Commit all offsets before exiting
        for topic in self.topics:
            offset_id = self.offset_ids.get(topic)
            if offset_id:
                self._save_offset(topic, offset_id)

        logger.info(
            f"Event consumption complete. Total events processed: {total_events_processed}"
        )

    def close(self) -> None:
        """Close the consumer and commit offsets."""
        for topic in self.topics:
            offset_id = self.offset_ids.get(topic)
            if offset_id:
                self._save_offset(topic, offset_id)
