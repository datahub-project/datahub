import functools
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from datahub.telemetry.telemetry import TIMEOUT, _default_telemetry_properties
from mixpanel import Consumer, Mixpanel
from pydantic import BaseModel, Field

from datahub_integrations import __version__
from datahub_integrations.app import graph
from datahub_integrations.slack.utils.datahub_user import graph_as_user

logger = logging.getLogger(__name__)

# Note that this is different from OSS Mixpanel's token. This one
# corresponds to the SaaS Mixpanel project.
MIXPANEL_TOKEN = "7cee38380de7a8469069c040a1fee320"

# Environment variable to control whether to send events directly to Mixpanel
# Default is False - integrations service should depend on GMS to write to Mixpanel
SEND_MIXPANEL_EVENTS_ENV = "DATAHUB_INTEGRATIONS_SEND_MIXPANEL_EVENTS"

# Check the environment variable once at module load time
# Default is False - only send to Mixpanel if explicitly enabled
SEND_MIXPANEL_EVENTS = os.environ.get(SEND_MIXPANEL_EVENTS_ENV, "").lower() in (
    "true",
    "1",
    "yes",
)

telemetry_client = Mixpanel(
    MIXPANEL_TOKEN,
    consumer=Consumer(request_timeout=int(TIMEOUT)),
)


@functools.cache
def _get_server_id() -> str:
    graph.test_connection()
    return graph.server_id


@functools.cache
def _get_origin() -> str:
    return graph.frontend_base_url


def _default_properties() -> dict:
    return {
        **_default_telemetry_properties(),
        "datahub_integrations_version": __version__,
    }


class BaseEvent(BaseModel):
    """Base class for all telemetry events."""

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of when the event occurred",
    )
    type: str

    user_urn: Optional[str] = None


def _send_to_api(event: BaseEvent) -> None:
    """Send the event to the DataHub tracking API."""

    event_actor = event.user_urn or "urn:li:corpuser:admin"
    # This helps impersonate the user when sending the event to the tracking API
    impersonated_graph = graph_as_user(event_actor)
    try:
        # Format the event data for the tracking API with a flat structure
        tracking_event = {
            "type": event.type,
            "timestamp": event.timestamp.isoformat(),
            "actorUrn": event_actor,
            "origin": _get_origin(),
            **event.model_dump(
                exclude={"timestamp", "type", "user_urn", "origin"}
            ),  # Include all other fields from the event
        }

        # Get the server URL from the graph client's config and append the tracking endpoint path
        server_url = impersonated_graph.config.server.rstrip("/")
        tracking_url = f"{server_url}/openapi/v1/tracking/track"

        # Use the graph client's session to post the event
        response = impersonated_graph._session.post(tracking_url, json=tracking_event)
        response.raise_for_status()
        logger.debug(
            f"Successfully sent {event.type} event to tracking API as {event_actor}"
        )
    except Exception as e:
        logger.error(f"Failed to send {event.type} event to tracking API: {str(e)}")


def track_saas_event(
    event: BaseEvent,
) -> None:
    """Track a SaaS event using Mixpanel and DataHub API.

    Args:
        event: The event to track. Must be a subclass of BaseEvent.
    """
    # Include the timestamp in ISO format in the properties
    # The TrackingService will handle the conversion to the appropriate format
    # for each destination (Mixpanel and Kafka)
    properties = {
        **_default_properties(),
        **event.model_dump(
            exclude={"timestamp", "origin", "user_urn"}
        ),  # Exclude timestamp, origin, user_urn from event.dict()
        "distinct_id": event.user_urn or _get_server_id(),
        "origin": _get_origin(),
        "timestamp": event.timestamp.isoformat(),  # Include ISO formatted timestamp
    }

    # Send to Mixpanel only if the environment variable is set
    if SEND_MIXPANEL_EVENTS:
        telemetry_client.track(
            event.user_urn or _get_server_id(),
            event.type,
            properties,
        )
        logger.debug("Sent telemetry event to Mixpanel")
    else:
        logger.debug("Skipping Mixpanel telemetry as environment variable is not set")

    # Send to DataHub API
    _send_to_api(event)
