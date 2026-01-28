import functools
import hashlib
import os
from datetime import datetime, timezone
from typing import Optional

import sentry_sdk
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.configuration.env_vars import get_sentry_dsn, get_sentry_environment
from datahub.telemetry.telemetry import TIMEOUT, _default_global_properties
from loguru import logger
from mixpanel import Consumer, Mixpanel
from pydantic import BaseModel, Field

from datahub_integrations import __version__
from datahub_integrations.app import graph
from datahub_integrations.slack.utils.datahub_user import graph_as_user

# Note that this is different from OSS Mixpanel's token. This one
# corresponds to the SaaS Mixpanel project.
MIXPANEL_TOKEN = "7cee38380de7a8469069c040a1fee320"

# Environment variable to control whether to send events directly to Mixpanel
# Default is False - integrations service should depend on GMS to write to Mixpanel
SEND_MIXPANEL_EVENTS_ENV = "DATAHUB_INTEGRATIONS_SEND_MIXPANEL_EVENTS"

# Environment variable to control whether to send events directly to Mixpanel
# Default is True - integrations service should always send telemetry events to GMS
# This is used primarily to skip telemetry during ai experiments.
SEND_TELEMETRY_EVENTS_ENV = "DATAHUB_INTEGRATIONS_SEND_TELEMETRY_EVENTS"

# Check the environment variable once at module load time
# Default is False - only send to Mixpanel if explicitly enabled
SEND_MIXPANEL_EVENTS = os.environ.get(SEND_MIXPANEL_EVENTS_ENV, "").lower() in (
    "true",
    "1",
    "yes",
)

SEND_TELEMETRY_EVENTS = get_boolean_env_variable(SEND_TELEMETRY_EVENTS_ENV, True)

# Sentry configuration for notification delivery failures.
SENTRY_DSN = get_sentry_dsn()
SENTRY_ENVIRONMENT = get_sentry_environment()

# Sampling rate for high-volume notification events. Default is 0.1 (10% sampling).
NOTIFICATION_EVENT_SAMPLE_RATE_ENV = (
    "DATAHUB_INTEGRATIONS_NOTIFICATION_EVENT_SAMPLE_RATE"
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


@functools.cache
def _init_sentry() -> bool:
    if not SENTRY_DSN:
        return False
    try:
        sentry_sdk.init(
            dsn=SENTRY_DSN,
            environment=SENTRY_ENVIRONMENT,
            release=__version__,
        )
        return True
    except Exception as exc:
        logger.warning(f"Failed to initialize Sentry: {exc}")
        return False


def report_notification_delivery_failure(
    *,
    notification_type: str,
    notification_channel: str,
    notification_id: str,
    recipient_count: int | None,
    error_type: str | None,
    error_message: str | None,
) -> None:
    if not _init_sentry():
        return
    try:
        with sentry_sdk.push_scope() as scope:
            scope.set_tag("notification_type", notification_type)
            scope.set_tag("notification_channel", notification_channel)
            scope.set_tag("notification_id", notification_id)
            if error_type:
                scope.set_tag("error_type", error_type)
            scope.set_context(
                "notification_delivery_failure",
                {
                    "recipient_count": recipient_count,
                    "error_message": error_message,
                },
            )
            sentry_sdk.capture_message("Notification delivery failure", level="error")
    except Exception as exc:
        logger.warning(f"Failed to report notification failure to Sentry: {exc}")


class BaseEvent(BaseModel):
    """Base class for all telemetry events."""

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of when the event occurred",
    )
    type: str

    user_urn: Optional[str] = None

    datahub_integrations_version: str = __version__


def _send_to_api(event: BaseEvent) -> None:
    """Send the event to the DataHub tracking API."""

    event_actor = event.user_urn or "urn:li:corpuser:admin"
    try:
        # This helps impersonate the user when sending the event to the tracking API
        impersonated_graph = graph_as_user(event_actor)
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
    if not SEND_TELEMETRY_EVENTS:
        logger.debug("Skipping telemetry as environment variable is not set")
        return

    if not _should_sample_event(event):
        logger.debug(f"Skipping telemetry due to sampling: {event.type}")
        return

    # Send to Mixpanel only if the environment variable is set
    if SEND_MIXPANEL_EVENTS:
        mixpanel_properties = {
            **_default_global_properties(),
            **event.model_dump(
                exclude={
                    "timestamp",
                    "user_urn",
                    "full_history",  # Entire conversation history as JSON
                    "reduction_sequence",  # Can be large JSON
                    "tool_input",
                }
            ),
            "distinct_id": event.user_urn or _get_server_id(),
            "origin": _get_origin(),
            "timestamp": event.timestamp.isoformat(),
        }

        try:
            telemetry_client.track(
                event.user_urn or _get_server_id(),
                event.type,
                mixpanel_properties,
            )
            logger.debug("Sent telemetry event to Mixpanel")
        except Exception as e:
            # Log the error but don't let Mixpanel failures affect the main flow
            logger.error(f"Failed to send telemetry event to Mixpanel: {str(e)}")
    else:
        logger.debug("Skipping Mixpanel telemetry as environment variable is not set")

    # Send to DataHub API (this should always work and includes full data)
    _send_to_api(event)


def _should_sample_event(event: BaseEvent) -> bool:
    if event.type not in {
        "NotificationSentEvent",
        "NotificationDeliveredEvent",
    }:
        return True

    raw_rate = os.environ.get(NOTIFICATION_EVENT_SAMPLE_RATE_ENV, "0.1")
    try:
        sample_rate = float(raw_rate)
    except ValueError:
        logger.warning(
            f"Invalid {NOTIFICATION_EVENT_SAMPLE_RATE_ENV} value '{raw_rate}', defaulting to 0.1"
        )
        sample_rate = 0.1

    if sample_rate >= 1.0:
        return True
    if sample_rate <= 0.0:
        return False

    # Deterministic sampling to keep sent/delivered/failure aligned by ID+channel.
    notification_id = getattr(event, "notificationId", None)
    if not notification_id:
        return True
    notification_channel = getattr(event, "notificationChannel", "")
    notification_type = getattr(event, "notificationType", "")
    sample_key = f"{notification_type}|{notification_channel}|{notification_id}"
    digest = hashlib.sha256(sample_key.encode("utf-8")).hexdigest()
    bucket = int(digest[:8], 16) / 0xFFFFFFFF
    return bucket < sample_rate
