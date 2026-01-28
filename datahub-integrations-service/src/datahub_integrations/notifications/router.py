from typing import List

import fastapi
from datahub.metadata.schema_classes import (
    NotificationRecipientTypeClass,
    NotificationRequestClass,
)

from datahub_integrations.notifications.constants import (
    EMAIL_SINK_ENABLED,
    NOTIFICATIONS_ENABLED,
    SLACK_SINK_ENABLED,
    TEAMS_SINK_ENABLED,
)
from datahub_integrations.notifications.notification_tracking import NotificationChannel
from datahub_integrations.notifications.sinks.email.email_sink import (
    EmailNotificationSink,
)
from datahub_integrations.notifications.sinks.sink import NotificationSink
from datahub_integrations.notifications.sinks.sink_manager import (
    NotificationManagerMode,
    NotificationSinkManager,
)
from datahub_integrations.notifications.sinks.slack.slack_sink import (
    SlackNotificationSink,
)
from datahub_integrations.notifications.sinks.teams.teams_sink import (
    TeamsNotificationSink,
)
from datahub_integrations.notifications.utils import get_notification_tracking_info
from datahub_integrations.telemetry.notification_events import NotificationSentEvent
from datahub_integrations.telemetry.telemetry import track_saas_event

router = fastapi.APIRouter()

sinks: List[NotificationSink] = []

if EMAIL_SINK_ENABLED:
    sinks.append(EmailNotificationSink())

if SLACK_SINK_ENABLED:
    sinks.append(SlackNotificationSink())

if TEAMS_SINK_ENABLED:
    sinks.append(TeamsNotificationSink())

sink_manager = NotificationSinkManager(
    sinks=sinks,
    mode=(
        NotificationManagerMode.ENABLED
        if NOTIFICATIONS_ENABLED
        else NotificationManagerMode.DISABLED
    ),
)


@router.post("/send")
async def send(request: dict) -> None:
    """Send a notification to a group of recipients to one or more sinks."""
    notification_request = NotificationRequestClass.from_obj(request)
    _maybe_emit_notification_sent_event(notification_request)
    await sink_manager.handle(notification_request)


def _maybe_emit_notification_sent_event(
    notification_request: NotificationRequestClass,
) -> None:
    tracking_info = get_notification_tracking_info(notification_request)
    if not tracking_info:
        return

    # We may deliver to multiple recipient types in a single request (e.g. slack + email).
    # Emit one NotificationSentEvent per channel so CTR denominators are channel-specific.
    counts_by_channel: dict[NotificationChannel, int] = {}
    for recipient in notification_request.recipients:
        recipient_type = recipient.type
        if recipient_type in (
            NotificationRecipientTypeClass.SLACK_DM,
            NotificationRecipientTypeClass.SLACK_CHANNEL,
        ):
            channel = NotificationChannel.SLACK
        elif recipient_type == NotificationRecipientTypeClass.EMAIL:
            channel = NotificationChannel.EMAIL
        elif recipient_type in (
            NotificationRecipientTypeClass.TEAMS_CHANNEL,
            NotificationRecipientTypeClass.TEAMS_DM,
        ):
            channel = NotificationChannel.TEAMS
        else:
            continue
        counts_by_channel[channel] = counts_by_channel.get(channel, 0) + 1

    for channel, count in counts_by_channel.items():
        track_saas_event(
            NotificationSentEvent(
                notificationType=tracking_info.notification_type,
                notificationChannel=channel,
                notificationId=tracking_info.notification_id,
                recipientCount=count,
            )
        )
