from typing import List

import fastapi
from datahub.metadata.schema_classes import (
    NotificationRecipientTypeClass,
    NotificationRequestClass,
    NotificationTemplateTypeClass,
)

from datahub_integrations.notifications.constants import (
    EMAIL_SINK_ENABLED,
    NOTIFICATIONS_ENABLED,
    SLACK_SINK_ENABLED,
    TEAMS_SINK_ENABLED,
)
from datahub_integrations.notifications.notification_tracking import (
    NotificationChannel,
    NotificationType,
)
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
from datahub_integrations.notifications.utils import build_assertion_notification_id
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
    template = notification_request.message.template
    params = notification_request.message.parameters or {}

    if template == NotificationTemplateTypeClass.BROADCAST_ASSERTION_STATUS_CHANGE:
        assertion_urn = params.get("assertionUrn")
        if not assertion_urn:
            return
        run_id = params.get("assertionRunId")
        ts_millis = params.get("assertionRunTimestampMillis")
        notification_id = build_assertion_notification_id(
            assertion_urn=assertion_urn,
            assertion_run_timestamp_millis=ts_millis,
            assertion_run_id=run_id,
        )
        notification_type = NotificationType.ASSERTION
    elif template in (
        NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT,
        NotificationTemplateTypeClass.BROADCAST_INCIDENT_STATUS_CHANGE,
    ):
        incident_urn = params.get("incidentUrn")
        if not incident_urn:
            return
        notification_id = incident_urn
        notification_type = NotificationType.INCIDENT
    else:
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
                notificationType=notification_type,
                notificationChannel=channel,
                notificationId=notification_id,
                recipientCount=count,
            )
        )
