from typing import List

import fastapi
from datahub.metadata.schema_classes import NotificationRequestClass

from datahub_integrations.notifications.constants import (
    EMAIL_SINK_ENABLED,
    NOTIFICATIONS_ENABLED,
    SLACK_SINK_ENABLED,
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

router = fastapi.APIRouter()

sinks: List[NotificationSink] = []

if EMAIL_SINK_ENABLED == "true":
    sinks.append(EmailNotificationSink())

if SLACK_SINK_ENABLED == "true":
    sinks.append(SlackNotificationSink())

sink_manager = NotificationSinkManager(
    sinks=sinks,
    mode=(
        NotificationManagerMode.ENABLED
        if NOTIFICATIONS_ENABLED == "true"
        else NotificationManagerMode.DISABLED
    ),
)


@router.post("/send")
async def send(request: dict) -> None:
    """Send a notification to a group of recipients to one or more sinks."""
    notification_request = NotificationRequestClass.from_obj(request)
    await sink_manager.handle(notification_request)
