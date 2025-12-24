from enum import Enum

from pydantic import Field

from datahub_integrations.notifications.notification_tracking import (
    NotificationChannel,
    NotificationType,
)
from datahub_integrations.telemetry.telemetry import BaseEvent


class NotificationSlackAction(str, Enum):
    RESOLVE = "resolve"
    REOPEN = "reopen"


class NotificationSentEvent(BaseEvent):
    """Emitted when a notification is sent (denominator for CTR)."""

    type: str = Field(default="NotificationSentEvent")
    notificationType: NotificationType
    notificationChannel: NotificationChannel
    notificationId: str
    recipientCount: int | None = None


class NotificationSlackActionEvent(BaseEvent):
    """Emitted when a user engages with a notification directly in Slack (e.g. resolve/reopen)."""

    type: str = Field(default="NotificationSlackActionEvent")
    action: NotificationSlackAction
    success: bool
    notificationType: NotificationType
    notificationChannel: NotificationChannel
    notificationId: str
