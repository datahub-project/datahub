from enum import Enum

NOTIFICATION_QUERY_PARAM_TYPE = "notification_type"
NOTIFICATION_QUERY_PARAM_ID = "notification_id"
NOTIFICATION_QUERY_PARAM_CHANNEL = "notification_channel"


class NotificationChannel(str, Enum):
    SLACK = "slack"
    EMAIL = "email"
    TEAMS = "teams"


class NotificationType(str, Enum):
    ASSERTION = "assertion"
    INCIDENT = "incident"
