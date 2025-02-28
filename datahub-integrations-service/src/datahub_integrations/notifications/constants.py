import os

# Global on-off switch for all notifications
NOTIFICATIONS_ENABLED = os.environ.get("NOTIFICATIONS_ENABLED", "true")

# Specifically enable or disable email + slack notifications. Disabled since you need an API key by default.
EMAIL_SINK_ENABLED = os.environ.get("EMAIL_SINK_ENABLED", "true")
SLACK_SINK_ENABLED = os.environ.get(
    "SLACK_SINK_ENABLED", "true"
)  # Enable if slack messages are not being sent by GMS or MAE consumer services.

MAX_NOTIFICATION_RETRIES = int(os.environ.get("MAX_NOTIFICATION_RETRIES", 3))

# Email
FROM_EMAIL_ADDRESS = "notifications@app.acryl.io"
FROM_EMAIL_TITLE = "DataHub Cloud"

# SendGrid API Key - Generate a key from the sendgrid dashboard for local usage.
SEND_GRID_API_KEY = os.environ.get("SENDGRID_API_KEY")

# SendGrid Templates - view and manage in sendgrid account
ENTITY_CHANGE_SUBSCRIPTION_TEMPLATE = "d-07f940f138dd44b3bdd8e6d3932a587b"
GLOBAL_CHANGE_NOTIFICATION_TEMPLATE = "d-20688003d5fd40ab94db0ca2e77bf858"
INGESTION_TEMPLATE = "d-e4af926a33e24b3394ec81e079d012f2"
CUSTOM_TEMPLATE = "d-d19885c33b0643f3b1cbef31be08d869"

# SendGrid Subscription Groups
GLOBAL_NOTIFICATIONS_UNSUBSCRIBE_GROUP_ID = 26417

# DataHub Public Base URL
DATAHUB_BASE_URL = os.environ.get("DATAHUB_BASE_URL", "http://localhost:9002")

# Run id used for edits outside of ingestion.
NON_INGESTION_RUN_ID = "no-run-id-provided"

# Default recipient name when one cannot be resolved. (Hi there)
DEFAULT_RECIPIENT_NAME = "there"

# Whether advanced incident actions should be added for incident messages (Priority, stage)
INCIDENT_ADVANCED_ACTIONS_ENABLED = os.environ.get(
    "INCIDENT_ADVANCED_ACTIONS_ENABLED", "false"
)

DATAHUB_SYSTEM_ACTOR = "urn:li:corpuser:__datahub_system"

MAX_ACTOR_TAGS = 10

INCIDENT_STATUS_RESOLVED = "RESOLVED"
INCIDENT_STATUS_ACTIVE = "ACTIVE"

ACTIVE_INCIDENT_COLOR = "#F5222D"  # RED
RESOLVED_INCIDENT_COLOR = "#52C41A"  # GREEN

# TODO: Whether we have enabled sharing and updating message ids for incidents on Slack
STATEFUL_SLACK_INCIDENT_MESSAGES_ENABLED = os.environ.get(
    "STATEFUL_SLACK_INCIDENT_MESSAGES_ENABLED", "false"
)
