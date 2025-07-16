from datahub.cli.env_utils import get_boolean_env_variable

from datahub_integrations.app import EXTERNAL_STATIC_PATH

DATAHUB_SLACK_ICON_URL = f"{EXTERNAL_STATIC_PATH}/datahub-slack-icon.png"

# Control whether the @datahub mention is enabled
DATAHUB_SLACK_AT_MENTION_ENABLED = get_boolean_env_variable(
    "DATAHUB_SLACK_AT_MENTION_ENABLED", default=False
)
