from datahub.cli.env_utils import get_boolean_env_variable

from datahub_integrations.app import EXTERNAL_STATIC_PATH

ACRYL_TEAMS_ICON_URL = f"{EXTERNAL_STATIC_PATH}/acryl-teams-icon.png"

# Control whether the @datahub mention is enabled
DATAHUB_TEAMS_AT_MENTION_ENABLED = get_boolean_env_variable(
    "DATAHUB_TEAMS_AT_MENTION_ENABLED", default=False
)

# Teams message card color
ACRYL_TEAMS_COLOR = "#142f39"

# Teams message length limits
MESSAGE_LENGTH_HARD_LIMIT = 4000
MESSAGE_LENGTH_SOFT_LIMIT = 2000
