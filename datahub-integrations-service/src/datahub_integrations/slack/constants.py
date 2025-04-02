from datahub_integrations.app import EXTERNAL_STATIC_PATH
from datahub_integrations.chat.chat_session import MESSAGE_LENGTH_SOFT_LIMIT

ACRYL_SLACK_ICON_URL = f"{EXTERNAL_STATIC_PATH}/acryl-slack-icon.png"

# The soft limit is passed to the LLM's prompt, in an effort
# to have it be concise. The hard limit is a fallback to ensure
# that we don't send way too much to Slack.
MESSAGE_LENGTH_HARD_LIMIT = 4000
assert MESSAGE_LENGTH_HARD_LIMIT >= 1.5 * MESSAGE_LENGTH_SOFT_LIMIT
