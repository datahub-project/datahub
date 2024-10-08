import os

# The amount of time between sleeps and checks on remote actions.
# Defaults to 30 minutes.
MAX_REMOTE_ACTION_REFRESH_INTERVAL_SECONDS = int(
    os.environ.get("MAX_REMOTE_ACTION_REFRESH_INTERVAL_SECONDS", "1800")
)

# Default executor id for the automations.
DEFAULT_EXECUTOR_ID = "default"
