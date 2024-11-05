import os

# The amount of time between sleeps and checks on remote actions.
# Defaults to 5 minutes.
MAX_REMOTE_ACTION_REFRESH_INTERVAL_SECONDS = int(
    os.environ.get("MAX_REMOTE_ACTION_REFRESH_INTERVAL_SECONDS", "300")
)

# Default executor id for the automations.
DEFAULT_EXECUTOR_ID = "default"
