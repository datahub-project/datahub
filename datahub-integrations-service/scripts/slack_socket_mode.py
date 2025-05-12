import json
import logging
import os
import pathlib
import sys

from slack_bolt.adapter.socket_mode import SocketModeHandler

from datahub_integrations.slack.config import SlackConnection
from datahub_integrations.slack.slack import get_slack_app

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # For development - using the slack websocket API.
    if len(sys.argv) > 1:
        logger.info(f"Reading config from {sys.argv[1]}")
        slack_details = pathlib.Path(sys.argv[1])
        config = SlackConnection.parse_obj(json.loads(slack_details.read_text()))
    else:
        logger.info("No config file provided, using default config")
        from datahub_integrations.slack.config import slack_config

        config = slack_config.get_config()

    app = get_slack_app(config)

    logger.info(app.client.team_info()["team"])

    APP_LEVEL_TOKEN = os.environ["APP_LEVEL_TOKEN"]
    SocketModeHandler(app, APP_LEVEL_TOKEN).start()
