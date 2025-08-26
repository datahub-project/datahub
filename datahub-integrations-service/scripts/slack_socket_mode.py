import os
import pathlib

import typer
from loguru import logger
from slack_bolt.adapter.socket_mode import SocketModeHandler

from datahub_integrations.slack.config import SlackConnection, SlackGlobalSettings
from datahub_integrations.slack.slack import get_slack_app


def main(
    config_path: str = typer.Option(
        None, "--config", "-c", help="Path to the Slack connection config file"
    ),
):
    # For development - use the slack websocket API to receive events.

    if config_path:
        logger.info(f"Reading config from {config_path}")
        slack_details = pathlib.Path(config_path)
        connection = SlackConnection.model_validate_json(slack_details.read_text())
    else:
        logger.info("No config file provided, using default config")
        from datahub_integrations.slack.config import slack_config

        connection = slack_config.get_connection()

    app = get_slack_app(
        connection,
        # In development, we always want to enable the @datahub mention.
        SlackGlobalSettings(datahub_at_mention_enabled=True),
    )

    logger.info(app.client.team_info()["team"])

    APP_LEVEL_TOKEN = os.environ["APP_LEVEL_TOKEN"]
    SocketModeHandler(app, APP_LEVEL_TOKEN).start()


if __name__ == "__main__":
    typer.run(main)
