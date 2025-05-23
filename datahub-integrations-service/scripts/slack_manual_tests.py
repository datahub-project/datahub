import json
import pathlib
from typing import Optional

import typer
from loguru import logger

from datahub_integrations.slack.config import SlackConnection
from datahub_integrations.slack.constants import ACRYL_SLACK_ICON_URL
from datahub_integrations.slack.slack import get_slack_app, get_slack_link_preview

app = typer.Typer(help="Slack manual testing utilities")


@app.command(help="Send a test message to a specified Slack channel")
def send_message(
    config_path: Optional[pathlib.Path] = typer.Option(
        None, "--config", help="Path to the Slack connection config file"
    ),
    channel: str = typer.Option(..., help="Slack channel ID to send the message to"),
    message: str = typer.Option(..., help="Message text to send"),
) -> None:
    """Send a test message to a specified Slack channel."""

    if config_path:
        logger.info(f"Reading config from {config_path}")
        slack_details = pathlib.Path(config_path)
        config = SlackConnection.parse_obj(json.loads(slack_details.read_text()))
    else:
        logger.info("No config file provided, using default config")
        from datahub_integrations.slack.config import slack_config

        config = slack_config.get_config()

    app = get_slack_app(config)

    app.client.chat_postMessage(
        channel=channel,
        text=message,
        icon_url=ACRYL_SLACK_ICON_URL,
        mrkdwn=True,
    )
    logger.info(f"Message sent to channel {channel}")


@app.command(help="Get a preview of a Slack message link")
def get_link_preview(
    url: str = typer.Argument(..., help="Slack message URL to get preview for"),
) -> None:
    """Get a preview of a Slack message link."""
    preview = get_slack_link_preview(url)
    logger.info(preview.model_dump_json(indent=2))


if __name__ == "__main__":
    app()
