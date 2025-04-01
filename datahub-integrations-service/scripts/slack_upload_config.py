import json
import pathlib

from loguru import logger

from datahub_integrations.slack.config import SlackConnection, slack_config

slack_details = pathlib.Path("slack_details.json")

if __name__ == "__main__":
    logger.info(f"Reading config from {slack_details}")
    config = SlackConnection.parse_obj(json.loads(slack_details.read_text()))

    logger.debug(config.json(indent=2))
    slack_config.save_config(config)
