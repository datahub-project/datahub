import pathlib

from loguru import logger

from datahub_integrations.slack.config import slack_config

slack_details = pathlib.Path("slack_details.json")

if __name__ == "__main__":
    config = slack_config.get_config()

    logger.debug(config.model_dump_json(indent=2))

    slack_details.write_text(config.model_dump_json(indent=2))
    logger.info(f"Wrote config to {slack_details}")
