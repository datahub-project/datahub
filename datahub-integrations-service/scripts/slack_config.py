import pathlib
from typing import Optional

import typer
from loguru import logger

from datahub_integrations.slack.config import SlackConnection, slack_config

app = typer.Typer()


@app.command()
def put(
    slack_details: pathlib.Path = typer.Argument(
        ..., help="Path to slack_details.json"
    ),
) -> None:
    logger.info(f"Reading config from {slack_details}")
    config = SlackConnection.model_validate_json(slack_details.read_text())
    logger.debug(config.model_dump_json(indent=2))

    slack_config.save_config(config)


@app.command()
def get(
    output: Optional[pathlib.Path] = typer.Option(None, help="Path to output file"),
) -> None:
    config = slack_config.get_connection()
    if output:
        output.write_text(config.model_dump_json(indent=2))
        logger.info(f"Wrote config to {output}")
    else:
        typer.echo(config.model_dump_json(indent=2))


@app.command()
def delete() -> None:
    config = SlackConnection()
    slack_config.save_config(config)


if __name__ == "__main__":
    app()
