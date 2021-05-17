import logging
import os
import pathlib
import sys

import click
from pydantic import ValidationError

import datahub as datahub_package
from datahub.check.check_cli import check
from datahub.configuration.config_loader import load_config_file
from datahub.ingestion.run.pipeline import Pipeline

logger = logging.getLogger(__name__)

# Configure some loggers.
logging.getLogger("urllib3").setLevel(logging.WARN)
# logging.getLogger("botocore").setLevel(logging.INFO)
# logging.getLogger("google").setLevel(logging.INFO)

# Configure logger.
BASE_LOGGING_FORMAT = (
    "[%(asctime)s] %(levelname)-8s {%(name)s:%(lineno)d} - %(message)s"
)
logging.basicConfig(format=BASE_LOGGING_FORMAT)


@click.group()
@click.option("--debug/--no-debug", default=False)
@click.version_option(
    version=datahub_package.nice_version_name(),
    prog_name=datahub_package.__package_name__,
)
def datahub(debug: bool) -> None:
    if debug or os.getenv("DATAHUB_DEBUG", False):
        logging.getLogger().setLevel(logging.INFO)
        logging.getLogger("datahub").setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.WARNING)
        logging.getLogger("datahub").setLevel(logging.INFO)
    # loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    # print(loggers)
    # breakpoint()


@datahub.command()
def version() -> None:
    """Print version number and exit"""
    click.echo(f"DataHub CLI version: {datahub_package.nice_version_name()}")
    click.echo(f"Python version: {sys.version}")


@datahub.command()
@click.option(
    "-c",
    "--config",
    type=click.Path(exists=True, dir_okay=False),
    help="Config file in .toml or .yaml format",
    required=True,
)
def ingest(config: str) -> None:
    """Main command for ingesting metadata into DataHub"""

    config_file = pathlib.Path(config)
    pipeline_config = load_config_file(config_file)

    try:
        logger.info(f"Using config: {pipeline_config}")
        pipeline = Pipeline.create(pipeline_config)
    except ValidationError as e:
        click.echo(e, err=True)
        sys.exit(1)

    pipeline.run()
    ret = pipeline.pretty_print_summary()
    sys.exit(ret)


datahub.add_command(check)


def main(**kwargs):
    # This wrapper prevents click from suppressing errors.
    try:
        sys.exit(datahub(standalone_mode=False, **kwargs))
    except click.ClickException as error:
        error.show()
        sys.exit(1)
