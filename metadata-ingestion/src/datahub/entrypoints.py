import logging
import os
import pathlib
import sys

import click
from pydantic import ValidationError

from datahub.check.check_cli import check
from datahub.configuration.common import ConfigurationError, ConfigurationMechanism
from datahub.configuration.toml import TomlConfigurationMechanism
from datahub.configuration.yaml import YamlConfigurationMechanism
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry

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
    if not config_file.is_file():
        raise ConfigurationError(f"Cannot open config file {config}")

    config_mech: ConfigurationMechanism
    if config_file.suffix in [".yaml", ".yml"]:
        config_mech = YamlConfigurationMechanism()
    elif config_file.suffix == ".toml":
        config_mech = TomlConfigurationMechanism()
    else:
        raise ConfigurationError(
            "Only .toml and .yml are supported. Cannot process file type {}".format(
                config_file.suffix
            )
        )

    with config_file.open() as fp:
        pipeline_config = config_mech.load_config(fp)

    try:
        logger.info(f"Using config: {pipeline_config}")
        pipeline = Pipeline.create(pipeline_config)
    except ValidationError as e:
        click.echo(e, err=True)
        sys.exit(1)

    pipeline.run()
    ret = pipeline.pretty_print_summary()
    sys.exit(ret)


@datahub.command()
def ingest_list_plugins() -> None:
    """List enabled ingestion plugins"""

    click.secho("Sources:", bold=True)
    click.echo(str(source_registry))
    click.echo()
    click.secho("Sinks:", bold=True)
    click.echo(str(sink_registry))
    click.echo()
    click.echo('If a plugin is disabled, try running: pip install ".[<plugin>]"')


datahub.add_command(check)
