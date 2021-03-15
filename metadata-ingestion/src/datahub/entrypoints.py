import logging
import pathlib
import sys

import click
from pydantic import ValidationError

from datahub.configuration.common import ConfigurationError, ConfigurationMechanism
from datahub.configuration.toml import TomlConfigurationMechanism
from datahub.configuration.yaml import YamlConfigurationMechanism
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.mce_file import check_mce_file
from datahub.ingestion.source.source_registry import source_registry

logger = logging.getLogger(__name__)

# Set to debug on the root logger.
logging.getLogger(None).setLevel(logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.WARN)
logging.getLogger("botocore").setLevel(logging.INFO)

# Configure logger.
BASE_LOGGING_FORMAT = (
    "[%(asctime)s] %(levelname)-8s {%(name)s:%(lineno)d} - %(message)s"
)
logging.basicConfig(level=logging.DEBUG, format=BASE_LOGGING_FORMAT)

DEFAULT_CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group()
def datahub():
    pass


@datahub.command(context_settings=DEFAULT_CONTEXT_SETTINGS)
@click.option(
    "-c",
    "--config",
    type=click.Path(exists=True, dir_okay=False),
    help="Config file in .toml or .yaml format",
    required=True,
)
def ingest(config: str):
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
        logger.debug(f"Using config: {pipeline_config}")
        pipeline = Pipeline.create(pipeline_config)
    except ValidationError as e:
        click.echo(e, err=True)
        sys.exit(1)

    pipeline.run()
    ret = pipeline.pretty_print_summary()
    sys.exit(ret)


@datahub.command(context_settings=DEFAULT_CONTEXT_SETTINGS)
def ingest_list_plugins():
    """List enabled ingestion plugins"""

    click.secho("Sources:", bold=True)
    click.echo(str(source_registry))
    click.echo()
    click.secho("Sinks:", bold=True)
    click.echo(str(sink_registry))
    click.echo()
    click.echo('If a plugin is disabled, try running: pip install ".[<plugin>]"')


@datahub.group()
def check():
    pass


@check.command()
@click.argument("json-file", type=click.Path(exists=True, dir_okay=False))
def mce_file(json_file: str):
    """Check the schema of a MCE JSON file"""

    report = check_mce_file(json_file)
    click.echo(report)
