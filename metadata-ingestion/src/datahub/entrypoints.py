import logging
import os
import sys

import click
import stackprinter

import datahub as datahub_package
from datahub.cli.check_cli import check
from datahub.cli.cli_utils import DATAHUB_CONFIG_PATH, write_datahub_config
from datahub.cli.delete_cli import delete
from datahub.cli.docker import docker
from datahub.cli.get_cli import get
from datahub.cli.ingest_cli import ingest
from datahub.cli.put_cli import put
from datahub.cli.telemetry import telemetry as telemetry_cli
from datahub.telemetry import telemetry

logger = logging.getLogger(__name__)

# Configure some loggers.
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("snowflake").setLevel(level=logging.WARNING)
# logging.getLogger("botocore").setLevel(logging.INFO)
# logging.getLogger("google").setLevel(logging.INFO)

# Configure logger.
BASE_LOGGING_FORMAT = (
    "[%(asctime)s] %(levelname)-8s {%(name)s:%(lineno)d} - %(message)s"
)
logging.basicConfig(format=BASE_LOGGING_FORMAT)

MAX_CONTENT_WIDTH = 120


@click.group(
    context_settings=dict(
        # Avoid truncation of help text.
        # See https://github.com/pallets/click/issues/486.
        max_content_width=MAX_CONTENT_WIDTH,
    )
)
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


@datahub.command()
@telemetry.with_telemetry
def version() -> None:
    """Print version number and exit."""

    click.echo(f"DataHub CLI version: {datahub_package.nice_version_name()}")
    click.echo(f"Python version: {sys.version}")


@datahub.command()
@telemetry.with_telemetry
def init() -> None:
    """Configure which datahub instance to connect to"""

    if os.path.isfile(DATAHUB_CONFIG_PATH):
        click.confirm(f"{DATAHUB_CONFIG_PATH} already exists. Overwrite?", abort=True)

    click.echo("Configure which datahub instance to connect to")
    host = click.prompt(
        "Enter your DataHub host", type=str, default="http://localhost:8080"
    )
    token = click.prompt(
        "Enter your DataHub access token (Supports env vars via `{VAR_NAME}` syntax)",
        type=str,
        default="",
    )
    write_datahub_config(host, token)

    click.echo(f"Written to {DATAHUB_CONFIG_PATH}")


datahub.add_command(check)
datahub.add_command(docker)
datahub.add_command(ingest)
datahub.add_command(delete)
datahub.add_command(get)
datahub.add_command(put)
datahub.add_command(telemetry_cli)


def main(**kwargs):
    # This wrapper prevents click from suppressing errors.
    try:
        sys.exit(datahub(standalone_mode=False, **kwargs))
    except click.exceptions.Abort:
        # Click already automatically prints an abort message, so we can just exit.
        sys.exit(1)
    except click.ClickException as error:
        error.show()
        sys.exit(1)
    except Exception as exc:
        logger.error(
            stackprinter.format(
                exc,
                line_wrap=MAX_CONTENT_WIDTH,
                truncate_vals=10 * MAX_CONTENT_WIDTH,
                suppressed_paths=[r"lib/python.*/site-packages/click/"],
            )
        )
        sys.exit(1)
