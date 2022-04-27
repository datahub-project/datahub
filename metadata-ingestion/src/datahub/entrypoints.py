import logging
import os
import platform
import sys

import click
import stackprinter
from pydantic import ValidationError

import datahub as datahub_package
from datahub.cli.check_cli import check
from datahub.cli.cli_utils import DATAHUB_CONFIG_PATH, write_datahub_config
from datahub.cli.delete_cli import delete
from datahub.cli.docker import docker
from datahub.cli.get_cli import get
from datahub.cli.ingest_cli import ingest
from datahub.cli.migrate import migrate
from datahub.cli.put_cli import put
from datahub.cli.telemetry import telemetry as telemetry_cli
from datahub.cli.timeline_cli import timeline
from datahub.configuration import SensitiveError
from datahub.configuration.common import ConfigurationError
from datahub.telemetry import telemetry
from datahub.utilities.server_config_util import get_gms_config

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
@click.option(
    "-dl",
    "--detect-memory-leaks",
    type=bool,
    is_flag=True,
    default=False,
    help="Run memory leak detection.",
)
@click.pass_context
def datahub(ctx: click.Context, debug: bool, detect_memory_leaks: bool) -> None:
    # Insulate 'datahub' and all child loggers from inadvertent changes to the
    # root logger by the external site packages that we import.
    # (Eg: https://github.com/reata/sqllineage/commit/2df027c77ea0a8ea4909e471dcd1ecbf4b8aeb2f#diff-30685ea717322cd1e79c33ed8d37903eea388e1750aa00833c33c0c5b89448b3R11
    #  changes the root logger's handler level to WARNING, causing any message below
    #  WARNING level to be dropped  after this module is imported, irrespective
    #  of the logger's logging level! The lookml source was affected by this).

    # 1. Create 'datahub' parent logger.
    datahub_logger = logging.getLogger("datahub")
    # 2. Setup the stream handler with formatter.
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter(BASE_LOGGING_FORMAT)
    stream_handler.setFormatter(formatter)
    datahub_logger.addHandler(stream_handler)
    # 3. Turn off propagation to the root handler.
    datahub_logger.propagate = False
    # 4. Adjust log-levels.
    if debug or os.getenv("DATAHUB_DEBUG", False):
        logging.getLogger().setLevel(logging.INFO)
        datahub_logger.setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.WARNING)
        datahub_logger.setLevel(logging.INFO)
    # loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    # print(loggers)
    # Setup the context for the memory_leak_detector decorator.
    ctx.ensure_object(dict)
    ctx.obj["detect_memory_leaks"] = detect_memory_leaks


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
datahub.add_command(migrate)
datahub.add_command(timeline)
try:
    import datahub_actions

    datahub.add_command(datahub_actions.cli.actions)
except ImportError:
    # TODO: Increase the log level once this approach has been validated.
    logger.debug(
        "Failed to load datahub actions framework. Please confirm that the acryl-datahub-actions package has been installed from PyPi."
    )


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
        kwargs = {}
        sensitive_cause = SensitiveError.get_sensitive_cause(exc)
        if sensitive_cause:
            kwargs = {"show_vals": None}
            exc = sensitive_cause

        # suppress stack printing for common configuration errors
        if isinstance(exc, (ConfigurationError, ValidationError)):
            logger.error(exc)
        else:
            logger.error(
                stackprinter.format(
                    exc,
                    line_wrap=MAX_CONTENT_WIDTH,
                    truncate_vals=10 * MAX_CONTENT_WIDTH,
                    suppressed_paths=[r"lib/python.*/site-packages/click/"],
                    **kwargs,
                )
            )
        logger.info(
            f"DataHub CLI version: {datahub_package.__version__} at {datahub_package.__file__}"
        )
        logger.info(
            f"Python version: {sys.version} at {sys.executable} on {platform.platform()}"
        )
        logger.info(f"GMS config {get_gms_config()}")
        sys.exit(1)
