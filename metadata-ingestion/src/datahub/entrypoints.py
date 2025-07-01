import logging
import multiprocessing
import os
import platform
import sys
from typing import ContextManager, Optional

import click

import datahub._version as datahub_version
from datahub.cli.check_cli import check
from datahub.cli.cli_utils import (
    fixup_gms_url,
    generate_access_token,
    make_shim_command,
)
from datahub.cli.config_utils import DATAHUB_CONFIG_PATH, write_gms_config
from datahub.cli.container_cli import container
from datahub.cli.delete_cli import delete
from datahub.cli.docker_cli import docker
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.cli.exists_cli import exists
from datahub.cli.get_cli import get
from datahub.cli.ingest_cli import ingest
from datahub.cli.migrate import migrate
from datahub.cli.put_cli import put
from datahub.cli.specific.assertions_cli import assertions
from datahub.cli.specific.datacontract_cli import datacontract
from datahub.cli.specific.dataproduct_cli import dataproduct
from datahub.cli.specific.dataset_cli import dataset
from datahub.cli.specific.forms_cli import forms
from datahub.cli.specific.group_cli import group
from datahub.cli.specific.structuredproperties_cli import properties
from datahub.cli.specific.user_cli import user
from datahub.cli.state_cli import state
from datahub.cli.telemetry import telemetry as telemetry_cli
from datahub.cli.timeline_cli import timeline
from datahub.configuration.common import should_show_stack_trace
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.telemetry import telemetry
from datahub.utilities._custom_package_loader import model_version_name
from datahub.utilities.logging_manager import configure_logging
from datahub.utilities.server_config_util import get_gms_config

logger = logging.getLogger(__name__)
_logging_configured: Optional[ContextManager] = None

MAX_CONTENT_WIDTH = 120

if sys.version_info >= (3, 12):
    click.secho(
        "Python versions above 3.11 are not actively tested with yet. Please use Python 3.11 for now.",
        fg="red",
        err=True,
    )


@click.group(
    context_settings=dict(
        # Avoid truncation of help text.
        # See https://github.com/pallets/click/issues/486.
        max_content_width=MAX_CONTENT_WIDTH,
    ),
)
@click.option(
    "--debug/--no-debug",
    type=bool,
    is_flag=True,
    default=False,
    help="Enable debug logging.",
)
@click.option(
    "--log-file",
    type=click.Path(dir_okay=False),
    default=None,
    help="Write debug-level logs to a file.",
)
@click.version_option(
    version=datahub_version.nice_version_name(),
    prog_name=datahub_version.__package_name__,
)
def datahub(
    debug: bool,
    log_file: Optional[str],
) -> None:
    debug = debug or get_boolean_env_variable("DATAHUB_DEBUG", False)

    # Note that we're purposely leaking the context manager here.
    # Technically we should wrap this with ctx.with_resource(). However, we have
    # some important error logging in the main() wrapper function that we don't
    # want to miss. If we wrap this with ctx.with_resource(), then click would
    # clean it up before those error handlers are processed.
    # So why is this ok? Because we're leaking a context manager, this will
    # still get cleaned up automatically when the memory is reclaimed, which is
    # worse-case at program exit. In a slightly better case, the context manager's
    # exit call will be triggered by the finally clause of the main() function.
    global _logging_configured
    if _logging_configured is not None:
        _logging_configured.__exit__(None, None, None)
    _logging_configured = None  # see if we can force python to GC this
    _logging_configured = configure_logging(debug=debug, log_file=log_file)
    _logging_configured.__enter__()


@datahub.command()
@click.option(
    "--include-server",
    type=bool,
    is_flag=True,
    default=False,
    help="If passed will show server config. Assumes datahub init has happened.",
)
@telemetry.with_telemetry()
def version(include_server: bool = False) -> None:
    """Print version number and exit."""

    click.echo(f"DataHub CLI version: {datahub_version.nice_version_name()}")
    click.echo(f"Models: {model_version_name()}")
    click.echo(f"Python version: {sys.version}")
    if include_server:
        server_config = get_default_graph(ClientMode.CLI).get_config()
        click.echo(f"Server config: {server_config}")


@datahub.command()
@click.option(
    "--use-password",
    type=bool,
    is_flag=True,
    default=False,
    help="If passed then uses password to initialise token.",
)
@telemetry.with_telemetry()
def init(use_password: bool = False) -> None:
    """Configure which datahub instance to connect to"""

    if os.path.isfile(DATAHUB_CONFIG_PATH):
        click.confirm(f"{DATAHUB_CONFIG_PATH} already exists. Overwrite?", abort=True)

    click.echo(
        "Configure which datahub instance to connect to (https://your-instance.acryl.io/gms for Acryl hosted users)"
    )
    host = click.prompt(
        "Enter your DataHub host", type=str, default="http://localhost:8080"
    )
    host = fixup_gms_url(host)
    if use_password:
        username = click.prompt("Enter your DataHub username", type=str)
        password = click.prompt(
            "Enter your DataHub password",
            type=str,
        )
        _, token = generate_access_token(
            username=username, password=password, gms_url=host
        )
    else:
        token = click.prompt(
            "Enter your DataHub access token",
            type=str,
            default="",
        )
    write_gms_config(host, token, merge_with_previous=False)

    click.echo(f"Written to {DATAHUB_CONFIG_PATH}")


datahub.add_command(check)
datahub.add_command(docker)
datahub.add_command(ingest)
datahub.add_command(delete)
datahub.add_command(exists)
datahub.add_command(get)
datahub.add_command(put)
datahub.add_command(state)
datahub.add_command(telemetry_cli)
datahub.add_command(migrate)
datahub.add_command(timeline)
datahub.add_command(user)
datahub.add_command(group)
datahub.add_command(dataproduct)
datahub.add_command(dataset)
datahub.add_command(properties)
datahub.add_command(forms)
datahub.add_command(datacontract)
datahub.add_command(assertions)
datahub.add_command(container)

try:
    from datahub.cli.iceberg_cli import iceberg

    datahub.add_command(iceberg)
except ImportError as e:
    logger.debug(f"Failed to load datahub iceberg command: {e}")
    datahub.add_command(
        make_shim_command(
            "iceberg", "run `pip install 'acryl-datahub[iceberg-catalog]'`"
        )
    )

try:
    from datahub.cli.lite_cli import lite

    datahub.add_command(lite)
except ImportError as e:
    logger.debug(f"Failed to load datahub lite command: {e}")
    datahub.add_command(
        make_shim_command("lite", "run `pip install 'acryl-datahub[datahub-lite]'`")
    )

try:
    from datahub_actions.cli.actions import actions

    datahub.add_command(actions)
except ImportError as e:
    logger.debug(f"Failed to load datahub actions framework: {e}")
    datahub.add_command(
        make_shim_command("actions", "run `pip install acryl-datahub-actions`")
    )


def main(**kwargs):
    # We use threads in a variety of places within our CLI. The multiprocessing
    # "fork" start method is not safe to use with threads.
    # MacOS and Windows already default to "spawn", and Linux will as well starting in Python 3.14.
    # https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
    # Eventually it may make sense to use "forkserver" as the default where available,
    # but we can revisit that in the future.
    multiprocessing.set_start_method("spawn", force=True)

    # This wrapper prevents click from suppressing errors.
    try:
        sys.exit(datahub(standalone_mode=False, **kwargs))
    except click.Abort:
        # Click already automatically prints an abort message, so we can just exit.
        sys.exit(1)
    except click.ClickException as error:
        error.show()
        sys.exit(1)
    except Exception as exc:
        if not should_show_stack_trace(exc):
            # Don't print the full stack trace for simple config errors.
            logger.debug("Error: %s", exc, exc_info=exc)
            click.secho(f"{exc}", fg="red")
        else:
            logger.exception(f"Command failed: {exc}")

        logger.debug(
            f"DataHub CLI version: {datahub_version.__version__} at {__file__}"
        )
        logger.debug(
            f"Python version: {sys.version} at {sys.executable} on {platform.platform()}"
        )
        gms_config = get_gms_config()
        if gms_config:
            logger.debug(f"GMS config {gms_config}")
        sys.exit(1)
    finally:
        global _logging_configured
        if _logging_configured:
            _logging_configured.__exit__(None, None, None)
            _logging_configured = None
