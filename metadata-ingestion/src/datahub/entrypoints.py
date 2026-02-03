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
    enable_auto_decorators,
    fixup_gms_url,
    generate_access_token,
    get_init_config_value,
    make_shim_command,
)
from datahub.cli.config_utils import DATAHUB_CONFIG_PATH, write_gms_config
from datahub.cli.container_cli import container
from datahub.cli.delete_cli import delete
from datahub.cli.docker_cli import docker
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.cli.exists_cli import exists
from datahub.cli.get_cli import get
from datahub.cli.graphql_cli import graphql
from datahub.cli.ingest_cli import ingest
from datahub.cli.migrate import migrate
from datahub.cli.put_cli import put
from datahub.cli.recording_cli import recording
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
def version(include_server: bool = False) -> None:
    """Print version number and exit."""

    click.echo(f"DataHub CLI version: {datahub_version.nice_version_name()}")
    click.echo(f"Models: {model_version_name()}")
    click.echo(f"Python version: {sys.version}")
    if include_server:
        server_config = get_default_graph(ClientMode.CLI).get_config()
        click.echo(f"Server config: {server_config}")


def _validate_init_inputs(
    use_password: bool,
    token: Optional[str],
    username: Optional[str],
    password: Optional[str],
    token_duration: str,
) -> None:
    """Validate init command inputs for consistency.

    Args:
        use_password: Whether password authentication is requested (deprecated)
        token: Token value (if provided)
        username: Username value (if provided)
        password: Password value (if provided)
        token_duration: Token expiration duration (if provided)

    Raises:
        click.UsageError: If inputs are invalid or inconsistent
    """
    # Check if credentials will come from CLI args or env vars
    username_provided = username or os.environ.get("DATAHUB_USERNAME")
    password_provided = password or os.environ.get("DATAHUB_PASSWORD")
    token_provided = token or os.environ.get("DATAHUB_GMS_TOKEN")

    # Auto-detect token generation mode
    should_generate_token = bool(username_provided and password_provided)

    # Validate: can't use both token and username/password
    if token_provided and should_generate_token:
        raise click.UsageError(
            "Cannot use both --token and username/password. "
            "Provide either:\n"
            "  - Direct token: --token <token>\n"
            "  - Generate from credentials: --username <user> --password <pass>"
        )

    # Validate: username and password must be provided together (only check CLI args)
    if (username and not password) or (password and not username):
        raise click.UsageError(
            "Both --username and --password required for token generation"
        )

    # Validate: token duration only applies when generating token
    if not should_generate_token and not use_password and token_duration != "ONE_HOUR":
        raise click.UsageError(
            "--token-duration only applies when generating token from username/password"
        )


@datahub.command()
@click.option(
    "--use-password",
    is_flag=True,
    default=False,
    hidden=True,
    help="(DEPRECATED) Auto-detected when --username and --password provided.",
)
@click.option(
    "--host",
    "-h",
    type=str,
    default=None,
    help="DataHub GMS host URL (default: http://localhost:8080, DataHub Cloud: https://your-instance.acryl.io/gms)",
)
@click.option(
    "--token",
    "-t",
    type=str,
    default=None,
    help="DataHub access token (alternative to username/password)",
)
@click.option(
    "--username",
    "-u",
    type=str,
    default=None,
    help="DataHub username (for token generation)",
)
@click.option(
    "--password",
    "-p",
    type=str,
    default=None,
    help="DataHub password (for token generation)",
)
@click.option(
    "--token-duration",
    type=click.Choice(
        [
            "ONE_HOUR",
            "ONE_DAY",
            "ONE_WEEK",
            "ONE_MONTH",
            "THREE_MONTHS",
            "SIX_MONTHS",
            "ONE_YEAR",
            "NO_EXPIRY",
        ],
        case_sensitive=False,
    ),
    default="ONE_HOUR",
    help="Token expiration duration (when generating from username/password)",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Overwrite existing config without confirmation",
)
def init(
    use_password: bool = False,
    host: Optional[str] = None,
    token: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    token_duration: str = "ONE_HOUR",
    force: bool = False,
) -> None:
    """Configure which DataHub instance to connect to.

    Supports both interactive and non-interactive modes for flexibility in different environments.

    \b
    Quickstart (local DataHub instance with default credentials):
        # Generates token from default username/password (datahub/datahub)
        # Connects to http://localhost:8080 by default
        datahub init --username datahub --password datahub

    \b
    Interactive Mode (prompts for input):
        datahub init

    \b
    Mode 1: Auto-Generate Token from Username/Password
        # Default duration (1 hour)
        datahub init --username alice --password secret

        # Custom duration (for long-running jobs)
        datahub init --username alice --password secret --token-duration ONE_MONTH

        # Non-expiring token (for CI/CD)
        datahub init --username alice --password secret --token-duration NO_EXPIRY

    \b
    Mode 2: Use Existing Token
        datahub init --token <your-existing-token>

    \b
    Environment Variables (for automation):
        export DATAHUB_GMS_URL=http://localhost:8080
        export DATAHUB_USERNAME=alice
        export DATAHUB_PASSWORD=secret
        datahub init --token-duration ONE_WEEK --force

    \b
    Available Token Durations:
        ONE_HOUR (default), ONE_DAY, ONE_WEEK, ONE_MONTH,
        THREE_MONTHS, SIX_MONTHS, ONE_YEAR, NO_EXPIRY

    \b
    DataHub Cloud (Acryl-hosted instances):
        datahub init --host https://your-instance.acryl.io/gms --token <your-token>
    """
    # Show deprecation warning if --use-password used
    if use_password:
        click.echo(
            "Warning: --use-password is deprecated. "
            "Token generation is now auto-detected when --username and --password are provided.",
            err=True,
        )

    # Validate input combinations
    _validate_init_inputs(use_password, token, username, password, token_duration)

    # Handle overwrite confirmation
    if os.path.isfile(DATAHUB_CONFIG_PATH) and not force:
        click.confirm(f"{DATAHUB_CONFIG_PATH} already exists. Overwrite?", abort=True)

    # Get host (CLI arg > Env var > Prompt)
    host_value = get_init_config_value(
        arg_value=host,
        env_var="DATAHUB_GMS_URL",
        prompt_text="Configure which datahub instance to connect to (https://your-instance.acryl.io/gms for DataHub Cloud)\nEnter your DataHub host",
        default="http://localhost:8080",
    )
    host_value = fixup_gms_url(host_value)

    # Determine token acquisition mode (check both CLI args and env vars)
    username_provided = username or os.environ.get("DATAHUB_USERNAME")
    password_provided = password or os.environ.get("DATAHUB_PASSWORD")
    should_generate_token = bool(username_provided and password_provided)

    if should_generate_token or use_password:
        # Generate token from credentials
        username_value = get_init_config_value(
            arg_value=username,
            env_var="DATAHUB_USERNAME",
            prompt_text="Enter your DataHub username",
        )

        password_value = get_init_config_value(
            arg_value=password,
            env_var="DATAHUB_PASSWORD",
            prompt_text="Enter your DataHub password",
            hide_input=True,
        )

        # Generate token with specified duration
        _, token_value = generate_access_token(
            username=username_value,
            password=password_value,
            gms_url=host_value,
            validity=token_duration.upper(),
        )

        click.echo(f"✓ Generated token (expires: {token_duration.upper()})")
    else:
        # Get token directly
        token_value = get_init_config_value(
            arg_value=token,
            env_var="DATAHUB_GMS_TOKEN",
            prompt_text="Enter your DataHub access token",
            default="",
        )

    # Write configuration
    write_gms_config(host_value, token_value, merge_with_previous=False)

    click.echo(f"✓ Configuration written to {DATAHUB_CONFIG_PATH}")


datahub.add_command(check)
datahub.add_command(docker)
datahub.add_command(ingest)
datahub.add_command(delete)
datahub.add_command(exists)
datahub.add_command(get)
datahub.add_command(graphql)
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
datahub.add_command(recording)

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

# Adding telemetry and upgrade decorators to all commands
enable_auto_decorators(datahub)


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
