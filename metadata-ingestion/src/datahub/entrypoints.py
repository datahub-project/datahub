import importlib.resources
import logging
import multiprocessing
import os
import platform
import re
import shlex
import sys
from contextlib import ExitStack, contextmanager
from typing import ContextManager, Iterator, Optional
from urllib.parse import urlparse

import click
import yaml

import datahub._version as datahub_version
from datahub.cli.agent_context import (
    make_agent_aware_command as _make_agent_aware_command,
)
from datahub.cli.check_cli import check
from datahub.cli.cli_utils import (
    enable_auto_decorators,
    fixup_gms_url,
    generate_access_token,
    get_init_config_value,
    make_shim_command,
)
from datahub.cli.config_utils import (
    DATAHUB_CONFIG_PATH as DATAHUB_CONFIG_PATH,
    env_profile,
    profile_path,
    resolve_profile,
    select_cli_profile,
    stored_server,
    write_gms_config,
)
from datahub.cli.container_cli import container
from datahub.cli.datapack.datapack_cli import datapack
from datahub.cli.delete_cli import delete
from datahub.cli.docker_cli import docker
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.cli.exists_cli import exists
from datahub.cli.get_cli import get
from datahub.cli.graphql_cli import graphql
from datahub.cli.ingest_cli import ingest
from datahub.cli.lineage_cli import lineage
from datahub.cli.migrate import migrate
from datahub.cli.profile_cli import profile as profile_command
from datahub.cli.put_cli import put
from datahub.cli.recipe_cli import recipe
from datahub.cli.recording_cli import recording
from datahub.cli.search_cli import search
from datahub.cli.specific.agent_skill_cli import agent_skill
from datahub.cli.specific.api_cli import api
from datahub.cli.specific.assertions_cli import assertions
from datahub.cli.specific.datacontract_cli import datacontract
from datahub.cli.specific.dataproduct_cli import dataproduct
from datahub.cli.specific.dataset_cli import dataset
from datahub.cli.specific.forms_cli import forms
from datahub.cli.specific.group_cli import group
from datahub.cli.specific.structuredproperties_cli import properties
from datahub.cli.specific.user_cli import user
from datahub.cli.state_cli import state
from datahub.cli.timeline_cli import timeline
from datahub.configuration.common import should_show_stack_trace
from datahub.configuration.env_vars import get_password, get_username
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.utilities._custom_package_loader import model_version_name
from datahub.utilities.logging_manager import configure_logging
from datahub.utilities.server_config_util import get_gms_config

logger = logging.getLogger(__name__)
_logging_configured: Optional[ContextManager] = None

MAX_CONTENT_WIDTH = 120

# Keep in sync with the Python matrix in .github/workflows/metadata-ingestion.yml.
if sys.version_info >= (3, 14):
    click.secho(
        "Python versions above 3.13 are not actively tested with yet. Please use Python 3.11 to 3.13 for now.",
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
@click.option(
    "--context",
    "-C",
    "context_pairs",
    multiple=True,
    metavar="KEY=VALUE",
    help="Additional context as key=value pairs (can be repeated).",
)
@click.option(
    "--profile", type=str, help="Named connection profile for this invocation."
)
@click.version_option(
    version=datahub_version.nice_version_name(),
    prog_name=datahub_version.__package_name__,
)
@click.pass_context
def datahub(
    ctx: click.Context,
    debug: bool,
    log_file: Optional[str],
    context_pairs: tuple,
    profile: Optional[str] = None,
) -> None:
    debug = debug or get_boolean_env_variable("DATAHUB_DEBUG", False)

    # Parse --context key=value pairs and store on Click context for telemetry.
    ctx.ensure_object(dict)
    ctx.obj["profile"] = profile
    profile_stack = ExitStack()
    ctx.call_on_close(profile_stack.close)
    profile_stack.enter_context(select_cli_profile(profile))
    if profile is not None:
        # The ContextVar does not reach worker threads, spawned processes or
        # child `datahub` commands; the environment does.
        profile_stack.enter_context(_exported_profile(profile))
    # Fail fast only on a profile the user named. A stale current-profile
    # pointer surfaces when a command reads config, so `version`, `--help`
    # and the profile commands that repair it keep working.
    if (
        profile is not None or env_profile() is not None
    ) and ctx.invoked_subcommand != "profile":
        resolve_profile(
            allow_create=ctx.invoked_subcommand == "init",
            for_init=ctx.invoked_subcommand == "init",
        )
    ctx.obj["context"] = {}
    for pair in context_pairs:
        if "=" in pair:
            k, v = pair.split("=", 1)
            ctx.obj["context"][k] = v

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
    token_duration: Optional[str],
    sso: bool = False,
    oauth: bool = False,
) -> None:
    """Validate init command inputs for consistency.

    Args:
        use_password: Whether password authentication is requested (deprecated)
        token: Token value (if provided)
        username: Username value (if provided)
        password: Password value (if provided)
        token_duration: Token expiration duration (if provided)
        sso: Whether SSO browser login is requested
        oauth: Whether native OAuth2 PKCE login is requested

    Raises:
        click.UsageError: If inputs are invalid or inconsistent
    """
    if oauth and sso:
        raise click.UsageError("--oauth and --sso cannot be used together.")

    # OAuth PKCE is mutually exclusive with all credential options
    if oauth:
        if token or os.environ.get("DATAHUB_GMS_TOKEN"):
            raise click.UsageError(
                "--oauth cannot be used with --token. "
                "Use --oauth alone to authenticate via browser OAuth2 login."
            )
        if username or password or get_username() or get_password():
            raise click.UsageError(
                "--oauth cannot be used with --username/--password. "
                "Use --oauth alone to authenticate via browser OAuth2 login."
            )
        if token_duration is not None:
            raise click.UsageError(
                "--token-duration cannot be used with --oauth. "
                "Token TTL is managed automatically by the OAuth2 server."
            )
        return

    # SSO is mutually exclusive with other auth methods
    if sso:
        if token or os.environ.get("DATAHUB_GMS_TOKEN"):
            raise click.UsageError(
                "--sso cannot be used with --token. "
                "Use --sso alone to authenticate via browser SSO."
            )
        if username or password or get_username() or get_password():
            raise click.UsageError(
                "--sso cannot be used with --username/--password. "
                "Use --sso alone to authenticate via browser SSO."
            )
        return

    # Check if credentials will come from CLI args or env vars
    username_provided = username or get_username()
    password_provided = password or get_password()
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
    if not should_generate_token and not use_password and token_duration is not None:
        raise click.UsageError(
            "--token-duration only applies when generating token from username/password"
        )


@datahub.command(cls=_make_agent_aware_command("INIT_AGENT_CONTEXT.md"))
@click.option("--description", type=str, help="Description shown in profile list.")
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
    default=None,
    help=(
        "Token expiration duration (default: ONE_MONTH for localhost, ONE_HOUR otherwise). "
        "NO_EXPIRY requires ACCESS_TOKEN_ALLOW_NO_EXPIRY=true on the server (disabled by default)."
    ),
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Overwrite existing config without confirmation",
)
@click.option(
    "--sso",
    is_flag=True,
    default=False,
    help="Open browser for SSO login (requires: pip install 'acryl-datahub[sso]' && playwright install chromium)",
)
@click.option(
    "--oauth",
    is_flag=True,
    default=False,
    help="Open browser for native OAuth2 PKCE login (no extra dependencies)",
)
@click.option(
    "--support",
    is_flag=True,
    default=False,
    help="Use support login for DataHub Cloud customer debugging (use with --oauth or --sso)",
)
@click.option(
    "--ticket-id",
    type=str,
    default=None,
    help="Support ticket ID (required with --oauth --support)",
)
@click.option(
    "--agent-context",
    is_flag=True,
    default=False,
    help="Print agent best-practices guide and exit",
)
def init(
    use_password: bool = False,
    host: Optional[str] = None,
    token: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    token_duration: Optional[str] = None,
    force: bool = False,
    sso: bool = False,
    oauth: bool = False,
    support: bool = False,
    ticket_id: Optional[str] = None,
    agent_context: bool = False,
    description: Optional[str] = None,
) -> None:
    """Configure which DataHub instance to connect to.

    Supports both interactive and non-interactive modes for flexibility in different environments.

    The global --profile <name> option selects the profile to create or update
    (~/.datahub/profiles/<name>/.datahubenv). It never changes the active profile.

    \b
    Quickstart (local DataHub instance with default credentials):
        # Generates token from default username/password (datahub/datahub)
        # Connects to http://localhost:8080 by default
        datahub init --profile local --username datahub --password datahub

    \b
    Interactive Mode (prompts for input):
        datahub init --profile local

    \b
    Mode 1: Auto-Generate Token from Username/Password
        datahub init --profile local --username alice --password secret
        datahub init --profile local --username alice --password secret \\
            --token-duration ONE_MONTH
        datahub init --profile local --username alice --password secret \\
            --token-duration NO_EXPIRY

    \b
    Mode 2: Use Existing Token
        datahub init --profile local --token <your-existing-token>

    \b
    Mode 3: SSO Browser Login (OIDC/SAML)
        datahub init --profile local --sso --host https://example.com/gms
        datahub init --profile local --sso --host https://example.com/gms \\
            --token-duration ONE_MONTH

    \b
    Mode 4: Native OAuth2 PKCE Login
        datahub init --profile local --oauth --host https://your-instance.acryl.io/gms
        Stores an access token + refresh token; auto-refreshes on expiry.
        Requires an instance with the OAuth2 server enabled. No extra
        dependencies (unlike --sso which requires Playwright).

    \b
    Support Login (DataHub Cloud — customer debugging):
        datahub init --profile local --oauth --support --ticket-id acme-123 \\
            --host https://acme.example.com/gms

    \b
    Environment Variables (for automation):
        export DATAHUB_GMS_URL=http://localhost:8080
        export DATAHUB_USERNAME=alice
        export DATAHUB_PASSWORD=secret
        datahub init --profile local --token-duration ONE_WEEK --force

    \b
    Available Token Durations:
        ONE_HOUR, ONE_DAY, ONE_WEEK, ONE_MONTH,
        THREE_MONTHS, SIX_MONTHS, ONE_YEAR, NO_EXPIRY
        (Default: ONE_MONTH for localhost, ONE_HOUR for remote instances)

    \b
    DataHub Cloud (Acryl-hosted instances):
        datahub init --profile local --token <token> \\
            --host https://your-instance.acryl.io/gms
    """
    if agent_context:
        text: str = (
            importlib.resources.files("datahub.cli.resources")
            .joinpath("INIT_AGENT_CONTEXT.md")
            .read_text(encoding="utf-8")
        )
        click.echo(text)
        return

    if support and not (sso or oauth):
        raise click.UsageError("--support requires --oauth or --sso")
    if ticket_id and not support:
        raise click.UsageError("--ticket-id requires --support")
    if support and oauth and not ticket_id:
        raise click.UsageError("--oauth --support requires --ticket-id")

    # Show deprecation warning if --use-password used
    if use_password:
        click.echo(
            "Warning: --use-password is deprecated. "
            "Token generation is now auto-detected when --username and --password are provided.",
            err=True,
        )

    # Validate input combinations
    _validate_init_inputs(
        use_password, token, username, password, token_duration, sso=sso, oauth=oauth
    )

    resolved = resolve_profile(allow_create=True, for_init=True)
    if resolved.path.is_file() and not force and sys.stdin.isatty():
        click.confirm(f"{resolved.path} already exists. Overwrite?", abort=True)

    # Get host (CLI arg > Env var > silent default if credentials provided > prompt)
    # When credentials are supplied non-interactively, skip the prompt and default to
    # localhost:8080 — users connecting to a different host will pass --host explicitly.
    _credentials_non_interactive = (
        bool((username or get_username()) and (password or get_password()))
        or bool(token or os.environ.get("DATAHUB_GMS_TOKEN"))
        or sso
    )
    if (
        host is None
        and not os.environ.get("DATAHUB_GMS_URL")
        and _credentials_non_interactive
    ):
        host_value = "http://localhost:8080"
    else:
        host_value = get_init_config_value(
            arg_value=host,
            env_var="DATAHUB_GMS_URL",
            prompt_text="Configure which datahub instance to connect to (https://your-instance.acryl.io/gms for DataHub Cloud)\nEnter your DataHub host",
            default="http://localhost:8080",
        )
    host_value = fixup_gms_url(host_value)

    if resolved.path.is_file() and not force and not sys.stdin.isatty():
        try:
            previous_server = stored_server(resolved.path)
        except (ValueError, OSError, yaml.YAMLError) as exc:
            raise click.ClickException(
                f"Cannot safely overwrite {resolved.path}. Inspect the file or add --force."
            ) from exc
        if previous_server is None or previous_server.rstrip("/") != host_value.rstrip(
            "/"
        ):
            suggested = (
                re.sub(
                    r"[^a-z0-9._-]",
                    "-",
                    (urlparse(host_value).hostname or "local").split(".")[0].lower(),
                ).strip("-_.")[:63]
                or "local"
            )
            base, counter = suggested[:59], 1
            while suggested in ("default", resolved.name) or (
                profile_path(suggested).exists()
            ):
                counter += 1
                suggested = f"{base}-{counter}"
            # Secrets are replaced with placeholders: this message lands in agent
            # transcripts and CI logs.
            auth_args = []
            for option, value in [
                ("--token", "<token>" if token is not None else None),
                ("--username", username),
                ("--password", "<password>" if password is not None else None),
                ("--token-duration", token_duration),
                ("--ticket-id", ticket_id),
                ("--description", description),
            ]:
                if value is not None:
                    auth_args.extend([option, value])
            for option, enabled in [
                ("--sso", sso),
                ("--oauth", oauth),
                ("--support", support),
                ("--use-password", use_password),
            ]:
                if enabled:
                    auth_args.append(option)
            command = shlex.join(
                [
                    "datahub",
                    "init",
                    "--profile",
                    suggested,
                    "--host",
                    host_value,
                    *auth_args,
                ]
            )
            raise click.ClickException(
                f'{resolved.path} (profile "{resolved.name}") points at {previous_server or "an invalid server"}.\n'
                f"Refusing to repoint it to {host_value} from a non-interactive session.\n"
                f"To add a separate profile for this instance, run:\n  {command}\n"
                f"Then pass --profile {suggested} (or set DATAHUB_PROFILE={suggested}) on later commands.\n"
                "To overwrite anyway, add --force."
            )

    # Resolve effective token duration: explicit > host-based default
    # Local dev gets a generous default so tokens don't expire mid-session.
    _is_localhost = "localhost" in host_value or "127.0.0.1" in host_value
    effective_duration = (
        token_duration.upper()
        if token_duration
        else ("ONE_MONTH" if _is_localhost else "ONE_HOUR")
    )

    # Determine token acquisition mode (check both CLI args and env vars)
    username_provided = username or get_username()
    password_provided = password or get_password()
    should_generate_token = bool(username_provided and password_provided)

    if oauth:
        # Native OAuth2 PKCE login — no Playwright dependency, stores refresh token
        from datahub.cli.config_utils import write_oauth_config
        from datahub.cli.oauth_cli import pkce_login

        result = pkce_login(host_value, support=support, ticket_id=ticket_id)
        click.echo("✓ Successfully authenticated with DataHub")
        if result.refresh_token:
            click.echo(
                "✓ Refresh token stored — access token will auto-renew on expiry"
            )
        write_oauth_config(
            host=host_value,
            access_token=result.access_token,
            client_id=result.client_id,
            refresh_token=result.refresh_token,
            token_endpoint=result.token_endpoint,
            path=resolved.path,
            description=description,
        )
        click.echo(
            f"Configuration written to {resolved.path} (profile {resolved.name!r})"
        )
        return
    elif sso:
        # SSO browser login flow
        from datahub.cli.cli_utils import guess_frontend_url_from_gms_url
        from datahub.cli.sso_cli import browser_sso_login

        frontend_url = guess_frontend_url_from_gms_url(host_value)
        _, token_value = browser_sso_login(
            frontend_url,
            effective_duration,
            support=support,
            ticket_id=ticket_id,
        )
        click.echo(f"✓ Generated token (expires: {effective_duration})")
    elif should_generate_token or use_password:
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
            validity=effective_duration,
        )

        click.echo(f"✓ Generated token (expires: {effective_duration})")
    else:
        # Get token directly
        token_value = get_init_config_value(
            arg_value=token,
            env_var="DATAHUB_GMS_TOKEN",
            prompt_text="Enter your DataHub access token",
            default="",
        )

    # Write configuration
    write_gms_config(
        host_value,
        token_value,
        merge_with_previous=False,
        path=resolved.path,
        description=description,
    )

    click.echo(f"Configuration written to {resolved.path} (profile {resolved.name!r})")


datahub.add_command(profile_command)
datahub.add_command(check)
datahub.add_command(docker)
datahub.add_command(ingest)
datahub.add_command(delete)
datahub.add_command(exists)
datahub.add_command(get)
datahub.add_command(graphql)
datahub.add_command(lineage)
datahub.add_command(put)
datahub.add_command(search)
datahub.add_command(state)
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
datahub.add_command(datapack)
datahub.add_command(api)
datahub.add_command(agent_skill)
datahub.add_command(recipe)

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

try:
    from datahub_agent_context.cli import agent

    datahub.add_command(agent)
except ImportError as e:
    logger.debug(f"Failed to load datahub-agent-context framework: {e}")
    from datahub.cli.specific.agent_cli import agent as agent_registry

    datahub.add_command(agent_registry)

# Adding telemetry and upgrade decorators to all commands
enable_auto_decorators(datahub)


@contextmanager
def _exported_profile(name: str) -> Iterator[None]:
    previous = os.environ.get("DATAHUB_PROFILE")
    os.environ["DATAHUB_PROFILE"] = name
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("DATAHUB_PROFILE", None)
        else:
            os.environ["DATAHUB_PROFILE"] = previous


def lift_profile_args(args: list[str]) -> list[str]:
    """Move profile options ahead of subcommands, respecting the option terminator."""
    profiles: list[str] = []
    remaining: list[str] = []
    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--":
            remaining.extend(args[index:])
            break
        if arg == "--profile":
            if index + 1 >= len(args) or args[index + 1].startswith("--"):
                raise click.UsageError("--profile requires a name.")
            profiles.extend(args[index : index + 2])
            index += 2
        elif arg.startswith("--profile="):
            profiles.append(arg)
            index += 1
        else:
            remaining.append(arg)
            index += 1
    return profiles + remaining


def main(args: Optional[list[str]] = None, **kwargs: object) -> None:
    # We use threads in a variety of places within our CLI. The multiprocessing
    # "fork" start method is not safe to use with threads.
    # MacOS and Windows already default to "spawn", and Linux will as well starting in Python 3.14.
    # https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
    # Eventually it may make sense to use "forkserver" as the default where available,
    # but we can revisit that in the future.
    multiprocessing.set_start_method("spawn", force=True)

    # This wrapper prevents click from suppressing errors.
    try:
        sys.exit(
            datahub(
                args=lift_profile_args(args if args is not None else sys.argv[1:]),
                standalone_mode=False,
                **kwargs,
            )
        )
    except click.Abort:
        # Click already automatically prints an abort message, so we can just exit.
        sys.exit(1)
    except click.ClickException as error:
        error.show()
        sys.exit(error.exit_code)
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
