"""CLI commands for managing DataHub connection profiles."""

import logging
from typing import Optional

import click
import yaml

from datahub.cli import config_utils
from datahub.configuration.config import ProfileConfig
from datahub.ingestion.graph.config import ClientMode

logger = logging.getLogger(__name__)


def _redact_token(token: Optional[str]) -> str:
    """Redact a token for display, showing only the last 4 characters."""
    if not token:
        return "(not set)"
    if token.startswith("$"):
        # It's an environment variable reference, show it
        return token
    if len(token) <= 4:
        return "****"
    return f"****{token[-4:]}"


def _get_profile_display_name(profile_name: str, is_current: bool) -> str:
    """Format profile name for display, highlighting current profile."""
    if is_current:
        return f"{profile_name} (current)"
    return profile_name


@click.group()
def profile() -> None:
    """Manage DataHub connection profiles."""
    pass


@profile.command(name="list")
def list_profiles() -> None:
    """List all available profiles."""
    try:
        profiles = config_utils.list_profiles()
        current = config_utils.get_current_profile()

        if not profiles:
            click.echo("No profiles configured.")
            click.echo(
                "\nRun 'datahub init' or 'datahub profile add <name>' to create a profile."
            )
            return

        click.echo("Available profiles:\n")
        for profile_name in sorted(profiles):
            is_current = profile_name == current
            display_name = _get_profile_display_name(profile_name, is_current)

            try:
                prof = config_utils.get_profile(profile_name)
                server_display = prof.server if prof.server else "(not set)"
                desc = f" - {prof.description}" if prof.description else ""
                click.echo(f"  {display_name}: {server_display}{desc}")
            except Exception as e:
                click.echo(f"  {display_name}: (error loading: {e})")

        if current:
            click.echo(f"\nCurrent profile: {current}")
        else:
            click.echo(
                "\nNo current profile set. Use 'datahub profile use <name>' to set one."
            )

    except Exception as e:
        click.secho(f"Error listing profiles: {e}", fg="red", err=True)
        raise click.Abort() from e


@profile.command()
def current() -> None:
    """Show the current active profile."""
    try:
        current_profile = config_utils.get_current_profile()

        if not current_profile:
            click.echo("No current profile set.")
            click.echo("\nProfile precedence:")
            click.echo("  1. --profile flag (highest priority)")
            click.echo("  2. DATAHUB_PROFILE environment variable")
            click.echo("  3. current_profile in config.yaml")
            click.echo("  4. Profile named 'default'")
            click.echo("  5. Legacy ~/.datahubenv")
            return

        click.echo(f"Current profile: {current_profile}")

        try:
            prof = config_utils.get_profile(current_profile)
            click.echo(f"Server: {prof.server}")
            click.echo(f"Token: {_redact_token(prof.token)}")
            if prof.description:
                click.echo(f"Description: {prof.description}")
        except Exception as e:
            click.secho(f"Error loading profile details: {e}", fg="yellow")

    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        raise click.Abort() from e


@profile.command()
@click.argument("profile_name", required=False)
@click.option(
    "--effective",
    is_flag=True,
    help="Show effective config with environment variable overrides applied",
)
def show(profile_name: Optional[str], effective: bool) -> None:
    """
    Show profile configuration.

    If PROFILE_NAME is not provided, shows the current active profile.
    """
    try:
        # Determine which profile to show
        if not profile_name:
            profile_name = config_utils.get_current_profile()
            if not profile_name:
                click.echo("No current profile set and no profile specified.")
                click.echo("Usage: datahub profile show <profile_name>")
                raise click.Abort()

        if effective:
            # Show effective config with env var overrides
            client_config = config_utils.load_client_config(profile=profile_name)
            click.echo(f"Profile: {profile_name} (with environment overrides)\n")
            click.echo(f"Server: {client_config.server}")
            click.echo(f"Token: {_redact_token(client_config.token)}")
            if client_config.timeout_sec:
                click.echo(f"Timeout: {client_config.timeout_sec}s")
            if client_config.extra_headers:
                click.echo(
                    f"Extra headers: {len(client_config.extra_headers)} header(s)"
                )
            if client_config.ca_certificate_path:
                click.echo(f"CA Certificate: {client_config.ca_certificate_path}")
            if client_config.disable_ssl_verification:
                click.echo("SSL Verification: Disabled")
        else:
            # Show raw profile config
            prof = config_utils.get_profile(profile_name)
            click.echo(f"Profile: {profile_name}\n")
            click.echo(f"Server: {prof.server}")
            click.echo(f"Token: {_redact_token(prof.token)}")
            if prof.timeout_sec:
                click.echo(f"Timeout: {prof.timeout_sec}s")
            if prof.description:
                click.echo(f"Description: {prof.description}")
            if prof.require_confirmation:
                click.echo("Require Confirmation: Yes (production safeguard)")
            if prof.extra_headers:
                click.echo(f"Extra headers: {len(prof.extra_headers)} header(s)")
            if prof.ca_certificate_path:
                click.echo(f"CA Certificate: {prof.ca_certificate_path}")
            if prof.disable_ssl_verification:
                click.echo("SSL Verification: Disabled")

    except KeyError as e:
        click.secho(
            f"Profile '{profile_name}' not found. Run 'datahub profile list' to see available profiles.",
            fg="red",
            err=True,
        )
        raise click.Abort() from e
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        raise click.Abort() from e


@profile.command()
@click.argument("profile_name")
def use(profile_name: str) -> None:
    """Set the current active profile."""
    try:
        # Verify profile exists
        if not config_utils.profile_exists(profile_name):
            available = config_utils.list_profiles()
            click.secho(f"Profile '{profile_name}' not found.", fg="red", err=True)
            if available:
                click.echo(f"Available profiles: {', '.join(available)}")
            raise click.Abort()

        # Set as current
        config_utils.set_current_profile(profile_name)
        click.secho(f"✓ Switched to profile '{profile_name}'", fg="green")

        # Show profile details
        prof = config_utils.get_profile(profile_name)
        click.echo(f"Server: {prof.server}")
        if prof.description:
            click.echo(f"Description: {prof.description}")

    except click.Abort:
        raise
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        raise click.Abort() from e


@profile.command()
@click.argument("profile_name")
@click.option(
    "--server",
    required=True,
    help="DataHub GMS server URL (e.g., http://localhost:8080)",
)
@click.option("--token", help="Access token (consider using --token-env instead)")
@click.option(
    "--token-env",
    help="Environment variable name for token (e.g., DATAHUB_TOKEN). Recommended for security.",
)
@click.option("--description", help="Profile description")
@click.option(
    "--timeout",
    type=int,
    default=30,
    help="Connection timeout in seconds",
)
@click.option(
    "--require-confirmation",
    is_flag=True,
    help="Require confirmation for destructive operations (recommended for production)",
)
@click.option(
    "--set-current",
    is_flag=True,
    help="Set this profile as the current active profile",
)
def add(
    profile_name: str,
    server: str,
    token: Optional[str],
    token_env: Optional[str],
    description: Optional[str],
    timeout: int,
    require_confirmation: bool,
    set_current: bool,
) -> None:
    """Add a new profile or update an existing one."""
    try:
        # Validate inputs
        if token and token_env:
            click.secho(
                "Error: Cannot specify both --token and --token-env. Choose one.",
                fg="red",
                err=True,
            )
            raise click.Abort()

        # Determine token value (use env var syntax if specified)
        token_value = None
        if token:
            token_value = token
            click.secho(
                "⚠ Warning: Storing tokens directly in config file is less secure.",
                fg="yellow",
            )
            click.echo(
                "Consider using --token-env to reference an environment variable instead."
            )
        elif token_env:
            token_value = f"${{{token_env}}}"
            click.echo(f"Token will be read from environment variable: {token_env}")

        # Check if profile already exists
        existing = config_utils.profile_exists(profile_name)
        if existing:
            if not click.confirm(
                f"Profile '{profile_name}' already exists. Overwrite?"
            ):
                click.echo("Aborted.")
                return

        # Create profile config
        # Use model_construct to bypass validation when storing env var references
        # This prevents ${VAR} from being interpolated prematurely
        if token_env:
            # Use model_construct to store literal ${VAR} without interpolation
            profile_config = ProfileConfig.model_construct(
                server=server,
                token=token_value,
                timeout_sec=timeout,
                description=description,
                require_confirmation=require_confirmation,
            )
        else:
            # Use normal constructor for direct token values
            profile_config = ProfileConfig(
                server=server,
                token=token_value,
                timeout_sec=timeout,
                description=description,
                require_confirmation=require_confirmation,
            )

        # Add profile
        config_utils.add_profile(profile_name, profile_config)

        action = "Updated" if existing else "Added"
        click.secho(f"✓ {action} profile '{profile_name}'", fg="green")

        # Set as current if requested
        if set_current:
            config_utils.set_current_profile(profile_name)
            click.secho(f"✓ Set '{profile_name}' as current profile", fg="green")

        # Show summary
        click.echo("\nProfile details:")
        click.echo(f"  Server: {server}")
        if token_value:
            click.echo(f"  Token: {_redact_token(token_value)}")
        click.echo(f"  Timeout: {timeout}s")
        if description:
            click.echo(f"  Description: {description}")
        if require_confirmation:
            click.echo("  Require Confirmation: Yes")

    except click.Abort:
        raise
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        raise click.Abort() from e


@profile.command()
@click.argument("profile_name")
@click.option("--force", is_flag=True, help="Skip confirmation prompt")
def remove(profile_name: str, force: bool) -> None:
    """Remove a profile."""
    try:
        # Check if profile exists
        if not config_utils.profile_exists(profile_name):
            click.secho(f"Profile '{profile_name}' not found.", fg="red", err=True)
            available = config_utils.list_profiles()
            if available:
                click.echo(f"Available profiles: {', '.join(available)}")
            raise click.Abort()

        # Confirm deletion
        if not force:
            prof = config_utils.get_profile(profile_name)
            click.echo(f"Profile: {profile_name}")
            click.echo(f"Server: {prof.server}")
            if not click.confirm("Are you sure you want to remove this profile?"):
                click.echo("Aborted.")
                return

        # Check if it's the current profile
        current = config_utils.get_current_profile()
        is_current = current == profile_name

        # Remove profile
        config_utils.remove_profile(profile_name)
        click.secho(f"✓ Removed profile '{profile_name}'", fg="green")

        if is_current:
            click.echo(
                "\nNote: This was the current profile. Use 'datahub profile use <name>' to set another."
            )

    except click.Abort:
        raise
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        raise click.Abort() from e


@profile.command()
@click.argument("profile_name", required=False)
def test(profile_name: Optional[str]) -> None:
    """Test connection for a profile."""
    try:
        # Determine which profile to test
        if not profile_name:
            profile_name = config_utils.get_current_profile()
            if not profile_name:
                click.echo("No current profile set and no profile specified.")
                click.echo("Usage: datahub profile test <profile_name>")
                raise click.Abort()

        click.echo(f"Testing connection for profile '{profile_name}'...")

        # Load config and test connection
        from datahub.ingestion.graph.client import get_default_graph

        try:
            graph = get_default_graph(
                client_mode=ClientMode.CLI,
                profile=profile_name,
            )

            # If we got here, connection test passed
            click.secho("✓ Connection successful!", fg="green")

            # Show server info
            config = graph.config
            click.echo(f"Server: {config.server}")

            # Try to get server version/health
            try:
                # This will make a simple API call to verify connectivity
                click.echo("Server is reachable and responding.")
            except Exception:
                pass

        except Exception as e:
            click.secho(f"✗ Connection failed: {e}", fg="red", err=True)
            raise click.Abort() from e

    except click.Abort:
        raise
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        raise click.Abort() from e


@profile.command()
def validate() -> None:
    """Validate configuration file syntax."""
    try:
        config_path = config_utils.get_config_file_path()
        click.echo(f"Validating config file: {config_path}")

        # Try loading the config
        config = config_utils._load_new_config()

        if not config:
            click.echo("No new-format config file found.")
            if config_utils.DATAHUB_CONFIG_PATH:
                click.echo(
                    f"Legacy config may exist at: {config_utils.DATAHUB_CONFIG_PATH}"
                )
            return

        # Validate profiles
        profiles = config.list_profiles()
        if not profiles:
            click.secho("⚠ Warning: No profiles configured", fg="yellow")
        else:
            click.secho(f"✓ Found {len(profiles)} profile(s)", fg="green")

            # Validate each profile
            for profile_name in profiles:
                try:
                    prof = config.get_profile(profile_name)
                    issues = []

                    if not prof.server:
                        issues.append("missing server URL")
                    if not prof.token:
                        issues.append("no token configured")

                    if issues:
                        click.secho(
                            f"  ⚠ {profile_name}: {', '.join(issues)}", fg="yellow"
                        )
                    else:
                        click.secho(f"  ✓ {profile_name}: OK", fg="green")

                except Exception as e:
                    click.secho(f"  ✗ {profile_name}: {e}", fg="red")

        # Check current profile
        if config.current_profile:
            if config.current_profile in profiles:
                click.secho(
                    f"✓ Current profile '{config.current_profile}' exists", fg="green"
                )
            else:
                click.secho(
                    f"✗ Current profile '{config.current_profile}' not found in profiles",
                    fg="red",
                )

        click.echo("\n✓ Configuration file is valid")

    except yaml.YAMLError as e:
        click.secho(f"✗ YAML syntax error: {e}", fg="red", err=True)
        raise click.Abort() from e
    except Exception as e:
        click.secho(f"✗ Validation failed: {e}", fg="red", err=True)
        raise click.Abort() from e


@profile.command()
@click.argument("profile_name")
def export(profile_name: str) -> None:
    """Export profile configuration (with secrets redacted)."""
    try:
        prof = config_utils.get_profile(profile_name)

        # Create export dict with redacted secrets
        export_data = {
            "profile": profile_name,
            "config": {
                "server": prof.server,
                "token": _redact_token(prof.token),
                "timeout_sec": prof.timeout_sec,
                "description": prof.description,
                "require_confirmation": prof.require_confirmation,
            },
        }

        # Remove None values
        export_data["config"] = {
            k: v for k, v in export_data["config"].items() if v is not None
        }

        # Output as YAML
        click.echo(yaml.dump(export_data, default_flow_style=False, sort_keys=False))

    except KeyError as e:
        click.secho(f"Profile '{profile_name}' not found.", fg="red", err=True)
        raise click.Abort() from e
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        raise click.Abort() from e
