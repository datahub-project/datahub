"""
For helper methods to contain manipulation of the config file in local system.
"""

import logging
import os
from typing import Optional, Tuple

import click
import yaml
from pydantic import BaseModel, ValidationError

from datahub.configuration.config import DataHubConfig, ProfileConfig
from datahub.configuration.env_vars import (
    get_active_profile,
    get_gms_host,
    get_gms_port,
    get_gms_protocol,
    get_gms_token,
    get_gms_url,
    get_skip_config,
    get_system_client_id,
    get_system_client_secret,
)
from datahub.ingestion.graph.config import DatahubClientConfig

logger = logging.getLogger(__name__)

CONDENSED_DATAHUB_CONFIG_PATH = "~/.datahubenv"
DATAHUB_CONFIG_PATH: str = os.path.expanduser(CONDENSED_DATAHUB_CONFIG_PATH)
DATAHUB_ROOT_FOLDER: str = os.path.expanduser("~/.datahub")
DATAHUB_CONFIG_DIR: str = os.path.expanduser("~/.datahub")
DATAHUB_NEW_CONFIG_PATH: str = os.path.join(DATAHUB_CONFIG_DIR, "config.yaml")
ENV_SKIP_CONFIG = "DATAHUB_SKIP_CONFIG"

ENV_DATAHUB_SYSTEM_CLIENT_ID = "DATAHUB_SYSTEM_CLIENT_ID"
ENV_DATAHUB_SYSTEM_CLIENT_SECRET = "DATAHUB_SYSTEM_CLIENT_SECRET"

ENV_METADATA_HOST_URL = "DATAHUB_GMS_URL"
ENV_METADATA_TOKEN = "DATAHUB_GMS_TOKEN"
ENV_METADATA_HOST = "DATAHUB_GMS_HOST"
ENV_METADATA_PORT = "DATAHUB_GMS_PORT"
ENV_METADATA_PROTOCOL = "DATAHUB_GMS_PROTOCOL"


class MissingConfigError(Exception):
    SHOW_STACK_TRACE = False


def get_system_auth() -> Optional[str]:
    system_client_id = get_system_client_id()
    system_client_secret = get_system_client_secret()
    if system_client_id is not None and system_client_secret is not None:
        return f"Basic {system_client_id}:{system_client_secret}"
    return None


def _should_skip_config() -> bool:
    return get_skip_config()


def persist_raw_datahub_config(config: dict) -> None:
    with open(DATAHUB_CONFIG_PATH, "w+") as outfile:
        yaml.dump(config, outfile, default_flow_style=False)
    return None


def get_raw_client_config() -> Optional[dict]:
    with open(DATAHUB_CONFIG_PATH) as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            click.secho(f"{DATAHUB_CONFIG_PATH} malformed, error: {exc}", bold=True)
            return None


class DatahubConfig(BaseModel):
    gms: DatahubClientConfig


def _get_config_from_env() -> Tuple[Optional[str], Optional[str]]:
    host = get_gms_host()
    port = get_gms_port()
    token = get_gms_token()
    protocol = get_gms_protocol()
    url = get_gms_url()
    if port is not None:
        url = f"{protocol}://{host}:{port}"
        return url, token
    # The reason for using host as URL is backward compatibility
    # If port is not being used we assume someone is using host env var as URL
    if url is None and host is not None:
        logger.warning(
            f"Do not use {ENV_METADATA_HOST} as URL. Use {ENV_METADATA_HOST_URL} instead"
        )
    return url or host, token


def require_config_from_env() -> Tuple[str, Optional[str]]:
    host, token = _get_config_from_env()
    if host is None:
        raise MissingConfigError("No GMS host was provided in env variables.")
    return host, token


def load_client_config(profile: Optional[str] = None) -> DatahubClientConfig:
    """
    Load DataHub client configuration with profile support.

    This function provides backward compatibility while supporting the new profile system.

    Args:
        profile: Optional profile name to load. If not provided, uses precedence order.

    Returns:
        DatahubClientConfig with all overrides applied

    Raises:
        MissingConfigError: If no configuration is found
    """
    # Check if we should use env vars exclusively
    gms_host_env, gms_token_env = _get_config_from_env()
    if gms_host_env and _should_skip_config():
        # Skip config file entirely
        return DatahubClientConfig(server=gms_host_env, token=gms_token_env)

    # If env vars are set, they will override profile settings in load_profile_config
    if gms_host_env and not profile:
        # For backward compatibility: if env vars are set and no profile specified,
        # just use env vars
        return DatahubClientConfig(server=gms_host_env, token=gms_token_env)

    # Try the new profile system first
    try:
        return load_profile_config(profile)
    except MissingConfigError:
        # If profile system fails and we have env vars, use them
        if gms_host_env:
            return DatahubClientConfig(server=gms_host_env, token=gms_token_env)

        # If skip config is set but no env vars, error
        if _should_skip_config():
            raise MissingConfigError(
                "You have set the skip config flag, but no GMS host or token was provided in env variables."
            ) from None

        # No config found at all
        raise


def _ensure_datahub_config() -> None:
    if not os.path.isfile(DATAHUB_CONFIG_PATH):
        raise MissingConfigError(
            f"No {CONDENSED_DATAHUB_CONFIG_PATH} file found, and no configuration was found in environment variables. "
            f"Run `datahub init` to create a {CONDENSED_DATAHUB_CONFIG_PATH} file."
        )


# ============================================================================
# New Profile Management Functions
# ============================================================================


def ensure_datahub_dir() -> None:
    """Create ~/.datahub/ directory if it doesn't exist."""
    os.makedirs(DATAHUB_CONFIG_DIR, exist_ok=True)


def get_config_file_path() -> str:
    """
    Get the path to the config file, preferring new format over legacy.

    Returns:
        Path to the config file (new or legacy)
    """
    if os.path.isfile(DATAHUB_NEW_CONFIG_PATH):
        return DATAHUB_NEW_CONFIG_PATH
    elif os.path.isfile(DATAHUB_CONFIG_PATH):
        return DATAHUB_CONFIG_PATH
    else:
        return DATAHUB_NEW_CONFIG_PATH


def _load_new_config() -> Optional[DataHubConfig]:
    """Load configuration from new ~/.datahub/config.yaml format."""
    if not os.path.isfile(DATAHUB_NEW_CONFIG_PATH):
        return None

    try:
        with open(DATAHUB_NEW_CONFIG_PATH) as f:
            config_dict = yaml.safe_load(f)
            if not config_dict:
                return None
            return DataHubConfig.model_validate(config_dict)
    except (yaml.YAMLError, ValidationError) as e:
        logger.error(f"Error loading {DATAHUB_NEW_CONFIG_PATH}: {e}")
        raise


def _determine_profile_name(
    profile_name: Optional[str], config: Optional[DataHubConfig]
) -> Optional[str]:
    """
    Determine which profile to use based on precedence order.

    Precedence (highest to lowest):
    1. Explicit profile_name parameter
    2. DATAHUB_PROFILE environment variable
    3. current_profile setting in config
    4. Profile named "default"
    5. None (will fall back to legacy or env vars)

    Args:
        profile_name: Explicitly requested profile name
        config: Loaded DataHubConfig (if available)

    Returns:
        Profile name to use, or None if no profile should be used
    """
    # 1. Explicit profile parameter
    if profile_name:
        return profile_name

    # 2. Environment variable
    env_profile = get_active_profile()
    if env_profile:
        return env_profile

    # 3. Current profile setting
    if config and config.current_profile:
        return config.current_profile

    # 4. Profile named "default"
    if config and "default" in config.profiles:
        return "default"

    return None


def load_profile_config(profile_name: Optional[str] = None) -> DatahubClientConfig:
    """
    Load DataHub client config from profile system with full precedence handling.

    Profile selection precedence (highest to lowest):
    1. Explicit profile_name parameter
    2. DATAHUB_PROFILE environment variable
    3. current_profile setting in config.yaml
    4. Profile named "default"
    5. Legacy ~/.datahubenv
    6. Error if no config found

    Config value overrides (applied AFTER profile selection):
    - DATAHUB_GMS_URL overrides profile's server
    - DATAHUB_GMS_TOKEN overrides profile's token
    - Other DATAHUB_* env vars override corresponding profile settings

    Args:
        profile_name: Specific profile to load (optional)

    Returns:
        DatahubClientConfig with all overrides applied

    Raises:
        MissingConfigError: If no configuration is found
        KeyError: If specified profile doesn't exist
    """
    # Try loading new config format
    config = _load_new_config()

    # Determine which profile to use
    selected_profile = _determine_profile_name(profile_name, config)

    # If we have a profile, use it
    if config and selected_profile:
        try:
            profile_config = config.get_profile(selected_profile)
            # Create a new DatahubClientConfig from the profile
            client_config = DatahubClientConfig(
                server=profile_config.server,
                token=profile_config.token,
                timeout_sec=profile_config.timeout_sec,
                retry_status_codes=profile_config.retry_status_codes,
                retry_max_times=profile_config.retry_max_times,
                extra_headers=profile_config.extra_headers,
                ca_certificate_path=profile_config.ca_certificate_path,
                client_certificate_path=profile_config.client_certificate_path,
                disable_ssl_verification=profile_config.disable_ssl_verification,
            )
        except KeyError as e:
            available = ", ".join(config.list_profiles())
            raise MissingConfigError(
                f"Profile '{selected_profile}' not found. Available profiles: {available}"
            ) from e
    else:
        # Fall back to legacy config if it exists
        if os.path.isfile(DATAHUB_CONFIG_PATH):
            try:
                client_config_dict = get_raw_client_config()
                datahub_config = DatahubConfig.model_validate(client_config_dict)
                client_config = datahub_config.gms
            except (ValidationError, AttributeError) as e:
                # Legacy format might not have the new structure
                raise MissingConfigError(
                    f"Could not load config from {DATAHUB_CONFIG_PATH}. "
                    f"Run `datahub init` to create a new configuration."
                ) from e
        else:
            raise MissingConfigError(
                "No configuration found. Run `datahub init` to create a configuration file."
            )

    # Apply environment variable overrides (these always win)
    gms_host_env, gms_token_env = _get_config_from_env()
    if gms_host_env:
        client_config.server = gms_host_env
    if gms_token_env:
        client_config.token = gms_token_env

    return client_config


def save_profile_config(config: DataHubConfig) -> None:
    """Write DataHubConfig to ~/.datahub/config.yaml."""
    ensure_datahub_dir()
    with open(DATAHUB_NEW_CONFIG_PATH, "w") as f:
        # Convert to dict and write as YAML
        config_dict = config.model_dump(exclude_none=True, exclude_unset=False)
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)


def list_profiles() -> list[str]:
    """List all available profile names."""
    config = _load_new_config()
    if config:
        return config.list_profiles()
    return []


def get_current_profile() -> Optional[str]:
    """Get the name of the currently active profile."""
    config = _load_new_config()
    if not config:
        return None
    return _determine_profile_name(None, config)


def set_current_profile(profile_name: str) -> None:
    """
    Set the current active profile.

    Args:
        profile_name: Name of the profile to make active

    Raises:
        MissingConfigError: If config file doesn't exist
        KeyError: If profile doesn't exist
    """
    config = _load_new_config()
    if not config:
        raise MissingConfigError(
            "No configuration file found. Run `datahub init` first."
        )

    # Verify profile exists
    if profile_name not in config.profiles:
        available = ", ".join(config.list_profiles())
        raise KeyError(
            f"Profile '{profile_name}' not found. Available profiles: {available}"
        )

    config.current_profile = profile_name
    save_profile_config(config)


def profile_exists(profile_name: str) -> bool:
    """Check if a profile exists."""
    config = _load_new_config()
    if not config:
        return False
    return profile_name in config.profiles


def get_profile(profile_name: str) -> ProfileConfig:
    """
    Get a specific profile configuration.

    Args:
        profile_name: Name of the profile to retrieve

    Returns:
        ProfileConfig for the requested profile

    Raises:
        MissingConfigError: If config file doesn't exist
        KeyError: If profile doesn't exist
    """
    config = _load_new_config()
    if not config:
        raise MissingConfigError(
            "No configuration file found. Run `datahub init` first."
        )
    return config.get_profile(profile_name)


def add_profile(profile_name: str, profile_config: ProfileConfig) -> None:
    """
    Add or update a profile.

    Args:
        profile_name: Name of the profile
        profile_config: Profile configuration

    Raises:
        MissingConfigError: If config file doesn't exist
    """
    config = _load_new_config()
    if not config:
        # Create new config if it doesn't exist
        config = DataHubConfig(profiles={})

    config.add_profile(profile_name, profile_config)
    save_profile_config(config)


def remove_profile(profile_name: str) -> None:
    """
    Remove a profile.

    Args:
        profile_name: Name of the profile to remove

    Raises:
        MissingConfigError: If config file doesn't exist
        KeyError: If profile doesn't exist
    """
    config = _load_new_config()
    if not config:
        raise MissingConfigError(
            "No configuration file found. Run `datahub init` first."
        )

    config.remove_profile(profile_name)

    # If we just removed the current profile, clear it
    if config.current_profile == profile_name:
        config.current_profile = None

    save_profile_config(config)


def migrate_legacy_config() -> bool:
    """
    Migrate legacy ~/.datahubenv to new profile system.

    Returns:
        True if migration was performed, False if not needed

    Raises:
        MissingConfigError: If legacy config doesn't exist
    """
    # Check if legacy config exists
    if not os.path.isfile(DATAHUB_CONFIG_PATH):
        raise MissingConfigError(f"No legacy config found at {DATAHUB_CONFIG_PATH}")

    # Check if new config already exists
    if os.path.isfile(DATAHUB_NEW_CONFIG_PATH):
        logger.info(
            f"New config already exists at {DATAHUB_NEW_CONFIG_PATH}, skipping migration"
        )
        return False

    # Load legacy config
    try:
        legacy_config_dict = get_raw_client_config()
        legacy_datahub_config = DatahubConfig.model_validate(legacy_config_dict)
        legacy_client_config = legacy_datahub_config.gms
    except (ValidationError, AttributeError) as e:
        raise MissingConfigError(
            f"Could not parse legacy config at {DATAHUB_CONFIG_PATH}: {e}"
        ) from e

    # Create new config with "default" profile
    new_config = DataHubConfig(
        version="1.0",
        current_profile="default",
        profiles={
            "default": ProfileConfig(
                server=legacy_client_config.server,
                token=legacy_client_config.token,
                timeout_sec=legacy_client_config.timeout_sec,
                description="Migrated from legacy config",
            )
        },
    )

    # Save new config
    save_profile_config(new_config)
    logger.info(
        f"Migrated legacy config to {DATAHUB_NEW_CONFIG_PATH} as 'default' profile"
    )
    return True


# ============================================================================
# Safety and Confirmation Functions
# ============================================================================


def get_profile_for_confirmation(
    profile_name: Optional[str] = None,
) -> Optional[ProfileConfig]:
    """
    Get profile configuration for confirmation checks.

    Args:
        profile_name: Explicit profile name, or None to use current

    Returns:
        ProfileConfig if found, None if using legacy config or env vars only
    """
    try:
        config = _load_new_config()
        if not config:
            return None

        selected_profile = _determine_profile_name(profile_name, config)
        if not selected_profile:
            return None

        return config.get_profile(selected_profile)
    except Exception:
        return None


def requires_confirmation(profile_name: Optional[str] = None) -> bool:
    """
    Check if the current or specified profile requires confirmation for destructive operations.

    Args:
        profile_name: Explicit profile name, or None to use current

    Returns:
        True if confirmation is required, False otherwise
    """
    profile = get_profile_for_confirmation(profile_name)
    return profile.require_confirmation if profile else False


def confirm_destructive_operation(
    operation: str,
    profile_name: Optional[str] = None,
    force: bool = False,
    extra_info: Optional[str] = None,
) -> bool:
    """
    Prompt user to confirm a destructive operation if profile requires it.

    Args:
        operation: Description of the operation (e.g., "delete entities", "update dataset")
        profile_name: Explicit profile name, or None to use current
        force: If True, skip confirmation prompt
        extra_info: Additional context to show user (e.g., URNs being affected)

    Returns:
        True if user confirms or confirmation not required, False otherwise
    """
    # If force flag is set, skip confirmation
    if force:
        return True

    # Check if confirmation is required
    profile = get_profile_for_confirmation(profile_name)
    if not profile or not profile.require_confirmation:
        return True

    # Get profile details for display
    config = _load_new_config()
    selected_profile = "unknown"
    if config:
        determined_name = _determine_profile_name(profile_name, config)
        if determined_name:
            selected_profile = determined_name
    server_url = profile.server if profile.server else "unknown"

    # Show warning
    click.echo()
    click.secho("⚠️  CONFIRMATION REQUIRED ⚠️", fg="red", bold=True)
    click.echo()
    click.secho(f"Profile: {selected_profile}", fg="yellow")
    click.secho(f"Server: {server_url}", fg="yellow")
    click.secho(f"Operation: {operation}", fg="yellow")
    if profile.description:
        click.secho(f"Description: {profile.description}", fg="yellow")
    if extra_info:
        click.echo()
        click.echo(extra_info)
    click.echo()

    # Prompt for confirmation
    click.secho(
        "⚠️  You are about to perform a destructive operation on the above profile.",
        fg="red",
    )
    click.echo()

    confirmation_text = selected_profile.upper()
    user_input = click.prompt(
        f"Type '{confirmation_text}' to confirm, or anything else to cancel",
        type=str,
        default="",
    )

    confirmed = user_input == confirmation_text
    if confirmed:
        click.secho("✓ Confirmed. Proceeding...", fg="green")
        click.echo()
    else:
        click.secho("✗ Confirmation failed. Operation cancelled.", fg="red")
        click.echo()

    return confirmed


# ============================================================================
# Legacy Functions (for backward compatibility)
# ============================================================================


def write_gms_config(
    host: str, token: Optional[str], merge_with_previous: bool = True
) -> None:
    config = DatahubConfig(gms=DatahubClientConfig(server=host, token=token))
    if merge_with_previous:
        try:
            previous_config = get_raw_client_config()
            assert isinstance(previous_config, dict)
        except Exception as e:
            # ok to fail on this
            previous_config = {}
            logger.debug(
                f"Failed to retrieve config from file {DATAHUB_CONFIG_PATH}: {e}. This isn't fatal."
            )
        config_dict = {**previous_config, **config.model_dump()}
    else:
        config_dict = config.model_dump()
    persist_raw_datahub_config(config_dict)
