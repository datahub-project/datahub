"""
For helper methods to contain manipulation of the config file in local system.
"""

import logging
import os
import sys
from typing import Optional, Tuple

import click
import yaml
from pydantic import BaseModel, ValidationError

from datahub.configuration.env_vars import (
    get_gms_host,
    get_gms_port,
    get_gms_protocol,
    get_gms_token,
    get_gms_url,
    get_skip_config,
    get_system_client_id,
    get_system_client_secret,
)
from datahub.ingestion.auth.env import ENV_AUTH_TYPE, build_auth_config_from_env
from datahub.ingestion.auth.oauth_session import (
    OAUTH_SESSION_AUTH_TYPE,
    has_oauth_session,
    read_session_token,
)
from datahub.ingestion.auth.registry import AuthConfig
from datahub.ingestion.graph.config import DatahubClientConfig

logger = logging.getLogger(__name__)

CONDENSED_DATAHUB_CONFIG_PATH = "~/.datahubenv"
DATAHUB_CONFIG_PATH: str = os.path.expanduser(CONDENSED_DATAHUB_CONFIG_PATH)
DATAHUB_ROOT_FOLDER: str = os.path.expanduser("~/.datahub")
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


class OAuthSessionConfig(BaseModel):
    """OAuth2 session tokens stored alongside GMS credentials in ~/.datahubenv."""

    client_id: str
    refresh_token: Optional[str] = None
    # Stored from the discovery document so refresh doesn't rely on a hardcoded path.
    token_endpoint: Optional[str] = None


class DatahubConfig(BaseModel):
    gms: DatahubClientConfig
    oauth: Optional[OAuthSessionConfig] = None


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


def get_url_from_env() -> Optional[str]:
    """The env-configured GMS URL, or None when the environment does not set one."""
    url, _ = _get_config_from_env()
    return url


def load_client_config(*, refresh_per_request: bool = False) -> DatahubClientConfig:
    """Resolve the client config from the environment or ~/.datahubenv.

    For a `datahub init --oauth` session the returned `token` is the session's
    current access token. With `refresh_per_request`, the config instead carries
    a token provider that refreshes it as it expires, for long-running clients.
    """
    # DATAHUB_AUTH_TYPE engages an env-configured OAuth token provider and takes
    # precedence over a static DATAHUB_GMS_TOKEN (the two are mutually exclusive
    # on DatahubClientConfig). Resolving it here is what lets processes that only
    # inherit environment variables — e.g. ingestion recipe subprocesses spawned
    # by the Remote Executor, whose default sink resolves through this function —
    # authenticate with short-lived OAuth tokens.
    auth_env = build_auth_config_from_env()
    if auth_env is not None and get_system_client_id() is not None:
        logger.warning(
            f"Both {ENV_AUTH_TYPE} and {ENV_DATAHUB_SYSTEM_CLIENT_ID} are set; "
            f"using {ENV_AUTH_TYPE} and ignoring the system client credentials."
        )

    gms_host_env, gms_token_env = _get_config_from_env()
    if gms_host_env:
        if auth_env is not None:
            return DatahubClientConfig(server=gms_host_env, auth=auth_env)
        # TODO We should also load system auth credentials here.
        return DatahubClientConfig(server=gms_host_env, token=gms_token_env)

    if _should_skip_config():
        raise MissingConfigError(
            "You have set the skip config flag, but no GMS host or token was provided in env variables."
        )

    try:
        _ensure_datahub_config()
        client_config_dict = get_raw_client_config()
        datahub_config: DatahubClientConfig = DatahubConfig.model_validate(
            client_config_dict
        ).gms
    except MissingConfigError:
        if auth_env is not None:
            # A fully env-configured OAuth container is missing only the server
            # URL — telling it to run `datahub init` would be misleading.
            raise MissingConfigError(
                f"{ENV_AUTH_TYPE} is set but no GMS server was provided. "
                f"Set {ENV_METADATA_HOST_URL} (or run `datahub init` to create "
                f"a {CONDENSED_DATAHUB_CONFIG_PATH} file)."
            ) from None
        raise
    except ValidationError as e:
        click.echo(f"Error loading your {CONDENSED_DATAHUB_CONFIG_PATH}")
        click.echo(e, err=True)
        sys.exit(1)

    if auth_env is not None:
        # Env-configured OAuth overrides a static token stored in the config
        # file, and supersedes the browser-flow (`datahub init --oauth`) token
        # refresh below.
        if datahub_config.token:
            logger.warning(
                f"{ENV_AUTH_TYPE} is set; ignoring the static token stored in "
                f"{CONDENSED_DATAHUB_CONFIG_PATH}."
            )
        return datahub_config.model_copy(update={"token": None, "auth": auth_env})

    if refresh_per_request and has_oauth_session(client_config_dict):
        return datahub_config.model_copy(
            update={
                "token": None,
                "auth": AuthConfig(
                    type=OAUTH_SESSION_AUTH_TYPE,
                    config={"config_file": DATAHUB_CONFIG_PATH},
                ),
            }
        )

    refreshed_token = refresh_oauth_token_if_needed()
    if refreshed_token is not None:
        datahub_config = datahub_config.model_copy(update={"token": refreshed_token})

    return datahub_config


def _ensure_datahub_config() -> None:
    if not os.path.isfile(DATAHUB_CONFIG_PATH):
        raise MissingConfigError(
            f"No {CONDENSED_DATAHUB_CONFIG_PATH} file found, and no configuration was found in environment variables. "
            f"Run `datahub init` to create a {CONDENSED_DATAHUB_CONFIG_PATH} file."
        )


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
        config_dict = {**previous_config, **config.model_dump(exclude={"oauth"})}
    else:
        config_dict = config.model_dump(exclude={"oauth"})
    persist_raw_datahub_config(config_dict)


def write_oauth_config(
    host: str,
    access_token: str,
    client_id: str,
    refresh_token: Optional[str],
    token_endpoint: Optional[str] = None,
) -> None:
    """Write GMS config together with OAuth2 session tokens to ~/.datahubenv."""
    config = DatahubConfig(gms=DatahubClientConfig(server=host, token=access_token))
    oauth = OAuthSessionConfig(
        client_id=client_id,
        refresh_token=refresh_token,
        token_endpoint=token_endpoint,
    )
    config_dict = config.model_dump()
    config_dict["oauth"] = oauth.model_dump(exclude_none=True)
    persist_raw_datahub_config(config_dict)


def refresh_oauth_token_if_needed() -> Optional[str]:
    """
    If OAuth2 session tokens are stored and the access token is within 5 minutes of
    expiry, refresh it using the stored refresh token.

    Returns the new access token if refreshed, None if no refresh was needed or if
    the refresh fails (failures are non-fatal — the existing token is left in place).
    """
    if not os.path.isfile(DATAHUB_CONFIG_PATH):
        return None

    try:
        raw = get_raw_client_config()
        if raw is None or not has_oauth_session(raw):
            return None
        token, _ = read_session_token(DATAHUB_CONFIG_PATH)
        return token if token != raw["gms"].get("token") else None
    except Exception as e:
        logger.debug("OAuth2 token refresh failed (non-fatal): %s", e, exc_info=True)
        return None
