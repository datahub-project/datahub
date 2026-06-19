"""
For helper methods to contain manipulation of the config file in local system.
"""

import base64
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional, Tuple

import click
import requests
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


def _decode_jwt_exp(token: str) -> Optional[datetime]:
    """Decode the exp claim from a JWT without verifying the signature."""
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload_b64 = parts[1]
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        exp = json.loads(base64.urlsafe_b64decode(payload_b64)).get("exp")
        return datetime.fromtimestamp(int(exp), tz=timezone.utc) if exp else None
    except (IndexError, json.JSONDecodeError, ValueError, OverflowError):
        return None


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


def load_client_config() -> DatahubClientConfig:
    gms_host_env, gms_token_env = _get_config_from_env()
    if gms_host_env:
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
    except ValidationError as e:
        click.echo(f"Error loading your {CONDENSED_DATAHUB_CONFIG_PATH}")
        click.echo(e, err=True)
        sys.exit(1)

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
        if not isinstance(raw, dict):
            return None

        oauth_section = raw.get("oauth")
        if not isinstance(oauth_section, dict):
            return None

        refresh_token = oauth_section.get("refresh_token")
        client_id = oauth_section.get("client_id")

        if not refresh_token or not client_id:
            return None

        # Decode expiry from the JWT — no exp claim means no refresh needed
        gms_token = (
            raw.get("gms", {}).get("token")
            if isinstance(raw.get("gms"), dict)
            else None
        )
        token_expiry = _decode_jwt_exp(gms_token) if gms_token else None
        if token_expiry is None:
            return None
        if (token_expiry - datetime.now(tz=timezone.utc)).total_seconds() > 300:
            return None

        gms_config = raw.get("gms", {})
        gms_server = gms_config.get("server") if isinstance(gms_config, dict) else None
        if not gms_server:
            return None

        # Prefer the token_endpoint stored from the discovery document; fall back to
        # the conventional path so configs written before this field was added still work.
        stored_endpoint = oauth_section.get("token_endpoint")
        token_endpoint = (
            stored_endpoint or f"{gms_server.rstrip('/')}/auth/oauth2/token"
        )
        logger.debug("Refreshing OAuth2 access token...")

        resp = requests.post(
            token_endpoint,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=5,
        )

        if resp.status_code != 200:
            logger.debug("Token refresh failed: HTTP %s", resp.status_code)
            return None

        token_data = resp.json()
        new_access_token: Optional[str] = token_data.get("access_token")
        if not new_access_token:
            logger.debug("Token refresh returned empty access token")
            return None

        # Rotate refresh token if server issues a new one (RFC 6749 §10.4)
        new_refresh_token: str = token_data.get("refresh_token") or refresh_token

        raw["gms"]["token"] = new_access_token
        raw["oauth"]["refresh_token"] = new_refresh_token
        raw["oauth"].pop(
            "token_expiry", None
        )  # remove legacy field from dev-iteration configs

        persist_raw_datahub_config(raw)
        logger.debug("OAuth2 access token refreshed successfully")
        return new_access_token

    except Exception as e:
        logger.debug("OAuth2 token refresh failed (non-fatal): %s", e, exc_info=True)
        return None
