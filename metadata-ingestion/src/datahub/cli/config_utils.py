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

from datahub.cli.env_utils import get_boolean_env_variable
from datahub.ingestion.graph.config import DatahubClientConfig

logger = logging.getLogger(__name__)

CONDENSED_DATAHUB_CONFIG_PATH = "~/.datahubenv"
DATAHUB_CONFIG_PATH = os.path.expanduser(CONDENSED_DATAHUB_CONFIG_PATH)
DATAHUB_ROOT_FOLDER = os.path.expanduser("~/.datahub")
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
    system_client_id = os.environ.get(ENV_DATAHUB_SYSTEM_CLIENT_ID)
    system_client_secret = os.environ.get(ENV_DATAHUB_SYSTEM_CLIENT_SECRET)
    if system_client_id is not None and system_client_secret is not None:
        return f"Basic {system_client_id}:{system_client_secret}"
    return None


def _should_skip_config() -> bool:
    return get_boolean_env_variable(ENV_SKIP_CONFIG, False)


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
    host = os.environ.get(ENV_METADATA_HOST)
    port = os.environ.get(ENV_METADATA_PORT)
    token = os.environ.get(ENV_METADATA_TOKEN)
    protocol = os.environ.get(ENV_METADATA_PROTOCOL, "http")
    url = os.environ.get(ENV_METADATA_HOST_URL)
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
        datahub_config: DatahubClientConfig = DatahubConfig.parse_obj(
            client_config_dict
        ).gms

        return datahub_config
    except ValidationError as e:
        click.echo(f"Error loading your {CONDENSED_DATAHUB_CONFIG_PATH}")
        click.echo(e, err=True)
        sys.exit(1)


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
        config_dict = {**previous_config, **config.dict()}
    else:
        config_dict = config.dict()
    persist_raw_datahub_config(config_dict)
