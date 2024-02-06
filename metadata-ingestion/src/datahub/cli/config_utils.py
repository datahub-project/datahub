"""
For helper methods to contain manipulation of the config file in local system.
"""

import logging
import os
import sys
from typing import Optional, Union

import click
import yaml
from pydantic import BaseModel, ValidationError

from datahub.cli.env_utils import get_boolean_env_variable

log = logging.getLogger(__name__)

DEFAULT_GMS_HOST = "http://localhost:8080"
CONDENSED_DATAHUB_CONFIG_PATH = "~/.datahubenv"
DATAHUB_CONFIG_PATH = os.path.expanduser(CONDENSED_DATAHUB_CONFIG_PATH)
DATAHUB_ROOT_FOLDER = os.path.expanduser("~/.datahub")
ENV_SKIP_CONFIG = "DATAHUB_SKIP_CONFIG"


class GmsConfig(BaseModel):
    server: str
    token: Optional[str] = None


class DatahubConfig(BaseModel):
    gms: GmsConfig


def persist_datahub_config(config: dict) -> None:
    with open(DATAHUB_CONFIG_PATH, "w+") as outfile:
        yaml.dump(config, outfile, default_flow_style=False)
    return None


def write_gms_config(
    host: str, token: Optional[str], merge_with_previous: bool = True
) -> None:
    config = DatahubConfig(gms=GmsConfig(server=host, token=token))
    if merge_with_previous:
        try:
            previous_config = get_client_config(as_dict=True)
            assert isinstance(previous_config, dict)
        except Exception as e:
            # ok to fail on this
            previous_config = {}
            log.debug(
                f"Failed to retrieve config from file {DATAHUB_CONFIG_PATH}: {e}. This isn't fatal."
            )
        config_dict = {**previous_config, **config.dict()}
    else:
        config_dict = config.dict()
    persist_datahub_config(config_dict)


def get_details_from_config():
    datahub_config = get_client_config(as_dict=False)
    assert isinstance(datahub_config, DatahubConfig)
    if datahub_config is not None:
        gms_config = datahub_config.gms

        gms_host = gms_config.server
        gms_token = gms_config.token
        return gms_host, gms_token
    else:
        return None, None


def should_skip_config() -> bool:
    return get_boolean_env_variable(ENV_SKIP_CONFIG, False)


def ensure_datahub_config() -> None:
    if not os.path.isfile(DATAHUB_CONFIG_PATH):
        click.secho(
            f"No {CONDENSED_DATAHUB_CONFIG_PATH} file found, generating one for you...",
            bold=True,
        )
        write_gms_config(DEFAULT_GMS_HOST, None)


def get_client_config(as_dict: bool = False) -> Union[Optional[DatahubConfig], dict]:
    with open(DATAHUB_CONFIG_PATH, "r") as stream:
        try:
            config_json = yaml.safe_load(stream)
            if as_dict:
                return config_json
            try:
                datahub_config = DatahubConfig.parse_obj(config_json)
                return datahub_config
            except ValidationError as e:
                click.echo(
                    f"Received error, please check your {CONDENSED_DATAHUB_CONFIG_PATH}"
                )
                click.echo(e, err=True)
                sys.exit(1)
        except yaml.YAMLError as exc:
            click.secho(f"{DATAHUB_CONFIG_PATH} malformed, error: {exc}", bold=True)
            return None
