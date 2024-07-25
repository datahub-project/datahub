"""
For helper methods to contain manipulation of the config file in local system.
"""

import logging
import os
from typing import Optional

import click
import yaml

from datahub.cli.env_utils import get_boolean_env_variable

log = logging.getLogger(__name__)

DEFAULT_GMS_HOST = "http://localhost:8080"
CONDENSED_DATAHUB_CONFIG_PATH = "~/.datahubenv"
DATAHUB_CONFIG_PATH = os.path.expanduser(CONDENSED_DATAHUB_CONFIG_PATH)
DATAHUB_ROOT_FOLDER = os.path.expanduser("~/.datahub")
ENV_SKIP_CONFIG = "DATAHUB_SKIP_CONFIG"


def persist_datahub_config(config: dict) -> None:
    with open(DATAHUB_CONFIG_PATH, "w+") as outfile:
        yaml.dump(config, outfile, default_flow_style=False)
    return None


def should_skip_config() -> bool:
    return get_boolean_env_variable(ENV_SKIP_CONFIG, False)


def get_client_config() -> Optional[dict]:
    with open(DATAHUB_CONFIG_PATH) as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            click.secho(f"{DATAHUB_CONFIG_PATH} malformed, error: {exc}", bold=True)
            return None
