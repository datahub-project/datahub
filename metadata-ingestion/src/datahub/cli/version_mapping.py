
import json
from dataclasses import dataclass
from pydantic import BaseModel
from typing import Any, Dict, List
import click
import yaml
import requests
import os
class QuickstartVersionMapping(BaseModel):
    composefile_git_ref: str
    docker_tag: str

class StableVersions(BaseModel):
    force: bool
    composefile_git_ref: str
    docker_tag: str

class QuickstartChecks(BaseModel):
    valid_until_git_ref: str
    required_containers: List[str]
    ensure_exit_success: List[str]

class QuickstartVersionMappingConfig(BaseModel):
    quickstart_version_mappings: Dict[str, QuickstartVersionMapping]
    stable_versions: StableVersions
    quickstart_checks: List[QuickstartChecks]

DEFAULT_LOCAL_CONFIG_PATH = "~/.datahub/quickstart/quickstart_version_mapping.yaml"
DEFAULT_REMOTE_CONFIG_PATH = "https://raw.githubusercontent.com/datahub-project/datahub/quickstart-stability/docker/quickstart/quickstart_version_mapping.yaml"

def save_quickstart_config(config: QuickstartVersionMappingConfig, path: str = DEFAULT_LOCAL_CONFIG_PATH):
    # create directory if it doesn't exist
    path = os.path.expanduser(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(config.dict(), f)
    click.echo(f"Saved quickstart config to {path}.")


def fetch_quickstart_config() -> QuickstartVersionMappingConfig:
    response = None
    config_raw = None
    try:
        response = requests.get(DEFAULT_REMOTE_CONFIG_PATH, timeout=5)
        config_raw = yaml.safe_load(response.text)
    except:
        click.echo("Couldn't connect to github")
        path = os.path.expanduser(DEFAULT_LOCAL_CONFIG_PATH)
        with open(path, "r") as f:
            config_raw = yaml.safe_load(f)
    config = QuickstartVersionMappingConfig.parse_obj(config_raw)
    save_quickstart_config(config)
    return config


@click.group()
def test():
    pass

@test.command()
def qs_test():
    config = fetch_quickstart_config()

    print(config)

