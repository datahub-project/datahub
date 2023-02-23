
import json
from dataclasses import dataclass
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
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

class QuickstartVersionToUse(BaseModel):
     docker_tag: str
     composefile_git_ref: str

class QuickstartVersionMappingConfig(BaseModel):
    quickstart_version_mappings: Dict[str, QuickstartVersionMapping]
    stable_versions: StableVersions
    quickstart_checks: List[QuickstartChecks]

    @classmethod
    def fetch_quickstart_config(cls):
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
        config = cls.parse_obj(config_raw)
        save_quickstart_config(config)
        return config

    def get_version_to_use(self, requested_version: Optional[str], stable: Optional[bool] = False) -> QuickstartVersionToUse: 
        # stable is requested or stable is forced
        if self.stable_versions.force or stable:
            return QuickstartVersionToUse(
                docker_tag=self.stable_versions.docker_tag,
                composefile_git_ref=self.stable_versions.composefile_git_ref,
            )
        if requested_version is None:
            return QuickstartVersionToUse(
                docker_tag=self.quickstart_version_mappings["default"].docker_tag,
                composefile_git_ref=self.quickstart_version_mappings["default"].composefile_git_ref,
            )
        elif requested_version in self.quickstart_version_mappings:
            return QuickstartVersionToUse(
                docker_tag=self.quickstart_version_mappings[requested_version].docker_tag,
                composefile_git_ref=self.quickstart_version_mappings[requested_version].composefile_git_ref,
            )
        else:
            return QuickstartVersionToUse(
                docker_tag=requested_version,
                composefile_git_ref=requested_version,
                )






#     def get_version_to_use(self, requested_version: str, stable: bool = False) -> QuickstartVersionMapping:

DEFAULT_LOCAL_CONFIG_PATH = "~/.datahub/quickstart/quickstart_version_mapping.yaml"
DEFAULT_REMOTE_CONFIG_PATH = "https://raw.githubusercontent.com/datahub-project/datahub/quickstart-stability/docker/quickstart/quickstart_version_mapping.yaml"

def save_quickstart_config(config: QuickstartVersionMappingConfig, path: str = DEFAULT_LOCAL_CONFIG_PATH):
    # create directory if it doesn't exist
    path = os.path.expanduser(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(config.dict(), f)
    click.echo(f"Saved quickstart config to {path}.")





@click.group()
def test():
    pass

@test.command()
def qs_test():
    config = QuickstartVersionMappingConfig.fetch_quickstart_config()

    print(config)

