
import json
from dataclasses import dataclass
from pydantic import BaseModel, PrivateAttr
from typing import Any, Dict, List, Optional, Tuple 
import click
import yaml
import requests
import os
import re

DEFAULT_LOCAL_CONFIG_PATH = "~/.datahub/quickstart/quickstart_version_mapping.yaml"
DEFAULT_REMOTE_CONFIG_PATH = "https://raw.githubusercontent.com/datahub-project/datahub/quickstart-stability/docker/quickstart/quickstart_version_mapping.yaml"

class QuickstartVersionMap(BaseModel):
    composefile_git_ref: str
    docker_tag: str

class QuickstartConstraints(BaseModel):
    valid_until_git_ref: str
    required_containers: List[str]
    ensure_exit_success: List[str]

class QuickstartExecutionPlan(BaseModel):
    docker_tag: str
    composefile_git_ref: str


class QuickstartVersionMappingConfig(BaseModel):
    quickstart_version_map: Dict[str, QuickstartVersionMap]

    @classmethod
    def _fetch_latest_version(cls) -> str:
        """
        Fetches the latest version from github.
        :return: The latest version.
        """
        response = requests.get("https://api.github.com/repos/datahub-project/datahub/releases/latest")
        response.raise_for_status()
        return json.loads(response.text)["tag_name"]

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

        # if stable is not defined in the config, we need to fetch the latest version from github
        if config.quickstart_version_map.get("stable") is None:
            try:
                release = cls._fetch_latest_version()
                config.quickstart_version_map["stable"] = QuickstartVersionMap(
                    composefile_git_ref=release, docker_tag=release
                )
            except:
                click.echo("Couldn't connect to github. --version stable will not work.")
        save_quickstart_config(config)
        return config

    def _parse_version(self, version: str) -> Tuple[int, int, int, int]:
        """
        Parses a version string into a tuple of integers.
        :param version: The version string to parse.
        :return: A tuple of integers representing the version.
        """
        version = re.sub(r"v", "", version)
        parsed_version = tuple(map(int, version.split(".")))
        # pad with zeros if necessary
        if len(parsed_version) == 2:
            parsed_version = parsed_version + (0, 0)
        elif len(parsed_version) == 3:
            parsed_version = parsed_version + (0,)
        return parsed_version

    def _compare_versions(self, version1: Tuple[int, int, int, int], version2: Tuple[int, int, int, int]) -> bool:
        """
        Compares two versions.
        :return: True if version1 is greater than version2, False otherwise.
        """
        for i in range(4):
            if version1[i] > version2[i]:
                return True
            elif version1[i] < version2[i]:
                return False
        return False

    def get_quickstart_execution_plan(self, requested_version: Optional[str]) -> QuickstartExecutionPlan:
        """
        From the requested version and stable flag, returns the execution plan for the quickstart.
        Including the docker tag, composefile git ref, required containers, and checks to run.
        :return: The execution plan for the quickstart.
        """
        if requested_version is None:
            requested_version = "default"
        version_map = self.quickstart_version_map.get(requested_version, QuickstartVersionMap(composefile_git_ref=requested_version, docker_tag=requested_version))
        return QuickstartExecutionPlan(
            docker_tag=version_map.docker_tag,
            composefile_git_ref=version_map.composefile_git_ref,
        )

def save_quickstart_config(config: QuickstartVersionMappingConfig, path: str = DEFAULT_LOCAL_CONFIG_PATH):
    # create directory if it doesn't exist
    path = os.path.expanduser(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(config.dict(), f)
    click.echo(f"Saved quickstart config to {path}.")
