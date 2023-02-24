
import json
from dataclasses import dataclass
from pydantic import BaseModel
from typing import Any, Dict, List, Optional, Tuple
import click
import yaml
import requests
import os
import re

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

class QuickstartExecutionPlan(BaseModel):
    docker_tag: str
    composefile_git_ref: str
    required_containers: List[str]
    ensure_exit_success: List[str]


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

    def _get_version_to_use(self, requested_version: Optional[str], stable: Optional[bool] = False) -> Tuple[str, str]: 
        """
        Returns the docker tag and composefile git ref to use for the quickstart.
        :return: A tuple of the docker tag and composefile git ref to use.
        """
        # stable is requested or stable is forced
        if self.stable_versions.force or stable:
            return self.stable_versions.docker_tag, self.stable_versions.composefile_git_ref
        if requested_version is None:
            return self.quickstart_version_mappings["default"].docker_tag, self.quickstart_version_mappings["default"].composefile_git_ref
        elif requested_version in self.quickstart_version_mappings:
            return self.quickstart_version_mappings[requested_version].docker_tag, self.quickstart_version_mappings[requested_version].composefile_git_ref,
        else:
            return requested_version, requested_version

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

    def _get_checks_for_version(self, compose_git_ref: str = "master") -> QuickstartChecks:
        """
        Returns the quickstart checks for the requested docker compose version.
        :return: The checks for the quickstart.
        """
        if compose_git_ref == "master":
            return self.quickstart_checks[0]
        parsed_compose_version = self._parse_version(compose_git_ref)
        # going from oldest to newest 
        for check in reversed(self.quickstart_checks):
            if check.valid_until_git_ref == "master":
                return check
            valid_until_version = self._parse_version(check.valid_until_git_ref)
            if not self._compare_versions(parsed_compose_version, valid_until_version):
                return check
        raise Exception(f"Couldn't find a valid execution plan for version {compose_git_ref}.") 

    def get_quickstart_execution_plan(self, requested_version: Optional[str], stable: Optional[bool] = False) -> QuickstartExecutionPlan:
        """
        From the requested version and stable flag, returns the execution plan for the quickstart.
        Including the docker tag, composefile git ref, required containers, and checks to run.
        :return: The execution plan for the quickstart.
        """
        docker_tag, git_ref = self._get_version_to_use(requested_version, stable)
        checks_to_run = self._get_checks_for_version(git_ref)
        return QuickstartExecutionPlan(
            docker_tag=docker_tag,
            composefile_git_ref=git_ref,
            required_containers=checks_to_run.required_containers,
            ensure_exit_success=checks_to_run.ensure_exit_success
        )

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
@click.option("--version", default=None, help="The version of the quickstart to use.")
def qs_test(version):
    config = QuickstartVersionMappingConfig.fetch_quickstart_config()
    plan = config.get_quickstart_execution_plan(version)
    print(plan)

