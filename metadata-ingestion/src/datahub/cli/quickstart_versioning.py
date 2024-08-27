import json
import logging
import os
import os.path
import re
from typing import Dict, Optional

import click
import requests
import yaml
from packaging.version import parse
from pydantic import BaseModel

logger = logging.getLogger(__name__)

LOCAL_QUICKSTART_MAPPING_FILE = os.environ.get("FORCE_LOCAL_QUICKSTART_MAPPING", "")
DEFAULT_LOCAL_CONFIG_PATH = "~/.datahub/quickstart/quickstart_version_mapping.yaml"
DEFAULT_REMOTE_CONFIG_PATH = "https://raw.githubusercontent.com/datahub-project/datahub/master/docker/quickstart/quickstart_version_mapping.yaml"


class QuickstartExecutionPlan(BaseModel):
    composefile_git_ref: str
    docker_tag: str
    mysql_tag: Optional[str]


def _is_it_a_version(version: str) -> bool:
    """
    Checks if a string is a valid version.
    :param version: The string to check.
    :return: True if the string is a valid version, False otherwise.
    """
    return re.match(r"^v?\d+\.\d+(\.\d+)?$", version) is not None


class QuickstartVersionMappingConfig(BaseModel):
    quickstart_version_map: Dict[str, QuickstartExecutionPlan]

    @classmethod
    def _fetch_latest_version(cls) -> str:
        """
        Fetches the latest version from github.
        :return: The latest version.
        """
        response = requests.get(
            "https://api.github.com/repos/datahub-project/datahub/releases/latest"
        )
        response.raise_for_status()
        return json.loads(response.text)["tag_name"]

    @classmethod
    def fetch_quickstart_config(cls) -> "QuickstartVersionMappingConfig":
        if LOCAL_QUICKSTART_MAPPING_FILE:
            logger.info(
                "LOCAL_QUICKSTART_MAPPING_FILE is set, will try to read from local file."
            )
            path = os.path.expanduser(LOCAL_QUICKSTART_MAPPING_FILE)
            with open(path) as f:
                config_raw = yaml.safe_load(f)
            return cls.parse_obj(config_raw)

        config_raw = None
        try:
            response = requests.get(DEFAULT_REMOTE_CONFIG_PATH, timeout=5)
            response.raise_for_status()
            config_raw = yaml.safe_load(response.text)
        except Exception as e:
            logger.debug(
                f"Couldn't connect to github: {e}, will try to read from local file."
            )
            try:
                path = os.path.expanduser(DEFAULT_LOCAL_CONFIG_PATH)
                with open(path) as f:
                    config_raw = yaml.safe_load(f)
            except Exception:
                logger.debug("Couldn't read from local file either.")

        if config_raw is None:
            logger.info(
                "Unable to connect to GitHub, using default quickstart version mapping config."
            )
            return QuickstartVersionMappingConfig(
                quickstart_version_map={
                    "default": QuickstartExecutionPlan(
                        composefile_git_ref="master", docker_tag="head", mysql_tag="8.2"
                    ),
                }
            )

        config = cls.parse_obj(config_raw)

        # If stable is not defined in the config, we need to fetch the latest version from github.
        if config.quickstart_version_map.get("stable") is None:
            try:
                release = cls._fetch_latest_version()
                config.quickstart_version_map["stable"] = QuickstartExecutionPlan(
                    composefile_git_ref=release, docker_tag=release, mysql_tag="8.2"
                )
            except Exception:
                click.echo(
                    "Couldn't connect to github. --version stable will not work."
                )
        save_quickstart_config(config)
        return config

    def get_quickstart_execution_plan(
        self,
        requested_version: Optional[str],
    ) -> QuickstartExecutionPlan:
        """
        From the requested version and stable flag, returns the execution plan for the quickstart.
        Including the docker tag, composefile git ref, required containers, and checks to run.
        :return: The execution plan for the quickstart.
        """
        if requested_version is None:
            requested_version = "default"
        composefile_git_ref = requested_version
        docker_tag = requested_version
        # Default to 8.2 if not specified in version map
        mysql_tag = "8.2"
        result = self.quickstart_version_map.get(
            requested_version,
            QuickstartExecutionPlan(
                composefile_git_ref=composefile_git_ref,
                docker_tag=docker_tag,
                mysql_tag=str(mysql_tag),
            ),
        )
        # new CLI version is downloading the composefile corresponding to the requested version
        # if the version is older than v0.10.1, it doesn't contain the setup job labels and the
        # the checks will fail, so in those cases we pick the composefile from v0.10.1 which contains
        # the setup job labels
        if _is_it_a_version(result.composefile_git_ref):
            if parse("v0.10.1") > parse(result.composefile_git_ref):
                # The merge commit where the labels were added
                # https://github.com/datahub-project/datahub/pull/7473
                result.composefile_git_ref = "1d3339276129a7cb8385c07a958fcc93acda3b4e"

        return result


def save_quickstart_config(
    config: QuickstartVersionMappingConfig, path: str = DEFAULT_LOCAL_CONFIG_PATH
) -> None:
    # create directory if it doesn't exist
    path = os.path.expanduser(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(config.dict(), f)
    logger.info(f"Saved quickstart config to {path}.")
