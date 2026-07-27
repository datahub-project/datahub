import json
import logging
import os.path
import re
import sys
from typing import Dict, Optional, Tuple

import click
import packaging
import requests
import yaml
from packaging.version import parse
from pydantic import BaseModel

from datahub._version import nice_version_name
from datahub.configuration.env_vars import get_force_local_quickstart_mapping

logger = logging.getLogger(__name__)

LOCAL_QUICKSTART_MAPPING_FILE = get_force_local_quickstart_mapping()
DEFAULT_LOCAL_CONFIG_PATH = "~/.datahub/quickstart/quickstart_version_mapping.yaml"
DEFAULT_REMOTE_CONFIG_PATH = "https://raw.githubusercontent.com/datahub-project/datahub/master/docker/quickstart/quickstart_version_mapping.yaml"

MINIMUM_SUPPORTED_VERSION = "v1.1.0"
MAGIC_ALIASES = frozenset({"head", "quickstart", "stable"})


def get_minimum_supported_version_message(version: str) -> str:
    MINIMUM_SUPPORTED_VERSION_MESSAGE = f"""
    DataHub CLI Version Compatibility Issue

    You're trying to install DataHub server version {version} which is not supported by this CLI version.

    This CLI (version {nice_version_name()}) only supports installing DataHub server versions {MINIMUM_SUPPORTED_VERSION} and above.

    To install older server versions:
    1. Uninstall current CLI: pip uninstall acryl-datahub
    2. Install older CLI: pip install acryl-datahub==1.1
    3. Run quickstart with your desired version: datahub docker quickstart --version <version>

    For more information: https://docs.datahub.com/docs/quickstart#install-datahub-server
    """
    return MINIMUM_SUPPORTED_VERSION_MESSAGE


class QuickstartExecutionPlan(BaseModel):
    composefile_git_ref: str
    docker_tag: str
    mysql_tag: Optional[str] = None


def _is_it_a_version(version: str) -> bool:
    """
    Checks if a string is a valid version.
    :param version: The string to check.
    :return: True if the string is a valid version, False otherwise.
    """
    return re.match(r"^v?\d+\.\d+(\.\d+)?$", version) is not None


def _is_passthrough_version(version: str) -> bool:
    return _is_it_a_version(version) or version.startswith("sha-")


def _is_magic_alias(version: str) -> bool:
    return version in MAGIC_ALIASES


def _master_quickstart_plan(mysql_tag: str = "8.2") -> QuickstartExecutionPlan:
    return QuickstartExecutionPlan(
        composefile_git_ref="master",
        docker_tag="quickstart",
        mysql_tag=mysql_tag,
    )


def _apply_head_tag_rewrite(result: QuickstartExecutionPlan) -> QuickstartExecutionPlan:
    if result.docker_tag == "head":
        return result.model_copy(update={"docker_tag": "quickstart"})
    return result


def _apply_compose_ref_rewrite(
    result: QuickstartExecutionPlan,
) -> QuickstartExecutionPlan:
    if _is_it_a_version(result.composefile_git_ref):
        if parse("v1.2.0") > parse(result.composefile_git_ref):
            return result.model_copy(
                update={
                    "composefile_git_ref": "21726bc3341490f4182b904626c793091ac95edd"
                }
            )
    return result


def _confirm_quickstart_plan(
    requested_version: str,
    fallback_plan: QuickstartExecutionPlan,
    reason: str,
    accept_version_default: bool,
) -> QuickstartExecutionPlan:
    msg = (
        f"{reason}\n"
        "Use this configuration instead?\n"
        f"  compose: {fallback_plan.composefile_git_ref}\n"
        f"  images:  {fallback_plan.docker_tag}\n"
        f"  mysql:   {fallback_plan.mysql_tag}\n"
        "Continue?"
    )

    if accept_version_default:
        click.secho(
            f"Using alternate quickstart configuration for version '{requested_version}'.",
            fg="yellow",
        )
        return fallback_plan

    if not sys.stdin.isatty():
        raise click.ClickException(
            f"Version '{requested_version}' requires confirmation in a non-interactive environment. "
            "Re-run with a valid --version, or pass --accept-version-default to use the suggested configuration."
        )

    if click.confirm(msg, default=False):
        return fallback_plan

    raise click.ClickException(
        f"Aborted. Fix --version '{requested_version}', or use --version default, head, quickstart, or a release tag."
    )


class QuickstartVersionMappingConfig(BaseModel):
    quickstart_version_map: Dict[str, QuickstartExecutionPlan]

    def _get_default_plan(self) -> QuickstartExecutionPlan:
        return self.quickstart_version_map.get("default", _master_quickstart_plan())

    def _resolve_magic_alias_plan(self, alias: str) -> QuickstartExecutionPlan:
        for key in (alias, "quickstart", "head"):
            if key in self.quickstart_version_map:
                return _apply_head_tag_rewrite(self.quickstart_version_map[key])
        return _master_quickstart_plan()

    def _needs_confirmation(
        self,
        requested_version: str,
        result: QuickstartExecutionPlan,
        in_map: bool,
    ) -> Optional[Tuple[str, QuickstartExecutionPlan]]:
        if requested_version == "default":
            return None

        if in_map and result.docker_tag == "head":
            fallback = result.model_copy(update={"docker_tag": "quickstart"})
            return (
                f"Version '{requested_version}' uses deprecated docker tag 'head' "
                "(images will use 'quickstart' instead).",
                fallback,
            )

        if _is_magic_alias(requested_version) and not in_map:
            fallback = self._resolve_magic_alias_plan(requested_version)
            return (
                f"Version '{requested_version}' is not in the quickstart version mapping "
                f"(will use compose from {fallback.composefile_git_ref} with image tag '{fallback.docker_tag}').",
                fallback,
            )

        if not in_map and not _is_passthrough_version(requested_version):
            fallback = _apply_head_tag_rewrite(self._get_default_plan())
            return (
                f"Version '{requested_version}' is not recognized in the quickstart version mapping.",
                fallback,
            )

        return None

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
            return cls.model_validate(config_raw)

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
                        composefile_git_ref="master",
                        docker_tag="quickstart",
                        mysql_tag="8.2",
                    ),
                }
            )

        config = cls.model_validate(config_raw)

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
        accept_version_default: bool = False,
    ) -> QuickstartExecutionPlan:
        """
        From the requested version and stable flag, returns the execution plan for the quickstart.
        Including the docker tag, composefile git ref, required containers, and checks to run.
        :return: The execution plan for the quickstart.
        """
        if requested_version is None:
            requested_version = "default"

        if requested_version == "default":
            result = _apply_head_tag_rewrite(self._get_default_plan())
            if not is_minimum_supported_version(requested_version):
                click.secho(
                    get_minimum_supported_version_message(version=requested_version),
                    fg="red",
                )
                raise click.ClickException("Minimum supported version not met")
            return _apply_compose_ref_rewrite(result)

        mysql_tag = "8.2"
        in_map = requested_version in self.quickstart_version_map
        if in_map:
            result = self.quickstart_version_map[requested_version]
        else:
            result = QuickstartExecutionPlan(
                composefile_git_ref=requested_version,
                docker_tag=requested_version,
                mysql_tag=str(mysql_tag),
            )

        confirmation = self._needs_confirmation(requested_version, result, in_map)
        if confirmation:
            reason, fallback_plan = confirmation
            result = _confirm_quickstart_plan(
                requested_version,
                fallback_plan,
                reason,
                accept_version_default,
            )

        result = _apply_head_tag_rewrite(result)

        if not is_minimum_supported_version(requested_version):
            click.secho(
                get_minimum_supported_version_message(version=requested_version),
                fg="red",
            )
            raise click.ClickException("Minimum supported version not met")

        return _apply_compose_ref_rewrite(result)


def save_quickstart_config(
    config: QuickstartVersionMappingConfig, path: str = DEFAULT_LOCAL_CONFIG_PATH
) -> None:
    # create directory if it doesn't exist
    path = os.path.expanduser(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(config.model_dump(), f)
    logger.info(f"Saved quickstart config to {path}.")


def is_minimum_supported_version(version: str) -> bool:
    if not _is_it_a_version(version):
        return True

    requested_version = packaging.version.parse(version)
    minimum_supported_version = packaging.version.parse(MINIMUM_SUPPORTED_VERSION)
    if requested_version < minimum_supported_version:
        return False

    return True
