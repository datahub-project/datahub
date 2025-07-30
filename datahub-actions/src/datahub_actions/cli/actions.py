# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import pathlib
import signal
import sys
import time
from typing import Any, List

import click
from click_default_group import DefaultGroup
from expandvars import UnboundVariable

import datahub_actions._version as actions_version
from datahub.configuration.config_loader import load_config_file
from datahub_actions.pipeline.pipeline import Pipeline
from datahub_actions.pipeline.pipeline_manager import PipelineManager

logger = logging.getLogger(__name__)

# Instantiate a singleton instance of the Pipeline Manager.
pipeline_manager = PipelineManager()


def pipeline_config_to_pipeline(pipeline_config: dict) -> Pipeline:
    logger.debug(
        f"Attempting to create Actions Pipeline using config {pipeline_config.get('name')}"
    )
    try:
        return Pipeline.create(pipeline_config)
    except Exception as e:
        raise Exception(
            f"Failed to instantiate Actions Pipeline using config {pipeline_config.get('name')}: {e}"
        ) from e


@click.group(cls=DefaultGroup, default="run")
def actions() -> None:
    """Execute one or more Actions Pipelines"""
    pass


def load_raw_config_file(config_file: pathlib.Path) -> dict:
    """
    Load a config file as raw YAML/JSON without variable expansion.

    Args:
        config_file: Path to the configuration file

    Returns:
        dict: Raw configuration dictionary

    Raises:
        Exception: If the file cannot be loaded or is invalid YAML/JSON
    """
    try:
        with open(config_file, "r") as f:
            import yaml

            return yaml.safe_load(f)
    except Exception as e:
        raise Exception(
            f"Failed to load raw configuration file {config_file}: {e}"
        ) from e


def is_pipeline_enabled(config: dict) -> bool:
    """
    Check if a pipeline configuration is enabled.

    Args:
        config: Raw configuration dictionary

    Returns:
        bool: True if pipeline is enabled, False otherwise
    """
    enabled = config.get("enabled", True)
    return not (enabled == "false" or enabled is False)


@actions.command(
    name="run",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option("-c", "--config", required=True, type=str, multiple=True)
@click.option("--debug/--no-debug", default=False)
@click.pass_context
def run(ctx: Any, config: List[str], debug: bool) -> None:
    """Execute one or more Actions Pipelines"""

    logger.info("DataHub Actions version: %s", actions_version.nice_version_name())

    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    pipelines: List[Pipeline] = []
    logger.debug("Creating Actions Pipelines...")

    # Phase 1: Initial validation of configs
    valid_configs = []
    for pipeline_config in config:
        pipeline_config_file = pathlib.Path(pipeline_config)
        try:
            # First just load the raw config to check if it's enabled
            raw_config = load_raw_config_file(pipeline_config_file)

            if not is_pipeline_enabled(raw_config):
                logger.warning(
                    f"Skipping pipeline {raw_config.get('name') or pipeline_config} as it is not enabled"
                )
                continue

            valid_configs.append(pipeline_config_file)

        except Exception as e:
            if len(config) == 1:
                raise Exception(
                    f"Failed to load raw configuration file {pipeline_config_file}"
                ) from e
            logger.warning(
                f"Failed to load pipeline configuration! Skipping action config file {pipeline_config_file}...: {e}"
            )

    # Phase 2: Full config loading and pipeline creation
    for pipeline_config_file in valid_configs:
        try:
            # Now load the full config with variable expansion
            pipeline_config_dict = load_config_file(pipeline_config_file)
            pipelines.append(pipeline_config_to_pipeline(pipeline_config_dict))
        except UnboundVariable as e:
            if len(valid_configs) == 1:
                raise Exception(
                    "Failed to load action configuration. Unbound variable(s) provided in config YAML."
                ) from e
            logger.warning(
                f"Failed to resolve variables in config file {pipeline_config_file}...: {e}"
            )
            continue

    # Exit early if no valid pipelines were created
    if not pipelines:
        logger.error(
            f"No valid pipelines were started from {len(config)} config(s). "
            "Check that at least one pipeline is enabled and all required environment variables are set."
        )
        sys.exit(1)

    logger.debug("Starting Actions Pipelines")

    # Start each pipeline
    for p in pipelines:
        pipeline_manager.start_pipeline(p.name, p)
        logger.info(f"Action Pipeline with name '{p.name}' is now running.")

    # Now, run forever only if we have valid pipelines
    while True:
        time.sleep(5)


@actions.command()
def version() -> None:
    """Print version number and exit."""
    click.echo(f"DataHub Actions version: {actions_version.nice_version_name()}")
    click.echo(f"Python version: {sys.version}")


# Handle shutdown signal. (ctrl-c)
def handle_shutdown(signum: int, frame: Any) -> None:
    logger.info("Stopping all running Action Pipelines...")
    pipeline_manager.stop_all()
    sys.exit(1)


signal.signal(signal.SIGINT, handle_shutdown)
