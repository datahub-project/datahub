import json
import logging
from typing import Optional

import click

from datahub.configuration.common import ConfigModel
from datahub.configuration.config_loader import load_config_file
from datahub.emitter.mce_builder import datahub_guid

logger = logging.getLogger(__name__)


def _make_ingestion_urn(name: str) -> str:
    guid = datahub_guid(
        {
            "name": name,
        }
    )
    return f"urn:li:dataHubIngestionSource:deploy-{guid}"


class DeployOptions(ConfigModel):
    name: str
    schedule: Optional[str] = None
    time_zone: str = "UTC"
    cli_version: Optional[str] = None
    executor_id: str = "default"


def deploy_source_vars(
    name: Optional[str],
    config: str,
    urn: Optional[str],
    executor_id: Optional[str],
    cli_version: Optional[str],
    schedule: Optional[str],
    time_zone: Optional[str],
    extra_pip: Optional[str],
    debug: bool = False,
) -> dict:
    pipeline_config = load_config_file(
        config,
        allow_stdin=True,
        allow_remote=True,
        resolve_env_vars=False,
    )

    deploy_options_raw = pipeline_config.pop("deployment", None)
    if deploy_options_raw is not None:
        deploy_options = DeployOptions.parse_obj(deploy_options_raw)

        if name:
            logger.info(f"Overriding deployment name {deploy_options.name} with {name}")
            deploy_options.name = name
    else:
        if not name:
            raise click.UsageError(
                "Either --name must be set or deployment_name specified in the config"
            )
        deploy_options = DeployOptions(name=name)

    # Use remaining CLI args to override deploy_options
    if schedule:
        deploy_options.schedule = schedule
    if time_zone:
        deploy_options.time_zone = time_zone
    if cli_version:
        deploy_options.cli_version = cli_version
    if executor_id:
        deploy_options.executor_id = executor_id

    logger.info(f"Using {repr(deploy_options)}")

    if not urn:
        # When urn/name is not specified, we will generate a unique urn based on the deployment name.
        urn = _make_ingestion_urn(deploy_options.name)
        logger.info(f"Using recipe urn: {urn}")

    variables: dict = {
        "urn": urn,
        "input": {
            "name": deploy_options.name,
            "type": pipeline_config["source"]["type"],
            "config": {
                "recipe": json.dumps(pipeline_config),
                "executorId": deploy_options.executor_id,
                "debugMode": debug,
                "version": deploy_options.cli_version,
            },
        },
    }

    if deploy_options.schedule is not None:
        variables["input"]["schedule"] = {
            "interval": deploy_options.schedule,
            "timezone": deploy_options.time_zone,
        }
    if extra_pip is not None:
        extra_args_list = (
            variables.get("input", {}).get("config", {}).get("extraArgs", [])
        )
        extra_args_list.append({"key": "extra_pip_requirements", "value": extra_pip})
        variables["input"]["config"]["extraArgs"] = extra_args_list

    return variables
