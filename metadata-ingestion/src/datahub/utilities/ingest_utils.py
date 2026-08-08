import json
import logging
from typing import Any, Dict, List, Optional

import click
from pydantic import ConfigDict, Field, field_validator

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
    model_config = ConfigDict(validate_assignment=True)

    name: str = Field(description="Name of the ingestion source.")
    schedule: Optional[str] = Field(
        default=None, description="Cron expression for the ingestion schedule."
    )
    time_zone: str = Field(default="UTC", description="Timezone for the cron schedule.")
    cli_version: Optional[str] = Field(
        default=None, description="DataHub CLI version to use for ingestion."
    )
    executor_id: str = Field(
        default="default", description="ID of the executor to run ingestion on."
    )
    extra_pip: Optional[str] = Field(
        default=None,
        description="JSON list of extra pip packages to install (e.g. '[\"pandas\"]').",
    )
    extra_env: Optional[str] = Field(
        default=None,
        description="Comma-separated KEY=VALUE pairs for extra environment variables.",
    )
    datahub_plugins: Optional[str] = Field(
        default=None,
        description="JSON list of external plugin specs (e.g. '[\"github:owner/repo\"]').",
    )

    @field_validator("extra_pip")
    @classmethod
    def _validate_extra_pip(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            parse_json_list(v, "extra_pip")
        return v

    @field_validator("datahub_plugins")
    @classmethod
    def _validate_datahub_plugins(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            parse_json_list(v, "datahub_plugins")
        return v

    @field_validator("extra_env")
    @classmethod
    def _validate_extra_env(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v.strip():
            _parse_extra_env(v)
        return v


def _parse_extra_env(raw: str) -> str:
    """Parse comma-separated KEY=VALUE pairs into a JSON object string."""
    env_dict = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if "=" not in pair:
            raise ValueError(
                f"Invalid extra_env entry: {pair!r}. "
                "Expected format: KEY=VALUE (comma-separated)."
            )
        key, value = pair.split("=", 1)
        env_dict[key.strip()] = value.strip()
    return json.dumps(env_dict)


def parse_json_list(raw: str, field_name: str = "value") -> List[str]:
    """Parse *raw* as a JSON-encoded list and return it.

    Returns an empty list when *raw* is empty/blank.
    Raises ``ValueError`` on malformed input.
    """
    if not raw or not raw.strip():
        return []
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as e:
        raise ValueError(
            f"{field_name} must be a valid JSON array of strings, got: {raw!r}"
        ) from e
    if not isinstance(parsed, list):
        raise ValueError(
            f"{field_name} must be a JSON array, got {type(parsed).__name__}: {raw!r}"
        )
    for i, item in enumerate(parsed):
        if not isinstance(item, str):
            raise ValueError(
                f"{field_name}[{i}] must be a string, got {type(item).__name__}"
            )
    return parsed


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
    extra_env: Optional[str] = None,
    datahub_plugins: Optional[str] = None,
) -> Dict[str, Any]:
    pipeline_config = load_config_file(
        config,
        allow_stdin=True,
        allow_remote=True,
        resolve_env_vars=False,
    )

    deploy_options_raw = pipeline_config.pop("deployment", None)
    if deploy_options_raw is not None:
        deploy_options = DeployOptions.model_validate(deploy_options_raw)

        if name:
            logger.info(f"Overriding deployment name {deploy_options.name} with {name}")
            deploy_options.name = name
    else:
        if not name:
            raise click.UsageError(
                "Either --name must be set or deployment_name specified in the config"
            )
        deploy_options = DeployOptions(name=name)

    # CLI args override deploy_options. validate_assignment=True on the model
    # ensures Pydantic field validators run on every assignment.
    cli_overrides = {
        "schedule": schedule,
        "time_zone": time_zone,
        "cli_version": cli_version,
        "executor_id": executor_id,
        "extra_pip": extra_pip,
        "extra_env": extra_env,
        "datahub_plugins": datahub_plugins,
    }
    for field_name, value in cli_overrides.items():
        if value:
            setattr(deploy_options, field_name, value)

    logger.info(f"Using {repr(deploy_options)}")

    if not urn:
        # When urn/name is not specified, we will generate a unique urn based on the deployment name.
        urn = _make_ingestion_urn(deploy_options.name)
        logger.info(f"Using recipe urn: {urn}")

    variables: Dict[str, Any] = {
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
    extra_args: list[Dict[str, str]] = variables["input"]["config"].setdefault(
        "extraArgs", []
    )

    if deploy_options.extra_pip is not None:
        extra_args.append(
            {"key": "extra_pip_requirements", "value": deploy_options.extra_pip}
        )

    if deploy_options.extra_env is not None:
        extra_args.append(
            {
                "key": "extra_env_vars",
                "value": _parse_extra_env(deploy_options.extra_env),
            }
        )

    if deploy_options.datahub_plugins is not None:
        extra_args.append(
            {"key": "datahub_plugins", "value": deploy_options.datahub_plugins}
        )

    # Remove the key entirely when no extra args were added
    if not extra_args:
        del variables["input"]["config"]["extraArgs"]

    return variables
