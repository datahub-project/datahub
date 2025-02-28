from typing import Any, Dict, List, Optional

from pydantic import Field, root_validator, validator

from datahub_executor.common.types import PermissiveBaseModel


class IngestionSourceSchedule(PermissiveBaseModel):
    interval: str

    timezone: str


class IngestionSourceConfig(PermissiveBaseModel):
    recipe: str

    executor_id: str = Field(alias="executorId")

    version: Optional[str]

    debug_mode: Optional[str] = Field(alias="debugMode")

    extra_args: Dict[str, Any] = {}

    @validator("debug_mode", pre=True, always=True)
    def validate_debug_mode(cls, debug_mode: Optional[str]) -> str:
        return debug_mode or "False"

    @root_validator(pre=True)
    def extract_info(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if "extraArgs" not in values or not isinstance(values["extraArgs"], List):
            return values

        expected_args = {
            "extra_env_vars",
            "extra_pip_requirements",
            "extra_pip_plugins",
        }
        new_extra_args: Dict[str, Any] = {}

        # override defaults is passed
        for item in values["extraArgs"]:
            if "key" in item and "value" in item:
                if item["key"] in expected_args:
                    if item["value"] is not None and item["value"] != "":
                        new_extra_args[item["key"]] = item["value"]

        values["extra_args"] = new_extra_args
        return values


class IngestionSource(PermissiveBaseModel):
    """TODO -"""

    urn: str

    type: str

    platform: Optional[str]

    schedule: Optional[IngestionSourceSchedule]

    config: IngestionSourceConfig
