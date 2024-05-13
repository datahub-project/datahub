from typing import Optional

from pydantic import Field, validator

from datahub_executor.common.types import PermissiveBaseModel


class IngestionSourceSchedule(PermissiveBaseModel):
    interval: str

    timezone: str


class IngestionSourceConfig(PermissiveBaseModel):
    recipe: str

    executor_id: str = Field(alias="executorId")

    version: Optional[str]

    debug_mode: Optional[str] = Field(alias="debugMode")

    @validator("debug_mode", pre=True, always=True)
    def validate_debug_mode(cls, debug_mode: Optional[str]) -> str:
        return debug_mode or "False"


class IngestionSource(PermissiveBaseModel):
    """TODO -"""

    urn: str

    type: str

    platform: Optional[str]

    schedule: Optional[IngestionSourceSchedule]

    config: IngestionSourceConfig
