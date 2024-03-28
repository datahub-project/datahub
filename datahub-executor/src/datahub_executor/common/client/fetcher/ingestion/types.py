from typing import Optional

from pydantic import Field

from datahub_executor.common.types import PermissiveBaseModel


class IngestionSourceSchedule(PermissiveBaseModel):
    interval: str

    timezone: str


class IngestionSourceConfig(PermissiveBaseModel):
    recipe: str

    executor_id: str = Field(alias="executorId")

    version: Optional[str]

    debug_mode: Optional[str] = Field(alias="debugMode")


class IngestionSource(PermissiveBaseModel):
    """TODO -"""

    urn: str

    type: str

    platform: Optional[str]

    schedule: Optional[IngestionSourceSchedule]

    config: IngestionSourceConfig
