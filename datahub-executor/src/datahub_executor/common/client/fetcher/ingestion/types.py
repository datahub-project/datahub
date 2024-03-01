from typing import Optional

from pydantic import Field

from datahub_executor.common.types import PermissiveBaseModel


class IngestionSourceSchedule(PermissiveBaseModel):
    interval: str

    timezone: str


class IngestionSourceConfig(PermissiveBaseModel):
    recipe: str

    version: Optional[str]

    executor_id: str = Field(alias="executorId")

    debug_mode: str = Field(alias="debugMode")


class IngestionSource(PermissiveBaseModel):
    """TODO -"""

    urn: str

    type: str

    platform: Optional[str]

    schedule: Optional[IngestionSourceSchedule]

    config: IngestionSourceConfig
