from datetime import timedelta
from typing import Union

from typing_extensions import Literal

from datahub.configuration.pydantic_migration_helpers import v1_ConfigModel, v1_Field


class CronTrigger(v1_ConfigModel):
    type: Literal["cron"]
    cron: str = v1_Field(
        description="The cron expression to use. See https://crontab.guru/ for help."
    )
    timezone: str = v1_Field(
        "UTC",
        description="The timezone to use for the cron schedule. Defaults to UTC.",
    )


class IntervalTrigger(v1_ConfigModel):
    type: Literal["interval"]
    interval: timedelta


class EntityChangeTrigger(v1_ConfigModel):
    type: Literal["on_table_change"]


class ManualTrigger(v1_ConfigModel):
    type: Literal["manual"]


class AssertionTrigger(v1_ConfigModel):
    __root__: Union[
        CronTrigger, IntervalTrigger, EntityChangeTrigger, ManualTrigger
    ] = v1_Field(discriminator="type")

    @property
    def trigger(self):
        return self.__root__
