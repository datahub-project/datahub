from datetime import timedelta
from typing import Union

import humanfriendly
from typing_extensions import Literal

from datahub.configuration.pydantic_migration_helpers import (
    v1_ConfigModel,
    v1_Field,
    v1_validator,
)


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

    @v1_validator("interval", pre=True)
    def lookback_interval_to_timedelta(cls, v):
        if isinstance(v, str):
            seconds = humanfriendly.parse_timespan(v)
            return timedelta(seconds=seconds)
        raise ValueError("Invalid value.")


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
