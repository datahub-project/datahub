from datetime import timedelta
from typing import Union

import humanfriendly
from pydantic import Field, RootModel, field_validator
from typing_extensions import Literal

from datahub.configuration.common import ConfigModel


class CronTrigger(ConfigModel):
    type: Literal["cron"]
    cron: str = Field(
        description="The cron expression to use. See https://crontab.guru/ for help."
    )
    timezone: str = Field(
        "UTC",
        description="The timezone to use for the cron schedule. Defaults to UTC.",
    )


class IntervalTrigger(ConfigModel):
    type: Literal["interval"]
    interval: timedelta

    @field_validator("interval", mode="before")
    @classmethod
    def lookback_interval_to_timedelta(cls, v):
        if isinstance(v, str):
            seconds = humanfriendly.parse_timespan(v)
            return timedelta(seconds=seconds)
        raise ValueError("Invalid value.")


class EntityChangeTrigger(ConfigModel):
    type: Literal["on_table_change"]


class ManualTrigger(ConfigModel):
    type: Literal["manual"]


class AssertionTrigger(
    RootModel[Union[CronTrigger, IntervalTrigger, EntityChangeTrigger, ManualTrigger]]
):
    root: Union[CronTrigger, IntervalTrigger, EntityChangeTrigger, ManualTrigger] = (
        Field(discriminator="type")
    )

    @property
    def trigger(self):
        return self.root
