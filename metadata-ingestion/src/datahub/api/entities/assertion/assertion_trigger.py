from datetime import timedelta
from typing import Union

import humanfriendly
from pydantic import BaseModel, Field, RootModel, field_validator
from typing_extensions import Literal


class CronTrigger(BaseModel):
    model_config = {"extra": "forbid"}

    type: Literal["cron"]
    cron: str = Field(
        description="The cron expression to use. See https://crontab.guru/ for help."
    )
    timezone: str = Field(
        "UTC",
        description="The timezone to use for the cron schedule. Defaults to UTC.",
    )


class IntervalTrigger(BaseModel):
    model_config = {"extra": "forbid"}

    type: Literal["interval"]
    interval: timedelta

    @field_validator("interval", mode="before")
    @classmethod
    def lookback_interval_to_timedelta(cls, v):
        if isinstance(v, str):
            seconds = humanfriendly.parse_timespan(v)
            return timedelta(seconds=seconds)
        raise ValueError("Invalid value.")


class EntityChangeTrigger(BaseModel):
    model_config = {"extra": "forbid"}

    type: Literal["on_table_change"]


class ManualTrigger(BaseModel):
    model_config = {"extra": "forbid"}

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
