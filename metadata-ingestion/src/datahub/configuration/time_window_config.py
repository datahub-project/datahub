import enum
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pydantic
from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.metadata.schema_classes import CalendarIntervalClass


@enum.unique
class BucketDuration(str, enum.Enum):
    DAY = CalendarIntervalClass.DAY
    HOUR = CalendarIntervalClass.HOUR


def get_time_bucket(original: datetime, bucketing: BucketDuration) -> datetime:
    """Floors the timestamp to the closest day or hour."""

    if bucketing == BucketDuration.HOUR:
        return original.replace(minute=0, second=0, microsecond=0)
    else:  # day
        return original.replace(hour=0, minute=0, second=0, microsecond=0)


def get_bucket_duration_delta(bucketing: BucketDuration) -> timedelta:
    if bucketing == BucketDuration.HOUR:
        return timedelta(hours=1)
    else:  # day
        return timedelta(days=1)


class BaseTimeWindowConfig(ConfigModel):
    bucket_duration: BucketDuration = Field(
        default=BucketDuration.DAY,
        description="Size of the time window to aggregate usage stats.",
    )

    # `start_time` and `end_time` will be populated by the pre-validators.
    # However, we must specify a "default" value here or pydantic will complain
    # if those fields are not set by the user.
    end_time: datetime = Field(default=None, description="Latest date of usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`)")  # type: ignore
    start_time: datetime = Field(default=None, description="Earliest date of usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`)")  # type: ignore

    @pydantic.validator("end_time", pre=True, always=True)
    def default_end_time(
        cls, v: Any, *, values: Dict[str, Any], **kwargs: Any
    ) -> datetime:
        return v or get_time_bucket(
            datetime.now(tz=timezone.utc)
            + get_bucket_duration_delta(values["bucket_duration"]),
            values["bucket_duration"],
        )

    @pydantic.validator("start_time", pre=True, always=True)
    def default_start_time(
        cls, v: Any, *, values: Dict[str, Any], **kwargs: Any
    ) -> datetime:
        return v or (
            values["end_time"]
            - get_bucket_duration_delta(values["bucket_duration"]) * 2
        )

    @pydantic.validator("start_time", "end_time")
    def ensure_timestamps_in_utc(cls, v: datetime) -> datetime:
        if v.tzinfo != timezone.utc:
            raise ValueError(
                'timezone is not UTC; try adding a "Z" to the value e.g. "2021-07-20T00:00:00Z"'
            )
        return v
