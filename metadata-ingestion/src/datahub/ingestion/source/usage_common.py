import enum
from datetime import datetime, timedelta, timezone
from typing import Optional

import pydantic

from datahub.configuration.common import ConfigModel
from datahub.metadata.schema_classes import WindowDurationClass


@enum.unique
class BucketDuration(str, enum.Enum):
    DAY = WindowDurationClass.DAY
    HOUR = WindowDurationClass.HOUR


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


class BaseUsageConfig(ConfigModel):
    # start_time and end_time will be populated by the validators.
    bucket_duration: BucketDuration = BucketDuration.DAY
    end_time: datetime = None  # type: ignore
    start_time: datetime = None  # type: ignore

    top_n_queries: Optional[pydantic.PositiveInt] = 10

    @pydantic.validator("end_time", pre=True, always=True)
    def default_end_time(cls, v, *, values, **kwargs):
        return v or get_time_bucket(
            datetime.now(tz=timezone.utc), values["bucket_duration"]
        )

    @pydantic.validator("start_time", pre=True, always=True)
    def default_start_time(cls, v, *, values, **kwargs):
        return v or (
            values["end_time"] - get_bucket_duration_delta(values["bucket_duration"])
        )
