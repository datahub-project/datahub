import enum
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import humanfriendly
import pydantic
from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.configuration.datetimes import parse_absolute_time, parse_relative_timespan
from datahub.metadata.schema_classes import CalendarIntervalClass
from datahub.utilities.str_enum import StrEnum


@enum.unique
class BucketDuration(StrEnum):
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
    end_time: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="Latest date of lineage/usage to consider. Default: Current time in UTC",
    )
    start_time: datetime = Field(
        default=None,
        description="Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'.",
    )  # type: ignore

    @pydantic.validator("start_time", pre=True, always=True)
    def default_start_time(
        cls, v: Any, values: Dict[str, Any], **kwargs: Any
    ) -> datetime:
        if v is None:
            return get_time_bucket(
                values["end_time"]
                - get_bucket_duration_delta(values["bucket_duration"]),
                values["bucket_duration"],
            )
        elif isinstance(v, str):
            # This is where start_time str is resolved to datetime
            try:
                delta = parse_relative_timespan(v)
                assert delta < timedelta(0), (
                    "Relative start time should start with minus sign (-) e.g. '-2 days'."
                )
                assert abs(delta) >= get_bucket_duration_delta(
                    values["bucket_duration"]
                ), (
                    "Relative start time should be in terms of configured bucket duration. e.g '-2 days' or '-2 hours'."
                )

                # The end_time's default value is not yet populated, in which case
                # we can just manually generate it here.
                if "end_time" not in values:
                    values["end_time"] = datetime.now(tz=timezone.utc)

                return get_time_bucket(
                    values["end_time"] + delta, values["bucket_duration"]
                )
            except humanfriendly.InvalidTimespan:
                # We do not floor start_time to the bucket start time if absolute start time is specified.
                # If user has specified absolute start time in recipe, it's most likely that he means it.
                return parse_absolute_time(v)

        return v

    @pydantic.validator("start_time", "end_time")
    def ensure_timestamps_in_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError(
                "Timestamps must be in UTC. Try adding a 'Z' to the value e.g. '2021-07-20T00:00:00Z'"
            )

        # If the timestamp is timezone-aware but not in UTC, convert it to UTC.
        v = v.astimezone(timezone.utc)

        return v

    def buckets(self) -> List[datetime]:
        """Returns list of timestamps for each DatasetUsageStatistics bucket.

        Includes all buckets in the time window, including partially contained buckets.
        """
        bucket_timedelta = get_bucket_duration_delta(self.bucket_duration)

        curr_bucket = get_time_bucket(self.start_time, self.bucket_duration)
        buckets = []
        while curr_bucket < self.end_time:
            buckets.append(curr_bucket)
            curr_bucket += bucket_timedelta

        return buckets

    def majority_buckets(self) -> List[datetime]:
        """Returns list of timestamps for each DatasetUsageStatistics bucket.

        Includes only buckets in the time window for which a majority of the bucket is ingested.
        """
        bucket_timedelta = get_bucket_duration_delta(self.bucket_duration)

        curr_bucket = get_time_bucket(self.start_time, self.bucket_duration)
        buckets = []
        while curr_bucket < self.end_time:
            start = max(self.start_time, curr_bucket)
            end = min(self.end_time, curr_bucket + bucket_timedelta)
            if end - start >= bucket_timedelta / 2:
                buckets.append(curr_bucket)
            curr_bucket += bucket_timedelta

        return buckets
