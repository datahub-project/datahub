import collections
import dataclasses
import enum
from datetime import datetime, timedelta, timezone
from typing import Callable, Counter, Generic, List, Optional, TypeVar

import pydantic

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.workunit import UsageStatsWorkUnit
from datahub.metadata.schema_classes import (
    FieldUsageCountsClass,
    UsageAggregationClass,
    UsageAggregationMetricsClass,
    UserUsageCountsClass,
    WindowDurationClass,
)


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


ResourceType = TypeVar("ResourceType")


@dataclasses.dataclass
class GenericAggregatedDataset(Generic[ResourceType]):
    bucket_start_time: datetime
    resource: ResourceType

    readCount: int = 0
    queryCount: int = 0
    queryFreq: Counter[str] = dataclasses.field(default_factory=collections.Counter)
    userFreq: Counter[str] = dataclasses.field(default_factory=collections.Counter)
    columnFreq: Counter[str] = dataclasses.field(default_factory=collections.Counter)

    def add_read_entry(
        self, user: str, query: Optional[str], fields: List[str]
    ) -> None:
        self.readCount += 1
        self.userFreq[user] += 1
        if query:
            self.queryCount += 1
            self.queryFreq[query] += 1
        for column in fields:
            self.columnFreq[column] += 1

    def make_usage_workunit(
        self,
        bucket_duration: BucketDuration,
        urn_builder: Callable[[ResourceType], str],
        top_n_queries: Optional[int],
    ) -> UsageStatsWorkUnit:
        return UsageStatsWorkUnit(
            id=f"{self.bucket_start_time.isoformat()}-{self.resource}",
            usageStats=UsageAggregationClass(
                bucket=int(self.bucket_start_time.timestamp() * 1000),
                duration=bucket_duration,
                resource=urn_builder(self.resource),
                metrics=UsageAggregationMetricsClass(
                    uniqueUserCount=len(self.userFreq),
                    users=[
                        UserUsageCountsClass(
                            user=builder.UNKNOWN_USER,
                            count=count,
                            userEmail=user_email,
                        )
                        for user_email, count in self.userFreq.most_common()
                    ],
                    totalSqlQueries=self.queryCount,
                    topSqlQueries=[
                        query for query, _ in self.queryFreq.most_common(top_n_queries)
                    ],
                    fields=[
                        FieldUsageCountsClass(
                            fieldName=column,
                            count=count,
                        )
                        for column, count in self.columnFreq.most_common()
                    ],
                ),
            ),
        )


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
