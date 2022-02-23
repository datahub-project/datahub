import collections
import dataclasses
from datetime import datetime
from typing import Callable, Counter, Generic, List, Optional, TypeVar

import pydantic

import datahub.emitter.mce_builder as builder
from datahub.configuration.time_window_config import (
    BaseTimeWindowConfig,
    BucketDuration,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetFieldUsageCountsClass,
    DatasetUsageStatisticsClass,
    DatasetUserUsageCountsClass,
    TimeWindowSizeClass,
)

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
        self, user_email: str, query: Optional[str], fields: List[str]
    ) -> None:
        self.readCount += 1
        self.userFreq[user_email] += 1
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
    ) -> MetadataWorkUnit:
        usageStats = DatasetUsageStatisticsClass(
            timestampMillis=int(self.bucket_start_time.timestamp() * 1000),
            eventGranularity=TimeWindowSizeClass(unit=bucket_duration, multiple=1),
            uniqueUserCount=len(self.userFreq),
            totalSqlQueries=self.queryCount,
            topSqlQueries=[
                query for query, _ in self.queryFreq.most_common(top_n_queries)
            ],
            userCounts=[
                DatasetUserUsageCountsClass(
                    user=builder.make_user_urn(user_email.split("@")[0]),
                    count=count,
                    userEmail=user_email,
                )
                for user_email, count in self.userFreq.most_common()
            ],
            fieldCounts=[
                DatasetFieldUsageCountsClass(
                    fieldPath=column,
                    count=count,
                )
                for column, count in self.columnFreq.most_common()
            ],
        )

        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            aspectName="datasetUsageStatistics",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=urn_builder(self.resource),
            aspect=usageStats,
        )

        return MetadataWorkUnit(
            id=f"{self.bucket_start_time.isoformat()}-{self.resource}", mcp=mcp
        )


class BaseUsageConfig(BaseTimeWindowConfig):
    top_n_queries: Optional[pydantic.PositiveInt] = 10
