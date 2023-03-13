import collections
import dataclasses
import logging
from datetime import datetime
from typing import Callable, Counter, Generic, List, Optional, TypeVar

import pydantic
from pydantic.fields import Field

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import (
    BaseTimeWindowConfig,
    BucketDuration,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    DatasetFieldUsageCountsClass,
    DatasetUsageStatisticsClass,
    DatasetUserUsageCountsClass,
    TimeWindowSizeClass,
)
from datahub.utilities.sql_formatter import format_sql_query, trim_query

logger = logging.getLogger(__name__)

ResourceType = TypeVar("ResourceType")

# The total number of characters allowed across all queries in a single workunit.
TOTAL_BUDGET_FOR_QUERY_LIST = 24000


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
        self,
        user_email: str,
        query: Optional[str],
        fields: List[str],
        user_email_pattern: AllowDenyPattern = AllowDenyPattern.allow_all(),
    ) -> None:
        if not user_email_pattern.allowed(user_email):
            return

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
        top_n_queries: int,
        format_sql_queries: bool,
        include_top_n_queries: bool,
        total_budget_for_query_list: int = TOTAL_BUDGET_FOR_QUERY_LIST,
        query_trimmer_string: str = " ...",
    ) -> MetadataWorkUnit:
        top_sql_queries: Optional[List[str]] = None
        if include_top_n_queries:
            budget_per_query: int = int(total_budget_for_query_list / top_n_queries)
            top_sql_queries = [
                trim_query(
                    format_sql_query(query, keyword_case="upper", reindent_aligned=True)
                    if format_sql_queries
                    else query,
                    budget_per_query=budget_per_query,
                    query_trimmer_string=query_trimmer_string,
                )
                for query, _ in self.queryFreq.most_common(top_n_queries)
            ]

        usageStats = DatasetUsageStatisticsClass(
            timestampMillis=int(self.bucket_start_time.timestamp() * 1000),
            eventGranularity=TimeWindowSizeClass(unit=bucket_duration, multiple=1),
            uniqueUserCount=len(self.userFreq),
            totalSqlQueries=self.queryCount,
            topSqlQueries=top_sql_queries,
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
            entityUrn=urn_builder(self.resource),
            aspect=usageStats,
        )

        return MetadataWorkUnit(
            id=f"{self.bucket_start_time.isoformat()}-{self.resource}", mcp=mcp
        )


class BaseUsageConfig(BaseTimeWindowConfig):
    top_n_queries: pydantic.PositiveInt = Field(
        default=10, description="Number of top queries to save to each table."
    )
    user_email_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for user emails to filter in usage.",
    )
    include_operational_stats: bool = Field(
        default=True, description="Whether to display operational stats."
    )

    include_read_operational_stats: bool = Field(
        default=False,
        description="Whether to report read operational stats. Experimental.",
    )

    format_sql_queries: bool = Field(
        default=False, description="Whether to format sql queries"
    )
    include_top_n_queries: bool = Field(
        default=True, description="Whether to ingest the top_n_queries."
    )

    @pydantic.validator("top_n_queries")
    def ensure_top_n_queries_is_not_too_big(cls, v: int) -> int:
        minimum_query_size = 20

        max_queries = int(TOTAL_BUDGET_FOR_QUERY_LIST / minimum_query_size)
        if v > max_queries:
            raise ValueError(
                f"top_n_queries is set to {v} but it can be maximum {max_queries}"
            )
        return v
