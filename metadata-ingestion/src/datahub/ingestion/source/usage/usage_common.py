import dataclasses
import logging
from collections import defaultdict
from datetime import datetime
from typing import (
    Callable,
    Counter,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import pydantic
from deprecated import deprecated
from pydantic.fields import Field

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import (
    BaseTimeWindowConfig,
    BucketDuration,
    get_time_bucket,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetUsageStatistics
from datahub.metadata.schema_classes import (
    CalendarIntervalClass,
    DatasetFieldUsageCountsClass,
    DatasetUsageStatisticsClass,
    DatasetUserUsageCountsClass,
    TimeWindowSizeClass,
    UsageAggregationClass,
    WindowDurationClass,
)
from datahub.utilities.sql_formatter import format_sql_query, trim_query
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)

ResourceType = TypeVar("ResourceType")

# The total number of characters allowed across all queries in a single workunit.
DEFAULT_QUERIES_CHARACTER_LIMIT = 24000


def default_user_urn_builder(email: str) -> str:
    return builder.make_user_urn(email.split("@")[0])


def make_usage_workunit(
    bucket_start_time: datetime,
    resource: ResourceType,
    query_count: int,
    query_freq: Optional[List[Tuple[str, int]]],
    user_freq: List[Tuple[str, int]],
    column_freq: List[Tuple[str, int]],
    bucket_duration: BucketDuration,
    resource_urn_builder: Callable[[ResourceType], str],
    top_n_queries: int,
    format_sql_queries: bool,
    queries_character_limit: int,
    user_urn_builder: Optional[Callable[[str], str]] = None,
    query_trimmer_string: str = " ...",
) -> MetadataWorkUnit:
    if user_urn_builder is None:
        user_urn_builder = default_user_urn_builder

    top_sql_queries: Optional[List[str]] = None
    if query_freq is not None:
        budget_per_query: int = int(queries_character_limit / top_n_queries)
        top_sql_queries = [
            trim_query(
                format_sql_query(query, keyword_case="upper", reindent_aligned=True)
                if format_sql_queries
                else query,
                budget_per_query=budget_per_query,
                query_trimmer_string=query_trimmer_string,
            )
            for query, _ in query_freq
        ]

    usageStats = DatasetUsageStatisticsClass(
        timestampMillis=int(bucket_start_time.timestamp() * 1000),
        eventGranularity=TimeWindowSizeClass(unit=bucket_duration, multiple=1),
        uniqueUserCount=len(user_freq),
        totalSqlQueries=query_count,
        topSqlQueries=top_sql_queries,
        userCounts=[
            DatasetUserUsageCountsClass(
                user=user_urn_builder(user),
                count=count,
                userEmail=user if "@" in user else None,
            )
            for user, count in user_freq
        ],
        fieldCounts=[
            DatasetFieldUsageCountsClass(
                fieldPath=column,
                count=count,
            )
            for column, count in column_freq
        ],
    )

    return MetadataChangeProposalWrapper(
        entityUrn=resource_urn_builder(resource),
        aspect=usageStats,
    ).as_workunit()


@dataclasses.dataclass
class GenericAggregatedDataset(Generic[ResourceType]):
    bucket_start_time: datetime
    resource: ResourceType

    readCount: int = 0
    queryCount: int = 0

    queryFreq: Counter[str] = dataclasses.field(default_factory=Counter)
    userFreq: Counter[str] = dataclasses.field(default_factory=Counter)
    columnFreq: Counter[str] = dataclasses.field(default_factory=Counter)

    def add_read_entry(
        self,
        user_email: Optional[str],
        query: Optional[str],
        fields: List[str],
        user_email_pattern: AllowDenyPattern = AllowDenyPattern.allow_all(),
    ) -> None:
        if user_email and not user_email_pattern.allowed(user_email):
            return

        self.readCount += 1
        if user_email is not None:
            self.userFreq[user_email] += 1

        if query:
            self.queryCount += 1
            self.queryFreq[query] += 1
        for column in fields:
            self.columnFreq[column] += 1

    def make_usage_workunit(
        self,
        bucket_duration: BucketDuration,
        resource_urn_builder: Callable[[ResourceType], str],
        top_n_queries: int,
        format_sql_queries: bool,
        include_top_n_queries: bool,
        queries_character_limit: int,
        user_urn_builder: Optional[Callable[[str], str]] = None,
        query_trimmer_string: str = " ...",
    ) -> MetadataWorkUnit:
        query_freq = (
            self.queryFreq.most_common(top_n_queries) if include_top_n_queries else None
        )
        return make_usage_workunit(
            bucket_start_time=self.bucket_start_time,
            resource=self.resource,
            query_count=self.queryCount,
            query_freq=query_freq,
            user_freq=self.userFreq.most_common(),
            column_freq=self.columnFreq.most_common(),
            bucket_duration=bucket_duration,
            resource_urn_builder=resource_urn_builder,
            user_urn_builder=user_urn_builder,
            top_n_queries=top_n_queries,
            format_sql_queries=format_sql_queries,
            queries_character_limit=queries_character_limit,
            query_trimmer_string=query_trimmer_string,
        )


class BaseUsageConfig(BaseTimeWindowConfig):
    queries_character_limit: int = Field(
        default=DEFAULT_QUERIES_CHARACTER_LIMIT,
        description=(
            "Total character limit for all queries in a single usage aspect."
            " Queries will be truncated to length `queries_character_limit / top_n_queries`."
        ),
        hidden_from_docs=True,  # Don't want to encourage people to break elasticsearch
    )

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
    def ensure_top_n_queries_is_not_too_big(cls, v: int, values: dict) -> int:
        minimum_query_size = 20

        max_queries = int(values["queries_character_limit"] / minimum_query_size)
        if v > max_queries:
            raise ValueError(
                f"top_n_queries is set to {v} but it can be maximum {max_queries}"
            )
        return v


class UsageAggregator(Generic[ResourceType]):
    # TODO: Move over other connectors to use this class

    def __init__(self, config: BaseUsageConfig):
        self.config = config
        self.aggregation: Dict[
            datetime, Dict[ResourceType, GenericAggregatedDataset[ResourceType]]
        ] = defaultdict(dict)

    def aggregate_event(
        self,
        *,
        resource: ResourceType,
        start_time: datetime,
        query: Optional[str],
        user: Optional[str],
        fields: List[str],
    ) -> None:
        floored_ts: datetime = get_time_bucket(start_time, self.config.bucket_duration)
        self.aggregation[floored_ts].setdefault(
            resource,
            GenericAggregatedDataset[ResourceType](
                bucket_start_time=floored_ts,
                resource=resource,
            ),
        ).add_read_entry(
            user,
            query,
            fields,
        )

    def generate_workunits(
        self,
        resource_urn_builder: Callable[[ResourceType], str],
        user_urn_builder: Optional[Callable[[str], str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        for time_bucket in self.aggregation.values():
            for aggregate in time_bucket.values():
                yield aggregate.make_usage_workunit(
                    bucket_duration=self.config.bucket_duration,
                    top_n_queries=self.config.top_n_queries,
                    format_sql_queries=self.config.format_sql_queries,
                    include_top_n_queries=self.config.include_top_n_queries,
                    resource_urn_builder=resource_urn_builder,
                    user_urn_builder=user_urn_builder,
                    queries_character_limit=self.config.queries_character_limit,
                )


@deprecated
def convert_usage_aggregation_class(
    obj: UsageAggregationClass,
) -> MetadataChangeProposalWrapper:
    # Legacy usage aggregation only supported dataset usage stats
    if guess_entity_type(obj.resource) == DatasetUrn.ENTITY_TYPE:
        aspect = DatasetUsageStatistics(
            timestampMillis=obj.bucket,
            eventGranularity=TimeWindowSizeClass(
                unit=convert_window_to_interval(obj.duration)
            ),
            uniqueUserCount=obj.metrics.uniqueUserCount,
            totalSqlQueries=obj.metrics.totalSqlQueries,
            topSqlQueries=obj.metrics.topSqlQueries,
            userCounts=[
                DatasetUserUsageCountsClass(
                    user=u.user, count=u.count, userEmail=u.userEmail
                )
                for u in obj.metrics.users
                if u.user is not None
            ]
            if obj.metrics.users
            else None,
            fieldCounts=[
                DatasetFieldUsageCountsClass(fieldPath=f.fieldName, count=f.count)
                for f in obj.metrics.fields
            ]
            if obj.metrics.fields
            else None,
        )
        return MetadataChangeProposalWrapper(entityUrn=obj.resource, aspect=aspect)
    else:
        raise Exception(
            f"Skipping unsupported usage aggregation - invalid entity type: {obj}"
        )


@deprecated
def convert_window_to_interval(window: Union[str, WindowDurationClass]) -> str:
    if window == WindowDurationClass.YEAR:
        return CalendarIntervalClass.YEAR
    elif window == WindowDurationClass.MONTH:
        return CalendarIntervalClass.MONTH
    elif window == WindowDurationClass.WEEK:
        return CalendarIntervalClass.WEEK
    elif window == WindowDurationClass.DAY:
        return CalendarIntervalClass.DAY
    elif window == WindowDurationClass.HOUR:
        return CalendarIntervalClass.HOUR
    else:
        raise Exception(f"Unsupported window duration: {window}")
