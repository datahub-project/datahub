import dataclasses
import logging
from datetime import datetime, timezone
from typing import (
    Callable,
    Counter,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
)

import pydantic
from pydantic import ValidationInfo, field_validator
from pydantic.fields import Field

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.configuration.time_window_config import (
    BaseTimeWindowConfig,
    BucketDuration,
    get_time_bucket,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    DatasetFieldUsageCountsClass,
    DatasetUsageStatisticsClass,
    DatasetUserUsageCountsClass,
    TimeWindowSizeClass,
)
from datahub.utilities.file_backed_collections import (
    ConnectionWrapper,
    FileBackedCounter,
    FileBackedDict,
    GroupedItemCounter,
)
from datahub.utilities.sql_formatter import format_sql_query, trim_query

logger = logging.getLogger(__name__)

ResourceType = TypeVar("ResourceType")

# The total number of characters allowed across all queries in a single workunit.
DEFAULT_QUERIES_CHARACTER_LIMIT = 24000


def default_user_urn_builder(email: str) -> str:
    return builder.make_user_urn(email.split("@")[0])


def extract_user_email(user: str) -> Optional[str]:
    """Extracts user email from user input

    >>> extract_user_email('urn:li:corpuser:abc@xyz.com')
    'abc@xyz.com'
    >>> extract_user_email('urn:li:corpuser:abc')
    >>> extract_user_email('abc@xyz.com')
    'abc@xyz.com'
    """
    if user.startswith(("urn:li:corpuser:", "urn:li:corpGroup:")):
        user = user.split(":")[-1]
    return user if "@" in user else None


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
        if top_n_queries < len(query_freq):
            logger.warning(
                f"Top N query limit exceeded on {str(resource)}.  Max number of queries {top_n_queries} <  {len(query_freq)}. Truncating top queries to {top_n_queries}."
            )
            query_freq = query_freq[0:top_n_queries]

        budget_per_query: int = int(queries_character_limit / top_n_queries)
        top_sql_queries = [
            trim_query(
                (
                    format_sql_query(query, keyword_case="upper", reindent_aligned=True)
                    if format_sql_queries
                    else query
                ),
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
                userEmail=extract_user_email(user),
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
        count: int = 1,
    ) -> None:
        if user_email and not user_email_pattern.allowed(user_email):
            return

        self.readCount += count
        if user_email is not None:
            self.userFreq[user_email] += count

        if query:
            self.queryCount += count
            self.queryFreq[query] += count
        for column in fields:
            self.columnFreq[column] += count

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
    queries_character_limit: HiddenFromDocs[int] = Field(
        # Hidden since we don't want to encourage people to break elasticsearch.
        default=DEFAULT_QUERIES_CHARACTER_LIMIT,
        description=(
            "Total character limit for all queries in a single usage aspect."
            " Queries will be truncated to length `queries_character_limit / top_n_queries`."
        ),
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

    @field_validator("top_n_queries", mode="after")
    @classmethod
    def ensure_top_n_queries_is_not_too_big(cls, v: int, info: ValidationInfo) -> int:
        minimum_query_size = 20
        values = info.data
        max_queries = int(values["queries_character_limit"] / minimum_query_size)
        if v > max_queries:
            raise ValueError(
                f"top_n_queries is set to {v} but it can be maximum {max_queries}"
            )
        return v


class _Dim:
    """The counted dimensions, stored as item-key prefixes in the usage counter."""

    USER = "user"
    QUERY = "query"
    COLUMN = "column"


class _UsageKeys:
    """Single source of truth for the usage counter's (group_key, item_key) encoding.

    Keys are colon-joined. The prefixes -- an integer bucket and a fixed dim name --
    never contain ':', so str.partition(':') round-trips arbitrary resource/value content.
    """

    # Item key recording group existence even when there are no real counts (denied or
    # empty events), so the group still emits an (empty) workunit.
    EXISTENCE = ""

    @staticmethod
    def group(bucket_millis: int, resource_key: str) -> str:
        return f"{bucket_millis}:{resource_key}"

    @staticmethod
    def split_group(group_key: str) -> Tuple[int, str]:
        bucket_str, _, resource_key = group_key.partition(":")
        return int(bucket_str), resource_key

    @staticmethod
    def item(dim: str, value: str) -> str:
        return f"{dim}:{value}"

    @staticmethod
    def split_item(item_key: str) -> Tuple[str, str]:
        dim, _, value = item_key.partition(":")
        return dim, value


class UsageAggregator(Generic[ResourceType], Closeable):
    """Aggregates usage events into per-(bucket, resource) workunits.

    Single-writer per instance: ``aggregate_event`` must not be called concurrently on
    the same aggregator. The counter (``FileBackedCounter``) is thread-safe, but the
    resource store (``FileBackedDict``) is not, so a shared instance would need an
    external lock. For parallel ingestion, use one aggregator per worker.
    """

    # TODO: Move over other connectors to use this class

    def __init__(
        self,
        config: BaseUsageConfig,
        *,
        shared_connection: Optional[ConnectionWrapper] = None,
        batch_size: int = 50000,
        counter: Optional[GroupedItemCounter] = None,
    ) -> None:
        self.config = config
        self._closed = False
        # Counts of (query|user|column) items per (bucket, resource). Defaults to the
        # disk-backed counter; an in-memory counter can be injected (e.g. for tests or
        # small workloads).
        self._counts: GroupedItemCounter = counter or FileBackedCounter(
            shared_connection=shared_connection,
            tablename="usage_counts",
            batch_size=batch_size,
        )
        # Resource objects, pickled to disk keyed by resource_key, so heap memory stays
        # flat regardless of resource cardinality.
        self._resources: FileBackedDict[ResourceType] = FileBackedDict(
            shared_connection=shared_connection,
            tablename="usage_resources",
        )

    @staticmethod
    def _bucket_millis(start_time: datetime, bucket_duration: BucketDuration) -> int:
        return int(get_time_bucket(start_time, bucket_duration).timestamp() * 1000)

    def aggregate_event(
        self,
        *,
        resource: ResourceType,
        start_time: datetime,
        query: Optional[str],
        user: Optional[str],
        fields: List[str],
        count: int = 1,
    ) -> None:
        bucket = self._bucket_millis(start_time, self.config.bucket_duration)
        # str(resource) is the identity key; resources whose str() are equal are
        # intentionally aggregated together (holds for UrnStr and usage-path
        # TableReference, whose last_updated is unset).
        resource_key = str(resource)
        group_key = _UsageKeys.group(bucket, resource_key)

        # FileBackedDict's bounded in-memory cache acts as the write buffer and dedups
        # repeated sets of the same key, so set unconditionally (last-write-wins; an
        # immaterial divergence from the prior first-write-wins, since resources with
        # equal str() are interchangeable).
        self._resources[resource_key] = resource
        self._add_read_entry(
            group_key, user=user, query=query, fields=fields, count=count
        )

    def _add_read_entry(
        self,
        group_key: str,
        *,
        user: Optional[str],
        query: Optional[str],
        fields: List[str],
        count: int,
    ) -> None:
        # Record the group's existence even for denied/empty events, so they still emit
        # an empty workunit (mirrors GenericAggregatedDataset.add_read_entry, which
        # always creates the aggregate before its denied-user early return).
        self._counts.increment(group_key, _UsageKeys.EXISTENCE, count=0)
        # A denied user records nothing further.
        if user and not self.config.user_email_pattern.allowed(user):
            return
        if user:
            self._counts.increment(group_key, _UsageKeys.item(_Dim.USER, user), count)
        if query:
            self._counts.increment(group_key, _UsageKeys.item(_Dim.QUERY, query), count)
        for field in fields:
            self._counts.increment(
                group_key, _UsageKeys.item(_Dim.COLUMN, field), count
            )

    def generate_workunits(
        self,
        resource_urn_builder: Callable[[ResourceType], str],
        user_urn_builder: Optional[Callable[[str], str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        for group_key, items in self._counts.most_common_by_group():
            bucket_millis, resource_key = _UsageKeys.split_group(group_key)
            resource = self._resources[resource_key]

            query_freq: List[Tuple[str, int]] = []
            user_freq: List[Tuple[str, int]] = []
            column_freq: List[Tuple[str, int]] = []
            query_count = 0
            for item_key, cnt in items:
                dim, item = _UsageKeys.split_item(item_key)
                if dim == _Dim.QUERY:
                    query_freq.append((item, cnt))
                    query_count += cnt
                elif dim == _Dim.USER:
                    user_freq.append((item, cnt))
                elif dim == _Dim.COLUMN:
                    column_freq.append((item, cnt))
                # dim == "" -> existence sentinel, skip

            yield make_usage_workunit(
                bucket_start_time=datetime.fromtimestamp(
                    bucket_millis / 1000, tz=timezone.utc
                ),
                resource=resource,
                query_count=query_count,
                query_freq=(
                    query_freq[: self.config.top_n_queries]
                    if self.config.include_top_n_queries
                    else None
                ),
                user_freq=user_freq,
                column_freq=column_freq,
                bucket_duration=self.config.bucket_duration,
                resource_urn_builder=resource_urn_builder,
                user_urn_builder=user_urn_builder,
                top_n_queries=self.config.top_n_queries,
                format_sql_queries=self.config.format_sql_queries,
                queries_character_limit=self.config.queries_character_limit,
            )

    def close(self) -> None:
        if self._closed:
            return
        self._counts.close()
        self._resources.close()
        self._closed = True
