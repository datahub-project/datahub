import dataclasses
import logging
import pickle
from datetime import datetime, timezone
from typing import (
    Callable,
    Counter,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Set,
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
from datahub.utilities.file_backed_collections import ConnectionWrapper
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


class UsageAggregator(Generic[ResourceType], Closeable):
    # TODO: Move over other connectors to use this class

    _UPSERT_COUNT = (
        "INSERT INTO usage_counts(bucket, resource_key, dim, item, cnt) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(bucket, resource_key, dim, item) "
        "DO UPDATE SET cnt = cnt + excluded.cnt"
    )

    def __init__(
        self,
        config: BaseUsageConfig,
        *,
        shared_connection: Optional[ConnectionWrapper] = None,
        batch_size: int = 50000,
    ) -> None:
        self.config = config
        self._batch_size = batch_size
        self._conn = shared_connection or ConnectionWrapper()
        self._owns_connection = shared_connection is None
        self._closed = False

        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS usage_resources("
            "resource_key TEXT PRIMARY KEY, resource BLOB)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS usage_aggregates("
            "bucket INTEGER, resource_key TEXT, PRIMARY KEY(bucket, resource_key))"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS usage_counts("
            "bucket INTEGER, resource_key TEXT, dim TEXT, item TEXT, cnt INTEGER, "
            "PRIMARY KEY(bucket, resource_key, dim, item))"
        )

        # All unbounded data lives in SQLite so heap memory stays flat regardless of
        # resource cardinality: the high-cardinality per-query/user/column counts go to
        # usage_counts, and the resource objects themselves are pickled to disk in
        # usage_resources (one row per distinct resource) rather than retained in memory.
        self._resource_buf: Dict[str, bytes] = {}

        # Write buffers, deduplicated/combined within a flush window. counts are
        # pre-summed so repeated increments to the same key become one upsert.
        self._aggregate_buf: Set[Tuple[int, str]] = set()
        self._count_buf: Dict[Tuple[int, str, str, str], int] = {}

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

        # Always record existence + resource, even for denied/empty events, so they
        # still emit an empty workunit (matches the in-memory implementation).
        self._aggregate_buf.add((bucket, resource_key))
        if resource_key not in self._resource_buf:
            self._resource_buf[resource_key] = pickle.dumps(resource)

        # Replicate add_read_entry's filter: a denied user records nothing further.
        if not (user and not self.config.user_email_pattern.allowed(user)):
            if user:
                self._bump(bucket, resource_key, "user", user, count)
            if query:
                self._bump(bucket, resource_key, "query", query, count)
            for field in fields:
                self._bump(bucket, resource_key, "column", field, count)

        if (
            len(self._count_buf) >= self._batch_size
            or len(self._aggregate_buf) >= self._batch_size
        ):
            self._flush()

    def _bump(
        self, bucket: int, resource_key: str, dim: str, item: str, count: int
    ) -> None:
        key = (bucket, resource_key, dim, item)
        self._count_buf[key] = self._count_buf.get(key, 0) + count

    def _flush(self) -> None:
        if self._resource_buf:
            self._conn.executemany(
                "INSERT OR IGNORE INTO usage_resources(resource_key, resource) "
                "VALUES (?, ?)",
                list(self._resource_buf.items()),
            )
            self._resource_buf.clear()
        if self._aggregate_buf:
            self._conn.executemany(
                "INSERT OR IGNORE INTO usage_aggregates(bucket, resource_key) "
                "VALUES (?, ?)",
                list(self._aggregate_buf),
            )
            self._aggregate_buf.clear()
        if self._count_buf:
            self._conn.executemany(
                self._UPSERT_COUNT,
                [
                    (bucket, rk, dim, item, cnt)
                    for (bucket, rk, dim, item), cnt in self._count_buf.items()
                ],
            )
            self._count_buf.clear()

    def generate_workunits(
        self,
        resource_urn_builder: Callable[[ResourceType], str],
        user_urn_builder: Optional[Callable[[str], str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        self._flush()
        # c.rowid ASC is a deterministic tie-break for equal counts: the ON CONFLICT
        # upsert preserves a row's rowid on update, so rowid reflects first-insertion
        # order of each (bucket, resource_key, dim, item). This mirrors
        # Counter.most_common's insertion-order tie-break, keeping output (and usage
        # golden files) stable.
        cursor = self._conn.execute(
            "SELECT a.bucket, a.resource_key, r.resource, c.dim, c.item, c.cnt "
            "FROM usage_aggregates a "
            "JOIN usage_resources r ON r.resource_key = a.resource_key "
            "LEFT JOIN usage_counts c "
            "  ON c.bucket = a.bucket AND c.resource_key = a.resource_key "
            "ORDER BY a.bucket, a.resource_key, c.dim, c.cnt DESC, c.rowid ASC"
        )

        cur_key: Optional[Tuple[int, str]] = None
        cur_resource: Optional[ResourceType] = None
        query_freq: List[Tuple[str, int]] = []
        user_freq: List[Tuple[str, int]] = []
        column_freq: List[Tuple[str, int]] = []
        query_count = 0

        def build() -> MetadataWorkUnit:
            assert cur_key is not None
            assert cur_resource is not None  # narrows Optional for mypy
            return make_usage_workunit(
                bucket_start_time=datetime.fromtimestamp(
                    cur_key[0] / 1000, tz=timezone.utc
                ),
                resource=cur_resource,
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

        for bucket, resource_key, resource_blob, dim, item, cnt in cursor:
            key = (bucket, resource_key)
            if key != cur_key:
                if cur_key is not None:
                    yield build()
                cur_key = key
                cur_resource = pickle.loads(resource_blob)
                query_freq, user_freq, column_freq, query_count = [], [], [], 0
            if dim is None:
                continue  # empty aggregate (LEFT JOIN miss)
            if dim == "query":
                query_freq.append((item, cnt))
                query_count += cnt
            elif dim == "user":
                user_freq.append((item, cnt))
            elif dim == "column":
                column_freq.append((item, cnt))
        if cur_key is not None:
            yield build()

    def close(self) -> None:
        if self._closed:
            return
        self._flush()
        if self._owns_connection:
            self._conn.close()
        self._closed = True
