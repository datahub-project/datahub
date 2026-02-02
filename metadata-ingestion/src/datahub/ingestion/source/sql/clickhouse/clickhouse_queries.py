import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import cached_property
from typing import Dict, Iterable, List, Optional

import pydantic
from pydantic import Field
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing_extensions import Self

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.time_window_config import (
    BaseTimeWindowConfig,
    get_time_bucket,
)
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sql.clickhouse.source import ClickHouseConfig
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlAggregatorReport,
    SqlParsingAggregator,
)
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)

CLICKHOUSE_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Default patterns for temporary tables in ClickHouse
DEFAULT_TEMP_TABLES_PATTERNS: List[str] = [
    r"^_.*",  # Tables starting with underscore
    r".*\.tmp_.*",  # Tables with tmp_ prefix
    r".*\.temp_.*",  # Tables with temp_ prefix
    r".*\._inner.*",  # Inner tables for materialized views
]


class ClickHouseQueriesExtractorConfig(ConfigModel):
    """Configuration for extracting queries from ClickHouse query_log."""

    window: BaseTimeWindowConfig = Field(
        default_factory=BaseTimeWindowConfig,
        description="Time window configuration for query log extraction.",
    )

    user_email_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for user emails/usernames to filter in usage.",
    )

    pushdown_deny_usernames: List[str] = Field(
        default=[],
        description="List of ClickHouse usernames to exclude from extraction. "
        "Uses exact matching. Example: ['system', 'default'].",
    )

    temporary_tables_pattern: List[str] = Field(
        default=DEFAULT_TEMP_TABLES_PATTERNS,
        description="Regex patterns for temporary tables to filter from lineage. "
        "Patterns match against 'database.table' format.",
    )

    include_lineage: bool = Field(
        default=True,
        description="Whether to extract table/column lineage from INSERT/CREATE queries.",
    )

    include_queries: bool = Field(
        default=True,
        description="Whether to emit Query entities.",
    )

    include_usage_statistics: bool = Field(
        default=True,
        description="Whether to extract dataset usage statistics from SELECT queries.",
    )

    include_query_usage_statistics: bool = Field(
        default=True,
        description="Whether to emit query-level usage statistics.",
    )

    include_operations: bool = Field(
        default=True,
        description="Whether to emit operation aspects (INSERT, UPDATE, etc.).",
    )

    query_log_table: str = Field(
        default="system.query_log",
        description="The query log table to read from. "
        "Can be customized to a view for clustered setups.",
    )

    top_n_queries: pydantic.PositiveInt = Field(
        default=10,
        description="Number of top queries to save to each table.",
    )

    @cached_property
    def _compiled_temporary_tables_pattern(self) -> List[re.Pattern[str]]:
        return [
            re.compile(pattern, re.IGNORECASE)
            for pattern in self.temporary_tables_pattern
        ]


class ClickHouseQueriesSourceConfig(ClickHouseQueriesExtractorConfig, ClickHouseConfig):
    """Configuration for the ClickHouse Queries source.

    Inherits connection settings from ClickHouseConfig and
    query extraction settings from ClickHouseQueriesExtractorConfig.
    """

    pass


@dataclass
class ClickHouseQueriesExtractorReport(Report):
    """Report for ClickHouse queries extraction."""

    query_log_fetch_timer: PerfTimer = field(default_factory=PerfTimer)
    audit_log_load_timer: PerfTimer = field(default_factory=PerfTimer)

    num_queries_parsed: int = 0
    num_queries_failed: int = 0
    num_lineage_queries: int = 0
    num_usage_queries: int = 0

    sql_aggregator: Optional[SqlAggregatorReport] = None


@dataclass
class ClickHouseQueriesSourceReport(SourceReport):
    """Source-level report wrapping extractor report."""

    window: Optional[BaseTimeWindowConfig] = None
    queries_extractor: Optional[ClickHouseQueriesExtractorReport] = None


class ClickHouseQueriesExtractor(Closeable):
    """Extracts query metadata from ClickHouse's system.query_log table.

    Generates lineage, usage statistics, and operations using SqlParsingAggregator.
    """

    def __init__(
        self,
        config: ClickHouseQueriesExtractorConfig,
        connection_config: ClickHouseConfig,
        structured_report: SourceReport,
        graph: Optional[DataHubGraph] = None,
        schema_resolver: Optional[SchemaResolver] = None,
    ):
        self.config = config
        self.connection_config = connection_config
        self.structured_report = structured_report
        self.report = ClickHouseQueriesExtractorReport()

        self.start_time, self.end_time = self._get_time_window()

        self.aggregator = SqlParsingAggregator(
            platform="clickhouse",
            platform_instance=connection_config.platform_instance,
            env=connection_config.env,
            schema_resolver=schema_resolver,
            graph=graph,
            eager_graph_load=False,
            generate_lineage=config.include_lineage,
            generate_queries=config.include_queries,
            generate_usage_statistics=config.include_usage_statistics,
            generate_query_usage_statistics=config.include_query_usage_statistics,
            usage_config=BaseUsageConfig(
                bucket_duration=config.window.bucket_duration,
                start_time=self.start_time,
                end_time=self.end_time,
                user_email_pattern=config.user_email_pattern,
                top_n_queries=config.top_n_queries,
            ),
            generate_operations=config.include_operations,
            is_temp_table=self._is_temp_table,
            format_queries=False,
        )
        self.report.sql_aggregator = self.aggregator.report

    def _get_time_window(self) -> tuple[datetime, datetime]:
        """Get the time window for query extraction, aligned to bucket boundaries."""
        start_time = self.config.window.start_time
        end_time = self.config.window.end_time

        # Align to bucket boundaries for accurate aggregation
        if self.config.include_usage_statistics:
            start_time = get_time_bucket(start_time, self.config.window.bucket_duration)

        return start_time, end_time

    def _is_temp_table(self, name: str) -> bool:
        """Check if a table name matches temporary table patterns."""
        for pattern in self.config._compiled_temporary_tables_pattern:
            if pattern.match(name):
                return True
        return False

    def _make_sql_engine(self) -> Engine:
        """Create SQLAlchemy engine for ClickHouse connection."""
        url = self.connection_config.get_sql_alchemy_url()
        logger.debug(f"Creating ClickHouse connection: {url}")
        return create_engine(url, **self.connection_config.options)

    def _build_query_log_query(self) -> str:
        """Build SQL query to fetch relevant entries from query_log."""
        start_time_str = self.start_time.strftime(CLICKHOUSE_DATETIME_FORMAT)
        end_time_str = self.end_time.strftime(CLICKHOUSE_DATETIME_FORMAT)

        # Build user exclusion filter
        user_filters = []
        for username in self.config.pushdown_deny_usernames:
            user_filters.append(f"user != '{username}'")
        user_filter_clause = " AND ".join(user_filters) if user_filters else "1=1"

        # Query kinds that produce lineage (INSERT, CREATE TABLE AS)
        # For usage, we also include SELECT
        query_kinds = ["'Insert'", "'Create'", "'Select'"]
        query_kinds_clause = ", ".join(query_kinds)

        return f"""
SELECT
    query_id,
    query,
    query_kind,
    user,
    event_time,
    query_duration_ms,
    read_rows,
    written_rows,
    current_database,
    normalized_query_hash
FROM {self.config.query_log_table}
WHERE type = 'QueryFinish'
  AND is_initial_query = 1
  AND event_time >= '{start_time_str}'
  AND event_time < '{end_time_str}'
  AND query_kind IN ({query_kinds_clause})
  AND {user_filter_clause}
  AND query NOT LIKE '%system.%'
ORDER BY event_time ASC
"""

    def _parse_query_log_row(self, row: Dict) -> Optional[ObservedQuery]:
        """Parse a query_log row into an ObservedQuery."""
        try:
            timestamp = row["event_time"]
            if isinstance(timestamp, datetime):
                timestamp = timestamp.astimezone(timezone.utc)

            query = row["query"]
            query_kind = row.get("query_kind", "")
            user = row.get("user", "")

            # Track query types for reporting
            if query_kind in ("Insert", "Create"):
                self.report.num_lineage_queries += 1
            elif query_kind == "Select":
                self.report.num_usage_queries += 1

            return ObservedQuery(
                query=query,
                session_id=row.get("query_id"),
                timestamp=timestamp,
                user=CorpUserUrn(user) if user else None,
                default_db=row.get("current_database"),
                query_hash=str(row.get("normalized_query_hash", "")),
            )
        except Exception as e:
            logger.warning(f"Failed to parse query log row: {e}")
            self.report.num_queries_failed += 1
            return None

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Main extraction method that fetches queries and generates workunits."""
        engine = self._make_sql_engine()

        with self.report.query_log_fetch_timer:
            query = self._build_query_log_query()
            logger.info(f"Fetching query log from ClickHouse: {query[:200]}...")

            try:
                result = engine.execute(text(query))
                rows = list(result)
                logger.info(f"Fetched {len(rows)} queries from query_log")
            except Exception as e:
                self.structured_report.failure(
                    "Failed to fetch query log from ClickHouse",
                    context=str(e),
                )
                return

        with self.report.audit_log_load_timer:
            for i, row in enumerate(rows):
                if i > 0 and i % 1000 == 0:
                    logger.info(f"Processed {i} query log entries")

                row_dict = dict(row._mapping)
                observed_query = self._parse_query_log_row(row_dict)

                if observed_query:
                    self.report.num_queries_parsed += 1
                    self.aggregator.add(observed_query)

        logger.info(
            f"Query log processing complete: {self.report.num_queries_parsed} parsed, "
            f"{self.report.num_queries_failed} failed, "
            f"{self.report.num_lineage_queries} lineage queries, "
            f"{self.report.num_usage_queries} usage queries"
        )

        yield from auto_workunit(self.aggregator.gen_metadata())

    def close(self) -> None:
        """Clean up resources."""
        self.aggregator.close()


@platform_name("ClickHouse")
@config_class(ClickHouseQueriesSourceConfig)
@support_status(SupportStatus.TESTING)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Extracts table-level lineage from INSERT/CREATE queries in query_log.",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Extracts column-level lineage via SQL parsing.",
)
@capability(
    SourceCapability.USAGE_STATS,
    "Extracts usage statistics from SELECT queries.",
)
class ClickHouseQueriesSource(Source):
    """
    This source extracts metadata from ClickHouse's query_log table:

    - **Lineage**: Table and column-level lineage from INSERT and CREATE TABLE AS queries
    - **Usage Statistics**: Dataset usage from SELECT queries
    - **Operations**: Operation aspects (INSERT, UPDATE, etc.)
    - **Query Entities**: Query metadata for analysis

    This source complements the `clickhouse` source (which extracts schema metadata)
    by adding runtime query-based insights.

    ### Key Features

    1. **Query-based lineage**: Captures data flow from actual INSERT/CREATE queries
    2. **Usage analytics**: Tracks which tables are queried and by whom
    3. **Unified extraction**: Single source for lineage + usage (replaces `clickhouse-usage`)

    ### Example Recipe

    ```yaml
    source:
      type: clickhouse-queries
      config:
        host_port: "localhost:8123"
        username: default
        password: ""

        window:
          start_time: "2024-01-01T00:00:00Z"
          end_time: "2024-01-08T00:00:00Z"
          bucket_duration: DAY

        include_lineage: true
        include_usage_statistics: true

        database_pattern:
          allow: ["production.*", "analytics.*"]
          deny: [".*_temp"]

    sink:
      type: datahub-rest
      config:
        server: "http://localhost:8080"
    ```

    :::note

    This source replaces `clickhouse-usage` by providing both lineage and usage
    extraction in a single source. Migrate by using `include_usage_statistics: true`.

    :::
    """

    def __init__(self, ctx: PipelineContext, config: ClickHouseQueriesSourceConfig):
        self.ctx = ctx
        self.config = config
        self.report = ClickHouseQueriesSourceReport()

        self.queries_extractor = ClickHouseQueriesExtractor(
            config=config,
            connection_config=config,
            structured_report=self.report,
            graph=self.ctx.graph,
        )
        self.report.queries_extractor = self.queries_extractor.report

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Self:
        config = ClickHouseQueriesSourceConfig.model_validate(config_dict)
        return cls(ctx, config)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.report.window = self.config.window
        return self.queries_extractor.get_workunits_internal()

    def get_report(self) -> ClickHouseQueriesSourceReport:
        return self.report

    def close(self) -> None:
        self.queries_extractor.close()
        super().close()
