import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from pydantic import field_validator
from pydantic.fields import Field
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from datahub.configuration.source_common import EnvConfigMixin
from datahub.configuration.time_window_config import get_time_bucket
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.clickhouse import (
    ClickHouseConfig,
    clickhouse_datetime_format,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)

logger = logging.getLogger(__name__)

# A ClickHouse table identifier is at most `database.table`; restrict to characters
# valid in an (optionally qualified) identifier. query_log_table is interpolated into
# the fetch SQL as an identifier (identifiers cannot be bound as query parameters), so
# validating it here is what prevents SQL injection through that config value.
_QUERY_LOG_TABLE_PATTERN = re.compile(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$")

# SELECT queries recorded in system.query_log. Each row's raw query text is fed
# to the SqlParsingAggregator as an observed query; the aggregator does the SQL
# parsing and usage aggregation (buckets, top users/queries), so this query only
# needs to surface the raw statement plus the user and timestamp.
CLICKHOUSE_USAGE_SQL = """\
SELECT query_id
     , query
     , user                AS username
     , query_start_time    AS starttime
     , event_time          AS endtime
     , normalized_query_hash
  FROM {query_log_table}
 WHERE is_initial_query
   AND type = 'QueryFinish'
   AND query_kind = 'Select'
   AND event_time >= '{start_time}'
   AND event_time < '{end_time}'
   AND query NOT LIKE '%%system.%%'
 ORDER BY event_time DESC"""


class ClickHouseUsageConfig(ClickHouseConfig, BaseUsageConfig, EnvConfigMixin):
    email_domain: str = Field(
        description="Email domain appended to ClickHouse usernames to build the "
        "user URNs surfaced in usage statistics."
    )
    options: dict = Field(default={}, description="")
    query_log_table: str = Field(default="system.query_log", exclude=True)

    @field_validator("query_log_table")
    @classmethod
    def validate_query_log_table(cls, v: str) -> str:
        """Validate the query log table identifier to prevent SQL injection."""
        if not _QUERY_LOG_TABLE_PATTERN.match(v):
            raise ValueError(
                f"Invalid query_log_table '{v}'. It must be an (optionally "
                "database-qualified) identifier containing only alphanumeric "
                "characters and underscores, e.g. 'system.query_log'."
            )
        return v

    def get_sql_alchemy_url(
        self,
        uri_opts: Optional[Dict[str, Any]] = None,
        current_db: Optional[str] = None,
    ) -> str:
        return super().get_sql_alchemy_url(uri_opts=uri_opts, current_db=current_db)


@platform_name("ClickHouse", id="clickhouse")
@config_class(ClickHouseUsageConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(
    SourceCapability.DELETION_DETECTION, "Enabled by default via stateful ingestion"
)
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.USAGE_STATS, "Enabled by default to get usage stats")
class ClickHouseUsageSource(Source):
    """
    Source that extracts usage statistics from ClickHouse by analyzing system.query_log.

    Implementation notes:
    - Queries system.query_log (or custom view via query_log_table config) for SELECT queries
    - Feeds each query into a SqlParsingAggregator as an observed query, so usage is
      computed through the same SQL-parsing path as the rest of the ClickHouse connector
      family (see the aggregator-based path in sql/clickhouse.py)
    - The aggregator produces DatasetUsageStatistics (buckets, top users/queries) with
      generate_usage_statistics=True

    This remains a standalone source (rather than folding into sql/clickhouse.py) to preserve
    the existing `clickhouse-usage` recipe entry point; it now delegates aggregation to the
    shared aggregator instead of the legacy usage_common GenericAggregatedDataset.
    """

    config: ClickHouseUsageConfig
    report: SourceReport

    def __init__(self, config: ClickHouseUsageConfig, ctx: PipelineContext) -> None:
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

        self.aggregator = SqlParsingAggregator(
            platform="clickhouse",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            graph=ctx.graph,
            eager_graph_load=False,
            generate_lineage=False,
            generate_queries=True,
            generate_usage_statistics=True,
            generate_query_usage_statistics=True,
            usage_config=self.config,
            is_allowed_table=self._is_allowed_table,
            format_queries=self.config.format_sql_queries,
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = ClickHouseUsageConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _is_allowed_table(self, name: str) -> bool:
        if "." in name:
            database = name.split(".", 1)[0]
            if not self.config.database_pattern.allowed(database):
                return False
        return self.config.table_pattern.allowed(
            name
        ) or self.config.view_pattern.allowed(name)

    def _make_usage_query(self) -> str:
        # Floor start_time to the bucket boundary so the first (partial) bucket is
        # fully populated, matching the aggregator's bucket list (buckets() floors
        # start_time internally) and the query-log window in sql/clickhouse.py.
        start_time = get_time_bucket(
            self.config.start_time, self.config.bucket_duration
        )
        # Security: the interpolated values are not injectable. start_time/end_time are
        # datetime objects rendered via strftime (fixed numeric format, no user text),
        # and query_log_table is validated to a safe identifier by validate_query_log_table.
        return CLICKHOUSE_USAGE_SQL.format(
            query_log_table=self.config.query_log_table,
            start_time=start_time.strftime(clickhouse_datetime_format),
            end_time=self.config.end_time.strftime(clickhouse_datetime_format),
        )

    def _make_sql_engine(self) -> Engine:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url = {url}")
        return create_engine(url, **self.config.options)

    def _get_observed_queries(self) -> Iterable[ObservedQuery]:
        engine = self._make_sql_engine()
        results = engine.execute(text(self._make_usage_query()))
        for row in results:
            row_dict = dict(row._mapping)

            query = row_dict.get("query")
            if not query:
                continue

            username = row_dict.get("username")
            if isinstance(username, str):
                username = username.strip()
            user_urn: Optional[CorpUserUrn] = None
            if username:
                user_email = (
                    username
                    if "@" in username
                    else f"{username}@{self.config.email_domain}"
                )
                user_urn = CorpUserUrn(user_email)

            timestamp = row_dict.get("starttime") or row_dict.get("endtime")
            if isinstance(timestamp, datetime):
                # system.query_log DateTime columns come back naive; they represent
                # UTC instants, so tag them as UTC rather than astimezone(), which
                # would (wrongly) reinterpret a naive value as the host's local time.
                timestamp = (
                    timestamp.replace(tzinfo=timezone.utc)
                    if timestamp.tzinfo is None
                    else timestamp.astimezone(timezone.utc)
                )

            yield ObservedQuery(
                query=query,
                session_id=row_dict.get("query_id"),
                timestamp=timestamp,
                user=user_urn,
                # Don't pass a default_db. ClickHouse uses 2-level (database.table)
                # naming, but for a 2-level dialect sqlglot slots an existing database
                # qualifier into `db` and lets default_db fill `catalog`, over-qualifying
                # already-qualified names into incorrect URNs like
                # "default.analytics_marts.table".
                default_db=None,
                query_hash=str(row_dict.get("normalized_query_hash", "")),
            )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for observed_query in self._get_observed_queries():
            self.aggregator.add(observed_query)

        yield from auto_workunit(self.aggregator.gen_metadata())

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        self.aggregator.close()
        super().close()
