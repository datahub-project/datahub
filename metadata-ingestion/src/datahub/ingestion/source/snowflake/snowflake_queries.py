import contextlib
import dataclasses
import functools
import json
import logging
import pathlib
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Union

import pydantic
from typing_extensions import Self

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.time_window_config import (
    BaseTimeWindowConfig,
    BucketDuration,
)
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.snowflake.constants import SnowflakeObjectDomain
from datahub.ingestion.source.snowflake.snowflake_config import (
    DEFAULT_TEMP_TABLES_PATTERNS,
    SnowflakeFilterConfig,
    SnowflakeIdentifierConfig,
)
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnection,
    SnowflakeConnectionConfig,
)
from datahub.ingestion.source.snowflake.snowflake_lineage_v2 import (
    SnowflakeLineageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeFilter,
    SnowflakeIdentifierBuilder,
    SnowflakeStructuredReportMixin,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import (
    KnownLineageMapping,
    ObservedQuery,
    PreparsedQuery,
    SqlAggregatorReport,
    SqlParsingAggregator,
    TableRename,
    TableSwap,
)
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
)
from datahub.sql_parsing.sqlglot_utils import get_query_fingerprint
from datahub.utilities.file_backed_collections import ConnectionWrapper, FileBackedList
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)

# Define a type alias
UserName = str
UserEmail = str
UsersMapping = Dict[UserName, UserEmail]


class SnowflakeQueriesExtractorConfig(ConfigModel):
    # TODO: Support stateful ingestion for the time windows.
    window: BaseTimeWindowConfig = BaseTimeWindowConfig()

    pushdown_deny_usernames: List[str] = pydantic.Field(
        default=[],
        description="List of snowflake usernames which will not be considered for lineage/usage/queries extraction. "
        "This is primarily useful for improving performance by filtering out users with extremely high query volumes.",
    )

    user_email_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for user emails to filter in usage.",
    )

    temporary_tables_pattern: List[str] = pydantic.Field(
        default=DEFAULT_TEMP_TABLES_PATTERNS,
        description="[Advanced] Regex patterns for temporary tables to filter in lineage ingestion. Specify regex to "
        "match the entire table name in database.schema.table format. Defaults are to set in such a way "
        "to ignore the temporary staging tables created by known ETL tools.",
    )

    local_temp_path: Optional[pathlib.Path] = pydantic.Field(
        default=None,
        description="Local path to store the audit log.",
        # TODO: For now, this is simply an advanced config to make local testing easier.
        # Eventually, we will want to store date-specific files in the directory and use it as a cache.
        hidden_from_docs=True,
    )

    include_lineage: bool = True
    include_queries: bool = True
    include_usage_statistics: bool = True
    include_query_usage_statistics: bool = True
    include_operations: bool = True


class SnowflakeQueriesSourceConfig(
    SnowflakeQueriesExtractorConfig, SnowflakeIdentifierConfig, SnowflakeFilterConfig
):
    connection: SnowflakeConnectionConfig


@dataclass
class SnowflakeQueriesExtractorReport(Report):
    copy_history_fetch_timer: PerfTimer = dataclasses.field(default_factory=PerfTimer)
    query_log_fetch_timer: PerfTimer = dataclasses.field(default_factory=PerfTimer)
    users_fetch_timer: PerfTimer = dataclasses.field(default_factory=PerfTimer)

    audit_log_load_timer: PerfTimer = dataclasses.field(default_factory=PerfTimer)
    sql_aggregator: Optional[SqlAggregatorReport] = None

    num_ddl_queries_dropped: int = 0
    num_users: int = 0


@dataclass
class SnowflakeQueriesSourceReport(SourceReport):
    window: Optional[BaseTimeWindowConfig] = None
    queries_extractor: Optional[SnowflakeQueriesExtractorReport] = None


class SnowflakeQueriesExtractor(SnowflakeStructuredReportMixin, Closeable):
    def __init__(
        self,
        connection: SnowflakeConnection,
        config: SnowflakeQueriesExtractorConfig,
        structured_report: SourceReport,
        filters: SnowflakeFilter,
        identifiers: SnowflakeIdentifierBuilder,
        graph: Optional[DataHubGraph] = None,
        schema_resolver: Optional[SchemaResolver] = None,
        discovered_tables: Optional[List[str]] = None,
    ):
        self.connection = connection

        self.config = config
        self.report = SnowflakeQueriesExtractorReport()
        self.filters = filters
        self.identifiers = identifiers
        self.discovered_tables = set(discovered_tables) if discovered_tables else None

        self._structured_report = structured_report

        # The exit stack helps ensure that we close all the resources we open.
        self._exit_stack = contextlib.ExitStack()

        self.aggregator: SqlParsingAggregator = self._exit_stack.enter_context(
            SqlParsingAggregator(
                platform=self.identifiers.platform,
                platform_instance=self.identifiers.identifier_config.platform_instance,
                env=self.identifiers.identifier_config.env,
                schema_resolver=schema_resolver,
                graph=graph,
                eager_graph_load=False,
                generate_lineage=self.config.include_lineage,
                generate_queries=self.config.include_queries,
                generate_usage_statistics=self.config.include_usage_statistics,
                generate_query_usage_statistics=self.config.include_query_usage_statistics,
                usage_config=BaseUsageConfig(
                    bucket_duration=self.config.window.bucket_duration,
                    start_time=self.config.window.start_time,
                    end_time=self.config.window.end_time,
                    user_email_pattern=self.config.user_email_pattern,
                    # TODO make the rest of the fields configurable
                ),
                generate_operations=self.config.include_operations,
                is_temp_table=self.is_temp_table,
                is_allowed_table=self.is_allowed_table,
                format_queries=False,
            )
        )
        self.report.sql_aggregator = self.aggregator.report

    @property
    def structured_reporter(self) -> SourceReport:
        return self._structured_report

    @functools.cached_property
    def local_temp_path(self) -> pathlib.Path:
        if self.config.local_temp_path:
            assert self.config.local_temp_path.is_dir()
            return self.config.local_temp_path

        path = pathlib.Path(tempfile.mkdtemp())
        path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Using local temp path: {path}")
        return path

    def is_temp_table(self, name: str) -> bool:
        if any(
            re.match(pattern, name, flags=re.IGNORECASE)
            for pattern in self.config.temporary_tables_pattern
        ):
            return True

        # This is also a temp table if
        #   1. this name would be allowed by the dataset patterns, and
        #   2. we have a list of discovered tables, and
        #   3. it's not in the discovered tables list
        if (
            self.filters.is_dataset_pattern_allowed(name, SnowflakeObjectDomain.TABLE)
            and self.discovered_tables
            and name not in self.discovered_tables
        ):
            return True

        return False

    def is_allowed_table(self, name: str) -> bool:
        if self.discovered_tables and name not in self.discovered_tables:
            return False

        return self.filters.is_dataset_pattern_allowed(
            name, SnowflakeObjectDomain.TABLE
        )

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        with self.report.users_fetch_timer:
            users = self.fetch_users()

        # TODO: Add some logic to check if the cached audit log is stale or not.
        audit_log_file = self.local_temp_path / "audit_log.sqlite"
        use_cached_audit_log = audit_log_file.exists()

        queries: FileBackedList[
            Union[
                KnownLineageMapping,
                PreparsedQuery,
                TableRename,
                TableSwap,
                ObservedQuery,
            ]
        ]
        if use_cached_audit_log:
            logger.info("Using cached audit log")
            shared_connection = ConnectionWrapper(audit_log_file)
            queries = FileBackedList(shared_connection)
        else:
            audit_log_file.unlink(missing_ok=True)

            shared_connection = ConnectionWrapper(audit_log_file)
            queries = FileBackedList(shared_connection)
            entry: Union[
                KnownLineageMapping,
                PreparsedQuery,
                TableRename,
                TableSwap,
                ObservedQuery,
            ]

            with self.report.copy_history_fetch_timer:
                for entry in self.fetch_copy_history():
                    queries.append(entry)

            with self.report.query_log_fetch_timer:
                for entry in self.fetch_query_log(users):
                    queries.append(entry)

        with self.report.audit_log_load_timer:
            for i, query in enumerate(queries):
                if i % 1000 == 0:
                    logger.info(f"Added {i} query log entries to SQL aggregator")
                self.aggregator.add(query)

        yield from auto_workunit(self.aggregator.gen_metadata())
        if not use_cached_audit_log:
            queries.close()
            shared_connection.close()
            audit_log_file.unlink(missing_ok=True)

    def fetch_users(self) -> UsersMapping:
        users: UsersMapping = dict()
        with self.structured_reporter.report_exc("Error fetching users from Snowflake"):
            logger.info("Fetching users from Snowflake")
            query = SnowflakeQuery.get_all_users()
            resp = self.connection.query(query)

            for row in resp:
                try:
                    users[row["NAME"]] = row["EMAIL"]
                    self.report.num_users += 1
                except Exception as e:
                    self.structured_reporter.warning(
                        "Error parsing user row",
                        context=f"{row}",
                        exc=e,
                    )
        return users

    def fetch_copy_history(self) -> Iterable[KnownLineageMapping]:
        # Derived from _populate_external_lineage_from_copy_history.

        query: str = SnowflakeQuery.copy_lineage_history(
            start_time_millis=int(self.config.window.start_time.timestamp() * 1000),
            end_time_millis=int(self.config.window.end_time.timestamp() * 1000),
            downstreams_deny_pattern=self.config.temporary_tables_pattern,
        )

        with self.structured_reporter.report_exc(
            "Error fetching copy history from Snowflake"
        ):
            logger.info("Fetching copy history from Snowflake")
            resp = self.connection.query(query)

            for row in resp:
                try:
                    result = (
                        SnowflakeLineageExtractor._process_external_lineage_result_row(
                            row,
                            discovered_tables=self.discovered_tables,
                            identifiers=self.identifiers,
                        )
                    )
                except Exception as e:
                    self.structured_reporter.warning(
                        "Error parsing copy history row",
                        context=f"{row}",
                        exc=e,
                    )
                else:
                    if result:
                        yield result

    def fetch_query_log(
        self, users: UsersMapping
    ) -> Iterable[Union[PreparsedQuery, TableRename, TableSwap, ObservedQuery]]:
        query_log_query = _build_enriched_query_log_query(
            start_time=self.config.window.start_time,
            end_time=self.config.window.end_time,
            bucket_duration=self.config.window.bucket_duration,
            deny_usernames=self.config.pushdown_deny_usernames,
        )

        with self.structured_reporter.report_exc(
            "Error fetching query log from Snowflake"
        ):
            logger.info("Fetching query log from Snowflake")
            resp = self.connection.query(query_log_query)

            for i, row in enumerate(resp):
                if i > 0 and i % 1000 == 0:
                    logger.info(f"Processed {i} query log rows so far")

                assert isinstance(row, dict)
                try:
                    entry = self._parse_audit_log_row(row, users)
                except Exception as e:
                    self.structured_reporter.warning(
                        "Error parsing query log row",
                        context=f"{row}",
                        exc=e,
                    )
                else:
                    if entry:
                        yield entry

    def _parse_audit_log_row(
        self, row: Dict[str, Any], users: UsersMapping
    ) -> Optional[Union[TableRename, TableSwap, PreparsedQuery, ObservedQuery]]:
        json_fields = {
            "DIRECT_OBJECTS_ACCESSED",
            "OBJECTS_MODIFIED",
            "OBJECT_MODIFIED_BY_DDL",
        }

        res = {}
        for key, value in row.items():
            if key in json_fields and value:
                value = json.loads(value)
            key = key.lower()
            res[key] = value

        direct_objects_accessed = res["direct_objects_accessed"]
        objects_modified = res["objects_modified"]
        object_modified_by_ddl = res["object_modified_by_ddl"]

        if object_modified_by_ddl and not objects_modified:
            known_ddl_entry: Optional[Union[TableRename, TableSwap]] = None
            with self.structured_reporter.report_exc(
                "Error fetching ddl lineage from Snowflake"
            ):
                known_ddl_entry = self.parse_ddl_query(
                    res["query_text"],
                    res["session_id"],
                    res["query_start_time"],
                    object_modified_by_ddl,
                )
            if known_ddl_entry:
                return known_ddl_entry
            elif direct_objects_accessed:
                # Unknown ddl relevant for usage. We want to continue execution here
                pass
            else:
                return None

        user = CorpUserUrn(
            self.identifiers.get_user_identifier(
                res["user_name"], users.get(res["user_name"])
            )
        )

        # Use direct_objects_accessed instead objects_modified
        # objects_modified returns $SYS_VIEW_X with no mapping
        has_stream_objects = any(
            obj.get("objectDomain") == "Stream" for obj in direct_objects_accessed
        )

        # If a stream is used, default to query parsing.
        if has_stream_objects:
            logger.debug("Found matching stream object")
            return ObservedQuery(
                query=res["query_text"],
                session_id=res["session_id"],
                timestamp=res["query_start_time"].astimezone(timezone.utc),
                user=user,
                default_db=res["default_db"],
                default_schema=res["default_schema"],
                query_hash=get_query_fingerprint(
                    res["query_text"], self.identifiers.platform, fast=True
                ),
            )

        upstreams = []
        column_usage = {}

        for obj in direct_objects_accessed:
            dataset = self.identifiers.gen_dataset_urn(
                self.identifiers.get_dataset_identifier_from_qualified_name(
                    obj["objectName"]
                )
            )

            columns = set()
            for modified_column in obj["columns"]:
                columns.add(
                    self.identifiers.snowflake_identifier(modified_column["columnName"])
                )

            upstreams.append(dataset)
            column_usage[dataset] = columns

        downstream = None
        column_lineage = None
        for obj in objects_modified:
            # We don't expect there to be more than one object modified.
            if downstream:
                self.structured_reporter.report_warning(
                    message="Unexpectedly got multiple downstream entities from the Snowflake audit log.",
                    context=f"{row}",
                )

            downstream = self.identifiers.gen_dataset_urn(
                self.identifiers.get_dataset_identifier_from_qualified_name(
                    obj["objectName"]
                )
            )
            column_lineage = []
            for modified_column in obj["columns"]:
                column_lineage.append(
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            dataset=downstream,
                            column=self.identifiers.snowflake_identifier(
                                modified_column["columnName"]
                            ),
                        ),
                        upstreams=[
                            ColumnRef(
                                table=self.identifiers.gen_dataset_urn(
                                    self.identifiers.get_dataset_identifier_from_qualified_name(
                                        upstream["objectName"]
                                    )
                                ),
                                column=self.identifiers.snowflake_identifier(
                                    upstream["columnName"]
                                ),
                            )
                            for upstream in modified_column["directSources"]
                            if upstream["objectDomain"]
                            in SnowflakeQuery.ACCESS_HISTORY_TABLE_VIEW_DOMAINS
                        ],
                    )
                )

        timestamp: datetime = res["query_start_time"]
        timestamp = timestamp.astimezone(timezone.utc)

        # TODO need to map snowflake query types to ours
        query_type = SNOWFLAKE_QUERY_TYPE_MAPPING.get(
            res["query_type"], QueryType.UNKNOWN
        )

        entry = PreparsedQuery(
            # Despite having Snowflake's fingerprints available, our own fingerprinting logic does a better
            # job at eliminating redundant / repetitive queries. As such, we include the fast fingerprint
            # here
            query_id=get_query_fingerprint(
                res["query_text"], self.identifiers.platform, fast=True
            ),
            query_text=res["query_text"],
            upstreams=upstreams,
            downstream=downstream,
            column_lineage=column_lineage,
            column_usage=column_usage,
            inferred_schema=None,
            confidence_score=1.0,
            query_count=res["query_count"],
            user=user,
            timestamp=timestamp,
            session_id=res["session_id"],
            query_type=query_type,
        )
        return entry

    def parse_ddl_query(
        self,
        query: str,
        session_id: str,
        timestamp: datetime,
        object_modified_by_ddl: dict,
    ) -> Optional[Union[TableRename, TableSwap]]:
        timestamp = timestamp.astimezone(timezone.utc)
        if object_modified_by_ddl[
            "operationType"
        ] == "ALTER" and object_modified_by_ddl["properties"].get("swapTargetName"):
            urn1 = self.identifiers.gen_dataset_urn(
                self.identifiers.get_dataset_identifier_from_qualified_name(
                    object_modified_by_ddl["objectName"]
                )
            )

            urn2 = self.identifiers.gen_dataset_urn(
                self.identifiers.get_dataset_identifier_from_qualified_name(
                    object_modified_by_ddl["properties"]["swapTargetName"]["value"]
                )
            )

            return TableSwap(urn1, urn2, query, session_id, timestamp)
        elif object_modified_by_ddl[
            "operationType"
        ] == "RENAME_TABLE" and object_modified_by_ddl["properties"].get("objectName"):
            original_un = self.identifiers.gen_dataset_urn(
                self.identifiers.get_dataset_identifier_from_qualified_name(
                    object_modified_by_ddl["objectName"]
                )
            )

            new_urn = self.identifiers.gen_dataset_urn(
                self.identifiers.get_dataset_identifier_from_qualified_name(
                    object_modified_by_ddl["properties"]["objectName"]["value"]
                )
            )

            return TableRename(original_un, new_urn, query, session_id, timestamp)
        else:
            self.report.num_ddl_queries_dropped += 1
            return None

    def close(self) -> None:
        self._exit_stack.close()


class SnowflakeQueriesSource(Source):
    def __init__(self, ctx: PipelineContext, config: SnowflakeQueriesSourceConfig):
        self.ctx = ctx
        self.config = config
        self.report = SnowflakeQueriesSourceReport()

        self.filters = SnowflakeFilter(
            filter_config=self.config,
            structured_reporter=self.report,
        )
        self.identifiers = SnowflakeIdentifierBuilder(
            identifier_config=self.config,
            structured_reporter=self.report,
        )

        self.connection = self.config.connection.get_connection()

        self.queries_extractor = SnowflakeQueriesExtractor(
            connection=self.connection,
            config=self.config,
            structured_report=self.report,
            filters=self.filters,
            identifiers=self.identifiers,
            graph=self.ctx.graph,
        )
        self.report.queries_extractor = self.queries_extractor.report

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Self:
        config = SnowflakeQueriesSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.report.window = self.config.window

        # TODO: Disable auto status processor?
        return self.queries_extractor.get_workunits_internal()

    def get_report(self) -> SnowflakeQueriesSourceReport:
        return self.report

    def close(self) -> None:
        self.connection.close()
        self.queries_extractor.close()


# Make sure we don't try to generate too much info for a single query.
_MAX_TABLES_PER_QUERY = 20


def _build_enriched_query_log_query(
    start_time: datetime,
    end_time: datetime,
    bucket_duration: BucketDuration,
    deny_usernames: Optional[List[str]],
) -> str:
    start_time_millis = int(start_time.timestamp() * 1000)
    end_time_millis = int(end_time.timestamp() * 1000)

    users_filter = ""
    if deny_usernames:
        user_not_in = ",".join(f"'{user.upper()}'" for user in deny_usernames)
        users_filter = f"user_name NOT IN ({user_not_in})"

    time_bucket_size = bucket_duration.value
    assert time_bucket_size in ("HOUR", "DAY", "MONTH")

    return f"""\
WITH
fingerprinted_queries as (
    SELECT *,
        -- TODO: Generate better fingerprints for each query by pushing down regex logic.
        query_history.query_parameterized_hash as query_fingerprint
    FROM
        snowflake.account_usage.query_history
    WHERE
        query_history.start_time >= to_timestamp_ltz({start_time_millis}, 3)
        AND query_history.start_time < to_timestamp_ltz({end_time_millis}, 3)
        AND execution_status = 'SUCCESS'
        AND {users_filter or "TRUE"}
)
, deduplicated_queries as (
    SELECT
        *,
        DATE_TRUNC(
            {time_bucket_size},
            CONVERT_TIMEZONE('UTC', start_time)
        ) AS bucket_start_time,
        COUNT(*) OVER (PARTITION BY bucket_start_time, query_fingerprint) AS query_count,
    FROM
        fingerprinted_queries
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY bucket_start_time, query_fingerprint ORDER BY start_time DESC) = 1
)
, raw_access_history AS (
    SELECT
        query_id,
        query_start_time,
        user_name,
        direct_objects_accessed,
        objects_modified,
        object_modified_by_ddl
    FROM
        snowflake.account_usage.access_history
    WHERE
        query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
        AND query_start_time < to_timestamp_ltz({end_time_millis}, 3)
        AND {users_filter or "TRUE"}
        AND query_id IN (
            SELECT query_id FROM deduplicated_queries
        )
)
, filtered_access_history AS (
    -- TODO: Add table filter clause.
    SELECT
        query_id,
        query_start_time,
        ARRAY_SLICE(
            FILTER(direct_objects_accessed, o -> o:objectDomain IN {SnowflakeQuery.ACCESS_HISTORY_TABLE_VIEW_DOMAINS_FILTER}),
            0, {_MAX_TABLES_PER_QUERY}
        ) as direct_objects_accessed,
        -- TODO: Drop the columns.baseSources subfield.
        FILTER(objects_modified, o -> o:objectDomain IN {SnowflakeQuery.ACCESS_HISTORY_TABLE_VIEW_DOMAINS_FILTER}) as objects_modified,
        case when object_modified_by_ddl:objectDomain IN {SnowflakeQuery.ACCESS_HISTORY_TABLE_VIEW_DOMAINS_FILTER} then object_modified_by_ddl else null end as object_modified_by_ddl
    FROM raw_access_history
    WHERE ( array_size(direct_objects_accessed) > 0 or array_size(objects_modified) > 0 or object_modified_by_ddl is not null )
)
, query_access_history AS (
    SELECT
        q.bucket_start_time,
        q.query_id,
        q.query_fingerprint,
        q.query_count,
        q.session_id AS "SESSION_ID",
        q.start_time AS "QUERY_START_TIME",
        q.total_elapsed_time AS "QUERY_DURATION",
        q.query_text AS "QUERY_TEXT",
        q.query_type AS "QUERY_TYPE",
        q.database_name as "DEFAULT_DB",
        q.schema_name as "DEFAULT_SCHEMA",
        q.rows_inserted AS "ROWS_INSERTED",
        q.rows_updated AS "ROWS_UPDATED",
        q.rows_deleted AS "ROWS_DELETED",
        q.user_name AS "USER_NAME",
        q.role_name AS "ROLE_NAME",
        a.direct_objects_accessed,
        a.objects_modified,
        a.object_modified_by_ddl
    FROM deduplicated_queries q
    JOIN filtered_access_history a USING (query_id)
)
SELECT * FROM query_access_history
-- Our query aggregator expects the queries to be added in chronological order.
-- It's easier for us to push down the sorting to Snowflake/SQL instead of doing it in Python.
ORDER BY QUERY_START_TIME ASC
"""


SNOWFLAKE_QUERY_TYPE_MAPPING = {
    "INSERT": QueryType.INSERT,
    "UPDATE": QueryType.UPDATE,
    "DELETE": QueryType.DELETE,
    "CREATE": QueryType.CREATE_OTHER,
    "CREATE_TABLE": QueryType.CREATE_DDL,
    "CREATE_VIEW": QueryType.CREATE_VIEW,
    "CREATE_TABLE_AS_SELECT": QueryType.CREATE_TABLE_AS_SELECT,
    "MERGE": QueryType.MERGE,
    "COPY": QueryType.UNKNOWN,
    "TRUNCATE_TABLE": QueryType.UNKNOWN,
}
