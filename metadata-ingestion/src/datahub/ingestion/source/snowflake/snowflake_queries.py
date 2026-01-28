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
from functools import cached_property
from typing import Any, Dict, Iterable, List, Optional, Union

import pydantic
from typing_extensions import Self

from datahub.configuration.common import AllowDenyPattern, ConfigModel, HiddenFromDocs
from datahub.configuration.time_window_config import (
    BaseTimeWindowConfig,
    BucketDuration,
    get_time_bucket,
)
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import SupportStatus, config_class, support_status
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.snowflake.constants import SnowflakeObjectDomain
from datahub.ingestion.source.snowflake.snowflake_config import (
    DEFAULT_TEMP_TABLES_PATTERNS,
    QueryDedupStrategyType,
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
from datahub.ingestion.source.snowflake.stored_proc_lineage import (
    StoredProcCall,
    StoredProcLineageReport,
    StoredProcLineageTracker,
)
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantQueriesRunSkipHandler,
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
from datahub.utilities.file_backed_collections import (
    ConnectionWrapper,
    FileBackedList,
)
from datahub.utilities.lossy_collections import LossyList
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
        description="List of snowflake usernames (SQL LIKE patterns, e.g., 'SERVICE_%', '%_PROD', 'TEST_USER') which will NOT be considered for lineage/usage/queries extraction. "
        "This is primarily useful for improving performance by filtering out users with extremely high query volumes.",
    )

    pushdown_allow_usernames: List[str] = pydantic.Field(
        default=[],
        description="List of snowflake usernames (SQL LIKE patterns, e.g., 'ANALYST_%', '%_USER', 'MAIN_ACCOUNT') which WILL be considered for lineage/usage/queries extraction. "
        "This is primarily useful for improving performance by filtering in only specific users. "
        "If not specified, all users not in deny list are included.",
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

    local_temp_path: HiddenFromDocs[Optional[pathlib.Path]] = pydantic.Field(
        # TODO: For now, this is simply an advanced config to make local testing easier.
        # Eventually, we will want to store date-specific files in the directory and use it as a cache.
        default=None,
        description="Local path to store the audit log.",
    )

    include_lineage: bool = True
    include_queries: bool = True
    include_usage_statistics: bool = True
    include_query_usage_statistics: bool = True
    include_operations: bool = True

    push_down_database_pattern_access_history: bool = pydantic.Field(
        default=False,
        description="If enabled, pushes down database pattern filtering to the access_history table for improved performance. "
        "This filters on the accessed objects in access_history.",
    )

    additional_database_names_allowlist: List[str] = pydantic.Field(
        default=[],
        description="Additional database names (no pattern matching) to be included in the access_history filter. "
        "Only applies if push_down_database_pattern_access_history=True. "
        "These databases will be included in the filter being pushed down regardless of database_pattern settings."
        "This may be required in the case of _eg_ temporary tables being created in a different database than the ones in the database_name patterns.",
    )

    query_dedup_strategy: QueryDedupStrategyType = QueryDedupStrategyType.STANDARD

    @cached_property
    def _compiled_temporary_tables_pattern(self) -> "List[re.Pattern[str]]":
        return [
            re.compile(pattern, re.IGNORECASE)
            for pattern in self.temporary_tables_pattern
        ]


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
    aggregator_generate_timer: PerfTimer = dataclasses.field(default_factory=PerfTimer)

    sql_aggregator: Optional[SqlAggregatorReport] = None
    stored_proc_lineage: Optional[StoredProcLineageReport] = None

    num_ddl_queries_dropped: int = 0
    num_stream_queries_observed: int = 0
    num_create_temp_view_queries_observed: int = 0
    num_users: int = 0
    num_queries_with_empty_column_name: int = 0
    queries_with_empty_column_name: LossyList[str] = dataclasses.field(
        default_factory=LossyList
    )


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
        redundant_run_skip_handler: Optional[RedundantQueriesRunSkipHandler] = None,
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
        self.redundant_run_skip_handler = redundant_run_skip_handler

        self._structured_report = structured_report

        # Adjust time window based on stateful ingestion state
        self.start_time, self.end_time = self._get_time_window()

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
                    start_time=self.start_time,
                    end_time=self.end_time,
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

    def _get_time_window(self) -> tuple[datetime, datetime]:
        if self.redundant_run_skip_handler:
            start_time, end_time = (
                self.redundant_run_skip_handler.suggest_run_time_window(
                    self.config.window.start_time,
                    self.config.window.end_time,
                )
            )
        else:
            start_time = self.config.window.start_time
            end_time = self.config.window.end_time

        # Usage statistics are aggregated per bucket (typically per day).
        # To ensure accurate aggregated metrics, we need to align the start_time
        # to the beginning of a bucket so that we include complete bucket periods.
        if self.config.include_usage_statistics:
            start_time = get_time_bucket(start_time, self.config.window.bucket_duration)

        return start_time, end_time

    def _update_state(self) -> None:
        if self.redundant_run_skip_handler:
            self.redundant_run_skip_handler.update_state(
                self.config.window.start_time,
                self.config.window.end_time,
                self.config.window.bucket_duration,
            )

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
            pattern.match(name)
            for pattern in self.config._compiled_temporary_tables_pattern
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

        if self.config.local_temp_path is None:
            self._exit_stack.callback(lambda: audit_log_file.unlink(missing_ok=True))

        shared_connection = self._exit_stack.enter_context(
            ConnectionWrapper(audit_log_file)
        )
        queries: FileBackedList[
            Union[
                KnownLineageMapping,
                PreparsedQuery,
                TableRename,
                TableSwap,
                ObservedQuery,
                StoredProcCall,
            ]
        ] = self._exit_stack.enter_context(FileBackedList(shared_connection))

        if use_cached_audit_log:
            logger.info(f"Using cached audit log at {audit_log_file}")
        else:
            # Check if any query-based features are enabled before fetching
            needs_query_data = any(
                [
                    self.config.include_lineage,
                    self.config.include_queries,
                    self.config.include_usage_statistics,
                    self.config.include_query_usage_statistics,
                    self.config.include_operations,
                ]
            )

            if not needs_query_data:
                logger.info(
                    "All query-based features are disabled. Skipping expensive query log fetch."
                )
            else:
                logger.info(f"Fetching audit log into {audit_log_file}")

                with self.report.copy_history_fetch_timer:
                    for copy_entry in self.fetch_copy_history():
                        queries.append(copy_entry)

                with self.report.query_log_fetch_timer:
                    for entry in self.fetch_query_log(users):
                        queries.append(entry)

        stored_proc_tracker: StoredProcLineageTracker = self._exit_stack.enter_context(
            StoredProcLineageTracker(
                platform=self.identifiers.platform,
                shared_connection=shared_connection,
            )
        )
        self.report.stored_proc_lineage = stored_proc_tracker.report

        with self.report.audit_log_load_timer:
            for i, query in enumerate(queries):
                if i % 1000 == 0:
                    logger.info(f"Added {i} query log entries to SQL aggregator")

                if isinstance(query, StoredProcCall):
                    stored_proc_tracker.add_stored_proc_call(query)
                    continue

                if not (
                    isinstance(query, PreparsedQuery)
                    and stored_proc_tracker.add_related_query(query)
                ):
                    # Only add to aggregator if it's not part of a stored procedure.
                    self.aggregator.add(query)

            # Generate and add stored procedure lineage entries.
            for lineage_entry in stored_proc_tracker.build_merged_lineage_entries():
                # TODO: Make this the lowest priority lineage - so that it doesn't override other lineage entries.
                self.aggregator.add(lineage_entry)

        with self.report.aggregator_generate_timer:
            yield from auto_workunit(self.aggregator.gen_metadata())

        # Update the stateful ingestion state after successful extraction
        self._update_state()

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
            start_time_millis=int(self.start_time.timestamp() * 1000),
            end_time_millis=int(self.end_time.timestamp() * 1000),
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
    ) -> Iterable[
        Union[PreparsedQuery, TableRename, TableSwap, ObservedQuery, StoredProcCall]
    ]:
        query_log_query = QueryLogQueryBuilder(
            start_time=self.start_time,
            end_time=self.end_time,
            bucket_duration=self.config.window.bucket_duration,
            deny_usernames=self.config.pushdown_deny_usernames,
            allow_usernames=self.config.pushdown_allow_usernames,
            dedup_strategy=self.config.query_dedup_strategy,
            database_pattern=self.filters.filter_config.database_pattern
            if self.config.push_down_database_pattern_access_history
            else None,
            additional_database_names=self.config.additional_database_names_allowlist
            if self.config.push_down_database_pattern_access_history
            else None,
        ).build_enriched_query_log_query()

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

    @classmethod
    def _has_temp_keyword(cls, query_text: str) -> bool:
        return (
            re.search(r"\bTEMP\b", query_text, re.IGNORECASE) is not None
            or re.search(r"\bTEMPORARY\b", query_text, re.IGNORECASE) is not None
        )

    def _parse_audit_log_row(
        self, row: Dict[str, Any], users: UsersMapping
    ) -> Optional[
        Union[TableRename, TableSwap, PreparsedQuery, ObservedQuery, StoredProcCall]
    ]:
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

        timestamp: datetime = res["query_start_time"]
        timestamp = timestamp.astimezone(timezone.utc)

        # TODO need to map snowflake query types to ours
        query_text: str = res["query_text"]
        snowflake_query_type: str = res["query_type"]
        query_type: QueryType = SNOWFLAKE_QUERY_TYPE_MAPPING.get(
            snowflake_query_type, QueryType.UNKNOWN
        )

        direct_objects_accessed = res["direct_objects_accessed"]
        objects_modified = res["objects_modified"]
        object_modified_by_ddl = res["object_modified_by_ddl"]

        if object_modified_by_ddl and not objects_modified:
            known_ddl_entry: Optional[Union[TableRename, TableSwap]] = None
            with self.structured_reporter.report_exc(
                "Error fetching ddl lineage from Snowflake"
            ):
                known_ddl_entry = self.parse_ddl_query(
                    query_text,
                    res["session_id"],
                    timestamp,
                    object_modified_by_ddl,
                    snowflake_query_type,
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
        extra_info = {
            "snowflake_query_id": res["query_id"],
            "snowflake_root_query_id": res["root_query_id"],
            "snowflake_query_type": res["query_type"],
            "snowflake_role_name": res["role_name"],
            "query_duration": res["query_duration"],
            "rows_inserted": res["rows_inserted"],
            "rows_updated": res["rows_updated"],
            "rows_deleted": res["rows_deleted"],
        }

        # There are a couple cases when we'd want to prefer our own SQL parsing
        # over Snowflake's metadata.
        # 1. For queries that use a stream, objects_modified returns $SYS_VIEW_X with no mapping.
        #    We can check direct_objects_accessed to see if there is a stream used, and if so,
        #    prefer doing SQL parsing over Snowflake's metadata.
        # 2. For queries that create a view, objects_modified is empty and object_modified_by_ddl
        #    contains the view name and columns. Because `object_modified_by_ddl` doesn't contain
        #    source columns e.g. lineage information, we must do our own SQL parsing. We're mainly
        #    focused on temporary views. It's fine if we parse a couple extra views, but in general
        #    we want view definitions to come from Snowflake's schema metadata and not from query logs.

        has_stream_objects = any(
            obj.get("objectDomain") == "Stream" for obj in direct_objects_accessed
        )
        is_create_view = query_type == QueryType.CREATE_VIEW
        is_create_temp_view = is_create_view and self._has_temp_keyword(query_text)

        if has_stream_objects or is_create_temp_view:
            if has_stream_objects:
                self.report.num_stream_queries_observed += 1
            elif is_create_temp_view:
                self.report.num_create_temp_view_queries_observed += 1

            return ObservedQuery(
                query=query_text,
                session_id=res["session_id"],
                timestamp=timestamp,
                user=user,
                default_db=res["default_db"],
                default_schema=res["default_schema"],
                query_hash=get_query_fingerprint(
                    query_text, self.identifiers.platform, fast=True
                ),
                extra_info=extra_info,
            )

        if snowflake_query_type == "CALL" and res["root_query_id"] is None:
            return StoredProcCall(
                # This is the top-level query ID that other entries will reference.
                snowflake_root_query_id=res["query_id"],
                query_text=query_text,
                timestamp=timestamp,
                user=user,
                default_db=res["default_db"],
                default_schema=res["default_schema"],
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
                column_name = modified_column["columnName"]
                # An empty column name in the audit log would cause an error when creating column URNs.
                # To avoid this and still extract lineage, the raw query text is parsed as a fallback.
                if not column_name or not column_name.strip():
                    query_id = res["query_id"]
                    self.report.num_queries_with_empty_column_name += 1
                    self.report.queries_with_empty_column_name.append(query_id)
                    logger.info(f"Query {query_id} has empty column name in audit log.")

                    return ObservedQuery(
                        query=query_text,
                        session_id=res["session_id"],
                        timestamp=timestamp,
                        user=user,
                        default_db=res["default_db"],
                        default_schema=res["default_schema"],
                        query_hash=get_query_fingerprint(
                            query_text, self.identifiers.platform, fast=True
                        ),
                        extra_info=extra_info,
                    )
                columns.add(self.identifiers.snowflake_identifier(column_name))

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

        entry = PreparsedQuery(
            # Despite having Snowflake's fingerprints available, our own fingerprinting logic does a better
            # job at eliminating redundant / repetitive queries. As such, we include the fast fingerprint
            # here
            query_id=get_query_fingerprint(
                query_text,
                self.identifiers.platform,
                fast=True,
                secondary_id=res["query_secondary_fingerprint"],
            ),
            query_text=query_text,
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
            extra_info=extra_info,
        )
        return entry

    def parse_ddl_query(
        self,
        query: str,
        session_id: str,
        timestamp: datetime,
        object_modified_by_ddl: dict,
        query_type: str,
    ) -> Optional[Union[TableRename, TableSwap]]:
        if (
            object_modified_by_ddl["operationType"] == "ALTER"
            and query_type == "RENAME_TABLE"
            and object_modified_by_ddl["properties"].get("objectName")
        ):
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
        elif object_modified_by_ddl[
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
        else:
            self.report.num_ddl_queries_dropped += 1
            return None

    def close(self) -> None:
        self._exit_stack.close()


@support_status(SupportStatus.CERTIFIED)
@config_class(SnowflakeQueriesSourceConfig)
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
        config = SnowflakeQueriesSourceConfig.model_validate(config_dict)
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
        super().close()


class QueryLogQueryBuilder:
    def __init__(
        self,
        start_time: datetime,
        end_time: datetime,
        bucket_duration: BucketDuration,
        deny_usernames: Optional[List[str]] = None,
        allow_usernames: Optional[List[str]] = None,
        max_tables_per_query: int = 20,
        dedup_strategy: QueryDedupStrategyType = QueryDedupStrategyType.STANDARD,
        database_pattern: Optional[AllowDenyPattern] = None,
        additional_database_names: Optional[List[str]] = None,
    ):
        self.start_time = start_time
        self.end_time = end_time
        self.start_time_millis = int(start_time.timestamp() * 1000)
        self.end_time_millis = int(end_time.timestamp() * 1000)
        self.max_tables_per_query = max_tables_per_query
        self.dedup_strategy = dedup_strategy

        self.users_filter = self._build_user_filter(deny_usernames, allow_usernames)

        self.access_history_database_filter = (
            self._build_access_history_database_filter_condition(
                database_pattern, additional_database_names
            )
        )

        self.time_bucket_size = bucket_duration.value
        assert self.time_bucket_size in ("HOUR", "DAY", "MONTH")

    def _build_user_filter(
        self,
        deny_usernames: Optional[List[str]] = None,
        allow_usernames: Optional[List[str]] = None,
    ) -> str:
        """
        Build user filter SQL condition based on deny and allow username patterns.

        Args:
            deny_usernames: List of username patterns to exclude (SQL LIKE patterns)
            allow_usernames: List of username patterns to include (SQL LIKE patterns)

        Returns:
            SQL WHERE condition string for filtering users
        """
        user_filters = []

        if deny_usernames:
            deny_conditions = []
            for pattern in deny_usernames:
                # Escape single quotes for SQL safety
                escaped_pattern = pattern.replace("'", "''")
                deny_conditions.append(f"user_name NOT ILIKE '{escaped_pattern}'")
            if deny_conditions:
                user_filters.append(f"({' AND '.join(deny_conditions)})")

        if allow_usernames:
            allow_conditions = []
            for pattern in allow_usernames:
                # Escape single quotes for SQL safety
                escaped_pattern = pattern.replace("'", "''")
                allow_conditions.append(f"user_name ILIKE '{escaped_pattern}'")
            if allow_conditions:
                user_filters.append(f"({' OR '.join(allow_conditions)})")

        return " AND ".join(user_filters) if user_filters else "TRUE"

    def _build_access_history_database_filter_condition(
        self,
        database_pattern: Optional[AllowDenyPattern],
        additional_database_names: Optional[List[str]] = None,
    ) -> str:
        """
        Build a SQL WHERE condition for database filtering in access_history based on AllowDenyPattern.

        IMPORTANT: This function handles the fundamental difference between DML and DDL operations in Snowflake's
        access_history table:

        - DML Operations (SELECT, INSERT, UPDATE, DELETE, etc.): Store accessed/modified objects in the
          `direct_objects_accessed` and `objects_modified` arrays
        - DDL Operations (CREATE, ALTER, DROP, RENAME, etc.): Store modified objects in the
          `object_modified_by_ddl` field (single object, not an array)

        Without checking `object_modified_by_ddl`, DDL operations like "ALTER TABLE person_info RENAME TO person_info_final"
        would be incorrectly filtered out because they don't populate the DML arrays, causing missing lineage
        and operational metadata.

        Filtering Logic:
        A query is included if it matches:
        - Any database name in additional_database_names (exact match), OR
        - Any database pattern in database_pattern.allow AND NOT any pattern in database_pattern.deny

        Args:
            database_pattern: The AllowDenyPattern configuration for database filtering
            additional_database_names: Additional database names to always include (no pattern matching)

        Returns:
            A SQL WHERE condition string, or "TRUE" if no filtering should be applied
        """
        if not database_pattern and not additional_database_names:
            return "TRUE"

        # Build the database filter conditions
        # Logic: Allow if (matches additional_database_names_allowlist) OR (matches database_pattern.allow AND NOT matches database_pattern.deny)
        # Note: Using UPPER() + RLIKE for case-insensitive matching is more performant than REGEXP_LIKE with 'i' flag

        # Build additional database names condition (exact matches) - these always get included
        additional_db_condition = None
        if additional_database_names:
            additional_db_conditions = []
            for db_name in additional_database_names:
                # Escape single quotes
                escaped_db_name = db_name.replace("'", "''")
                additional_db_conditions.append(
                    f"SPLIT_PART(UPPER(o:objectName), '.', 1) = '{escaped_db_name.upper()}'"
                )
            if additional_db_conditions:
                additional_db_condition = " OR ".join(additional_db_conditions)

        # Build database pattern condition (allow AND NOT deny)
        database_pattern_condition = None
        if database_pattern:
            allow_patterns = database_pattern.allow
            deny_patterns = database_pattern.deny

            pattern_parts = []

            # Add allow patterns (if not the default "allow all")
            if allow_patterns and allow_patterns != [".*"]:
                allow_conditions = []
                for pattern in allow_patterns:
                    # Escape single quotes that might be present in the regex pattern
                    escaped_pattern = pattern.replace("'", "''")
                    allow_conditions.append(
                        f"SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE '{escaped_pattern}'"
                    )
                if allow_conditions:
                    pattern_parts.append(
                        allow_conditions[0]
                        if len(allow_conditions) == 1
                        else f"({' OR '.join(allow_conditions)})"
                    )

            # Add deny patterns
            if deny_patterns:
                deny_conditions = []
                for pattern in deny_patterns:
                    # Escape single quotes that might be present in the regex pattern
                    escaped_pattern = pattern.replace("'", "''")
                    deny_conditions.append(
                        f"SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '{escaped_pattern}'"
                    )
                if deny_conditions:
                    pattern_parts.append(
                        deny_conditions[0]
                        if len(deny_conditions) == 1
                        else f"({' AND '.join(deny_conditions)})"
                    )

            if pattern_parts:
                database_pattern_condition = " AND ".join(pattern_parts)

        # Combine conditions: additional_database_names OR database_pattern
        filter_conditions = []
        if additional_db_condition:
            filter_conditions.append(
                f"({additional_db_condition})"
                if len(additional_db_condition.split(" OR ")) > 1
                else additional_db_condition
            )
        if database_pattern_condition:
            filter_conditions.append(
                f"({database_pattern_condition})"
                if len(database_pattern_condition.split(" AND ")) > 1
                else database_pattern_condition
            )

        if filter_conditions:
            database_filter_condition = (
                filter_conditions[0]
                if len(filter_conditions) == 1
                else " OR ".join(filter_conditions)
            )

            # Build a condition that checks if any objects in the arrays match the database pattern
            # This implements "at least one" matching behavior: queries are allowed if they touch
            # at least one database that matches the pattern, even if they also touch other databases
            # Use ARRAY_SIZE with FILTER which is more compatible with Snowflake
            direct_objects_condition = f"ARRAY_SIZE(FILTER(direct_objects_accessed, o -> {database_filter_condition})) > 0"
            objects_modified_condition = f"ARRAY_SIZE(FILTER(objects_modified, o -> {database_filter_condition})) > 0"

            # CRITICAL: Handle DDL operations by checking object_modified_by_ddl field
            # DDL operations like ALTER TABLE RENAME store their data here instead of in the arrays
            # We need to adapt the filter condition for a single object rather than an array
            ddl_filter_condition = database_filter_condition.replace(
                "o:objectName", "object_modified_by_ddl:objectName"
            )
            object_modified_by_ddl_condition = f"({ddl_filter_condition})"

            return f"({direct_objects_condition} OR {objects_modified_condition} OR {object_modified_by_ddl_condition})"
        else:
            return "TRUE"

    def _query_fingerprinted_queries(self):
        if self.dedup_strategy == QueryDedupStrategyType.STANDARD:
            secondary_fingerprint_sql = """
    CASE 
        WHEN CONTAINS(query_history.query_text, '-- Hex query metadata:')
        -- Extract project id and hash it
        THEN CAST(HASH(
            REGEXP_SUBSTR(query_history.query_text, '"project_id"\\\\s*:\\\\s*"([^"]+)"', 1, 1, 'e', 1),
            REGEXP_SUBSTR(query_history.query_text, '"context"\\\\s*:\\\\s*"([^"]+)"', 1, 1, 'e', 1)
        ) AS VARCHAR)
        ELSE NULL 
    END"""
        elif self.dedup_strategy == QueryDedupStrategyType.NONE:
            secondary_fingerprint_sql = "NULL"
        else:
            raise NotImplementedError(
                f"Strategy {self.dedup_strategy} is not implemented by the QueryLogQueryBuilder"
            )
        return f"""
SELECT *,
    -- TODO: Generate better fingerprints for each query by pushing down regex logic.
    query_history.query_parameterized_hash as query_fingerprint,
    -- Optional and additional hash to be used for query deduplication and final query identity
    {secondary_fingerprint_sql} as query_secondary_fingerprint
FROM
    snowflake.account_usage.query_history
WHERE
    query_history.start_time >= to_timestamp_ltz({self.start_time_millis}, 3) -- {self.start_time.isoformat()}
    AND query_history.start_time < to_timestamp_ltz({self.end_time_millis}, 3) -- {self.end_time.isoformat()}
    AND execution_status = 'SUCCESS'
    AND {self.users_filter}"""

    def _query_deduplicated_queries(self):
        if self.dedup_strategy == QueryDedupStrategyType.STANDARD:
            return f"""
SELECT
    *,
    DATE_TRUNC(
        {self.time_bucket_size},
        CONVERT_TIMEZONE('UTC', start_time)
    ) AS bucket_start_time,
    COUNT(*) OVER (PARTITION BY bucket_start_time, query_fingerprint, query_secondary_fingerprint) AS query_count,
FROM
    fingerprinted_queries
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY bucket_start_time, query_fingerprint, query_secondary_fingerprint ORDER BY start_time DESC) = 1"""
        elif self.dedup_strategy == QueryDedupStrategyType.NONE:
            return f"""
SELECT
    *,
    DATE_TRUNC(
        {self.time_bucket_size},
        CONVERT_TIMEZONE('UTC', start_time)
    ) AS bucket_start_time,
    1 AS query_count,
FROM
            fingerprinted_queries"""
        else:
            raise NotImplementedError(
                f"Strategy {self.dedup_strategy} is not implemented by the QueryLogQueryBuilder"
            )

    def build_enriched_query_log_query(self) -> str:
        return f"""\
WITH
fingerprinted_queries as (
{self._query_fingerprinted_queries()}
)
, deduplicated_queries as (
{self._query_deduplicated_queries()}
)
, raw_access_history AS (
    SELECT
        query_id,
        root_query_id,
        query_start_time,
        user_name,
        direct_objects_accessed,
        objects_modified,
        object_modified_by_ddl
    FROM
        snowflake.account_usage.access_history
    WHERE
        query_start_time >= to_timestamp_ltz({self.start_time_millis}, 3) -- {self.start_time.isoformat()}
        AND query_start_time < to_timestamp_ltz({self.end_time_millis}, 3) -- {self.end_time.isoformat()}
        AND {self.users_filter}
        AND query_id IN (
            SELECT query_id FROM deduplicated_queries
        )
        AND {self.access_history_database_filter}
)
, filtered_access_history AS (
    -- TODO: Add table filter clause.
    SELECT
        query_id,
        root_query_id,
        query_start_time,
        ARRAY_SLICE(
            FILTER(direct_objects_accessed, o -> o:objectDomain IN {SnowflakeQuery.ACCESS_HISTORY_TABLE_VIEW_DOMAINS_FILTER}),
            0, {self.max_tables_per_query}
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
        q.query_secondary_fingerprint,
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
        a.root_query_id,
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
