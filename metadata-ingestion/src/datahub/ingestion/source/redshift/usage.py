import logging
import time
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union

import cachetools
import redshift_connector
from pydantic import ValidationError, field_validator
from pydantic.fields import Field
from pydantic.main import BaseModel

import datahub.emitter.mce_builder as builder
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source_helpers import (
    auto_empty_dataset_usage_statistics,
    auto_workunit,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.query import (
    RedshiftCommonQuery,
    RedshiftProvisionedQuery,
    RedshiftServerlessQuery,
)
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftDataDictionary,
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantUsageRunSkipHandler,
)
from datahub.ingestion.source_report.ingestion_stage import (
    USAGE_EXTRACTION_OPERATIONAL_STATS,
    USAGE_EXTRACTION_USAGE_AGGREGATION,
)
from datahub.metadata.schema_classes import OperationClass, OperationTypeClass
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    PreparsedQuery,
    SqlParsingAggregator,
)
from datahub.utilities.file_backed_collections import FileBackedList
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)

REDSHIFT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class RedshiftAccessEvent(BaseModel):
    userid: int
    username: str
    query: int
    tbl: int
    text: str = Field(alias="querytxt")
    database: str
    schema_: str = Field(alias="schema")
    table: str
    operation_type: Optional[str] = None
    starttime: datetime
    endtime: datetime

    @field_validator("starttime", "endtime", mode="before")
    @classmethod
    def ensure_utc_datetime(cls, v):
        """Ensure datetime fields are treated as UTC for consistency with Pydantic V1 behavior.

        Pydantic V2 assumes local timezone for naive datetime strings, whereas Pydantic V1 assumed UTC.
        This validator restores V1 behavior to maintain timestamp consistency.
        """
        if isinstance(v, str):
            # Parse as naive datetime, then assume UTC (matching V1 behavior)
            dt = datetime.fromisoformat(v)
            if dt.tzinfo is None:
                # Treat naive datetime as UTC (this was the V1 behavior)
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        elif isinstance(v, datetime) and v.tzinfo is None:
            # If we get a naive datetime object, assume UTC
            return v.replace(tzinfo=timezone.utc)
        return v


class RedshiftUsageExtractor:
    """
    This plugin extracts usage statistics for datasets in Amazon Redshift.

    Note: Usage information is computed by querying the following system tables -
    1. stl_scan
    2. svv_table_info
    3. stl_query
    4. svl_user_info

    To grant access this plugin for all system tables, please alter your datahub Redshift user the following way:
    ```sql
    ALTER USER datahub_user WITH SYSLOG ACCESS UNRESTRICTED;
    ```
    This plugin has the below functionalities -
    1. For a specific dataset this plugin ingests the following statistics -
       1. top n queries.
       2. top users.
    2. Aggregation of these statistics into buckets, by day or hour granularity.

    :::note

    This source only does usage statistics. To get the tables, views, and schemas in your Redshift warehouse, ingest using the `redshift` source described above.

    :::

    :::note

    Redshift system tables have some latency in getting data from queries. In addition, these tables only maintain logs for 2-5 days. You can find more information from the official documentation [here](https://aws.amazon.com/premiumsupport/knowledge-center/logs-redshift-database-cluster/).

    :::

    """

    def __init__(
        self,
        config: RedshiftConfig,
        connection: redshift_connector.Connection,
        report: RedshiftReport,
        dataset_urn_builder: Callable[[str], str],
        redundant_run_skip_handler: Optional[RedundantUsageRunSkipHandler] = None,
        context: Optional[PipelineContext] = None,
    ):
        self.config = config
        self.report = report
        self.connection = connection
        self.dataset_urn_builder = dataset_urn_builder
        # Only needed when include_column_usage_stats is enabled, so the aggregator can
        # resolve column schemas (for column-level usage) from the backend graph.
        self.context = context

        self.redundant_run_skip_handler = redundant_run_skip_handler
        self.start_time, self.end_time = (
            self.report.usage_start_time,
            self.report.usage_end_time,
        ) = self.get_time_window()

        self.queries: RedshiftCommonQuery = RedshiftProvisionedQuery()
        if self.config.is_serverless:
            self.queries = RedshiftServerlessQuery()

    def get_time_window(self) -> Tuple[datetime, datetime]:
        if self.redundant_run_skip_handler:
            return self.redundant_run_skip_handler.suggest_run_time_window(
                self.config.start_time, self.config.end_time
            )
        else:
            return self.config.start_time, self.config.end_time

    def _should_ingest_usage(self):
        if (
            self.redundant_run_skip_handler
            and self.redundant_run_skip_handler.should_skip_this_run(
                cur_start_time=self.config.start_time,
                cur_end_time=self.config.end_time,
            )
        ):
            # Skip this run
            self.report.report_warning(
                "usage-extraction",
                "Skip this run as there was already a run for current ingestion window.",
            )
            return False

        return True

    def get_usage_workunits(
        self,
        all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]],
    ) -> Iterable[MetadataWorkUnit]:
        if not self._should_ingest_usage():
            return

        if not self.config.include_column_usage_stats:
            # Default (stl_scan) path: wrap with empty-usage backfill so every known
            # table gets a usage aspect even if it had no queries in the window.
            yield from auto_empty_dataset_usage_statistics(
                self._get_workunits_internal(all_tables),
                config=BaseTimeWindowConfig(
                    start_time=self.start_time,
                    end_time=self.end_time,
                    bucket_duration=self.config.bucket_duration,
                ),
                dataset_urns={
                    self.dataset_urn_builder(f"{database}.{schema}.{table.name}")
                    for database in all_tables
                    for schema in all_tables[database]
                    for table in all_tables[database][schema]
                },
            )
        else:
            # v2 mode: usage is owned by the lineage aggregator.  This extractor
            # only emits operational stats (if enabled).
            yield from self._get_workunits_internal(all_tables)

        if self.redundant_run_skip_handler:
            # Update the checkpoint state for this run.
            self.redundant_run_skip_handler.update_state(
                self.config.start_time,
                self.config.end_time,
                self.config.bucket_duration,
            )

    def _get_workunits_internal(
        self,
        all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]],
    ) -> Iterable[MetadataWorkUnit]:
        self.report.num_usage_workunits_emitted = 0
        self.report.num_usage_stat_skipped = 0
        self.report.num_operational_stats_filtered = 0

        if self.config.include_operational_stats:
            with (
                self.report.new_stage(USAGE_EXTRACTION_OPERATIONAL_STATS),
                PerfTimer() as timer,
            ):
                # Generate operation aspect workunits
                yield from self._gen_operation_aspect_workunits(
                    self.connection, all_tables
                )
                self.report.operational_metadata_extraction_sec[
                    self.config.database
                ] = timer.elapsed_seconds(digits=2)

        if not self.config.include_column_usage_stats:
            # Default (stl_scan) path: generate usage statistics via the SQL parsing
            # aggregator fed with pre-resolved table reads from stl_scan.
            with self.report.new_stage(USAGE_EXTRACTION_USAGE_AGGREGATION):
                start = self.start_time.strftime(REDSHIFT_DATETIME_FORMAT)
                end = self.end_time.strftime(REDSHIFT_DATETIME_FORMAT)
                query: str = self.queries.usage_query(
                    start_time=start, end_time=end, database=self.config.database
                )
                aggregator = self._make_usage_aggregator()
                try:
                    # Drain the access events into a local cache before feeding the
                    # aggregator. add_preparsed_query fingerprints and formats each
                    # query (SQL parsing, slow); doing that inline while iterating the
                    # live Redshift cursor would hold the cursor open for the whole
                    # aggregation and risk a timeout on large query histories.
                    with FileBackedList[RedshiftAccessEvent]() as access_events:
                        for event in self._gen_access_events_from_history_query(
                            query, connection=self.connection, all_tables=all_tables
                        ):
                            access_events.append(event)

                        for event in access_events:
                            aggregator.add_preparsed_query(
                                self._access_event_to_preparsed_query(event)
                            )

                    for wu in auto_workunit(aggregator.gen_metadata()):
                        self.report.num_usage_workunits_emitted += 1
                        yield wu
                finally:
                    aggregator.close()

    def _gen_operation_aspect_workunits(
        self,
        connection: redshift_connector.Connection,
        all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]],
    ) -> Iterable[MetadataWorkUnit]:
        # Generate access events
        query: str = self.queries.operation_aspect_query(
            start_time=self.start_time.strftime(REDSHIFT_DATETIME_FORMAT),
            end_time=self.end_time.strftime(REDSHIFT_DATETIME_FORMAT),
        )
        access_events_iterable: Iterable[RedshiftAccessEvent] = (
            self._gen_access_events_from_history_query(
                query, connection, all_tables=all_tables
            )
        )

        # Generate operation aspect work units from the access events
        yield from (
            mcpw.as_workunit()
            for mcpw in self._drop_repeated_operations(
                self._gen_operation_aspect_workunits_from_access_events(
                    access_events_iterable, all_tables=all_tables
                )
            )
        )

    def _should_process_event(
        self,
        event: RedshiftAccessEvent,
        all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]],
    ) -> bool:
        # Check schema/table allow/deny patterns
        return not (
            event.database not in all_tables
            or event.schema_ not in all_tables[event.database]
            or not any(
                event.table == t.name for t in all_tables[event.database][event.schema_]
            )
        )

    def _gen_access_events_from_history_query(
        self,
        query: str,
        connection: redshift_connector.Connection,
        all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]],
    ) -> Iterable[RedshiftAccessEvent]:
        cursor = RedshiftDataDictionary.get_query_result(conn=connection, query=query)
        results = cursor.fetchmany()
        field_names = [i[0] for i in cursor.description]
        while results:
            for row in results:
                try:
                    access_event = RedshiftAccessEvent(
                        userid=row[field_names.index("userid")],
                        username=row[field_names.index("username")],
                        query=row[field_names.index("query")],
                        querytxt=(
                            row[field_names.index("querytxt")].strip()
                            if row[field_names.index("querytxt")]
                            else None
                        ),
                        tbl=row[field_names.index("tbl")],
                        database=row[field_names.index("database")],
                        schema=row[field_names.index("schema")],
                        table=row[field_names.index("table")],
                        starttime=row[field_names.index("starttime")],
                        endtime=row[field_names.index("endtime")],
                        operation_type=(
                            row[field_names.index("operation_type")]
                            if "operation_type" in field_names
                            else None
                        ),
                    )
                except ValidationError as e:
                    logging.warning(
                        f"Validation error on access event creation from row {row}. The error was: {e} Skipping ...."
                    )
                    self.report.num_usage_stat_skipped += 1
                    continue

                if not self._should_process_event(access_event, all_tables=all_tables):
                    self.report.num_usage_stat_skipped += 1
                    continue

                yield access_event
            results = cursor.fetchmany()

    def _drop_repeated_operations(
        self, events: Iterable[MetadataChangeProposalWrapper]
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Drop repeated operations on the same entity.

        ASSUMPTION: Events are ordered by lastUpdatedTimestamp, descending.

        Operations are only dropped if they were within 1 minute of each other,
        and have the same operation type, user, and entity.

        This is particularly useful when we see a string of insert operations
        that are all really part of the same overall operation.
        """

        OPERATION_CACHE_MAXSIZE = 1000
        DROP_WINDOW_SEC = 10

        # All timestamps are in milliseconds.
        timestamp_low_watermark = 0

        def timer():
            return -timestamp_low_watermark

        # dict of entity urn -> (last event's actor, operation type)
        # TODO: Remove the type ignore and use TTLCache[key_type, value_type] directly once that's supported in Python 3.9.
        last_events: Dict[str, Tuple[Optional[str], str]] = cachetools.TTLCache(  # type: ignore[assignment]
            maxsize=OPERATION_CACHE_MAXSIZE, ttl=DROP_WINDOW_SEC * 1000, timer=timer
        )

        for event in events:
            assert isinstance(event.aspect, OperationClass)

            timestamp_low_watermark = min(
                timestamp_low_watermark, event.aspect.lastUpdatedTimestamp
            )

            urn = event.entityUrn
            assert urn
            assert isinstance(event.aspect.operationType, str)
            value: Tuple[Optional[str], str] = (
                event.aspect.actor,
                event.aspect.operationType,
            )
            if urn in last_events and last_events[urn] == value:
                self.report.num_repeated_operations_dropped += 1
                continue

            last_events[urn] = value
            yield event

    def _gen_operation_aspect_workunits_from_access_events(
        self,
        events_iterable: Iterable[RedshiftAccessEvent],
        all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]],
    ) -> Iterable[MetadataChangeProposalWrapper]:
        self.report.num_operational_stats_workunits_emitted = 0
        for event in events_iterable:
            if not (
                event.database
                and event.username
                and event.schema_
                and event.table
                and event.endtime
                and event.operation_type
            ):
                continue

            if not self._should_process_event(event, all_tables=all_tables):
                self.report.num_operational_stats_filtered += 1
                continue

            assert event.operation_type in ["insert", "delete"]

            reported_time: int = int(time.time() * 1000)
            last_updated_timestamp: int = int(event.endtime.timestamp() * 1000)
            user_email: str = event.username
            operation_aspect = OperationClass(
                timestampMillis=reported_time,
                lastUpdatedTimestamp=last_updated_timestamp,
                actor=builder.make_user_urn(user_email.split("@")[0]),
                operationType=(
                    OperationTypeClass.INSERT
                    if event.operation_type == "insert"
                    else OperationTypeClass.DELETE
                ),
            )

            resource: str = f"{event.database}.{event.schema_}.{event.table}".lower()
            yield MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn_builder(resource), aspect=operation_aspect
            )
            self.report.num_operational_stats_workunits_emitted += 1

    def _make_usage_aggregator(self) -> SqlParsingAggregator:
        # By default usage is fed as already-resolved table reads (PreparsedQuery
        # with known upstreams from stl_scan), so the aggregator never parses the
        # (often truncated) Redshift query text to derive tables — it only
        # attributes the read to the known table, preserving stl_scan accuracy.
        # When include_column_usage_stats is set, queries are parsed instead (see
        # _get_workunits_internal), which adds column-level usage; the backend
        # graph is passed so column references resolve to real schema fields.
        # The aggregator's query store is file-backed and the cursor is drained up
        # front, so large histories don't hold the Redshift cursor open or blow up
        # memory. format_queries=False so top-N query text is formatted exactly
        # once, in make_usage_workunit (matching the previous behavior).
        return SqlParsingAggregator(
            platform="redshift",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            graph=self.context.graph if self.context else None,
            generate_lineage=False,
            generate_queries=False,
            generate_query_usage_statistics=False,
            generate_usage_statistics=True,
            generate_operations=False,
            usage_config=self.config,
            format_queries=False,
            is_allowed_table=self._is_allowed_table,
        )

    def _is_allowed_table(self, name: str) -> bool:
        # Don't attribute usage to tables the user excluded via table_pattern.
        # name is the `db.schema.table` dataset name from the aggregator.
        return self.config.table_pattern.allowed(name)

    def _user_urn(self, username: str) -> CorpUserUrn:
        # Preserve the legacy user identity: the urn id is the local part of the
        # email (domain stripped), so existing CorpUser links are unchanged.
        user_email = username if username else "unknown"
        if "@" not in user_email and self.config.email_domain:
            user_email += f"@{self.config.email_domain}"
        return CorpUserUrn.from_string(builder.make_user_urn(user_email.split("@")[0]))

    def _access_event_to_preparsed_query(
        self, event: RedshiftAccessEvent
    ) -> PreparsedQuery:
        resource: str = f"{event.database}.{event.schema_}.{event.table}".lower()
        return PreparsedQuery(
            query_id=None,
            query_text=event.text or "",
            upstreams=[self.dataset_urn_builder(resource)],
            user=self._user_urn(event.username),
            timestamp=event.starttime,
            query_count=1,
        )

    def report_status(self, step: str, status: bool) -> None:
        if self.redundant_run_skip_handler:
            self.redundant_run_skip_handler.report_current_run_status(step, status)
