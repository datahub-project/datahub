import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pydantic

from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.emitter.mce_builder import make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.source_helpers import auto_empty_dataset_usage_statistics
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.constants import SnowflakeEdition
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnection,
    SnowflakePermissionError,
)
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakeFilter,
    SnowflakeIdentifierBuilder,
)
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantUsageRunSkipHandler,
)
from datahub.ingestion.source_report.ingestion_stage import (
    USAGE_EXTRACTION_OPERATIONAL_STATS,
    USAGE_EXTRACTION_USAGE_AGGREGATION,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetFieldUsageCounts,
    DatasetUsageStatistics,
    DatasetUserUsageCounts,
)
from datahub.metadata.com.linkedin.pegasus2avro.timeseries import TimeWindowSize
from datahub.metadata.schema_classes import OperationClass, OperationTypeClass
from datahub.sql_parsing.sqlglot_utils import try_format_query
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.sql_formatter import trim_query

logger: logging.Logger = logging.getLogger(__name__)

OPERATION_STATEMENT_TYPES = {
    "INSERT": OperationTypeClass.INSERT,
    "UPDATE": OperationTypeClass.UPDATE,
    "DELETE": OperationTypeClass.DELETE,
    "CREATE": OperationTypeClass.CREATE,
    "CREATE_TABLE": OperationTypeClass.CREATE,
    "CREATE_TABLE_AS_SELECT": OperationTypeClass.CREATE,
    "MERGE": OperationTypeClass.CUSTOM,
    "COPY": OperationTypeClass.CUSTOM,
    "TRUNCATE_TABLE": OperationTypeClass.CUSTOM,
    # TODO: Dataset for below query types are not detected by snowflake in snowflake.access_history.objects_modified.
    # However it seems possible to support these using sql parsing in future.
    # When this support is added, snowflake_query.operational_data_for_time_window needs to be updated.
    # "CREATE_VIEW": OperationTypeClass.CREATE,
    # "CREATE_EXTERNAL_TABLE": OperationTypeClass.CREATE,
    # "ALTER_TABLE_MODIFY_COLUMN": OperationTypeClass.ALTER,
    # "ALTER_TABLE_ADD_COLUMN": OperationTypeClass.ALTER,
    # "RENAME_COLUMN": OperationTypeClass.ALTER,
    # "ALTER_SET_TAG": OperationTypeClass.ALTER,
    # "ALTER_TABLE_DROP_COLUMN": OperationTypeClass.ALTER,
    # "ALTER": OperationTypeClass.ALTER,
}


class PermissiveModel(pydantic.BaseModel):
    class Config:
        extra = "allow"


class SnowflakeColumnReference(PermissiveModel):
    columnName: str
    columnId: Optional[int] = None
    objectName: Optional[str] = None
    objectDomain: Optional[str] = None
    objectId: Optional[int] = None


class SnowflakeObjectAccessEntry(PermissiveModel):
    columns: Optional[List[SnowflakeColumnReference]] = None
    objectDomain: str
    objectName: str
    # Seems like it should never be null, but in practice have seen null objectIds
    objectId: Optional[int] = None
    stageKind: Optional[str] = None


class SnowflakeJoinedAccessEvent(PermissiveModel):
    query_start_time: datetime
    query_text: str
    query_type: str
    rows_inserted: Optional[int] = None
    rows_updated: Optional[int] = None
    rows_deleted: Optional[int] = None
    base_objects_accessed: List[SnowflakeObjectAccessEntry]
    direct_objects_accessed: List[SnowflakeObjectAccessEntry]
    objects_modified: List[SnowflakeObjectAccessEntry]

    user_name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    display_name: Optional[str] = None
    email: Optional[str] = None
    role_name: str


class SnowflakeUsageExtractor(SnowflakeCommonMixin, Closeable):
    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        connection: SnowflakeConnection,
        filter: SnowflakeFilter,
        identifiers: SnowflakeIdentifierBuilder,
        redundant_run_skip_handler: Optional[RedundantUsageRunSkipHandler],
    ) -> None:
        self.config: SnowflakeV2Config = config
        self.report: SnowflakeV2Report = report
        self.filter = filter
        self.identifiers = identifiers
        self.connection = connection

        self.redundant_run_skip_handler = redundant_run_skip_handler
        self.start_time, self.end_time = (
            self.report.usage_start_time,
            self.report.usage_end_time,
        ) = self.get_time_window()

    def get_time_window(self) -> Tuple[datetime, datetime]:
        if self.redundant_run_skip_handler:
            return self.redundant_run_skip_handler.suggest_run_time_window(
                self.config.start_time, self.config.end_time
            )
        else:
            return self.config.start_time, self.config.end_time

    def get_usage_workunits(
        self, discovered_datasets: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        if not self._should_ingest_usage():
            return

        self.report.set_ingestion_stage("*", USAGE_EXTRACTION_USAGE_AGGREGATION)
        if self.report.edition == SnowflakeEdition.STANDARD.value:
            logger.info(
                "Snowflake Account is Standard Edition. Usage and Operation History Feature is not supported."
            )
            return

        logger.info("Checking usage date ranges")

        self._check_usage_date_ranges()

        # If permission error, execution returns from here
        if (
            self.report.min_access_history_time is None
            or self.report.max_access_history_time is None
        ):
            return

        # NOTE: In earlier `snowflake-usage` connector, users with no email were not considered in usage counts as well as in operation
        # Now, we report the usage as well as operation metadata even if user email is absent

        if self.config.include_usage_stats:
            yield from auto_empty_dataset_usage_statistics(
                self._get_workunits_internal(discovered_datasets),
                config=BaseTimeWindowConfig(
                    start_time=self.start_time,
                    end_time=self.end_time,
                    bucket_duration=self.config.bucket_duration,
                ),
                dataset_urns={
                    self.identifiers.gen_dataset_urn(dataset_identifier)
                    for dataset_identifier in discovered_datasets
                },
            )

        self.report.set_ingestion_stage("*", USAGE_EXTRACTION_OPERATIONAL_STATS)

        if self.config.include_operational_stats:
            # Generate the operation workunits.
            access_events = self._get_snowflake_history()
            for event in access_events:
                yield from self._get_operation_aspect_work_unit(
                    event, discovered_datasets
                )

        if self.redundant_run_skip_handler:
            # Update the checkpoint state for this run.
            self.redundant_run_skip_handler.update_state(
                self.config.start_time,
                self.config.end_time,
                self.config.bucket_duration,
            )

    def _get_workunits_internal(
        self, discovered_datasets: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        with PerfTimer() as timer:
            logger.info("Getting aggregated usage statistics")
            try:
                results = self.connection.query(
                    SnowflakeQuery.usage_per_object_per_time_bucket_for_time_window(
                        start_time_millis=int(self.start_time.timestamp() * 1000),
                        end_time_millis=int(self.end_time.timestamp() * 1000),
                        time_bucket_size=self.config.bucket_duration,
                        use_base_objects=self.config.apply_view_usage_to_tables,
                        top_n_queries=self.config.top_n_queries,
                        include_top_n_queries=self.config.include_top_n_queries,
                        email_domain=self.config.email_domain,
                        email_filter=self.config.user_email_pattern,
                        table_deny_pattern=self.config.temporary_tables_pattern,
                    ),
                )
            except Exception as e:
                logger.debug(e, exc_info=e)
                self.warn_if_stateful_else_error(
                    "usage-statistics",
                    f"Populating table usage statistics from Snowflake failed due to error {e}.",
                )
                self.report_status(USAGE_EXTRACTION_USAGE_AGGREGATION, False)
                return

            self.report.usage_aggregation.query_secs = timer.elapsed_seconds()
            self.report.usage_aggregation.query_row_count = results.rowcount

        with self.report.usage_aggregation.result_fetch_timer as fetch_timer:
            for row in results:
                with fetch_timer.pause(), self.report.usage_aggregation.result_skip_timer as skip_timer:
                    if results.rownumber is not None and results.rownumber % 1000 == 0:
                        logger.debug(f"Processing usage row number {results.rownumber}")
                        logger.debug(self.report.usage_aggregation.as_string())

                    if not self.filter.is_dataset_pattern_allowed(
                        row["OBJECT_NAME"],
                        row["OBJECT_DOMAIN"],
                    ):
                        logger.debug(
                            f"Skipping usage for {row['OBJECT_DOMAIN']} {row['OBJECT_NAME']}, as table is not allowed by recipe."
                        )
                        continue

                    dataset_identifier = (
                        self.identifiers.get_dataset_identifier_from_qualified_name(
                            row["OBJECT_NAME"]
                        )
                    )
                    if dataset_identifier not in discovered_datasets:
                        logger.debug(
                            f"Skipping usage for {row['OBJECT_DOMAIN']} {dataset_identifier}, as table is not accessible."
                        )
                        continue
                    with skip_timer.pause(), self.report.usage_aggregation.result_map_timer as map_timer:
                        wu = self.build_usage_statistics_for_dataset(
                            dataset_identifier, row
                        )
                        if wu:
                            with map_timer.pause():
                                yield wu

    def build_usage_statistics_for_dataset(
        self, dataset_identifier: str, row: dict
    ) -> Optional[MetadataWorkUnit]:
        try:
            stats = DatasetUsageStatistics(
                timestampMillis=int(row["BUCKET_START_TIME"].timestamp() * 1000),
                eventGranularity=TimeWindowSize(
                    unit=self.config.bucket_duration, multiple=1
                ),
                totalSqlQueries=row["TOTAL_QUERIES"],
                uniqueUserCount=row["TOTAL_USERS"],
                topSqlQueries=(
                    self._map_top_sql_queries(row["TOP_SQL_QUERIES"])
                    if self.config.include_top_n_queries
                    else None
                ),
                userCounts=self._map_user_counts(row["USER_COUNTS"]),
                fieldCounts=self._map_field_counts(row["FIELD_COUNTS"]),
            )
            return MetadataChangeProposalWrapper(
                entityUrn=self.identifiers.gen_dataset_urn(dataset_identifier),
                aspect=stats,
            ).as_workunit()
        except Exception as e:
            logger.debug(
                f"Failed to parse usage statistics for dataset {dataset_identifier} due to error {e}.",
                exc_info=e,
            )
            self.report.warning(
                "Failed to parse usage statistics for dataset", dataset_identifier
            )

        return None

    def _map_top_sql_queries(self, top_sql_queries_str: str) -> List[str]:
        with self.report.usage_aggregation.queries_map_timer:
            top_sql_queries = json.loads(top_sql_queries_str)
            budget_per_query: int = int(
                self.config.queries_character_limit / self.config.top_n_queries
            )
            return sorted(
                [
                    (
                        trim_query(
                            try_format_query(query, self.platform), budget_per_query
                        )
                        if self.config.format_sql_queries
                        else trim_query(query, budget_per_query)
                    )
                    for query in top_sql_queries
                ]
            )

    def _map_user_counts(
        self,
        user_counts_str: str,
    ) -> List[DatasetUserUsageCounts]:
        with self.report.usage_aggregation.users_map_timer:
            user_counts = json.loads(user_counts_str)
            filtered_user_counts = []
            for user_count in user_counts:
                user_email = user_count.get("email")
                if (
                    not user_email
                    and self.config.email_domain
                    and user_count["user_name"]
                ):
                    user_email = "{}@{}".format(
                        user_count["user_name"], self.config.email_domain
                    ).lower()
                if not user_email or not self.config.user_email_pattern.allowed(
                    user_email
                ):
                    continue

                filtered_user_counts.append(
                    DatasetUserUsageCounts(
                        user=make_user_urn(
                            self.get_user_identifier(
                                user_count["user_name"],
                                user_email,
                                self.config.email_as_user_identifier,
                            )
                        ),
                        count=user_count["total"],
                        # NOTE: Generated emails may be incorrect, as email may be different than
                        # username@email_domain
                        userEmail=user_email,
                    )
                )
            return sorted(filtered_user_counts, key=lambda v: v.user)

    def _map_field_counts(self, field_counts_str: str) -> List[DatasetFieldUsageCounts]:
        with self.report.usage_aggregation.fields_map_timer:
            field_counts = json.loads(field_counts_str)
            return sorted(
                [
                    DatasetFieldUsageCounts(
                        fieldPath=self.identifiers.snowflake_identifier(
                            field_count["col"]
                        ),
                        count=field_count["total"],
                    )
                    for field_count in field_counts
                ],
                key=lambda v: v.fieldPath,
            )

    def _get_snowflake_history(self) -> Iterable[SnowflakeJoinedAccessEvent]:
        logger.info("Getting access history")
        with PerfTimer() as timer:
            query = self._make_operations_query()
            try:
                assert self.connection is not None
                results = self.connection.query(query)
            except Exception as e:
                logger.debug(e, exc_info=e)
                self.warn_if_stateful_else_error(
                    "operation",
                    f"Populating table operation history from Snowflake failed due to error {e}.",
                )
                self.report_status(USAGE_EXTRACTION_OPERATIONAL_STATS, False)
                return
            self.report.access_history_query_secs = round(timer.elapsed_seconds(), 2)

        for row in results:
            yield from self._process_snowflake_history_row(row)

    def _make_operations_query(self) -> str:
        start_time = int(self.start_time.timestamp() * 1000)
        end_time = int(self.end_time.timestamp() * 1000)
        return SnowflakeQuery.operational_data_for_time_window(start_time, end_time)

    def _check_usage_date_ranges(self) -> None:
        with PerfTimer() as timer:
            try:
                assert self.connection is not None
                results = self.connection.query(
                    SnowflakeQuery.get_access_history_date_range()
                )
            except Exception as e:
                if isinstance(e, SnowflakePermissionError):
                    error_msg = "Failed to get usage. Please grant imported privileges on SNOWFLAKE database. "
                    self.warn_if_stateful_else_error(
                        "usage-permission-error", error_msg
                    )
                else:
                    logger.debug(e, exc_info=e)
                    self.report.warning(
                        "usage",
                        f"Extracting the date range for usage data from Snowflake failed due to error {e}.",
                    )
                self.report_status("date-range-check", False)
            else:
                for db_row in results:
                    if (
                        len(db_row) < 2
                        or db_row["MIN_TIME"] is None
                        or db_row["MAX_TIME"] is None
                    ):
                        self.report.warning(
                            "check-usage-data",
                            f"Missing data for access_history {db_row}.",
                        )
                        break
                    self.report.min_access_history_time = db_row["MIN_TIME"].astimezone(
                        tz=timezone.utc
                    )
                    self.report.max_access_history_time = db_row["MAX_TIME"].astimezone(
                        tz=timezone.utc
                    )
                    self.report.access_history_range_query_secs = round(
                        timer.elapsed_seconds(), 2
                    )

    def _get_operation_aspect_work_unit(
        self, event: SnowflakeJoinedAccessEvent, discovered_datasets: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        if event.query_start_time and event.query_type:
            start_time = event.query_start_time
            query_type = event.query_type
            user_email = event.email
            user_name = event.user_name
            operation_type = OPERATION_STATEMENT_TYPES.get(
                query_type, OperationTypeClass.CUSTOM
            )
            reported_time: int = int(time.time() * 1000)
            last_updated_timestamp: int = int(start_time.timestamp() * 1000)
            user_urn = make_user_urn(
                self.get_user_identifier(
                    user_name, user_email, self.config.email_as_user_identifier
                )
            )

            # NOTE: In earlier `snowflake-usage` connector this was base_objects_accessed, which is incorrect
            for obj in event.objects_modified:
                resource = obj.objectName

                dataset_identifier = (
                    self.identifiers.get_dataset_identifier_from_qualified_name(
                        resource
                    )
                )

                if dataset_identifier not in discovered_datasets:
                    logger.debug(
                        f"Skipping operations for table {dataset_identifier}, as table schema is not accessible"
                    )
                    continue

                operation_aspect = OperationClass(
                    timestampMillis=reported_time,
                    lastUpdatedTimestamp=last_updated_timestamp,
                    actor=user_urn,
                    operationType=operation_type,
                    customOperationType=(
                        query_type
                        if operation_type is OperationTypeClass.CUSTOM
                        else None
                    ),
                )
                mcp = MetadataChangeProposalWrapper(
                    entityUrn=self.identifiers.gen_dataset_urn(dataset_identifier),
                    aspect=operation_aspect,
                )
                wu = MetadataWorkUnit(
                    id=f"{start_time.isoformat()}-operation-aspect-{resource}",
                    mcp=mcp,
                )
                yield wu

    def _process_snowflake_history_row(
        self, event_dict: dict
    ) -> Iterable[SnowflakeJoinedAccessEvent]:
        try:  # big hammer try block to ensure we don't fail on parsing events
            self.report.rows_processed += 1

            # no use processing events that don't have a query text
            if not event_dict["QUERY_TEXT"]:
                self.report.rows_missing_query_text += 1
                return
            self.parse_event_objects(event_dict)
            event = SnowflakeJoinedAccessEvent(
                **{k.lower(): v for k, v in event_dict.items()}
            )
            yield event
        except Exception as e:
            self.report.rows_parsing_error += 1
            self.report.warning(
                "operation",
                f"Failed to parse operation history row {event_dict}, {e}",
            )

    def parse_event_objects(self, event_dict: Dict) -> None:
        event_dict["BASE_OBJECTS_ACCESSED"] = [
            obj
            for obj in json.loads(event_dict["BASE_OBJECTS_ACCESSED"])
            if self._is_object_valid(obj)
        ]
        if len(event_dict["BASE_OBJECTS_ACCESSED"]) == 0:
            self.report.rows_zero_base_objects_accessed += 1

        event_dict["DIRECT_OBJECTS_ACCESSED"] = [
            obj
            for obj in json.loads(event_dict["DIRECT_OBJECTS_ACCESSED"])
            if self._is_object_valid(obj)
        ]
        if len(event_dict["DIRECT_OBJECTS_ACCESSED"]) == 0:
            self.report.rows_zero_direct_objects_accessed += 1

        event_dict["OBJECTS_MODIFIED"] = [
            obj
            for obj in json.loads(event_dict["OBJECTS_MODIFIED"])
            if self._is_object_valid(obj)
        ]
        if len(event_dict["OBJECTS_MODIFIED"]) == 0:
            self.report.rows_zero_objects_modified += 1

        event_dict["QUERY_START_TIME"] = (event_dict["QUERY_START_TIME"]).astimezone(
            tz=timezone.utc
        )

        if (
            not event_dict["EMAIL"]
            and self.config.email_domain
            and event_dict["USER_NAME"]
        ):
            # NOTE: Generated emails may be incorrect, as email may be different than
            # username@email_domain
            event_dict[
                "EMAIL"
            ] = f'{event_dict["USER_NAME"]}@{self.config.email_domain}'.lower()

        if not event_dict["EMAIL"]:
            self.report.rows_missing_email += 1

    def _is_unsupported_object_accessed(self, obj: Dict[str, Any]) -> bool:
        unsupported_keys = ["locations"]

        if obj.get("objectDomain") in ["Stage"]:
            return True

        return any([obj.get(key) is not None for key in unsupported_keys])

    def _is_object_valid(self, obj: Dict[str, Any]) -> bool:
        if self._is_unsupported_object_accessed(
            obj
        ) or not self.filter.is_dataset_pattern_allowed(
            obj.get("objectName"), obj.get("objectDomain")
        ):
            return False
        return True

    def _should_ingest_usage(self) -> bool:
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

    def report_status(self, step: str, status: bool) -> None:
        if self.redundant_run_skip_handler:
            self.redundant_run_skip_handler.report_current_run_status(step, status)

    def close(self) -> None:
        pass
