import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import pydantic
from snowflake.connector import SnowflakeConnection

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakeQueryMixin,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetFieldUsageCounts,
    DatasetUsageStatistics,
    DatasetUserUsageCounts,
)
from datahub.metadata.com.linkedin.pegasus2avro.timeseries import TimeWindowSize
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    OperationClass,
    OperationTypeClass,
)
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.sql_formatter import format_sql_query, trim_query

logger: logging.Logger = logging.getLogger(__name__)

OPERATION_STATEMENT_TYPES = {
    "INSERT": OperationTypeClass.INSERT,
    "UPDATE": OperationTypeClass.UPDATE,
    "DELETE": OperationTypeClass.DELETE,
    "CREATE": OperationTypeClass.CREATE,
    "CREATE_TABLE": OperationTypeClass.CREATE,
    "CREATE_TABLE_AS_SELECT": OperationTypeClass.CREATE,
}


class PermissiveModel(pydantic.BaseModel):
    class Config:
        extra = "allow"


class SnowflakeColumnReference(PermissiveModel):
    columnId: int
    columnName: str


class SnowflakeObjectAccessEntry(PermissiveModel):
    columns: Optional[List[SnowflakeColumnReference]]
    objectDomain: str
    objectId: int
    objectName: str
    stageKind: Optional[str]


class SnowflakeJoinedAccessEvent(PermissiveModel):
    query_start_time: datetime
    query_text: str
    query_type: str
    rows_inserted: Optional[int]
    rows_updated: Optional[int]
    rows_deleted: Optional[int]
    base_objects_accessed: List[SnowflakeObjectAccessEntry]
    direct_objects_accessed: List[SnowflakeObjectAccessEntry]
    objects_modified: List[SnowflakeObjectAccessEntry]

    user_name: str
    first_name: Optional[str]
    last_name: Optional[str]
    display_name: Optional[str]
    email: Optional[str]
    role_name: str


class SnowflakeUsageExtractor(SnowflakeQueryMixin, SnowflakeCommonMixin):
    def __init__(self, config: SnowflakeV2Config, report: SnowflakeV2Report) -> None:
        self.config: SnowflakeV2Config = config
        self.report: SnowflakeV2Report = report
        self.logger = logger

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        conn = self.config.get_connection()

        logger.info("Checking usage date ranges")
        self._check_usage_date_ranges(conn)
        if (
            self.report.min_access_history_time is None
            or self.report.max_access_history_time is None
        ):
            return

        # NOTE: In earlier `snowflake-usage` connector, users with no email were not considered in usage counts as well as in operation
        # Now, we report the usage as well as operation metadata even if user email is absent

        if self.config.include_usage_stats:
            yield from self.get_usage_workunits(conn)

        if self.config.include_operational_stats:
            # Generate the operation workunits.
            access_events = self._get_snowflake_history(conn)
            for event in access_events:
                yield from self._get_operation_aspect_work_unit(event)

        conn.close()

    def get_usage_workunits(
        self, conn: SnowflakeConnection
    ) -> Iterable[MetadataWorkUnit]:

        with PerfTimer() as timer:
            logger.info("Getting aggregated usage statistics")
            results = self.query(
                conn,
                SnowflakeQuery.usage_per_object_per_time_bucket_for_time_window(
                    start_time_millis=int(self.config.start_time.timestamp() * 1000),
                    end_time_millis=int(self.config.end_time.timestamp() * 1000),
                    time_bucket_size=self.config.bucket_duration,
                    use_base_objects=self.config.apply_view_usage_to_tables,
                    top_n_queries=self.config.top_n_queries,
                    include_top_n_queries=self.config.include_top_n_queries,
                ),
            )
            self.report.usage_aggregation_query_secs = timer.elapsed_seconds()

        for row in results:
            assert row["OBJECT_NAME"] is not None, "Null objectName not allowed"
            if not self._is_dataset_pattern_allowed(
                row["OBJECT_NAME"],
                row["OBJECT_DOMAIN"],
            ):
                continue

            stats = DatasetUsageStatistics(
                timestampMillis=int(row["BUCKET_START_TIME"].timestamp() * 1000),
                eventGranularity=TimeWindowSize(
                    unit=self.config.bucket_duration, multiple=1
                ),
                totalSqlQueries=row["TOTAL_QUERIES"],
                uniqueUserCount=row["TOTAL_USERS"],
                topSqlQueries=self._map_top_sql_queries(
                    json.loads(row["TOP_SQL_QUERIES"])
                )
                if self.config.include_top_n_queries
                else None,
                userCounts=self._map_user_counts(json.loads(row["USER_COUNTS"])),
                fieldCounts=[
                    DatasetFieldUsageCounts(
                        fieldPath=self.snowflake_identifier(field_count["col"]),
                        count=field_count["total"],
                    )
                    for field_count in json.loads(row["FIELD_COUNTS"])
                ],
            )
            dataset_urn = make_dataset_urn_with_platform_instance(
                self.platform,
                self.get_dataset_identifier_from_qualified_name(row["OBJECT_NAME"]),
                self.config.platform_instance,
                self.config.env,
            )
            yield self.wrap_aspect_as_workunit(
                "dataset",
                dataset_urn,
                "datasetUsageStatistics",
                stats,
            )

    def _map_top_sql_queries(self, top_sql_queries: Dict) -> List[str]:
        total_budget_for_query_list: int = 24000
        budget_per_query: int = int(
            total_budget_for_query_list / self.config.top_n_queries
        )
        return [
            trim_query(format_sql_query(query), budget_per_query)
            if self.config.format_sql_queries
            else query
            for query in top_sql_queries
        ]

    def _map_user_counts(self, user_counts: Dict) -> List[DatasetUserUsageCounts]:
        filtered_user_counts = []
        for user_count in user_counts:
            user_email = user_count.get(
                "email",
                "{0}@{1}".format(
                    user_count["user_name"], self.config.email_domain
                ).lower()
                if self.config.email_domain
                else None,
            )
            if user_email is None or not self.config.user_email_pattern.allowed(
                user_email
            ):
                continue

            filtered_user_counts.append(
                DatasetUserUsageCounts(
                    user=make_user_urn(
                        self.get_user_identifier(user_count["user_name"], user_email)
                    ),
                    count=user_count["total"],
                    # NOTE: Generated emails may be incorrect, as email may be different than
                    # username@email_domain
                    userEmail=user_email,
                )
            )
        return filtered_user_counts

    def _get_snowflake_history(
        self, conn: SnowflakeConnection
    ) -> Iterable[SnowflakeJoinedAccessEvent]:

        logger.info("Getting access history")
        with PerfTimer() as timer:
            query = self._make_operations_query()
            results = self.query(conn, query)
            self.report.access_history_query_secs = round(timer.elapsed_seconds(), 2)

        for row in results:
            yield from self._process_snowflake_history_row(row)

    def _make_operations_query(self) -> str:
        start_time = int(self.config.start_time.timestamp() * 1000)
        end_time = int(self.config.end_time.timestamp() * 1000)
        return SnowflakeQuery.operational_data_for_time_window(start_time, end_time)

    def _check_usage_date_ranges(self, conn: SnowflakeConnection) -> Any:

        with PerfTimer() as timer:
            try:
                results = self.query(
                    conn, SnowflakeQuery.get_access_history_date_range()
                )
            except Exception as e:
                self.warn(
                    "check-usage-data",
                    f"Extracting the date range for usage data from Snowflake failed."
                    f"Please check your permissions. Continuing...\nError was {e}.",
                )
            else:
                for db_row in results:
                    if (
                        len(db_row) < 2
                        or db_row["MIN_TIME"] is None
                        or db_row["MAX_TIME"] is None
                    ):
                        self.warn(
                            "check-usage-data",
                            f"Missing data for access_history {db_row} - Check if using Enterprise edition of Snowflake",
                        )
                        continue
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
        self, event: SnowflakeJoinedAccessEvent
    ) -> Iterable[MetadataWorkUnit]:
        if event.query_start_time and event.query_type in OPERATION_STATEMENT_TYPES:
            start_time = event.query_start_time
            query_type = event.query_type
            user_email = event.email
            user_name = event.user_name
            operation_type = OPERATION_STATEMENT_TYPES[query_type]
            reported_time: int = int(time.time() * 1000)
            last_updated_timestamp: int = int(start_time.timestamp() * 1000)
            user_urn = make_user_urn(self.get_user_identifier(user_name, user_email))

            # NOTE: In earlier `snowflake-usage` connector this was base_objects_accessed, which is incorrect
            for obj in event.objects_modified:

                resource = obj.objectName
                dataset_urn = make_dataset_urn_with_platform_instance(
                    self.platform,
                    self.get_dataset_identifier_from_qualified_name(resource),
                    self.config.platform_instance,
                    self.config.env,
                )
                operation_aspect = OperationClass(
                    timestampMillis=reported_time,
                    lastUpdatedTimestamp=last_updated_timestamp,
                    actor=user_urn,
                    operationType=operation_type,
                )
                mcp = MetadataChangeProposalWrapper(
                    entityType="dataset",
                    aspectName="operation",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=dataset_urn,
                    aspect=operation_aspect,
                )
                wu = MetadataWorkUnit(
                    id=f"{start_time.isoformat()}-operation-aspect-{resource}",
                    mcp=mcp,
                )
                self.report.report_workunit(wu)
                yield wu

    def _process_snowflake_history_row(
        self, row: Any
    ) -> Iterable[SnowflakeJoinedAccessEvent]:
        self.report.rows_processed += 1
        # Make some minor type conversions.
        if hasattr(row, "_asdict"):
            # Compat with SQLAlchemy 1.3 and 1.4
            # See https://docs.sqlalchemy.org/en/14/changelog/migration_14.html#rowproxy-is-no-longer-a-proxy-is-now-called-row-and-behaves-like-an-enhanced-named-tuple.
            event_dict = row._asdict()
        else:
            event_dict = dict(row)

        # no use processing events that don't have a query text
        if not event_dict["QUERY_TEXT"]:
            self.report.rows_missing_query_text += 1
            return

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

        try:  # big hammer try block to ensure we don't fail on parsing events
            event = SnowflakeJoinedAccessEvent(
                **{k.lower(): v for k, v in event_dict.items()}
            )
            yield event
        except Exception as e:
            self.report.rows_parsing_error += 1
            self.warn(
                "usage",
                f"Failed to parse usage line {event_dict}, {e}",
            )

    def _is_unsupported_object_accessed(self, obj: Dict[str, Any]) -> bool:
        unsupported_keys = ["locations"]

        if obj.get("objectDomain") in ["Stage"]:
            return True

        return any([obj.get(key) is not None for key in unsupported_keys])

    def _is_object_valid(self, obj: Dict[str, Any]) -> bool:
        if self._is_unsupported_object_accessed(
            obj
        ) or not self._is_dataset_pattern_allowed(
            obj.get("objectName"), obj.get("objectDomain")
        ):
            return False
        return True

    def warn(self, key: str, reason: str) -> None:
        self.report.report_warning(key, reason)
        self.logger.warning(f"{key} => {reason}")

    def error(self, key: str, reason: str) -> None:
        self.report.report_failure(key, reason)
        self.logger.error(f"{key} => {reason}")
