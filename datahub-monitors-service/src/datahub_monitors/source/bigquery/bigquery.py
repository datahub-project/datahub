import logging
from datetime import datetime
from typing import Any, Iterable, List, Optional

import pytz
from datahub.ingestion.source.bigquery_v2.common import (
    BQ_DATETIME_FORMAT,
    _make_gcp_logging_client,
)

from datahub_monitors.assertion.engine.evaluator.filter_builder import FilterBuilder
from datahub_monitors.connection.bigquery.bigquery_connection import BigQueryConnection
from datahub_monitors.exceptions import (
    InvalidParametersException,
    SourceQueryFailedException,
)
from datahub_monitors.source.bigquery.time_utils import (
    convert_millis_to_timestamp_type,
    convert_value_for_comparison,
)
from datahub_monitors.source.source import Source
from datahub_monitors.source.types import SourceOperationParams
from datahub_monitors.types import EntityEvent, EntityEventType

from ..utils.sql import (
    setup_high_watermark_field_value_query,
    setup_high_watermark_row_count_query,
)
from .types import (
    DEFAULT_OPERATION_TYPES_FILTER,
    SUPPORTED_HIGH_WATERMARK_COLUMN_TYPES,
    SUPPORTED_LAST_MODIFIED_COLUMN_TYPES,
)

logger = logging.getLogger(__name__)


# Note that we only support Email address for username filter inside of this source!
class BigQuerySource(Source):
    """A source for extracting information from BigQuery"""

    connection: BigQueryConnection
    source_name: str = "BigQuery"

    def __init__(self, connection: BigQueryConnection):
        super().__init__(connection)
        self.connection = connection

    # TODO: Convert from DataHub Operation Type to BQ type.
    def _get_operation_types_filter(self, parameters: dict) -> str:
        if parameters is not None:
            if (
                "operation_types" in parameters
                and parameters["operation_types"] is not None
                and len(parameters["operation_types"]) > 0
            ):
                result = ""
                operation_types = parameters["operation_types"]
                for operation_type in operation_types:
                    result = result + f'"{operation_type}" OR '
                return result[:-4]
        return DEFAULT_OPERATION_TYPES_FILTER

    def _get_user_name_filter(self, parameters: dict) -> Optional[str]:
        if parameters is not None:
            if "user_name" in parameters and parameters["user_name"] is not None:
                return parameters["user_name"].lower()
        return None

    def _generate_filter(
        self,
        project: str,
        dataset: str,
        table: str,
        start_time_millis: int,
        end_time_millis: int,
        parameters: dict,
    ) -> str:
        # We adjust the filter values a bit, since we need to make sure that the join
        # between query events and read events is complete.

        start_time = datetime.fromtimestamp(start_time_millis / 1000.0)
        start_time_utc = start_time.astimezone(pytz.utc).strftime(BQ_DATETIME_FORMAT)

        end_time = datetime.fromtimestamp(end_time_millis / 1000.0)
        end_time_utc = end_time.astimezone(pytz.utc).strftime(BQ_DATETIME_FORMAT)

        operation_types_filter = self._get_operation_types_filter(parameters)
        user_name_filter = self._get_user_name_filter(parameters)

        filter = f"""
            resource.type=("bigquery_project" OR "bigquery_dataset")
            AND
            (
                protoPayload.methodName="google.cloud.bigquery.v2.JobService.InsertJob"
                AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE"
                AND NOT protoPayload.metadata.jobChange.job.jobStatus.errorResult:*
                AND protoPayload.metadata.jobChange.job.jobConfig.queryConfig.destinationTable="projects/{project}/datasets/{dataset}/tables/{table}"
                AND protoPayload.metadata.jobChange.job.jobConfig.queryConfig.statementType=
                    (
                        {operation_types_filter}
                    )
            )
            {f'AND protoPayload.authenticationInfo.principalEmail="{user_name_filter}"' if user_name_filter is not None else ''}
            AND timestamp >= "{start_time_utc}"
            AND timestamp < "{end_time_utc}"
            """

        return filter

    def _extract_audit_logs_for_table(
        self,
        client: Any,
        project: str,
        dataset: str,
        table: str,
        start_time_millis: int,
        end_time_millis: int,
        parameters: dict,
    ) -> Iterable[Any]:
        filter = self._generate_filter(
            project, dataset, table, start_time_millis, end_time_millis, parameters
        )
        entries = client.list_entries(
            filter_=filter,
            page_size=self.connection.config.log_page_size,
        )
        for entry in enumerate(entries):
            yield entry

    def _execute_query(self, query: str) -> List[Any]:
        try:
            return self.connection.get_client().query(query)
        except Exception as e:
            raise SourceQueryFailedException(
                message=f"Source query (BigQuery) failed with error: {e}", query=query
            )

    def _build_audit_log_results(self, entries: Iterable[Any]) -> List[EntityEvent]:
        results = []
        for entry in entries:
            # Extract the correct entries.
            logger.debug(entry)

            # operation_type = entry[1].payload["metadata"]["jobChange"]["job"]["jobConfig"]["queryConfig"]["statementType"]
            timestamp = entry[1].timestamp

            # TODO: Determine whether we want to produce an operation or anything as part of this!

            entity_event = EntityEvent(
                EntityEventType.AUDIT_LOG_OPERATION, int(timestamp.timestamp() * 1000)
            )
            results.append(entity_event)

        return results

    def _build_information_schema_results(self, rows: List[Any]) -> List[EntityEvent]:
        return [
            EntityEvent(EntityEventType.INFORMATION_SCHEMA_UPDATE, row[0])
            for row in rows
        ]

    # TODO: Support EXPORTED Audit logs
    def _get_audit_log_operation_events(
        self, operation_params: SourceOperationParams, parameters: dict
    ) -> List[EntityEvent]:
        # TODO: Hit the cache first. --> Embedded data store. We really should not be issuing this query for every entity.
        # TODO: Make this class a singleton.
        # TODO: Do we need to support external audit logs?

        logging_client = _make_gcp_logging_client(
            operation_params.project, self.connection.config.extra_client_options
        )
        entries = self._extract_audit_logs_for_table(
            logging_client,
            operation_params.project,
            operation_params.dataset,
            operation_params.table,
            operation_params.start_time_millis,
            operation_params.end_time_millis,
            parameters,
        )

        return self._build_audit_log_results(entries)

    # Note that this method cannot effectively leverage partitions because it comes from the audit log
    def _get_dataset_last_updated_events(
        self, operation_params: SourceOperationParams
    ) -> List[EntityEvent]:
        # Compares using UTC date.
        query = f"""
            SELECT last_modified_time
            FROM {operation_params.project}.{operation_params.dataset}.__TABLES__
            WHERE table_id="{operation_params.table}"
                AND last_modified_time >= {operation_params.start_time_millis}
                AND last_modified_time <= {operation_params.end_time_millis}
        ;"""

        logger.debug(query)

        return self._build_information_schema_results(self._execute_query(query))

    # This is the ONLY approach which allows for partition spec definition.
    # TODO: Add support for partitioning.
    def _get_field_last_updated_events(
        self, operation_params: SourceOperationParams, parameters: dict
    ) -> List[EntityEvent]:
        # Supported Column Types:
        # TODO: Verify that every single type here works.
        # DATE: Represents a logical calendar date. The format is YYYY-[M]M-[D]D.
        # DATETIME: Represents a year, month, day, hour, minute, second, and subsecond. The format is YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]].
        # TIMESTAMP: Represents an absolute point in time with millisecond precision. The format is YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]][time zone].

        # Unsupported Column Types:
        # STRING, VARCHAR -> Would need to collect a date format.
        # NUMBER -> Would need to collect a units.
        # What if we only run this if the audit log gives us information... The audit log tells us something?

        if (
            "path" in parameters
            and "type" in parameters
            and "native_type" in parameters
        ):
            date_column = parameters["path"]
            column_type = parameters["native_type"]
            filter_sql = FilterBuilder(parameters.get("filter")).get_sql()

            if column_type.upper() not in SUPPORTED_LAST_MODIFIED_COLUMN_TYPES:
                raise InvalidParametersException(
                    message=f"Unsupported date column type {column_type} provided. Failing assertion evaluation!",
                    parameters=parameters,
                )

            start_datetime = convert_millis_to_timestamp_type(
                operation_params.start_time_millis, column_type
            )
            end_datetime = convert_millis_to_timestamp_type(
                operation_params.end_time_millis, column_type
            )

            # Compares using UTC date.
            query = f"""
                SELECT {date_column} as last_altered_date
                FROM {operation_params.project}.{operation_params.dataset}.{operation_params.table}
                WHERE {date_column} >= ({start_datetime})
                AND {date_column} <= ({end_datetime})
                {f"AND {filter_sql}" if filter_sql else ''}
                ORDER BY {date_column} DESC
            ;"""

            return self._build_field_update_results(
                [row[0] for row in self._execute_query(query)]
            )

        raise InvalidParametersException(
            message="Missing required inputs: column path and column type.",
            parameters=parameters,
        )

    def _get_supported_high_watermark_column_types(self) -> List[str]:
        return SUPPORTED_HIGH_WATERMARK_COLUMN_TYPES

    def _get_high_watermark_field_value(
        self,
        column_name: str,
        column_type: str,
        operation_params: SourceOperationParams,
        filter_sql: str,
        previous_value: Optional[str],
    ) -> Optional[str]:
        # if this is a date or timestamp we need to convert
        if column_type in ["DATE", "DATETIME", "TIMESTAMP"] and previous_value:
            previous_value = convert_value_for_comparison(previous_value, column_type)

        get_value_query = setup_high_watermark_field_value_query(
            column_name,
            f"{operation_params.database}.{operation_params.schema}.{operation_params.table}",
            filter_sql,
            previous_value,
        )
        rows = self._execute_query(get_value_query)
        current_field_value = None
        # TODO - find the right client method to get the first/single row instead of iterating here
        for row in rows:
            current_field_value = row[0]

        return current_field_value

    def _get_high_watermark_row_count(
        self,
        column_name: str,
        column_type: str,
        operation_params: SourceOperationParams,
        filter_sql: str,
        current_field_value: str,
    ) -> int:
        # if this is a date or timestamp we need to convert
        if column_type in ["DATE", "DATETIME", "TIMESTAMP"] and current_field_value:
            current_field_value = convert_value_for_comparison(
                current_field_value, column_type
            )

        get_count_query = setup_high_watermark_row_count_query(
            column_name,
            f"{operation_params.database}.{operation_params.schema}.{operation_params.table}",
            filter_sql,
            current_field_value,
        )
        rows = self._execute_query(get_count_query)
        current_row_count = 0
        for row in rows:
            current_row_count = row[0]

        return current_row_count
