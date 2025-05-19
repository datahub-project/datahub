import logging
from datetime import datetime
from typing import Any, Iterable, List, Optional, Union

import pytz
from datahub.ingestion.source.bigquery_v2.common import BQ_DATETIME_FORMAT
from google.api_core.exceptions import (
    BadRequest,
    Forbidden,
    InternalServerError,
    NotFound,
    ServiceUnavailable,
)
from tenacity import retry, stop_after_attempt, wait_exponential
from tenacity.before_sleep import before_sleep_log

from datahub_executor.common.assertion.engine.evaluator.filter_builder import (
    FilterBuilder,
)
from datahub_executor.common.connection.bigquery.bigquery_connection import (
    BigQueryConnection,
)
from datahub_executor.common.exceptions import (
    CustomSQLErrorException,
    FieldAssertionErrorException,
    InvalidParametersException,
    SourceQueryFailedException,
)
from datahub_executor.common.source.bigquery.time_utils import (
    convert_millis_to_timestamp_type,
    convert_value_for_comparison,
)
from datahub_executor.common.source.source import Source
from datahub_executor.common.source.sql.field_metrics_sql_generator import (
    FieldMetricsSQLGenerator,
)
from datahub_executor.common.source.sql.field_values_sql_generator import (
    FieldValuesSQLGenerator,
)
from datahub_executor.common.source.sql.utils import (
    setup_high_watermark_field_value_query,
    setup_high_watermark_row_count_query,
    setup_row_count_query,
)
from datahub_executor.common.source.types import DatabaseParams, SourceOperationParams
from datahub_executor.common.types import (
    AssertionStdOperator,
    AssertionStdParameters,
    EntityEvent,
    EntityEventType,
    FieldMetricType,
    FieldTransform,
    FreshnessFieldSpec,
    SchemaFieldSpec,
)

from .sql.field_metrics_sql_generator import BigQueryFieldMetricsSQLGenerator
from .sql.field_values_sql_generator import BigQueryFieldValuesSQLGenerator
from .types import (
    DEFAULT_OPERATION_TYPES_FILTER,
    HIGH_WATERMARK_DATE_AND_TIME_TYPES,
    SUPPORTED_HIGH_WATERMARK_COLUMN_TYPES,
    SUPPORTED_LAST_MODIFIED_COLUMN_TYPES,
)

logger = logging.getLogger(__name__)


# Note that we only support Email address for username filter inside of this source!
class BigQuerySource(Source):
    """A source for extracting information from BigQuery"""

    connection: BigQueryConnection
    source_name: str = "BigQuery"
    field_values_sql_generator: FieldValuesSQLGenerator
    field_metrics_sql_generator: FieldMetricsSQLGenerator

    def __init__(self, connection: BigQueryConnection):
        super().__init__(connection)
        self.connection = connection
        self.field_values_sql_generator = BigQueryFieldValuesSQLGenerator()
        self.field_metrics_sql_generator = BigQueryFieldMetricsSQLGenerator()

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

    def _get_database_string(
        self, params: Union[DatabaseParams, SourceOperationParams]
    ) -> str:
        return f"{params.project}.{params.dataset}.{params.table}"

    def _convert_value_for_comparison(self, column_value: str, column_type: str) -> str:
        return convert_value_for_comparison(column_value, column_type)

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
                protoPayload.methodName=
                    (
                        "google.cloud.bigquery.v2.JobService.Query"
                        OR
                        "google.cloud.bigquery.v2.JobService.InsertJob"
                    )
                AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE"
                AND NOT protoPayload.metadata.jobChange.job.jobStatus.errorResult:*
                AND protoPayload.metadata.jobChange.job.jobConfig.queryConfig.destinationTable="projects/{project}/datasets/{dataset}/tables/{table}"
                AND protoPayload.metadata.jobChange.job.jobConfig.queryConfig.statementType=
                    (
                        {operation_types_filter}
                    )
            )
            {f'AND protoPayload.authenticationInfo.principalEmail="{user_name_filter}"' if user_name_filter is not None else ""}
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
            max_results=self.row_limit,
        )
        for entry in enumerate(entries):
            yield entry

    def _execute_fetchall_query(self, query: str) -> List[Any]:
        try:
            return self.connection.get_client().query(query)
        except (
            NotFound,
            Forbidden,
            BadRequest,
            InternalServerError,
            ServiceUnavailable,
        ) as e:
            raise SourceQueryFailedException(
                message=f"Source query (BigQuery) failed with error: {e.message}",
                query=query,
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

        logging_client = self.connection.config.make_gcp_logging_client(
            operation_params.project
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
            LIMIT {self.row_limit}
        ;"""

        logger.debug(query)

        return self._build_information_schema_results(
            self._execute_fetchall_query(query)
        )

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
                FROM {self._get_database_string(operation_params)}
                WHERE {date_column} >= ({start_datetime})
                AND {date_column} <= ({end_datetime})
                {f"AND {filter_sql}" if filter_sql else ""}
                ORDER BY {date_column} DESC
                LIMIT {self.row_limit}
            ;"""

            return self._build_field_update_results(
                [row[0] for row in self._execute_fetchall_query(query)]
            )

        raise InvalidParametersException(
            message="Missing required inputs: column path and column type.",
            parameters=parameters,
        )

    def _get_supported_high_watermark_column_types(self) -> List[str]:
        return SUPPORTED_HIGH_WATERMARK_COLUMN_TYPES

    def _get_supported_high_watermark_date_and_time_types(self) -> List[str]:
        return HIGH_WATERMARK_DATE_AND_TIME_TYPES

    def _get_high_watermark_field_value(
        self,
        column_name: str,
        column_type: str,
        operation_params: SourceOperationParams,
        filter_sql: str,
        previous_value: Optional[str],
    ) -> Optional[str]:
        # if this is a date or timestamp we need to convert
        if (
            column_type in self._get_supported_high_watermark_date_and_time_types()
            and previous_value
        ):
            previous_value = convert_value_for_comparison(previous_value, column_type)

        get_value_query = setup_high_watermark_field_value_query(
            column_name,
            self._get_database_string(operation_params),
            filter_sql,
            previous_value,
        )

        try:
            rows = self._execute_fetchall_query(get_value_query)
            current_field_value = None
            # TODO - find the right client method to get the first/single row instead of iterating here
            for row in rows:
                current_field_value = row[0]
        except (
            NotFound,
            Forbidden,
            BadRequest,
            InternalServerError,
            ServiceUnavailable,
        ) as e:
            raise SourceQueryFailedException(
                message=f"Source query (BigQuery) failed with error: {e.message}",
                query=get_value_query,
            )

        return current_field_value

    def _get_high_watermark_row_count(
        self,
        column_name: str,
        column_type: str,
        operation_params: SourceOperationParams,
        filter_sql: str,
        current_field_value: str,
    ) -> Optional[int]:
        # if this is a date or timestamp we need to convert
        if (
            column_type in self._get_supported_high_watermark_date_and_time_types()
            and current_field_value
        ):
            current_field_value = convert_value_for_comparison(
                current_field_value, column_type
            )

        get_count_query = setup_high_watermark_row_count_query(
            column_name,
            self._get_database_string(operation_params),
            filter_sql,
            current_field_value,
        )

        try:
            # This should be a single row since it is a count * query
            rows = self._execute_fetchall_query(get_count_query)
            if len(rows) == 0:
                return None
            current_row_count = 0
            for row in rows:
                current_row_count = row[0]

            return current_row_count
        except (
            NotFound,
            Forbidden,
            BadRequest,
            InternalServerError,
            ServiceUnavailable,
        ) as e:
            raise SourceQueryFailedException(
                message=f"Source query (BigQuery) failed with error: {e.message}",
                query=get_count_query,
            )

    def _get_num_rows_via_stats_table(
        self, database_params: DatabaseParams
    ) -> Optional[int]:
        query = f"""
            SELECT row_count
            FROM {database_params.project}.`{database_params.dataset}`.__TABLES__
            WHERE table_id='{database_params.table}';"""

        logger.debug(query)

        try:
            rows = self._execute_fetchall_query(query)
            if len(rows) == 0:
                return None
            current_row_count = 0
            for row in rows:
                current_row_count = int(row[0])

            return current_row_count
        except (
            NotFound,
            Forbidden,
            BadRequest,
            InternalServerError,
            ServiceUnavailable,
        ) as e:
            raise SourceQueryFailedException(
                message=f"Source query (BigQuery) failed with error: {e.message}",
                query=query,
            )

    def _get_num_rows_via_count(
        self, database_params: DatabaseParams, filter_sql: str
    ) -> Optional[int]:
        query = setup_row_count_query(
            self._get_database_string(database_params),
            filter_sql,
        )

        logger.debug(query)

        try:
            rows = self._execute_fetchall_query(query)
            if len(rows) == 0:
                return None
            current_row_count = 0
            for row in rows:
                current_row_count = int(row[0])
            return current_row_count
        except (
            NotFound,
            Forbidden,
            BadRequest,
            InternalServerError,
            ServiceUnavailable,
        ) as e:
            raise SourceQueryFailedException(
                message=f"Source query (BigQuery) failed with error: {e.message}",
                query=query,
            )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR, True),
    )
    def _execute_custom_sql(
        self,
        custom_sql: str,
    ) -> float:
        logger.debug(custom_sql)
        rows = self._execute_fetchall_query(custom_sql)

        try:
            for row in rows:
                if len(row) != 1:
                    # this SQL should return ONE value only
                    raise CustomSQLErrorException(
                        f"Custom SQL returned {len(row)} values, expected one!"
                    )

                try:
                    return float(row[0])
                except (ValueError, TypeError):
                    raise CustomSQLErrorException(
                        f"Custom SQL returned non-numeric value '{row[0]}'"
                    )
        except (
            NotFound,
            Forbidden,
            BadRequest,
            InternalServerError,
            ServiceUnavailable,
        ) as e:
            # try/except here not around the _execute_fetchall_query because it seems the
            # google client has lazy evaluation, so doesn't fail until the 'for row in rows:' line
            raise CustomSQLErrorException(e.message)

        raise CustomSQLErrorException("Custom SQL returned 0 rows, expected one!")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR, True),
    )
    def _get_field_values_count(
        self,
        database_params: DatabaseParams,
        field: SchemaFieldSpec,
        operator: AssertionStdOperator,
        parameters: Optional[AssertionStdParameters],
        exclude_nulls: bool,
        transform: Optional[FieldTransform],
        filter_sql: Optional[str],
        prev_changed_rows_value: Optional[str],
        changed_rows_field: Optional[FreshnessFieldSpec],
    ) -> int:
        query = self._build_field_values_query(
            database_params,
            field,
            operator,
            parameters,
            exclude_nulls,
            transform,
            filter_sql,
            prev_changed_rows_value,
            changed_rows_field,
        )
        rows = self._execute_fetchall_query(query)

        try:
            for row in rows:
                if len(row) != 1:
                    raise FieldAssertionErrorException(
                        f"Field Values query returned {len(row)} rows, expected one!"
                    )

                try:
                    return int(row[0])
                except (ValueError, TypeError):
                    raise FieldAssertionErrorException(
                        f"Field Values query returned non-numeric value '{row[0]}'"
                    )
        except (
            NotFound,
            Forbidden,
            BadRequest,
            InternalServerError,
            ServiceUnavailable,
        ) as e:
            # try/except here not around the _execute_fetchall_query because it seems the
            # google client has lazy evaluation, so doesn't fail until the 'for row in rows:' line
            raise FieldAssertionErrorException(e.message)

        raise FieldAssertionErrorException(
            "Field Values query returned 0 rows, expected one!"
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR, True),
    )
    def _get_field_metric_value(
        self,
        database_params: DatabaseParams,
        field: SchemaFieldSpec,
        metric: FieldMetricType,
        filter_sql: Optional[str],
        prev_high_watermark_value: Optional[str],
        changed_rows_field: Optional[SchemaFieldSpec],
    ) -> float:
        # if applicable, setup a "last checked" sql fragment to filter the query further
        # eg. last_modified >= TO_TIMESTAMP('2023-11-11 12:00:00')
        last_checked_sql_fragment = self._setup_last_checked_sql_fragment(
            prev_high_watermark_value, changed_rows_field
        )
        query = self.field_metrics_sql_generator.setup_query(
            self._get_database_string(database_params),
            field,
            metric,
            filter_sql,
            last_checked_sql_fragment,
        )
        rows = self._execute_fetchall_query(query)

        try:
            for row in rows:
                if len(row) != 1:
                    raise FieldAssertionErrorException(
                        f"Field Values query returned {len(row)} rows, expected one!"
                    )

                try:
                    return float(row[0])
                except (ValueError, TypeError):
                    raise FieldAssertionErrorException(
                        f"Field Values query returned non-numeric value '{row[0]}'"
                    )
        except (
            NotFound,
            Forbidden,
            BadRequest,
            InternalServerError,
            ServiceUnavailable,
        ) as e:
            # try/except here not around the _execute_fetchall_query because it seems the
            # google client has lazy evaluation, so doesn't fail until the 'for row in rows:' line
            raise FieldAssertionErrorException(e.message)

        raise FieldAssertionErrorException(
            "Field Values query returned 0 rows, expected one!"
        )
