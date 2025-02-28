import logging
from datetime import date, datetime, timezone
from typing import Any, List, Optional, Tuple, Union

from datahub.utilities.urns.urn import Urn
from tenacity import retry, stop_after_attempt, wait_exponential
from tenacity.before_sleep import before_sleep_log

from datahub_executor.common.assertion.engine.evaluator.filter_builder import (
    FilterBuilder,
)
from datahub_executor.common.assertion.types import AssertionDatabaseParams
from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.exceptions import (
    AssertionResultException,
    CustomSQLErrorException,
    FieldAssertionErrorException,
    InvalidParametersException,
    InvalidSourceTypeException,
    SourceQueryFailedException,
)
from datahub_executor.common.types import (
    AssertionStdOperator,
    AssertionStdParameters,
    DatasetFilter,
    DatasetVolumeAssertionParameters,
    DatasetVolumeSourceType,
    EntityEvent,
    EntityEventType,
    FieldMetricType,
    FieldTransform,
    FreshnessFieldSpec,
    SchemaFieldSpec,
)

from .sql.field_metrics_sql_generator import FieldMetricsSQLGenerator
from .sql.field_values_sql_generator import FieldValuesSQLGenerator
from .types import DatabaseParams, SourceOperationParams

logger = logging.getLogger(__name__)


class Source:
    """Base class for a connector responsible for fetching information from external sources. Parallel concept to a normal ingestion source."""

    connection: Connection
    source_name: str
    field_values_sql_generator: FieldValuesSQLGenerator
    field_metrics_sql_generator: FieldMetricsSQLGenerator
    row_limit: int = (
        5  # row limit to prevent large queries but still show recent events
    )

    def __init__(self, connection: Connection):
        self.connection = connection

    def _execute_fetchall_query(self, query: str) -> List[Any]:
        raise NotImplementedError()

    def _get_database_string(
        self, params: Union[DatabaseParams, SourceOperationParams]
    ) -> str:
        raise NotImplementedError()

    def _convert_value_for_comparison(self, column_value: str, column_type: str) -> str:
        raise NotImplementedError()

    def _get_audit_log_operation_events(
        self, operation_params: SourceOperationParams, parameters: dict
    ) -> List[EntityEvent]:
        raise NotImplementedError()

    def _get_dataset_last_updated_events(
        self, operation_params: SourceOperationParams
    ) -> List[EntityEvent]:
        raise NotImplementedError()

    def _get_field_last_updated_events(
        self, operation_params: SourceOperationParams, parameters: dict
    ) -> List[EntityEvent]:
        raise NotImplementedError()

    def _get_num_rows_via_stats_table(self, database_params: DatabaseParams) -> int:
        raise NotImplementedError()

    def _get_num_rows_via_count(
        self, database_params: DatabaseParams, filter_sql: str
    ) -> int:
        raise NotImplementedError()

    def _get_single_value_from_custom_sql(self, custom_sql: str) -> Union[int, float]:
        raise NotImplementedError()

    def _get_supported_high_watermark_column_types(self) -> List[str]:
        raise NotImplementedError()

    def _get_supported_high_watermark_date_and_time_types(self) -> List[str]:
        raise NotImplementedError()

    def _get_high_watermark_field_value(
        self,
        column_name: str,
        column_type: str,
        operation_params: SourceOperationParams,
        filter_sql: str,
        previous_value: Optional[str],
    ) -> Optional[str]:
        raise NotImplementedError()

    def _get_high_watermark_row_count(
        self,
        column_name: str,
        column_type: str,
        operation_params: SourceOperationParams,
        filter_sql: str,
        current_field_value: str,
    ) -> int:
        raise NotImplementedError()

    def _get_high_watermark_for_column(
        self,
        operation_params: SourceOperationParams,
        parameters: dict,
        previous_value: Optional[str],
    ) -> Tuple[str, int]:
        if (
            "path" in parameters
            and "type" in parameters
            and "native_type" in parameters
        ):
            column_name = parameters["path"]
            column_type = parameters["native_type"]
            filter_sql = FilterBuilder(parameters.get("filter")).get_sql()

            if (
                column_type.upper()
                not in self._get_supported_high_watermark_column_types()
            ):
                raise InvalidParametersException(
                    message=f"Unsupported high watermark column {column_type} provided. Failing assertion evaluation!",
                    parameters=parameters,
                )

            current_field_value = self._get_high_watermark_field_value(
                column_name, column_type, operation_params, filter_sql, previous_value
            )
            if current_field_value is None:
                return ("", 0)

            current_row_count = self._get_high_watermark_row_count(
                column_name,
                column_type,
                operation_params,
                filter_sql,
                current_field_value,
            )
            return (str(current_field_value), current_row_count)

        raise InvalidParametersException(
            message="Missing required inputs: column path and column type.",
            parameters=parameters,
        )

    def _get_database_params(
        self, entity_urn: str, database_parameters: AssertionDatabaseParams
    ) -> DatabaseParams:
        if database_parameters.qualified_name:
            dataset_name_parts = database_parameters.qualified_name.split(".")
        else:
            urn_obj = Urn.create_from_string(entity_urn)
            dataset_name = urn_obj.get_entity_id()[1]
            dataset_name_parts = dataset_name.split(".")

        if len(dataset_name_parts) > 3:
            # Handle platform instance.
            dataset_name_parts = dataset_name_parts[:3]

        # we'll use table name if available since it will have the proper casing
        # with some data sources (snowflake) we have a bug where table_name has proper case
        # but the qualified name does not (currently all lower case) so we use table_name were we have it.
        return DatabaseParams(
            dataset_part_0=dataset_name_parts[0],
            dataset_part_1=dataset_name_parts[1],
            dataset_part_2=(
                database_parameters.table_name
                if database_parameters.table_name
                else dataset_name_parts[2]
            ),
        )

    def _get_operation_params(
        self, entity_urn: str, window: List[int], parameters: dict
    ) -> SourceOperationParams:
        database_parameters = parameters.get(
            "database", AssertionDatabaseParams(qualified_name=None, table_name=None)
        )
        return SourceOperationParams(
            start_time_millis=window[0],
            end_time_millis=window[1],
            database_params=self._get_database_params(entity_urn, database_parameters),
        )

    def _build_field_update_results(
        self, dates: List[Union[date, datetime]]
    ) -> List[EntityEvent]:
        results = []

        for datetime_obj in dates:
            # Check whether we are dealing with a date object (without any time)
            # If yes, convert it.
            if isinstance(datetime_obj, date) and not isinstance(
                datetime_obj, datetime
            ):
                datetime_obj = datetime.combine(datetime_obj, datetime.min.time())

            datetime_obj = datetime_obj.replace(tzinfo=timezone.utc)

            # Convert to timestamp ms
            timestamp = int(datetime_obj.timestamp() * 1000)

            entity_event = EntityEvent(EntityEventType.FIELD_UPDATE, timestamp)
            results.append(entity_event)

        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR, True),
    )
    def _try_get_entity_events(
        self,
        event_type: EntityEventType,
        operation_params: SourceOperationParams,
        parameters: dict,
    ) -> List[EntityEvent]:
        if event_type == EntityEventType.AUDIT_LOG_OPERATION:
            # Scan the audit log to see if there are any events falling into the previous window.
            return self._get_audit_log_operation_events(operation_params, parameters)
        elif event_type == EntityEventType.INFORMATION_SCHEMA_UPDATE:
            # Hit something else!
            return self._get_dataset_last_updated_events(operation_params)
        elif event_type == EntityEventType.FIELD_UPDATE:
            # Build and issue a query!
            return self._get_field_last_updated_events(operation_params, parameters)
        else:
            return self._try_get_source_specific_entity_events(
                event_type, operation_params, parameters
            )

    def _try_get_source_specific_entity_events(
        self,
        event_type: EntityEventType,
        operation_params: SourceOperationParams,
        parameters: dict,
    ) -> List[EntityEvent]:
        raise InvalidSourceTypeException(
            message=f"Unsupported entity event type {event_type} provided. {self.source_name} connector does not support retrieving these events.",
            source_type=event_type,
        )

    def get_entity_events(
        self,
        entity_urn: str,
        event_type: EntityEventType,
        window: List[int],
        parameters: dict,
    ) -> List[EntityEvent]:
        operation_params = self._get_operation_params(entity_urn, window, parameters)
        return self._try_get_entity_events(event_type, operation_params, parameters)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR, True),
    )
    def _try_get_current_high_watermark_for_column(
        self,
        event_type: EntityEventType,
        operation_params: SourceOperationParams,
        parameters: dict,
        previous_value: Optional[str],
    ) -> Tuple[str, int]:
        if event_type == EntityEventType.FIELD_UPDATE:
            return self._get_high_watermark_for_column(
                operation_params, parameters, previous_value
            )
        else:
            raise InvalidSourceTypeException(
                message=f"Unsupported entity event type {event_type} provided. {self.source_name} connector does not support retrieving these events.",
                source_type=event_type,
            )

    def get_current_high_watermark_for_column(
        self,
        entity_urn: str,
        event_type: EntityEventType,
        window: List[int],
        parameters: dict,
        previous_value: Optional[str],
    ) -> Tuple[str, int]:
        operation_params = self._get_operation_params(entity_urn, window, parameters)
        return self._try_get_current_high_watermark_for_column(
            event_type, operation_params, parameters, previous_value
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR, True),
    )
    def _get_row_count(
        self,
        database_params: DatabaseParams,
        volume_parameters: DatasetVolumeAssertionParameters,
        filter_params: Optional[dict],
    ) -> int:
        if volume_parameters.source_type == DatasetVolumeSourceType.INFORMATION_SCHEMA:
            return self._get_num_rows_via_stats_table(database_params)
        elif volume_parameters.source_type == DatasetVolumeSourceType.QUERY:
            filter_sql = FilterBuilder(filter_params).get_sql() if filter_params else ""
            return self._get_num_rows_via_count(database_params, filter_sql)

        raise InvalidParametersException(
            message=f"Unsupported source type {volume_parameters.source_type} provided. {self.source_name} connector does not support retrieving these events.",
            parameters=volume_parameters.__dict__,
        )

    def get_row_count(
        self,
        entity_urn: str,
        database_parameters: AssertionDatabaseParams,
        volume_parameters: DatasetVolumeAssertionParameters,
        filter_params: Optional[dict],
    ) -> int:
        database_params = self._get_database_params(entity_urn, database_parameters)
        return self._get_row_count(database_params, volume_parameters, filter_params)

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

        if len(rows) != 1:
            # this SQL should return ONE row only
            raise CustomSQLErrorException(
                f"Custom SQL returned {len(rows)} rows, expected one!"
            )

        row = rows[0]
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

    def execute_custom_sql(
        self,
        entity_urn: str,
        database_parameters: AssertionDatabaseParams,
        custom_sql: str,
    ) -> float:
        # database_params = self._get_database_params(entity_urn, database_parameters)
        return self._execute_custom_sql(custom_sql)

    def _setup_last_checked_sql_fragment(
        self,
        prev_high_watermark_value: Optional[str],
        changed_rows_field: Optional[FreshnessFieldSpec],
    ) -> Optional[str]:
        last_checked = None
        if changed_rows_field and prev_high_watermark_value:
            if (
                not changed_rows_field.native_type
                or changed_rows_field.native_type.upper()
                not in self._get_supported_high_watermark_column_types()
            ):
                raise AssertionResultException(
                    message=f"Unsupported high watermark column type {changed_rows_field.native_type} provided. Failing assertion evaluation!",
                )

            if (
                changed_rows_field.native_type.upper()
                in self._get_supported_high_watermark_date_and_time_types()
                and prev_high_watermark_value
            ):
                prev_high_watermark_value = self._convert_value_for_comparison(
                    prev_high_watermark_value, changed_rows_field.native_type
                )

            last_checked = f"{changed_rows_field.path} >= {prev_high_watermark_value}"

        return last_checked

    def _build_field_values_query(
        self,
        database_params: DatabaseParams,
        field: SchemaFieldSpec,
        operator: AssertionStdOperator,
        parameters: Optional[AssertionStdParameters],
        exclude_nulls: bool,
        transform: Optional[FieldTransform],
        filter_sql: Optional[str],
        prev_high_watermark_value: Optional[str],
        changed_rows_field: Optional[FreshnessFieldSpec],
    ) -> str:
        # if applicable, setup a "last checked" sql fragment to filter the query further
        # eg. last_modified >= TO_TIMESTAMP('2023-11-11 12:00:00')
        last_checked_sql_fragment = self._setup_last_checked_sql_fragment(
            prev_high_watermark_value, changed_rows_field
        )

        return self.field_values_sql_generator.setup_query(
            self._get_database_string(database_params),
            field,
            operator,
            parameters,
            exclude_nulls,
            filter_sql,
            transform,
            last_checked_sql_fragment,
        )

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
        prev_high_watermark_value: Optional[str],
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
            prev_high_watermark_value,
            changed_rows_field,
        )
        rows = self._execute_fetchall_query(query)

        if len(rows) != 1:
            raise SourceQueryFailedException(
                f"Field Values query returned {len(rows)} rows, expected one!",
                query=query,
            )

        row = rows[0]
        try:
            return int(row[0])
        except (ValueError, TypeError):
            raise SourceQueryFailedException(
                f"Field Values query returned non-numeric value '{row[0]}'", query=query
            )

    def get_field_values_count(
        self,
        entity_urn: str,
        database_parameters: AssertionDatabaseParams,
        field: SchemaFieldSpec,
        operator: AssertionStdOperator,
        parameters: Optional[AssertionStdParameters],
        exclude_nulls: bool,
        transform: Optional[FieldTransform],
        filter: Optional[DatasetFilter],
        prev_high_watermark_value: Optional[str],
        changed_rows_field: Optional[FreshnessFieldSpec],
    ) -> int:
        database_params = self._get_database_params(entity_urn, database_parameters)
        filter_sql = FilterBuilder(filter.__dict__).get_sql() if filter else None

        return self._get_field_values_count(
            database_params,
            field,
            operator,
            parameters,
            exclude_nulls,
            transform,
            filter_sql,
            prev_high_watermark_value,
            changed_rows_field,
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
        changed_rows_field: Optional[FreshnessFieldSpec],
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

        if len(rows) != 1:
            raise FieldAssertionErrorException(
                f"Field Metrics query returned {len(rows)} rows, expected one!",
                query=query,
            )

        row = rows[0]
        try:
            return float(row[0])
        except (ValueError, TypeError):
            raise FieldAssertionErrorException(
                f"Field Metrics query returned non-numeric value '{row[0]}'",
                query=query,
            )

    def get_field_metric_value(
        self,
        entity_urn: str,
        database_parameters: AssertionDatabaseParams,
        field: SchemaFieldSpec,
        metric: FieldMetricType,
        filter: Optional[DatasetFilter],
        prev_high_watermark_value: Optional[str],
        changed_rows_field: Optional[FreshnessFieldSpec],
    ) -> float:
        database_params = self._get_database_params(entity_urn, database_parameters)
        filter_sql = FilterBuilder(filter.__dict__).get_sql() if filter else None

        return self._get_field_metric_value(
            database_params,
            field,
            metric,
            filter_sql,
            prev_high_watermark_value,
            changed_rows_field,
        )
