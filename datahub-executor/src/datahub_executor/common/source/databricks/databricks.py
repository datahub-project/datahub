import logging
from typing import Any, Iterable, List, Optional, Union

from datahub.utilities.time import datetime_to_ts_millis

from datahub_executor.common.assertion.engine.evaluator.filter_builder import (
    FilterBuilder,
)
from datahub_executor.common.connection.databricks.databricks_connection import (
    DatabricksConnection,
)
from datahub_executor.common.exceptions import (
    InvalidParametersException,
    InvalidSourceTypeException,
    SourceQueryFailedException,
)
from datahub_executor.common.source.databricks.sql.field_metrics_sql_generator import (
    DatabricksFieldMetricsSQLGenerator,
)
from datahub_executor.common.source.databricks.sql.field_values_sql_generator import (
    DatabricksFieldValuesSQLGenerator,
)
from datahub_executor.common.source.databricks.time_utils import (
    convert_millis_to_timestamp_type,
    convert_value_for_comparison,
)
from datahub_executor.common.source.databricks.types import (
    HIGH_WATERMARK_DATE_AND_TIME_TYPES,
    SUPPORTED_HIGH_WATERMARK_COLUMN_TYPES,
    SUPPORTED_LAST_MODIFIED_COLUMN_TYPES,
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
from datahub_executor.common.types import EntityEvent, EntityEventType

logger = logging.getLogger(__name__)
HIVE_METASTORE = "hive_metastore"


class DatabricksSource(Source):
    """
    Source to monitor Databricks unity catalog + legacy hive metastore tables
    for unity catalog enabled databricks workspaces.
    Unity catalog supports Managed or External Tables - https://docs.databricks.com/en/data-governance/unity-catalog/index.html#tables
    Similarly Hive metastore supports Managed and External Tables - https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-hiveformat.html
    Managed Tables are stored in location managed by databricks and their lifecycle is entirely handled in databricks.
    External Tables are stored in location managed by cloud providers.

    Unity Catalog Managed tables always use the Delta table format.
    Rest can be in any of these formats - DELTA, CSV, JSON, AVRO, PARQUET, ORC, TEXT
    """

    connection: DatabricksConnection
    source_name: str = "Databricks"
    field_values_sql_generator: FieldValuesSQLGenerator
    field_metrics_sql_generator: FieldMetricsSQLGenerator

    def __init__(self, connection: DatabricksConnection):
        super().__init__(connection)

        self.field_values_sql_generator = DatabricksFieldValuesSQLGenerator()
        self.field_metrics_sql_generator = DatabricksFieldMetricsSQLGenerator()

    def _execute_fetchall_query(self, query: str) -> List[Any]:
        client = self.connection.get_client()
        try:
            return client.cursor().execute(query).fetchall()
        except Exception as e:
            raise SourceQueryFailedException(
                message=f"Source query (Databricks) failed with error: {e}", query=query
            )

    def _execute_fetchone_query(self, query: str) -> List[Any]:
        client = self.connection.get_client()
        try:
            return client.cursor().execute(query).fetchone()
        except Exception as e:
            raise SourceQueryFailedException(
                message=f"Source query (Databricks) failed with error: {e}", query=query
            )

    def _get_database_string(
        self, params: Union[DatabaseParams, SourceOperationParams]
    ) -> str:
        return f"`{params.catalog}`.`{params.schema}`.`{params.table}`"

    def _get_num_rows_via_count(
        self, database_params: DatabaseParams, filter_sql: str
    ) -> int:
        query = setup_row_count_query(
            self._get_database_string(database_params),
            filter_sql,
        )
        resp = self._execute_fetchone_query(query)
        return resp[0] if resp else 0

    def _get_operation_types_filter(
        self, operation_types: Optional[List[str]]
    ) -> Optional[str]:
        if operation_types:
            values = ",".join([f'"{op}"' for op in operation_types])
            return f"operation IN ({values})"
        return None

    def _get_user_name_filter(self, user_name: Optional[str]) -> Optional[str]:
        if user_name:
            return f'userName = "{user_name}"'
        return None

    def _get_audit_log_operation_events(
        self, operation_params: SourceOperationParams, parameters: dict
    ) -> List[EntityEvent]:
        # Delta Table Describe History - https://docs.databricks.com/en/delta/history.html
        # This would work for only delta tables.
        # This would throw error for non-delta tables.

        # Notes on Unity Catalog Audit Log (As of Jan 2024)
        # https://docs.databricks.com/en/administration-guide/account-settings/audit-logs.html#unity-catalog-events
        # Based on investigative tests done on UC audit log for managed as well as external table, audit table
        # `system.access.audit` does not reliably capture `unityCatalog->updateTables` event required for freshness
        # checks. This may change in future and we should build UC audit log based freshness checks at that point.

        return self._delta_table_audit_log_operation_events(
            operation_params, parameters
        )

    def _delta_table_audit_log_operation_events(
        self, operation_params: SourceOperationParams, parameters: dict
    ) -> List[EntityEvent]:
        qname = self._get_database_string(operation_params)
        operation_filter = self._get_operation_types_filter(
            parameters.get("operation_types")
        )
        username_filter = self._get_user_name_filter(parameters.get("user_name"))
        query = f"""
            WITH table_audit_log AS 
                (DESCRIBE HISTORY {qname})
            SELECT timestamp FROM table_audit_log 
            WHERE 
                TO_TIMESTAMP(FROM_UNIXTIME({operation_params.start_time_millis}/1000)) <= timestamp 
                AND timestamp <= TO_TIMESTAMP(FROM_UNIXTIME({operation_params.end_time_millis}/1000))
                {"AND " + operation_filter if operation_filter else ""}
                {"AND " + username_filter if username_filter else ""}
                LIMIT {self.row_limit};
        """
        logger.debug(query)
        try:
            rows = self._execute_fetchall_query(query)
        except SourceQueryFailedException as e:
            # Running DESCRIBE HISTORY query on Non-Delta tables results in
            # error "DESCRIBE HISTORY is only supported for Delta tables"
            if "only supported for Delta tables" in e.message:
                event_type = EntityEventType.AUDIT_LOG_OPERATION
                raise InvalidSourceTypeException(
                    f"Source event type {event_type} not supported for Non-Delta tables.",
                    source_type=event_type,
                )
            else:
                raise

        return list(self._build_audit_log_results(rows))

    def _build_audit_log_results(self, rows: List[Any]) -> Iterable[EntityEvent]:
        for row in rows:
            yield EntityEvent(
                EntityEventType.AUDIT_LOG_OPERATION, datetime_to_ts_millis(row[0])
            )

    def _get_dataset_last_updated_events(
        self, operation_params: SourceOperationParams
    ) -> List[EntityEvent]:
        # Describe detail - https://docs.databricks.com/en/delta/table-details.html
        # This would work for only delta tables.
        # This would throw error for non-delta tables.

        return self._delta_tables_get_last_updated_events(operation_params)

    def _get_field_last_updated_events(
        self, operation_params: SourceOperationParams, parameters: dict
    ) -> List[EntityEvent]:
        # Supported Column Types:
        # DATE: Represents a date (year, month, day). This type does not include a time component.
        # TIMESTAMP: Represents a point in time with up to microsecond precision. This type includes both date and time components. Snowflake has several variations of this type:
        # TIMESTAMP_NTZ: A timestamp without a time zone. NTZ stands for "No Time Zone".
        #
        # Unsupported Column Types:
        # TODO: Attempt to use [TO_TIMESTAMP](https://docs.databricks.com/en/sql/language-manual/functions/to_timestamp.html)
        # or TO_DATE for STRING, VARCHAR columns
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

            # Convert the window timestamps into values that are suitable for comparison.
            start_datetime = convert_millis_to_timestamp_type(
                operation_params.start_time_millis, column_type.upper()
            )
            end_datetime = convert_millis_to_timestamp_type(
                operation_params.end_time_millis, column_type.upper()
            )

            # The goal is to filter for any rows which have changed within this boundary of time.
            # If there are greater than 0 rows, then the table is considered to have changed.
            query = f"""
                SELECT {date_column} as last_altered_date
                FROM {self._get_database_string(operation_params)}
                WHERE {date_column} >= ({start_datetime})
                AND {date_column} <= ({end_datetime})
                {f"AND {filter_sql}" if filter_sql else ""}
                ORDER BY {date_column} DESC
                LIMIT {self.row_limit}
                ;
            """
            logger.debug(query)

            return self._build_field_update_results(
                [row[0] for row in self._execute_fetchall_query(query)]
            )

        raise InvalidParametersException(
            message="Missing required inputs: column path and column type.",
            parameters=parameters,
        )

    def _delta_tables_get_last_updated_events(
        self, operation_params: SourceOperationParams
    ) -> List[EntityEvent]:
        qname = self._get_database_string(operation_params)
        query = f"DESCRIBE DETAIL {qname};"
        logger.debug(query)

        return self._build_describe_detail_results(
            self._execute_fetchone_query(query), operation_params
        )

    def _build_describe_detail_results(
        self, row: List[Any], operation_params: SourceOperationParams
    ) -> List[EntityEvent]:
        event_type = EntityEventType.INFORMATION_SCHEMA_UPDATE
        # columns of row
        # format,id,name,description,location,createdAt,lastModified,partitionColumns,clusteringColumns,numFiles,sizeInBytes,properties,minReaderVersion,minWriterVersion,tableFeatures,statistics
        # Ref - Describe detail - https://docs.databricks.com/en/delta/table-details.html

        # From observation, lastModified is empty for Non delta table
        if row[6] is None:
            raise InvalidSourceTypeException(
                f"Source event type {event_type} not supported for Non-Delta tables.",
                source_type=event_type,
            )
        elif (
            operation_params.start_time_millis
            <= datetime_to_ts_millis(row[6])
            <= operation_params.end_time_millis
        ):
            return [
                EntityEvent(
                    event_type,
                    datetime_to_ts_millis(row[6]),
                )
            ]
        else:
            return []

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
            column_type.upper()
            in self._get_supported_high_watermark_date_and_time_types()
            and previous_value
        ):
            previous_value = convert_value_for_comparison(
                previous_value, column_type.upper()
            )

        get_value_query = setup_high_watermark_field_value_query(
            column_name,
            self._get_database_string(operation_params),
            filter_sql,
            previous_value,
        )
        resp = self._execute_fetchone_query(get_value_query)
        return resp[0] if resp else None

    def _get_high_watermark_row_count(
        self,
        column_name: str,
        column_type: str,
        operation_params: SourceOperationParams,
        filter_sql: str,
        current_field_value: str,
    ) -> int:
        # if this is a date or timestamp we need to convert
        if (
            column_type.upper()
            in self._get_supported_high_watermark_date_and_time_types()
            and current_field_value
        ):
            current_field_value = convert_value_for_comparison(
                current_field_value, column_type.upper()
            )
        get_count_query = setup_high_watermark_row_count_query(
            column_name,
            self._get_database_string(operation_params),
            filter_sql,
            current_field_value,
        )
        resp = self._execute_fetchone_query(get_count_query)
        return resp[0] if resp else 0

    def _convert_value_for_comparison(self, column_value: str, column_type: str) -> str:
        return convert_value_for_comparison(column_value, column_type.upper())

    def _try_get_source_specific_entity_events(
        self,
        event_type: EntityEventType,
        operation_params: SourceOperationParams,
        parameters: dict,
    ) -> List[EntityEvent]:
        if event_type == EntityEventType.FILE_METADATA_UPDATE:
            # only supported for databricks
            return self._get_dataset_file_last_updated_events(operation_params)
        return super()._try_get_source_specific_entity_events(
            event_type, operation_params, parameters
        )

    def _get_dataset_file_last_updated_events(
        self, operation_params: SourceOperationParams
    ) -> List[EntityEvent]:
        # Convert the window timestamps into values that are suitable for comparison.
        start_datetime = convert_millis_to_timestamp_type(
            operation_params.start_time_millis, "TIMESTAMP"
        )
        end_datetime = convert_millis_to_timestamp_type(
            operation_params.end_time_millis, "TIMESTAMP"
        )

        date_column = "_metadata.file_modification_time"
        # The goal is to filter for any files which have changed within this boundary of time.
        query = f"""
            SELECT MAX({date_column})
            FROM {self._get_database_string(operation_params)}
            WHERE {date_column} >= ({start_datetime})
            AND {date_column} <= ({end_datetime});
        """

        logger.debug(query)

        # Note for tables with "hive" format -
        # As per databricks file metadata docs - https://docs.databricks.com/en/ingestion/file-metadata-column.html
        # The column _metadata is available for all input file formats.
        # However, this is not working for hive metastore tables created with hive format using
        # https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-hiveformat.html
        # The query throws below error:
        # [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with
        # name `_metadata`.`file_modification_time` cannot be resolved.
        #
        # As a workaround, it is possible to query the same details using table's storage location
        # and inferred format name. (accessible via describe extended <table> query. Also available in DatasetProperties aspect)
        # For example, if table `hive_metastore.default.student_parquet` is stored at location
        # `dbfs:/user/hive/warehouse/student_parquet` with Serde Library org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
        # Below query can be used to achieve same result.
        # select max(_metadata.file_modification_time) from parquet.`dbfs:/user/hive/warehouse/student_parquet`
        # However, this is not done to start with, as this seems to be relatively less used format in databricks.
        #
        # Also, currently there is no clear way to identify such tables in DataHub, except that `data_source_format` property
        # is not present in properties of such table. This is an ingestion bug and should be fixed.

        return list(
            self._build_file_last_updated_results(self._execute_fetchone_query(query))
        )

    def _build_file_last_updated_results(self, row: List[Any]) -> Iterable[EntityEvent]:
        # If no files have changed within this boundary, row[0] is NULL
        if row[0]:
            yield EntityEvent(
                EntityEventType.FILE_METADATA_UPDATE, datetime_to_ts_millis(row[0])
            )
