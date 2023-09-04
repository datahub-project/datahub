import logging
from typing import Any, List, Optional

from datahub_monitors.assertion.engine.evaluator.filter_builder import FilterBuilder
from datahub_monitors.connection.snowflake.snowflake_connection import (
    SnowflakeConnection,
)
from datahub_monitors.exceptions import (
    InvalidParametersException,
    SourceQueryFailedException,
)
from datahub_monitors.source.snowflake.time_utils import (
    convert_millis_to_timestamp_type,
    convert_value_for_comparison,
)
from datahub_monitors.source.source import Source
from datahub_monitors.source.types import DatabaseParams, SourceOperationParams
from datahub_monitors.types import EntityEvent, EntityEventType

from ..utils.sql import (
    setup_high_watermark_field_value_query,
    setup_high_watermark_row_count_query,
    setup_row_count_query,
)
from .types import (
    DEFAULT_OPERATION_TYPES_FILTER,
    HIGH_WATERMARK_DATE_AND_TIME_TYPES,
    SUPPORTED_HIGH_WATERMARK_COLUMN_TYPES,
    SUPPORTED_LAST_MODIFIED_COLUMN_TYPES,
)

logger = logging.getLogger(__name__)


# Should we support snowflake Streams... We do support COPY...
class SnowflakeSource(Source):
    """A source for extracting information from Snowflake"""

    connection: SnowflakeConnection
    source_name: str = "Snowflake"

    def __init__(self, connection: SnowflakeConnection):
        super().__init__(connection)

        try:
            # Set our default timezone to UTC so that comparisons with
            # timezone columns are always peformed in UTC.
            query = "ALTER SESSION SET TIMEZONE = 'UTC';"
            cur = self.connection.get_client().cursor()
            cur.execute(query)
        except Exception as e:
            raise SourceQueryFailedException(
                message=f"Source query (Snowflake) failed with error: {e}", query=query
            )

    def _get_operation_types_filter(self, parameters: dict) -> str:
        if parameters:
            if "operation_types" in parameters and parameters["operation_types"]:
                result = ""
                operation_types = parameters["operation_types"]
                for operation_type in operation_types:
                    result = result + f"'{operation_type}',"
                return result[:-1]
        return DEFAULT_OPERATION_TYPES_FILTER

    def _get_user_name_filter(self, parameters: dict) -> Optional[str]:
        if parameters:
            if "user_name" in parameters and parameters["user_name"] is not None:
                return parameters["user_name"].lower()
        return None

    def _execute_fetchall_query(self, query: str) -> List[Any]:
        try:
            cur = self.connection.get_client().cursor()
            cur.execute(query)
            return cur.fetchall()
        except Exception as e:
            raise SourceQueryFailedException(
                message=f"Source query (Snowflake) failed with error: {e}", query=query
            )

    def _execute_fetchone_query(self, query: str) -> List[Any]:
        try:
            cur = self.connection.get_client().cursor()
            cur.execute(query)
            return cur.fetchone()
        except Exception as e:
            raise SourceQueryFailedException(
                message=f"Source query (Snowflake) failed with error: {e}", query=query
            )

    def _build_audit_log_results(self, rows: List[Any]) -> List[EntityEvent]:
        return [
            EntityEvent(EntityEventType.AUDIT_LOG_OPERATION, row[6]) for row in rows
        ]

    def _build_information_schema_results(self, rows: List[Any]) -> List[EntityEvent]:
        return [
            EntityEvent(EntityEventType.INFORMATION_SCHEMA_UPDATE, row[2])
            for row in rows
        ]

    # Note that this method cannot effectively leverage partitions because it comes from the audit log.
    #
    # Notice that the audit log may be delayed by up to 3 hours, so this may not be great for real time.
    # https://docs.snowflake.com/en/sql-reference/account-usage.
    #
    # For shorter time-frame assertions, column-values or information schema checks are recommended.
    def _get_audit_log_operation_events(
        self, operation_params: SourceOperationParams, parameters: dict
    ) -> List[EntityEvent]:
        # TODO: Hit the cache first. --> Embedded data store. We really should not be issuing this query for every entity.
        # TODO: Make this class a singleton.
        # TODO: Do we need to support external audit logs?

        # Compute audit log filters based on parameters
        operation_types_filter = self._get_operation_types_filter(parameters)
        user_name_filter = self._get_user_name_filter(parameters)

        # NOTE - known potential bug here, we are doing a lowercase normalization in this query
        # BUT Snowflake does allow creation of the same name, but different casing for db, schema, table
        # eg. TEST_DB.PUBLIC.MY_TABLE is different from TEST_DB.PUBLIC.My_Table
        # so this query could potentially be reporting back on multiple tables.  At least until we can get the
        # fully qualified name here of all parts of the DB string.

        # Formulate query
        query = f"""
            WITH exploded_access_history AS (
            SELECT 
                access_history.query_id as query_id,
                access_history.user_name as user_name,
                access_history.query_start_time as query_start_time,
                updated_objects.value as updated_objects
            FROM 
                snowflake.account_usage.access_history access_history,
                LATERAL FLATTEN(input => access_history.objects_modified) updated_objects
            WHERE access_history.query_start_time >= to_timestamp_ltz({operation_params.start_time_millis}, 3)
                AND access_history.query_start_time < to_timestamp_ltz({operation_params.end_time_millis}, 3)
                {f"AND LOWER(access_history.user_name) = '{user_name_filter}'" if user_name_filter is not None else ''}
            )

            SELECT
                query_history.query_text AS "QUERY_TEXT",
                query_history.query_type AS "OPERATION_TYPE",
                query_history.rows_inserted AS "ROWS_INSERTED",
                query_history.rows_updated AS "ROWS_UPDATED",
                query_history.rows_deleted AS "ROWS_DELETED",
                exploded_access_history.user_name AS "USER_NAME",
                (DATE_PART('EPOCH', exploded_access_history.query_start_time) * 1000) AS "QUERY_START_MS",
                exploded_access_history.updated_objects:objectName::STRING AS "MODIFIED_OBJECT"
            FROM
                exploded_access_history as exploded_access_history
            INNER JOIN
                (SELECT * FROM snowflake.account_usage.query_history 
                WHERE query_history.start_time >= to_timestamp_ltz({operation_params.start_time_millis}, 3)
                    AND query_history.start_time < to_timestamp_ltz({operation_params.end_time_millis}, 3) 
                    AND query_history.query_type in ({operation_types_filter})) query_history
                ON exploded_access_history.query_id = query_history.query_id
            WHERE                
                REGEXP_REPLACE(LOWER(exploded_access_history.updated_objects:objectName::STRING), '\\"|\\'', '') in ('{operation_params.catalog.lower()}.{operation_params.schema.lower()}.{operation_params.table.lower()}')
            ORDER BY query_history.start_time DESC
;"""
        logger.debug(query)

        return self._build_audit_log_results(self._execute_fetchall_query(query))

    # Note that this method cannot effectively leverage partitions because it comes from the information schema
    def _get_dataset_last_updated_events(
        self, operation_params: SourceOperationParams
    ) -> List[EntityEvent]:
        # Can we cache this? We could also issue this query periodically without any filters, and save it locally.
        # Ideally the connector would do this for us!
        # What if we only run this if... The audit log tells us something?

        # NOTE - known bug here, catalog and schema are coming from the qualifiedName, which ingestion is not saving
        # with the right casing, so there is a potential that this query will fail until that bug is fixed.
        # so for now, let's leave the .upper() calls here so that most will work

        # Use the properly cased table name
        # use upper for schema name because we don't have the proper case
        # use upper for catalog/database name because we don't have the proper case
        query = f"""
            SELECT table_name, table_type, (DATE_PART('EPOCH', last_altered) * 1000) as last_altered
            FROM {operation_params.catalog.upper()}.information_schema.tables
            WHERE last_altered >= to_timestamp_ltz({operation_params.start_time_millis}, 3)
            AND last_altered < to_timestamp_ltz({operation_params.end_time_millis}, 3)
            AND table_name = '{operation_params.table}'
            AND table_schema = '{operation_params.schema.upper()}'
            AND table_catalog = '{operation_params.catalog.upper()}';"""

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
        # DATE: Represents a date (year, month, day). This type does not include a time component.
        # TIMESTAMP: Represents a point in time with up to microsecond precision. This type includes both date and time components. Snowflake has several variations of this type:
        # TIMESTAMP_NTZ: A timestamp without a time zone. NTZ stands for "No Time Zone".
        # TIMESTAMP_TZ: A timestamp with a time zone. TZ stands for "Time Zone". It is ASSUMED that these are in UTC.
        # TIMESTAMP_LTZ: A timestamp with a local time zone. LTZ stands for "Local Time Zone". This is Snowflake's default TIMESTAMP type.
        # DATETIME: This type is equivalent to TIMESTAMP_NTZ and is available for compatibility with other database systems.
        #
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

            # Convert the window timestamps into values that are suitable for comparison.
            start_datetime = convert_millis_to_timestamp_type(
                operation_params.start_time_millis, column_type
            )
            end_datetime = convert_millis_to_timestamp_type(
                operation_params.end_time_millis, column_type
            )

            # The goal is to basically extract the high watermark for the column identified here.
            query = f"""
                SELECT {date_column} as last_altered_date
                FROM {operation_params.catalog}.{operation_params.schema}."{operation_params.table}"
                WHERE {date_column} >= ({start_datetime})
                AND {date_column} <= ({end_datetime})
                {f"AND {filter_sql}" if filter_sql else ''}
                ORDER BY {date_column} DESC
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
        if column_type in HIGH_WATERMARK_DATE_AND_TIME_TYPES and previous_value:
            previous_value = convert_value_for_comparison(previous_value, column_type)

        get_value_query = setup_high_watermark_field_value_query(
            column_name,
            f'{operation_params.database}.{operation_params.schema}."{operation_params.table}"',
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
        if column_type in HIGH_WATERMARK_DATE_AND_TIME_TYPES and current_field_value:
            current_field_value = convert_value_for_comparison(
                current_field_value, column_type
            )

        get_count_query = setup_high_watermark_row_count_query(
            column_name,
            f'{operation_params.database}.{operation_params.schema}."{operation_params.table}"',
            filter_sql,
            current_field_value,
        )
        resp = self._execute_fetchone_query(get_count_query)
        return resp[0] if resp else 0

    def _get_num_rows_via_stats_table(self, database_params: DatabaseParams) -> int:
        query = f"""
            SELECT row_count
            FROM {database_params.catalog.upper()}.information_schema.tables
            WHERE table_name = '{database_params.table}'
            AND table_schema = '{database_params.schema.upper()}'
            AND table_catalog = '{database_params.catalog.upper()}';"""

        logger.debug(query)
        resp = self._execute_fetchone_query(query)
        return resp[0] if resp else 0

    def _get_num_rows_via_count(
        self, database_params: DatabaseParams, filter_sql: str
    ) -> int:
        query = setup_row_count_query(
            f'{database_params.database}.{database_params.schema}."{database_params.table}"',
            filter_sql,
        )

        logger.debug(query)
        resp = self._execute_fetchone_query(query)
        return resp[0] if resp else 0
