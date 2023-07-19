import logging
from datetime import date, datetime, timezone
from typing import List, Optional

from datahub.utilities.urns.urn import Urn
from tenacity import retry, stop_after_attempt, wait_exponential

from datahub_monitors.connection.snowflake.snowflake_connection import (
    SnowflakeConnection,
)
from datahub_monitors.exceptions import (
    InvalidParametersException,
    InvalidSourceTypeException,
    SourceQueryFailedException,
)
from datahub_monitors.source.snowflake.time_utils import (
    convert_millis_to_timestamp_type,
)
from datahub_monitors.source.source import Source
from datahub_monitors.types import EntityEvent, EntityEventType

logger = logging.getLogger(__name__)

APPLICATION_NAME: str = "acryl_datahub_monitors"
DEFAULT_OPERATION_TYPES_FILTER = "'INSERT', 'UPDATE', 'CREATE', 'CREATE_TABLE', 'CREATE_TABLE_AS_SELECT', 'COPY'"  # Note that Alter is not included :)
SUPPORTED_COLUMN_TYPES = [
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP_TZ",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_NTZ",
    "DATETIME",
]


# Should we support snowflake Streams... We do support COPY...
class SnowflakeSource(Source):
    """A source for extracting information from Snowflake"""

    connection: SnowflakeConnection

    def __init__(self, connection: SnowflakeConnection):
        super().__init__(connection)

        try:
            # Set our default timezone to UTC so that comparisons with
            # timezone columns are always peformed in UTC.
            cur = self.connection.get_client().cursor()
            query = "ALTER SESSION SET TIMEZONE = 'UTC';"
            cur.execute(query)
        except Exception as e:
            raise SourceQueryFailedException(
                message=f"Query failed with error: {e}", query=query
            )

    def _get_operation_types_filter(self, parameters: dict) -> str:
        if parameters is not None:
            if (
                "operation_types" in parameters
                and parameters["operation_types"] is not None
            ):
                result = ""
                operation_types = parameters["operation_types"]
                for operation_type in operation_types:
                    result = result + f"'{operation_type}',"
                return result[:-1]
        return DEFAULT_OPERATION_TYPES_FILTER

    def _get_user_name_filter(self, parameters: dict) -> Optional[str]:
        if parameters is not None:
            if "user_name" in parameters and parameters["user_name"] is not None:
                return parameters["user_name"].lower()
        return None

    # Note that this method cannot effectively leverage partitions because it comes from the audit log.
    #
    # Notice that the audit log may be delayed by up to 3 hours, so this may not be great for real time.
    # https://docs.snowflake.com/en/sql-reference/account-usage.
    #
    # For shorter time-frame assertions, column-values or information schema checks are recommended.
    def _get_audit_log_operation_events(
        self, entity_urn: str, window: List[int], parameters: dict
    ) -> List[EntityEvent]:
        # TODO: Hit the cache first. --> Embedded data store. We really should not be issuing this query for every entity.
        # TODO: Make this class a singleton.
        # TODO: Do we need to support external audit logs?

        # Define table and time range
        start_time_millis = window[0]
        end_time_millis = window[1]

        # Compute audit log filters based on parameters
        operation_types_filter = self._get_operation_types_filter(parameters)
        user_name_filter = self._get_user_name_filter(parameters)

        urn_obj = Urn.create_from_string(entity_urn)
        dataset_name = urn_obj.get_entity_id()[1].lower()

        dataset_name_parts = dataset_name.split(".")

        if len(dataset_name_parts) > 3:
            # Handle platform instance.
            dataset_name_parts = dataset_name_parts[:3]

        catalog = dataset_name_parts[0]
        schema = dataset_name_parts[1]
        table = dataset_name_parts[2]

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
            WHERE access_history.query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
                AND access_history.query_start_time < to_timestamp_ltz({end_time_millis}, 3)
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
                WHERE query_history.start_time >= to_timestamp_ltz({start_time_millis}, 3)
                    AND query_history.start_time < to_timestamp_ltz({end_time_millis}, 3) 
                    AND query_history.query_type in ({operation_types_filter})) query_history
                ON exploded_access_history.query_id = query_history.query_id
            WHERE                
                REGEXP_REPLACE(LOWER(exploded_access_history.updated_objects:objectName::STRING), '\\"\\'', '') in ('{catalog}.{schema}.{table}')
            ORDER BY query_history.start_time DESC
        ;"""

        logger.debug(query)

        try:
            cur = self.connection.get_client().cursor()
            cur.execute(query)
        except Exception as e:
            raise SourceQueryFailedException(
                message=f"Query failed with error: {e}", query=query
            )

        # Fetch results
        rows = cur.fetchall()
        results = []
        for row in rows:
            # Build results!
            timestamp = row[6]
            entity_event = EntityEvent(EntityEventType.AUDIT_LOG_OPERATION, timestamp)
            results.append(entity_event)

        return results

    # Note that this method cannot effectively leverage partitions because it comes from the information schema
    def _get_dataset_last_updated_events(
        self, entity_urn: str, window: List[int], parameters: dict
    ) -> List[EntityEvent]:
        # Can we cache this? We could also issue this query periodically without any filters, and save it locally.
        # Ideally the connector would do this for us!
        # What if we only run this if... The audit log tells us something?

        # Define table and time range
        start_time_millis = window[0]
        end_time_millis = window[1]

        urn_obj = Urn.create_from_string(entity_urn)
        dataset_name = urn_obj.get_entity_id()[1].lower()
        dataset_name_parts = dataset_name.split(".")

        if len(dataset_name_parts) > 3:
            # Handle platform instance.
            dataset_name_parts = dataset_name_parts[:3]

        catalog = dataset_name_parts[0]
        schema = dataset_name_parts[1]
        table = dataset_name_parts[2]

        query = f"""
            SELECT table_name, table_type, (DATE_PART('EPOCH', last_altered) * 1000) as last_altered
            FROM {catalog.upper()}.information_schema.tables
            WHERE last_altered >= to_timestamp_ltz({start_time_millis}, 3)
            AND last_altered < to_timestamp_ltz({end_time_millis}, 3)
            AND table_name = '{table.upper()}'
            AND table_schema = '{schema.upper()}' 
            AND table_catalog = '{catalog.upper()}';"""

        logger.debug(query)

        try:
            cur = self.connection.get_client().cursor()
            cur.execute(query)
        except Exception as e:
            raise SourceQueryFailedException(
                message=f"Query failed with error: {e}", query=query
            )

        # Fetch results
        rows = cur.fetchall()
        results = []
        for row in rows:
            # Build results!
            timestamp = row[2]
            entity_event = EntityEvent(
                EntityEventType.INFORMATION_SCHEMA_UPDATE, timestamp
            )
            results.append(entity_event)
        return results

    # This is the ONLY approach which allows for partition spec definition.
    # TODO: Add support for partitioning.
    def _get_field_last_updated_events(
        self, entity_urn: str, window: List[int], parameters: dict
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

        start_time_millis = window[0]
        end_time_millis = window[1]

        urn_obj = Urn.create_from_string(entity_urn)
        dataset_name = urn_obj.get_entity_id()[1].lower()
        dataset_name_parts = dataset_name.split(".")

        if len(dataset_name_parts) > 3:
            # Handle platform instance.
            dataset_name_parts = dataset_name_parts[:3]

        catalog = dataset_name_parts[0]
        schema = dataset_name_parts[1]
        table = dataset_name_parts[2]

        if (
            "path" in parameters
            and "type" in parameters
            and "native_type" in parameters
        ):
            date_column = parameters["path"]
            column_type = parameters["native_type"]

            if column_type.upper() not in SUPPORTED_COLUMN_TYPES:
                raise InvalidParametersException(
                    message=f"Unsupported date column type {column_type} provided. Failing assertion evaluation!",
                    parameters=parameters,
                )

            # Convert the window timestamps into values that are suitable for comparison.
            start_datetime = convert_millis_to_timestamp_type(
                start_time_millis, column_type
            )
            end_datetime = convert_millis_to_timestamp_type(
                end_time_millis, column_type
            )

            # The goal is to basically extract the high watermark for the column identified here.
            query = f"""
                SELECT {date_column} as last_altered_date
                FROM {catalog}.{schema}.{table}
                WHERE {date_column} >= ({start_datetime})
                AND {date_column} <= ({end_datetime})
                ORDER BY {date_column} DESC
                ;
            """

            logger.debug(query)

            try:
                cur = self.connection.get_client().cursor()
                cur.execute(query)
            except Exception as e:
                raise SourceQueryFailedException(
                    message=f"Query failed with error: {e}", query=query
                )

            # Fetch results
            rows = cur.fetchall()
            results = []
            for row in rows:
                datetime_obj = row[0]

                # Check whether we are dealing with a date object (without any time)
                # If yes, convert it.
                if isinstance(row[0], date) and not isinstance(row[0], datetime):
                    datetime_obj = datetime.combine(row[0], datetime.min.time())

                datetime_obj = datetime_obj.replace(tzinfo=timezone.utc)

                # Convert to timestamp ms
                timestamp = int(datetime_obj.timestamp() * 1000)

                entity_event = EntityEvent(EntityEventType.FIELD_UPDATE, timestamp)
                results.append(entity_event)

            return results

        raise InvalidParametersException(
            message="Missing required inputs: column path and column type.",
            parameters=parameters,
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        reraise=True,
    )
    def _try_get_entity_events(
        self,
        entity_urn: str,
        event_type: EntityEventType,
        window: List[int],
        parameters: dict,
    ) -> List[EntityEvent]:
        if event_type == EntityEventType.AUDIT_LOG_OPERATION:
            # Scan the audit log to see if there are any events falling into the previous window.
            return self._get_audit_log_operation_events(entity_urn, window, parameters)
        elif event_type == EntityEventType.INFORMATION_SCHEMA_UPDATE:
            # Hit something else!
            return self._get_dataset_last_updated_events(entity_urn, window, parameters)
        elif event_type == EntityEventType.FIELD_UPDATE:
            # Build and issue a query!
            return self._get_field_last_updated_events(entity_urn, window, parameters)
        else:
            raise InvalidSourceTypeException(
                message=f"Unsupported entity event type {event_type} provided. Redshift connector does not support retrieving these events.",
                source_type=event_type,
            )

    def get_entity_events(
        self,
        entity_urn: str,
        event_type: EntityEventType,
        window: List[int],
        parameters: dict,
    ) -> List[EntityEvent]:
        return self._try_get_entity_events(entity_urn, event_type, window, parameters)
