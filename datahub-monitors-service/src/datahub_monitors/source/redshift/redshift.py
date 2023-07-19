import logging
from datetime import date, datetime, timezone
from typing import List, Optional

from datahub.utilities.urns.urn import Urn
from tenacity import retry, stop_after_attempt, wait_exponential

from datahub_monitors.connection.redshift.redshift_connection import RedshiftConnection
from datahub_monitors.exceptions import (
    InvalidParametersException,
    InvalidSourceTypeException,
    SourceQueryFailedException,
)
from datahub_monitors.source.redshift.time_utils import convert_millis_to_timestamp_type
from datahub_monitors.source.source import Source
from datahub_monitors.types import EntityEvent, EntityEventType

logger = logging.getLogger(__name__)

APPLICATION_NAME: str = "acryl_datahub_monitors"
# Note that well need to handle casing.
DEFAULT_OPERATION_TYPES_FILTER = "'INSERT', 'UPDATE', 'COPY', 'CREATE TABLE AS'"
SUPPORTED_COLUMN_TYPES = [
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP WITHOUT TIME ZONE",
    "TIMESTAMPTZ",
    "TIMESTAMP WITH TIME ZONE",
]


"""
Okay. Boy oh boy. Redshift has a lot of limitations.

About the approaches to fetching freshness: 

- Audit Log Operations: We basically have to parse chunks of query text (after putting them together manually) to determine which tables were created,
  updated, copied into, or inserted into. If we ONLY cared about inserts, we could just use the STL insert table like ingestion does (maybe start with this).
  Most likely, we'll need to start querying an exporting Audit Log table provided by Amazon to something like S3. We'd still need to parse queries there, however,
  and things like will not be real-time. (So not a great option.)

  For now, we only support checking INSERT operations using the STL_INSERT table. 

  TODO: 
  
  - look into extracting copy commit info: https://docs.aws.amazon.com/redshift/latest/dg/r_STL_LOAD_COMMITS.html
  - look into query parsing for extracting CREATE, CREATE TABLE AS, etc (for dbt) -> This means we need to batch load and query
    parse async.

- Dataset Last Updated: Not available natively in Redshift

- Query Last Updated: Query a table for the Last Updated Timestamp. We'll likely be left to do this. 
"""


class RedshiftSource(Source):
    """A source for extracting information from Redshift"""

    connection: RedshiftConnection

    def __init__(self, connection: RedshiftConnection):
        super().__init__(connection)
        self.connection = connection

    def _get_user_name_filter(self, parameters: dict) -> Optional[str]:
        if parameters is not None:
            if "user_name" in parameters and parameters["user_name"] is not None:
                return parameters["user_name"].lower()
        return None

    # Note that this method cannot effectively leverage partitions because it comes from the audit log.
    #
    # Also, notice that this path may be quite expensive since we cannot filter the STL_INSERT table
    # by the table id necessarily, and this requires multiple view-scans. In large deployments, we will need
    # additional testing to verify the performance & cost characteristics.
    def _get_audit_log_operation_events(
        self, entity_urn: str, window: List[int], parameters: dict
    ) -> List[EntityEvent]:
        # We ONLY support inserts. If you're asking for other things, we'll simply ignore it.
        if "operation_types" in parameters:
            operation_types = parameters["operation_types"]
            if "INSERT" not in operation_types:
                logger.warn(
                    "Found unexpected operation types requested for Redshift. Redshift only supports INSERT operation type. Adjusting."
                )

        # Define table and time range
        start_time_millis = window[0]
        end_time_millis = window[1]

        # Compute audit log filters based on parameters
        user_name_filter = self._get_user_name_filter(parameters)

        urn_obj = Urn.create_from_string(entity_urn)
        dataset_name = urn_obj.get_entity_id()[1].lower()

        dataset_name_parts = dataset_name.split(".")

        if len(dataset_name_parts) > 3:
            # Handle platform instance.
            dataset_name_parts = dataset_name_parts[:3]

        database = dataset_name_parts[0]
        schema = dataset_name_parts[1]
        table = dataset_name_parts[2]

        # Formulate query for inserts. This might need to change for creates.
        query = f"""
            SELECT
                sq.querytxt AS query,
                sui.usename AS username,
                si.endtime AS endtime
            FROM stl_insert si
                JOIN svv_table_info sti ON si.tbl = sti.table_id
                JOIN stl_query sq ON si.query = sq.query
                JOIN svl_user_info sui ON sq.userid = sui.usesysid
            WHERE si.endtime >= (TIMESTAMP 'epoch' + {start_time_millis}/1000 * interval '1 second')
                AND si.endtime < (TIMESTAMP 'epoch' + {end_time_millis}/1000 * interval '1 second')
                AND sq.startTime >= (TIMESTAMP 'epoch' + {start_time_millis}/1000 * interval '1 second')
                AND sq.endtime < (TIMESTAMP 'epoch' + {end_time_millis}/1000 * interval '1 second')
                AND sq.aborted = 0
                AND si.rows > 0
                AND sti.database = '{database}'
                AND sti.schema = '{schema}'
                AND sti.table = '{table}'
                {f"AND sui.usename = '{user_name_filter}'" if user_name_filter is not None else ''}
            ORDER BY endtime DESC;
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
            # Build results!
            datetime = row[2]
            datetime = datetime.replace(tzinfo=timezone.utc)

            # Convert to timestamp ms
            timestamp = int(datetime.timestamp() * 1000)
            entity_event = EntityEvent(EntityEventType.AUDIT_LOG_OPERATION, timestamp)
            results.append(entity_event)

        return results

    # Note that this method cannot effectively leverage partitions because it comes from the audit log
    def _get_dataset_last_updated_events(
        self, entity_urn: str, window: List[int], parameters: dict
    ) -> List[EntityEvent]:
        logger.warn(
            "Attempted to fetch Redshift Table last updated time, but this is not currently supported for the Redshift connector. Returning no results.."
        )
        return []

    # This is the ONLY approach which allows for partition spec definition.
    # TODO: Add support for partitioning.
    def _get_field_last_updated_events(
        self, entity_urn: str, window: List[int], parameters: dict
    ) -> List[EntityEvent]:
        # Supported Column Types:
        # DATE: Represents a calendar date; doesn't include time information.
        # TIMESTAMP: Represents a date and time; doesn't include timezone information.
        # TIMESTAMPTZ: Same as TIMESTAMP, but includes timezone information.
        # Unsupported Column Types:
        # STRING, VARCHAR -> Would need to collect a date format.
        # NUMBER -> Would need to collect a units.

        # Define table and time range
        start_time_millis = window[0]
        end_time_millis = window[1]

        # Compute audit log filters based on parameters
        self._get_user_name_filter(parameters)

        urn_obj = Urn.create_from_string(entity_urn)
        dataset_name = urn_obj.get_entity_id()[1].lower()

        dataset_name_parts = dataset_name.split(".")

        if len(dataset_name_parts) > 3:
            # Handle platform instance.
            dataset_name_parts = dataset_name_parts[:3]

        database = dataset_name_parts[0]
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
                raise Exception(
                    f"Unsupported date column type {column_type} provided. Failing assertion evaluation!"
                )

            # Convert our epoch time into datetime format that redshift can accept.
            # TODO: Support number, varchat time here as well (with format)
            start_datetime = convert_millis_to_timestamp_type(
                start_time_millis, column_type
            )
            end_datetime = convert_millis_to_timestamp_type(
                end_time_millis, column_type
            )

            # The goal is to basically extract the high watermark for the column identified here.
            query = f"""
                SELECT {date_column} as last_altered_date
                FROM {database}.{schema}.{table}
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
                # Build results!
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
