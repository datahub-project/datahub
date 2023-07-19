import logging
from datetime import date, datetime, timezone
from typing import Any, Iterable, List, Optional, Tuple

import pytz
from datahub.ingestion.source.bigquery_v2.common import (
    BQ_DATETIME_FORMAT,
    _make_gcp_logging_client,
)
from datahub.utilities.urns.urn import Urn
from tenacity import retry, stop_after_attempt, wait_exponential

from datahub_monitors.connection.bigquery.bigquery_connection import BigQueryConnection
from datahub_monitors.exceptions import (
    InvalidParametersException,
    InvalidSourceTypeException,
    SourceQueryFailedException,
)
from datahub_monitors.source.bigquery.time_utils import convert_millis_to_timestamp_type
from datahub_monitors.source.source import Source
from datahub_monitors.types import EntityEvent, EntityEventType

logger = logging.getLogger(__name__)

APPLICATION_NAME: str = "acryl_datahub_monitors"
DEFAULT_OPERATION_TYPES_FILTER = '"INSERT" OR "UPDATE" OR "CREATE_TABLE" OR "CREATE_TABLE_AS_SELECT" OR "CREATE_EXTERNAL_TABLE" OR "CREATE_SNAPSHOT_TABLE"'  # Note that Alter is not included :)
SUPPORTED_COLUMN_TYPES = ["DATE", "DATETIME", "TIMESTAMP"]


# Note that we only support Email address for username filter inside of this source!
class BigQuerySource(Source):
    """A source for extracting information from BigQuery"""

    connection: BigQueryConnection

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

    # TODO: Support EXPORTED Audit logs
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

        urn_obj = Urn.create_from_string(entity_urn)
        dataset_name = urn_obj.get_entity_id()[1].lower()

        dataset_name_parts = dataset_name.split(".")

        if len(dataset_name_parts) > 3:
            # Handle platform instance.
            dataset_name_parts = dataset_name_parts[:3]

        project = dataset_name_parts[0]
        dataset = dataset_name_parts[1]
        table = dataset_name_parts[2]

        logging_client = _make_gcp_logging_client(
            project, self.connection.config.extra_client_options
        )
        entries = self._extract_audit_logs_for_table(
            logging_client,
            project,
            dataset,
            table,
            start_time_millis,
            end_time_millis,
            parameters,
        )

        results = []

        for entry in entries:
            # Extract the correct rows.
            logger.debug(entry)

            # operation_type = entry[1].payload["metadata"]["jobChange"]["job"]["jobConfig"]["queryConfig"]["statementType"]
            timestamp = entry[1].timestamp

            # TODO: Determine whether we want to produce an operation or anything as part of this!

            entity_event = EntityEvent(
                EntityEventType.AUDIT_LOG_OPERATION, int(timestamp.timestamp() * 1000)
            )
            results.append(entity_event)

        return results

    # Note that this method cannot effectively leverage partitions because it comes from the audit log
    def _get_dataset_last_updated_events(
        self, entity_urn: str, window: List[int], parameters: dict
    ) -> List[EntityEvent]:
        # Define table and time range
        start_time_millis = window[0]
        end_time_millis = window[1]

        urn_obj = Urn.create_from_string(entity_urn)
        dataset_name = urn_obj.get_entity_id()[1].lower()
        dataset_name_parts = dataset_name.split(".")

        if len(dataset_name_parts) > 3:
            # Handle platform instance.
            dataset_name_parts = dataset_name_parts[:3]

        project = dataset_name_parts[0]
        dataset = dataset_name_parts[1]
        table = dataset_name_parts[2]

        # Compares using UTC date.
        query = f"""
            SELECT last_modified_time
            FROM `{project}.{dataset}.__TABLES__`
            WHERE table_id="{table}"
                AND last_modified_time >= {start_time_millis}
                AND last_modified_time <= {end_time_millis}
        ;"""

        logger.debug(query)

        try:
            query_job = self.connection.get_client().query(query)  # API request
        except Exception as e:
            raise SourceQueryFailedException(
                message=f"Query failed with error: {e}", query=query
            )

        results = []

        # Iterate over the result
        for row in query_job:
            # Build results!
            timestamp = row[0]
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
        # TODO: Verify that every single type here works.
        # DATE: Represents a logical calendar date. The format is YYYY-[M]M-[D]D.
        # DATETIME: Represents a year, month, day, hour, minute, second, and subsecond. The format is YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]].
        # TIMESTAMP: Represents an absolute point in time with millisecond precision. The format is YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]][time zone].

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

        project = dataset_name_parts[0]
        dataset = dataset_name_parts[1]
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

            start_datetime = convert_millis_to_timestamp_type(
                start_time_millis, column_type
            )
            end_datetime = convert_millis_to_timestamp_type(
                end_time_millis, column_type
            )

            # Compares using UTC date.
            query = f"""
                SELECT {date_column} as last_altered_date
                FROM {project}.{dataset}.{table}
                WHERE {date_column} >= ({start_datetime})
                AND {date_column} <= ({end_datetime})
                ORDER BY {date_column} DESC
            ;"""

            try:
                query_job = self.connection.get_client().query(query)  # API request
            except Exception as e:
                raise SourceQueryFailedException(
                    message=f"Query failed with error: {e}", query=query
                )

            results = []

            # Iterate over the result
            for row in query_job:
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

    def get_current_high_watermark_for_column(
        self,
        entity_urn: str,
        event_type: EntityEventType,
        window: List[int],
        parameters: dict,
        previous_value: Optional[str],
    ) -> Tuple[str, int]:
        raise Exception("Not implemented")
