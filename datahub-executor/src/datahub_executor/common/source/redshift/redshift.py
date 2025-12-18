import logging
from datetime import timezone
from typing import Any, List, Optional, Union

from datahub._version import __version__

from datahub_executor.common.assertion.engine.evaluator.filter_builder import (
    FilterBuilder,
)
from datahub_executor.common.connection.redshift.redshift_connection import (
    RedshiftConnection,
)
from datahub_executor.common.exceptions import (
    InvalidParametersException,
    SourceQueryFailedException,
)
from datahub_executor.common.source.redshift.time_utils import (
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
from datahub_executor.common.types import EntityEvent, EntityEventType

from .sql.field_metrics_sql_generator import RedshiftFieldMetricsSQLGenerator
from .sql.field_values_sql_generator import RedshiftFieldValuesSQLGenerator
from .types import (
    HIGH_WATERMARK_DATE_AND_TIME_TYPES,
    SUPPORTED_HIGH_WATERMARK_COLUMN_TYPES,
    SUPPORTED_LAST_MODIFIED_COLUMN_TYPES,
)

logger = logging.getLogger(__name__)


def _add_redshift_query_tag(query: str) -> str:
    """
    Adds AWS Redshift query tagging comment to identify DataHub queries.
    Format: -- partner: DataHub -v <version>

    This is required for AWS Redshift Ready program compliance and helps
    identify DataHub traffic in Redshift query logs.
    """
    tag_comment = f"-- partner: DataHub -v {__version__}\n"
    return tag_comment + query


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
    source_name: str = "Redshift"
    field_values_sql_generator: FieldValuesSQLGenerator
    field_metrics_sql_generator: FieldMetricsSQLGenerator

    def __init__(self, connection: RedshiftConnection):
        super().__init__(connection)
        self.connection = connection
        self.field_values_sql_generator = RedshiftFieldValuesSQLGenerator()
        self.field_metrics_sql_generator = RedshiftFieldMetricsSQLGenerator()

    def _get_user_name_filter(self, parameters: dict) -> Optional[str]:
        if parameters is not None:
            if "user_name" in parameters and parameters["user_name"] is not None:
                return parameters["user_name"].lower()
        return None

    def _get_database_string(
        self, params: Union[DatabaseParams, SourceOperationParams]
    ) -> str:
        return f"{params.database}.{params.schema}.{params.table}"

    def _convert_value_for_comparison(self, column_value: str, column_type: str) -> str:
        return convert_value_for_comparison(column_value, column_type)

    def _execute_fetchall_query_internal(self, query: str) -> List[Any]:
        # Add query tagging for AWS Redshift Ready program
        tagged_query = _add_redshift_query_tag(query)

        with self.connection.get_client().cursor() as cur:
            try:
                cur.execute(tagged_query)
                return cur.fetchall()
            except Exception as e:
                try:
                    cur.execute("rollback")
                except Exception:
                    # If rollback fails, the connection will be closed anyway
                    pass
                raise SourceQueryFailedException(
                    message=f"Source query (Redshift) failed with error: {e}",
                    query=tagged_query,
                )

    def _execute_fetchone_query(self, query: str) -> List[Any]:
        # Add query tagging for AWS Redshift Ready program
        tagged_query = _add_redshift_query_tag(query)

        with self.connection.get_client().cursor() as cur:
            try:
                cur.execute(tagged_query)
                return cur.fetchone()
            except Exception as e:
                try:
                    cur.execute("rollback")
                except Exception:
                    # If rollback fails, the connection will be closed anyway
                    pass
                raise SourceQueryFailedException(
                    message=f"Source query (Redshift) failed with error: {e}",
                    query=tagged_query,
                )

    def _build_audit_log_results(self, rows: List[Any]) -> List[EntityEvent]:
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

    # Note that this method cannot effectively leverage partitions because it comes from the audit log.
    #
    # Also, notice that this path may be quite expensive since we cannot filter the STL_INSERT table
    # by the table id necessarily, and this requires multiple view-scans. In large deployments, we will need
    # additional testing to verify the performance & cost characteristics.
    def _get_audit_log_operation_events(
        self, operation_params: SourceOperationParams, parameters: dict
    ) -> List[EntityEvent]:
        # We ONLY support inserts. If you're asking for other things, we'll simply ignore it.
        if "operation_types" in parameters:
            operation_types = parameters["operation_types"]
            if operation_types is not None and "INSERT" not in operation_types:
                raise InvalidParametersException(
                    message="Found unexpected operation types requested for Redshift. Redshift only supports INSERT operation type. Adjusting.",
                    parameters=parameters,
                )

        # Compute audit log filters based on parameters
        user_name_filter = self._get_user_name_filter(parameters)

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
            WHERE si.endtime >= (TIMESTAMP 'epoch' + {operation_params.start_time_millis}/1000 * interval '1 second')
                AND si.endtime < (TIMESTAMP 'epoch' + {operation_params.end_time_millis}/1000 * interval '1 second')
                AND sq.startTime >= (TIMESTAMP 'epoch' + {operation_params.start_time_millis}/1000 * interval '1 second')
                AND sq.endtime < (TIMESTAMP 'epoch' + {operation_params.end_time_millis}/1000 * interval '1 second')
                AND sq.aborted = 0
                AND si.rows > 0
                AND sti.database = '{operation_params.database}'
                AND sti.schema = '{operation_params.schema}'
                AND sti.table = '{operation_params.table}'
                {f"AND sui.usename = '{user_name_filter}'" if user_name_filter is not None else ""}
            ORDER BY endtime DESC
            LIMIT {self.row_limit};
        """

        logger.debug(query)

        return self._build_audit_log_results(self._execute_fetchall_query(query))

    # Note that this method cannot effectively leverage partitions because it comes from the audit log
    def _get_dataset_last_updated_events(
        self, operation_params: SourceOperationParams
    ) -> List[EntityEvent]:
        logger.warning(
            "Attempted to fetch Redshift Table last updated time, but this is not currently supported for the Redshift connector. Returning no results.."
        )
        return []

    # This is the ONLY approach which allows for partition spec definition.
    # TODO: Add support for partitioning.
    def _get_field_last_updated_events(
        self, operation_params: SourceOperationParams, parameters: dict
    ) -> List[EntityEvent]:
        # Supported Column Types:
        # DATE: Represents a calendar date; doesn't include time information.
        # TIMESTAMP: Represents a date and time; doesn't include timezone information.
        # TIMESTAMPTZ: Same as TIMESTAMP, but includes timezone information.
        # Unsupported Column Types:
        # STRING, VARCHAR -> Would need to collect a date format.
        # NUMBER -> Would need to collect a units.

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

            # Convert our epoch time into datetime format that redshift can accept.
            # TODO: Support number, varchat time here as well (with format)
            start_datetime = convert_millis_to_timestamp_type(
                operation_params.start_time_millis, column_type
            )
            end_datetime = convert_millis_to_timestamp_type(
                operation_params.end_time_millis, column_type
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
        resp = self._execute_fetchone_query(get_value_query)
        return resp[0] if resp else None

    def _get_high_watermark_row_count(
        self,
        column_name: str,
        column_type: str,
        operation_params: SourceOperationParams,
        filter_sql: str,
        current_field_value: str,
    ) -> Optional[int]:
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
        resp = self._execute_fetchone_query(get_count_query)
        return int(resp[0]) if resp else None

    def _get_num_rows_via_stats_table(
        self, database_params: DatabaseParams
    ) -> Optional[int]:
        query = f"""
            SELECT "tbl_rows"
            FROM svv_table_info
            WHERE database='{database_params.database}'
            AND schema='{database_params.schema}'
            AND "table"='{database_params.table}';"""

        logger.debug(query)
        resp = self._execute_fetchone_query(query)
        return int(resp[0]) if resp else None

    def _get_num_rows_via_count(
        self, database_params: DatabaseParams, filter_sql: str
    ) -> Optional[int]:
        query = setup_row_count_query(
            self._get_database_string(database_params),
            filter_sql,
        )

        logger.debug(query)
        resp = self._execute_fetchone_query(query)
        return int(resp[0]) if resp else None
