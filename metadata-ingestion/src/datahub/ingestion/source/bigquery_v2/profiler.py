import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, cast

from dateutil.relativedelta import relativedelta

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    RANGE_PARTITION_NAME,
    BigqueryColumn,
    BigqueryTable,
)
from datahub.ingestion.source.sql.sql_generic import BaseTable
from datahub.ingestion.source.sql.sql_generic_profiler import (
    GenericProfiler,
    TableProfilerRequest,
)
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler

logger = logging.getLogger(__name__)


class BigQueryPartitionTransformer:
    @classmethod
    def transform_select_query(cls, query: str, execution_engine_dict: dict) -> str:
        """
        Transform select queries to include partition filters when needed
        """
        if not execution_engine_dict.get("partition_handling"):
            return query

        project = execution_engine_dict.get("project")
        dataset = execution_engine_dict.get("dataset")
        if not (project and dataset):
            return query

        table = execution_engine_dict.get("table")
        partition_profiler = execution_engine_dict.get("partition_profiler")

        if table and partition_profiler:
            return partition_profiler.wrap_query_for_partition_table(
                table, query, project, dataset
            )

        return query


class BigqueryProfiler(GenericProfiler):
    config: BigQueryV2Config
    report: BigQueryV2Report

    def __init__(
        self,
        config: BigQueryV2Config,
        report: BigQueryV2Report,
        state_handler: Optional[ProfilingHandler] = None,
    ) -> None:
        super().__init__(config, report, "bigquery", state_handler)
        self.config = config
        self.report = report

    @staticmethod
    def get_partition_range_from_partition_id(
        partition_id: str, partition_datetime: Optional[datetime]
    ) -> Tuple[datetime, datetime]:
        partition_range_map: Dict[int, Tuple[relativedelta, str]] = {
            4: (relativedelta(years=1), "%Y"),
            6: (relativedelta(months=1), "%Y%m"),
            8: (relativedelta(days=1), "%Y%m%d"),
            10: (relativedelta(hours=1), "%Y%m%d%H"),
        }

        duration: relativedelta
        if partition_range_map.get(len(partition_id)):
            (delta, format) = partition_range_map[len(partition_id)]
            duration = delta
            if not partition_datetime:
                partition_datetime = datetime.strptime(partition_id, format)
            else:
                partition_datetime = datetime.strptime(
                    partition_datetime.strftime(format), format
                )

        else:
            raise ValueError(
                f"check your partition_id {partition_id}. It must be yearly/monthly/daily/hourly."
            )
        upper_bound_partition_datetime = partition_datetime + duration
        return partition_datetime, upper_bound_partition_datetime

    def _handle_range_partition(
        self,
        table: BigqueryTable,
        partition: str,
        project: str,
        schema: str,
    ) -> Optional[List[str]]:
        """Handle range partition type, maintaining exact logic from original."""
        assert table.partition_info is not None, (
            "partition_info should not be None here"
        )

        if table.partition_info.partition_column:
            return [f"{table.partition_info.partition_column.name} >= {partition}"]
        else:
            logger.warning(f"Partitioned table {table.name} without partition column")
            self.report.profiling_skipped_invalid_partition_ids[
                f"{project}.{schema}.{table.name}"
            ] = partition
            return None

    def _handle_time_partition(
        self,
        table: BigqueryTable,
        partition: str,
        partition_datetime: Optional[datetime],
        project: str,
        schema: str,
    ) -> Optional[List[str]]:
        """Handle time-based partition, maintaining exact logic from original."""
        assert table.partition_info is not None, (
            "partition_info should not be None here"
        )

        logger.debug(f"{table.name} is partitioned and partition column is {partition}")
        try:
            (
                partition_datetime_value,
                upper_bound_partition_datetime,
            ) = self.get_partition_range_from_partition_id(
                partition, partition_datetime
            )
        except ValueError as e:
            logger.error(
                f"Unable to get partition range for partition id: {partition} it failed with exception {e}"
            )
            self.report.profiling_skipped_invalid_partition_ids[
                f"{project}.{schema}.{table.name}"
            ] = partition
            return None

        partition_data_type: str = "TIMESTAMP"
        partition_column_name = "_PARTITIONTIME"
        if table.partition_info.partition_column:
            partition_column_name = table.partition_info.partition_column.name
            partition_data_type = table.partition_info.partition_column.data_type

        if table.partition_info.type in ("HOUR", "DAY", "MONTH", "YEAR"):
            return [
                f"`{partition_column_name}` BETWEEN {partition_data_type}('{partition_datetime_value}') "
                f"AND {partition_data_type}('{upper_bound_partition_datetime}')"
            ]
        else:
            logger.warning(f"Not supported partition type {table.partition_info.type}")
            self.report.profiling_skipped_invalid_partition_type[
                f"{project}.{schema}.{table.name}"
            ] = table.partition_info.type
            return None

    def _handle_column_partition(
        self,
        field: str,
        column: Optional[BigqueryColumn],
        partition_datetime_value: datetime,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> Optional[str]:
        """Handle individual column partition, maintaining exact logic from original."""
        if not column:
            logger.warning(
                f"Partitioned table {table.name} missing column info for {field}"
            )
            self.report.profiling_skipped_invalid_partition_ids[
                f"{project}.{schema}.{table.name}"
            ] = field
            return None

        if field == "year":
            value = partition_datetime_value.year
            return f"`{column.name}` = {value}"
        elif field == "month":
            value = partition_datetime_value.month
            return f"`{column.name}` = {value}"
        elif field == "day":
            value = partition_datetime_value.day
            return f"`{column.name}` = {value}"
        elif column.data_type in (
            "STRING",
            "VARCHAR",
            "NVARCHAR",
            "CHAR",
            "NCHAR",
        ):
            return f"`{column.name}` = '{str(partition_datetime_value)}'"
        elif column.data_type in (
            "INT64",
            "INT",
            "INTEGER",
            "SMALLINT",
            "INT32",
            "BIGINT",
            "TINYINT",
            "BYTEINT",
        ):
            # For integer types, use the timestamp value like in the original code
            return f"`{column.name}` = {int(partition_datetime_value.timestamp())}"
        elif column.data_type in (
            "FLOAT64",
            "FLOAT",
            "DOUBLE",
            "REAL",
            "NUMERIC",
            "DECIMAL",
            "BIGNUMERIC",
            "BIGDECIMAL",
        ):
            return f"`{column.name}` = {float(partition_datetime_value.timestamp())}"
        elif column.data_type in (
            "TIMESTAMP",
            "TIMESTAMP_LTZ",
            "TIMESTAMP_NTZ",
        ):
            return f"`{column.name}` = TIMESTAMP('{partition_datetime_value}')"
        elif column.data_type == "DATETIME":
            return f"`{column.name}` = DATETIME('{partition_datetime_value}')"
        elif column.data_type == "DATE":
            return f"`{column.name}` = DATE('{partition_datetime_value.date()}')"
        elif column.data_type == "TIME":
            return f"`{column.name}` = TIME('{partition_datetime_value.time()}')"
        elif column.data_type in ("BOOL", "BOOLEAN"):
            return f"`{column.name}` = TRUE"
        elif column.data_type == "BYTES":
            return f"`{column.name}` = B'{str(partition_datetime_value)}'"
        elif column.data_type == "ARRAY":
            logger.warning(f"Array type not supported for partitioning: {column.name}")
            return None
        elif column.data_type == "STRUCT":
            logger.warning(f"Struct type not supported for partitioning: {column.name}")
            return None
        elif column.data_type == "GEOGRAPHY":
            logger.warning(
                f"Geography type might not be suitable for partitioning: {column.name}"
            )
            return f"`{column.name}` = ST_GEOGPOINT(0, 0)"
        elif column.data_type == "JSON":
            return f"`{column.name}` = JSON('{{\"default\": true}}')"
        elif column.data_type == "INTERVAL":
            logger.warning(
                f"Interval type might not be suitable for partitioning: {column.name}"
            )
            return None
        else:
            logger.warning(
                f"Unknown partition column type {column.data_type} for {column.name}"
            )
            return f"`{column.name}` = {column.data_type}('{partition_datetime_value}')"

    def _handle_multi_column_partitioning(
        self,
        table: BigqueryTable,
        partition_datetime_value: datetime,
        project: str,
        schema: str,
    ) -> Optional[List[str]]:
        """Handle multi-column partitioning, maintaining exact logic from original."""
        assert table.partition_info is not None, (
            "partition_info should not be None here"
        )

        partition_where_clauses = []
        for field, column in zip(
            table.partition_info.fields, table.partition_info.columns or []
        ):
            clause = self._handle_column_partition(
                field, column, partition_datetime_value, table, project, schema
            )
            if clause is None:
                return None
            partition_where_clauses.append(clause)
        return partition_where_clauses

    def generate_partition_profiler_query(
        self,
        project: str,
        schema: str,
        table: BigqueryTable,
        partition_datetime: Optional[datetime] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Method returns partition id if table is partitioned or sharded and generate custom partition query for
        partitioned table.
        See more about partitioned tables at https://cloud.google.com/bigquery/docs/partitioned-tables
        """
        logger.debug(
            f"generate partition profiler query for project: {project} schema: {schema} and table {table.name}, "
            f"partition_datetime: {partition_datetime}"
        )

        partition = table.max_partition_id

        # First try to get multi-partition filters for tables that require partition elimination
        multi_partition_filters = self._generate_multi_partition_filter(
            table, project, schema
        )
        if multi_partition_filters:
            where_clause = " AND ".join(multi_partition_filters)
            partition = "multi"  # Use a marker to indicate we're using multi-partition filtering
        elif table.partition_info and partition:
            partition_where_clauses: List[str] = []

            # Handle legacy single column partitioning
            if isinstance(table.partition_info.partition_field, str):
                if table.partition_info.type == RANGE_PARTITION_NAME:
                    clauses = self._handle_range_partition(
                        table, partition, project, schema
                    )
                    if clauses is None:
                        return None, None
                    partition_where_clauses.extend(clauses)
                else:
                    clauses = self._handle_time_partition(
                        table, partition, partition_datetime, project, schema
                    )
                    if clauses is None:
                        return None, None
                    partition_where_clauses.extend(clauses)

            # Handle multiple partition columns
            elif isinstance(table.partition_info.fields, list):
                partition_datetime_value = partition_datetime or datetime.now(
                    timezone.utc
                )
                clauses = self._handle_multi_column_partitioning(
                    table, partition_datetime_value, project, schema
                )
                if clauses is None:
                    return None, None
                partition_where_clauses.extend(clauses)

            if not partition_where_clauses:
                return None, None

            where_clause = " AND ".join(partition_where_clauses)
        elif table.max_shard_id:
            # For sharded table we want to get the partition id but not needed to generate custom query
            return table.max_shard_id, None
        else:
            return None, None

        # Generate the appropriate query using the where clause
        if table.external:
            custom_sql = f"""
WITH partitioned_data AS (
    SELECT * 
    FROM `{project}.{schema}.{table.name}`
    WHERE {where_clause}
)
SELECT * FROM partitioned_data"""
        else:
            custom_sql = f"""
SELECT *
FROM `{project}.{schema}.{table.name}`
WHERE {where_clause}"""

        return partition, custom_sql.strip()

    def _generate_multi_partition_filter(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> Optional[List[str]]:
        """
        Generate partition filters for tables with multiple partition columns.
        For tables that require partition elimination, we'll filter on the most recent partition.
        """
        if not table.partition_info or not table.partition_info.columns:
            return None

        current_time = datetime.now(timezone.utc)
        partition_filters = []

        for column in table.partition_info.columns:
            if not column:
                continue

            # Handle different partition column types
            if column.name.lower() == "year":
                partition_filters.append(f"`{column.name}` = {current_time.year}")
            elif column.name.lower() == "month":
                partition_filters.append(f"`{column.name}` = {current_time.month}")
            elif column.name.lower() == "day":
                partition_filters.append(f"`{column.name}` = {current_time.day}")
            elif column.name.lower() in ("timestamp", "date", "datetime", "time"):
                partition_filters.append(
                    f"`{column.name}` = {column.data_type}('{current_time}')"
                )
            else:
                # For other types of partition columns, we'll need the max value
                try:
                    # Try to get the maximum value for this partition column
                    max_value_query = f"""
                    SELECT MAX({column.name}) as max_value
                    FROM `{project}.{schema}.{table.name}`
                    """
                    query_job = self.config.get_bigquery_client().query(max_value_query)
                    results = list(query_job)  # Convert iterator to list
                    if results and results[0].max_value is not None:
                        max_value = results[0].max_value
                        partition_filters.append(f"`{column.name}` = '{max_value}'")
                    else:
                        logger.warning(
                            f"No valid max value found for partition column {column.name}"
                        )
                        return None
                except Exception as e:
                    logger.warning(
                        f"Unable to get max value for partition column {column.name}: {e}"
                    )
                    return None

        return partition_filters if partition_filters else None

    def get_workunits(
        self, project_id: str, tables: Dict[str, List[BigqueryTable]]
    ) -> Iterable[MetadataWorkUnit]:
        profile_requests: List[TableProfilerRequest] = []

        for dataset in tables:
            for table in tables[dataset]:
                normalized_table_name = BigqueryTableIdentifier(
                    project_id=project_id, dataset=dataset, table=table.name
                ).get_table_name()

                if table.external and not self.config.profiling.profile_external_tables:
                    self.report.profiling_skipped_other[f"{project_id}.{dataset}"] += 1
                    logger.info(
                        f"Skipping profiling of external table {project_id}.{dataset}.{table.name}"
                    )
                    continue

                # Emit the profile work unit
                logger.debug(
                    f"Creating profile request for table {normalized_table_name}"
                )
                profile_request = self.get_profile_request(table, dataset, project_id)
                if profile_request is not None:
                    self.report.report_entity_profiled(profile_request.pretty_name)
                    profile_requests.append(profile_request)
                else:
                    logger.debug(
                        f"Table {normalized_table_name} was not eliagible for profiling."
                    )

        if len(profile_requests) == 0:
            return
        yield from self.generate_profile_workunits(
            profile_requests,
            max_workers=self.config.profiling.max_workers,
            platform=self.platform,
            profiler_args=self.get_profile_args(),
        )

    def get_profile_args(self) -> Dict[str, Any]:
        """
        Override to add partition-aware query generation and query transformation
        """
        profiler_args = super().get_profile_args()
        profiler_args["partition_profiler"] = self
        profiler_args["query_transformer"] = (
            BigQueryPartitionTransformer.transform_select_query
        )
        profiler_args["table"] = None
        return profiler_args

    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        return BigqueryTableIdentifier(
            project_id=db_name, dataset=schema_name, table=table_name
        ).get_table_name()

    def get_batch_kwargs(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> dict:
        """
        Override to handle partition-aware querying
        """
        bq_table = cast(BigqueryTable, table)
        batch_kwargs = dict(
            schema=db_name,  # <project>
            table=f"{schema_name}.{table.name}",  # <dataset>.<table>
        )

        if bq_table.external or bq_table.partition_info:
            batch_kwargs.update(
                {
                    "partition_handling": "true",  # Make this a string
                    "project": db_name,
                    "dataset": schema_name,
                    "table_name": bq_table.name,  # Use table name instead of table object
                }
            )

        return batch_kwargs

    def wrap_query_for_partition_table(
        self,
        table: BigqueryTable,
        base_query: str,
        project: str,
        schema: str,
    ) -> str:
        """
        Helper method to wrap a base query with partition filters
        Ensures all required partition columns have appropriate filters by getting max values.
        """
        current_time = datetime.now(timezone.utc)
        partition_filters = []

        # Get BQ client for querying partition values
        bq_client = self.config.get_bigquery_client()

        # Handle required partition columns
        if hasattr(table, "columns") and table.columns:
            # Get all partition columns
            partition_columns = [
                col for col in table.columns if col.is_partition_column
            ]

            # Also check partition info columns if available
            if (
                table.partition_info
                and hasattr(table.partition_info, "columns")
                and table.partition_info.columns
            ):
                partition_columns.extend(
                    col
                    for col in table.partition_info.columns
                    if col not in partition_columns
                )

            # Add filters for each partition column
            for col in partition_columns:
                col_name_lower = col.name.lower()

                # Handle common time-based partition columns
                if col_name_lower == "year":
                    partition_filters.append(f"`{col.name}` = {current_time.year}")
                elif col_name_lower == "month":
                    partition_filters.append(f"`{col.name}` = {current_time.month}")
                elif col_name_lower == "day":
                    partition_filters.append(f"`{col.name}` = {current_time.day}")
                elif col.data_type and col.data_type.lower() in (
                    "timestamp",
                    "date",
                    "datetime",
                ):
                    partition_filters.append(
                        f"`{col.name}` = {col.data_type}('{current_time}')"
                    )
                else:
                    # For any other partition column, we need to get a specific value
                    try:
                        # Query for the max value to use in partition filter
                        query = f"""
                        SELECT MAX({col.name}) as max_val 
                        FROM `{project}.{schema}.{table.name}`
                        """
                        query_job = bq_client.query(query)
                        results = list(query_job)

                        if not results or results[0].max_val is None:
                            logger.warning(
                                f"No values found for partition column {col.name}"
                            )
                            return base_query

                        max_val = results[0].max_val

                        # Handle different data types
                        if isinstance(max_val, (int, float)):
                            partition_filters.append(f"`{col.name}` = {max_val}")
                        else:
                            partition_filters.append(f"`{col.name}` = '{max_val}'")

                    except Exception as e:
                        logger.error(
                            f"Error getting partition value for {col.name}: {e}"
                        )
                        return base_query

        if not partition_filters:
            return base_query

        # Inject the partition filters into the base query
        where_clause = " AND ".join(partition_filters)
        logger.debug(f"Adding partition filters: {where_clause}")

        if " WHERE " in base_query.upper():
            modified_query = base_query.replace(
                " WHERE ", f" WHERE {where_clause} AND ", 1
            )
        else:
            modified_query = f"{base_query} WHERE {where_clause}"

        return modified_query

    def get_profile_request(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> Optional[TableProfilerRequest]:
        profile_request = super().get_profile_request(table, schema_name, db_name)

        if not profile_request:
            return None

        # Below code handles profiling changes required for partitioned or sharded tables
        # 1. Skip profile if partition profiling is disabled.
        # 2. Else update `profile_request.batch_kwargs` with partition and custom_sql

        bq_table = cast(BigqueryTable, table)

        # Handle external table checks first
        if bq_table.external and not self.config.profiling.profile_external_tables:
            self.report.profiling_skipped_other[f"{db_name}.{schema_name}"] += 1
            logger.info(
                f"Skipping profiling of external table {db_name}.{schema_name}.{table.name}"
            )
            return None

        (partition, custom_sql) = self.generate_partition_profiler_query(
            db_name, schema_name, bq_table, self.config.profiling.partition_datetime
        )

        if partition is None and bq_table.partition_info:
            self.report.report_warning(
                title="Profile skipped for partitioned table",
                message="profile skipped as partitioned table is empty or partition id or type was invalid",
                context=profile_request.pretty_name,
            )
            return None

        if (
            partition is not None
            and not self.config.profiling.partition_profiling_enabled
        ):
            logger.debug(
                f"{profile_request.pretty_name} and partition {partition} is skipped because profiling.partition_profiling_enabled property is disabled"
            )
            self.report.profiling_skipped_partition_profiling_disabled.append(
                profile_request.pretty_name
            )
            return None

        if partition:
            logger.debug("Updating profiling request for partitioned/sharded tables")
            kwargs_update = {
                "custom_sql": custom_sql,
                "partition": partition,
            }

            # Add external table metadata if needed
            if bq_table.external:
                kwargs_update.update(
                    {
                        "external_table": "true",  # Use string instead of bool
                        "external_partition": "true",  # Use string instead of bool
                    }
                )

            profile_request.batch_kwargs.update(kwargs_update)

        return profile_request
