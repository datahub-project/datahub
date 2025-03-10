import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple, cast

from dateutil.relativedelta import relativedelta

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryTable,
)
from datahub.ingestion.source.sql.sql_generic import BaseTable
from datahub.ingestion.source.sql.sql_generic_profiler import (
    GenericProfiler,
    TableProfilerRequest,
)
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler

logger = logging.getLogger(__name__)


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

    def _get_external_table_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        current_time: datetime,
    ) -> Optional[List[str]]:
        """Get partition filters specifically for external tables.

        Args:
            table: BigqueryTable instance containing table metadata
            project: The BigQuery project ID
            schema: The dataset/schema name
            current_time: Current UTC datetime

        Returns:
            List of partition filter strings if partition columns found and filters could be constructed
            Empty list if no partitions found
            None if partition filters could not be determined
        """
        try:
            # For external tables, we need to check specifically for partitioning columns
            # and also look at the DDL if available to detect hive-style partitioning

            # First, try to get partition columns directly from INFORMATION_SCHEMA
            query = f"""SELECT column_name, data_type
FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table.name}' AND is_partitioning_column = 'YES'"""
            query_job = self.config.get_bigquery_client().query(query)
            results = list(query_job)

            partition_cols_with_types = {
                row.column_name: row.data_type for row in results
            }

            # If we didn't find any partition columns through INFORMATION_SCHEMA,
            # check the DDL for external table declarations that have partition info
            if not partition_cols_with_types and table.ddl:
                # Very simple DDL parsing to look for PARTITION BY statements
                if "PARTITION BY" in table.ddl.upper():
                    ddl_lines = table.ddl.upper().split("\n")
                    for line in ddl_lines:
                        if "PARTITION BY" in line:
                            # Look for column names mentioned in the PARTITION BY clause
                            # This is a basic extraction and may need enhancement for complex DDLs
                            parts = (
                                line.split("PARTITION BY")[1]
                                .split("OPTIONS")[0]
                                .strip()
                            )
                            potential_cols = [
                                col.strip(", `()") for col in parts.split()
                            ]

                            # Get all columns to check data types for potential partition columns
                            all_cols_query = f"""SELECT column_name, data_type
FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table.name}'"""
                            all_cols_job = self.config.get_bigquery_client().query(
                                all_cols_query
                            )
                            all_cols_results = list(all_cols_job)
                            all_cols_dict = {
                                row.column_name.upper(): row.data_type
                                for row in all_cols_results
                            }

                            # Add potential partition columns with their types
                            for col in potential_cols:
                                if col in all_cols_dict:
                                    partition_cols_with_types[col] = all_cols_dict[col]

            partition_filters = []

            # Process all identified partition columns
            for col_name, data_type in partition_cols_with_types.items():
                logger.debug(
                    f"Processing external table partition column: {col_name} with type {data_type}"
                )

                # For each partition column, we need to find a valid value
                query = f"""
WITH PartitionStats AS (
    SELECT {col_name} as val,
           COUNT(*) as record_count
    FROM `{project}.{schema}.{table.name}`
    WHERE {col_name} IS NOT NULL
    GROUP BY {col_name}
    HAVING record_count > 0
    ORDER BY {col_name} DESC
    LIMIT 1
)
SELECT val, record_count
FROM PartitionStats"""

                try:
                    query_job = self.config.get_bigquery_client().query(query)
                    results = list(query_job.result(timeout=30))

                    if not results or results[0].val is None:
                        logger.warning(
                            f"No non-empty partition values found for column {col_name}"
                        )
                        continue

                    val = results[0].val
                    record_count = results[0].record_count
                    logger.info(
                        f"Selected external partition {col_name}={val} with {record_count} records"
                    )

                    # Format the filter based on the data type
                    data_type_upper = data_type.upper() if data_type else ""
                    if data_type_upper in ("STRING", "VARCHAR"):
                        partition_filters.append(f"`{col_name}` = '{val}'")
                    elif data_type_upper == "DATE":
                        partition_filters.append(f"`{col_name}` = DATE '{val}'")
                    elif data_type_upper in ("TIMESTAMP", "DATETIME"):
                        if isinstance(val, datetime):
                            partition_filters.append(
                                f"`{col_name}` = TIMESTAMP '{val.strftime('%Y-%m-%d %H:%M:%S')}'"
                            )
                        else:
                            partition_filters.append(
                                f"`{col_name}` = TIMESTAMP '{val}'"
                            )
                    else:
                        # Default to numeric or other type
                        partition_filters.append(f"`{col_name}` = {val}")

                except Exception as e:
                    logger.warning(
                        f"Error determining value for partition column {col_name}: {e}"
                    )
                    continue

            return partition_filters

        except Exception as e:
            logger.error(f"Error checking external table partitioning: {e}")
            return None

    # Add this method to improve detection of partition columns from INFORMATION_SCHEMA if not found in partition_info
    def _get_required_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> Optional[List[str]]:
        """Get partition filters for all required partition columns.

        Args:
            table: BigqueryTable instance containing table metadata
            project: The BigQuery project ID
            schema: The dataset/schema name

        Returns:
            List of partition filter strings if partition columns found and filters could be constructed
            Empty list if no partitions found
            None if partition filters could not be determined
        """
        current_time = datetime.now(timezone.utc)
        partition_filters = []

        # Get required partition columns from table info
        required_partition_columns = set()

        # Get from explicit partition_info if available
        if table.partition_info:
            if isinstance(table.partition_info.fields, list):
                required_partition_columns.update(table.partition_info.fields)

            if table.partition_info.columns:
                required_partition_columns.update(
                    col.name for col in table.partition_info.columns if col
                )

        # If no partition columns found from partition_info, query INFORMATION_SCHEMA
        if not required_partition_columns:
            try:
                query = f"""SELECT column_name
FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table.name}' AND is_partitioning_column = 'YES'"""
                query_job = self.config.get_bigquery_client().query(query)
                results = list(query_job)
                required_partition_columns = {row.column_name for row in results}
                logger.debug(
                    f"Found partition columns from schema: {required_partition_columns}"
                )
            except Exception as e:
                logger.error(f"Error querying partition columns: {e}")

        # If still no partition columns found, check for external table partitioning
        if not required_partition_columns:
            logger.debug(f"No partition columns found for table {table.name}")
            if table.external:
                return self._get_external_table_partition_filters(
                    table, project, schema, current_time
                )
            else:
                return None

        logger.debug(f"Required partition columns: {required_partition_columns}")

        # Get column data types to handle casting correctly
        column_data_types = {}
        try:
            query = f"""SELECT column_name, data_type
FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table.name}'"""
            query_job = self.config.get_bigquery_client().query(query)
            results = list(query_job)
            column_data_types = {row.column_name: row.data_type for row in results}
        except Exception as e:
            logger.error(f"Error fetching column data types: {e}")

        # Handle standard time-based columns without querying
        standard_time_columns = {"year", "month", "day", "hour"}
        time_based_columns = {
            col
            for col in required_partition_columns
            if col.lower() in standard_time_columns
        }
        other_columns = required_partition_columns - time_based_columns

        for col_name in time_based_columns:
            col_name_lower = col_name.lower()
            col_data_type = column_data_types.get(col_name, "STRING")

            if col_name_lower == "year":
                value = current_time.year
            elif col_name_lower == "month":
                value = current_time.month
            elif col_name_lower == "day":
                value = current_time.day
            elif col_name_lower == "hour":
                value = current_time.hour
            else:
                continue

            # Handle casting based on column type
            if col_data_type.upper() in {"STRING"}:
                partition_filters.append(f"`{col_name}` = '{value}'")
            else:
                partition_filters.append(f"`{col_name}` = {value}")

        # Fetch latest value for non-time-based partitions
        for col_name in other_columns:
            try:
                # Query to get latest non-empty partition
                query = f"""WITH PartitionStats AS (
    SELECT {col_name} as val,
           COUNT(*) as record_count
    FROM `{project}.{schema}.{table.name}`
    WHERE {col_name} IS NOT NULL
    GROUP BY {col_name}
    HAVING record_count > 0
    ORDER BY {col_name} DESC
    LIMIT 1
)
SELECT val, record_count
FROM PartitionStats"""
                logger.debug(f"Executing query for partition value: {query}")

                query_job = self.config.get_bigquery_client().query(query)
                results = list(query_job.result(timeout=30))

                if not results or results[0].val is None:
                    logger.warning(
                        f"No non-empty partition values found for column {col_name}"
                    )
                    continue

                val = results[0].val
                record_count = results[0].record_count
                logger.info(
                    f"Selected partition {col_name}={val} with {record_count} records"
                )

                if isinstance(val, (int, float)):
                    partition_filters.append(f"`{col_name}` = {val}")
                else:
                    partition_filters.append(f"`{col_name}` = '{val}'")

            except Exception as e:
                logger.error(f"Error getting partition value for {col_name}: {e}")

        logger.debug(f"Final partition filters: {partition_filters}")
        return partition_filters if partition_filters else None

    def get_batch_kwargs(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> dict:
        """Handle partition-aware querying for all operations including COUNT."""
        bq_table = cast(BigqueryTable, table)
        base_kwargs = {
            "schema": db_name,  # <project>
            "table": f"{schema_name}.{table.name}",  # <dataset>.<table>
            "project": db_name,
            "dataset": schema_name,
            "table_name": bq_table.name,
        }

        # Different handling path for external tables vs native tables
        if bq_table.external:
            logger.info(f"Processing external table: {bq_table.name}")
            partition_filters = self._get_external_table_partition_filters(
                bq_table, db_name, schema_name, datetime.now(timezone.utc)
            )
        else:
            partition_filters = self._get_required_partition_filters(
                bq_table, db_name, schema_name
            )

        if partition_filters is None:
            logger.warning(
                f"Could not construct partition filters for {bq_table.name}. "
                "This may cause partition elimination errors."
            )
            return base_kwargs

        # If no partition filters needed, return base kwargs
        if not partition_filters:
            return base_kwargs

        # Construct query with partition filters
        partition_where = " AND ".join(partition_filters)
        logger.debug(f"Using partition filters: {partition_where}")

        custom_sql = f"""SELECT * 
FROM `{db_name}.{schema_name}.{table.name}`
WHERE {partition_where}"""

        base_kwargs.update({"custom_sql": custom_sql, "partition_handling": "true"})

        return base_kwargs

    def get_profile_request(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> Optional[TableProfilerRequest]:
        """Get profile request with appropriate partition handling."""
        profile_request = super().get_profile_request(table, schema_name, db_name)

        if not profile_request:
            return None

        bq_table = cast(BigqueryTable, table)

        # Skip external tables if configured to do so
        if bq_table.external and not self.config.profiling.profile_external_tables:
            self.report.report_warning(
                title="Profiling skipped for external table",
                message="profiling.profile_external_tables is disabled",
                context=profile_request.pretty_name,
            )
            return None

        # Get partition filters
        partition_filters = self._get_required_partition_filters(
            bq_table, db_name, schema_name
        )

        # If we got None back, that means there was an error getting partition filters
        if partition_filters is None:
            self.report.report_warning(
                title="Profile skipped for partitioned table",
                message="Could not construct partition filters - required for partition elimination",
                context=profile_request.pretty_name,
            )
            return None

        if not self.config.profiling.partition_profiling_enabled:
            logger.debug(
                f"{profile_request.pretty_name} is skipped because profiling.partition_profiling_enabled property is disabled"
            )
            self.report.profiling_skipped_partition_profiling_disabled.append(
                profile_request.pretty_name
            )
            return None

        # Only add partition handling if we actually have partition filters
        if partition_filters:
            partition_where = " AND ".join(partition_filters)
            custom_sql = f"""SELECT * 
    FROM `{db_name}.{schema_name}.{bq_table.name}`
    WHERE {partition_where}"""

            logger.debug(f"Using partition filters: {partition_where}")
            profile_request.batch_kwargs.update(
                dict(custom_sql=custom_sql, partition_handling="true")
            )

        return profile_request

    def get_workunits(
        self, project_id: str, tables: Dict[str, List[BigqueryTable]]
    ) -> Iterable[MetadataWorkUnit]:
        """Get profile workunits handling both internal and external tables."""
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

                # Emit profile work unit
                logger.debug(
                    f"Creating profile request for table {normalized_table_name}"
                )
                profile_request = self.get_profile_request(table, dataset, project_id)
                if profile_request is not None:
                    self.report.report_entity_profiled(profile_request.pretty_name)
                    profile_requests.append(profile_request)
                else:
                    logger.debug(
                        f"Table {normalized_table_name} was not eligible for profiling."
                    )

        if len(profile_requests) == 0:
            return

        yield from self.generate_profile_workunits(
            profile_requests,
            max_workers=self.config.profiling.max_workers,
            platform=self.platform,
            profiler_args=self.get_profile_args(),
        )

    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        """Get dataset name in BigQuery format."""
        return BigqueryTableIdentifier(
            project_id=db_name, dataset=schema_name, table=table_name
        ).get_table_name()
