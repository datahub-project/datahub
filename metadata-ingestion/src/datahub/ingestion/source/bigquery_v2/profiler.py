import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple, cast

from dateutil.relativedelta import relativedelta
from google.cloud.bigquery import SchemaField

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
from datahub.metadata.schema_classes import SchemaFieldClass, SchemaFieldDataTypeClass

logger = logging.getLogger(__name__)


class BigQueryComplexTypeHandler:
    """Handles profiling logic for complex BigQuery types"""

    def __init__(self):
        self.simple_types = {
            "STRING",
            "BYTES",
            "INTEGER",
            "INT64",
            "FLOAT",
            "FLOAT64",
            "NUMERIC",
            "BIGNUMERIC",
            "BOOLEAN",
            "BOOL",
            "DATE",
            "DATETIME",
            "TIME",
            "TIMESTAMP",
        }

    def generate_profiling_sql(
        self,
        field_path: str,
        field_type: str,
        table_name: str,
        is_repeated: bool = False,
    ) -> Optional[str]:
        """Generates profiling SQL based on field type with support for repeated fields"""
        if field_type not in self.simple_types:
            logger.debug(
                f"Skipping unsupported type {field_type} for field {field_path}"
            )
            return None

        if is_repeated:
            # For repeated fields, unnest and analyze the array elements
            return self._generate_array_profiling_sql(
                field_path, field_type, table_name
            )
        else:
            # For regular fields use direct profiling
            return self._generate_scalar_profiling_sql(
                field_path, field_type, table_name
            )

    def _generate_scalar_profiling_sql(
        self, field_path: str, field_type: str, table_name: str
    ) -> str:
        """Generate SQL for scalar field types"""
        qualified_field = f"`{field_path}`"

        # Common metrics for all types
        metrics = f"""COUNT(*) as total_count,
COUNTIF({qualified_field} IS NOT NULL) as non_null_count"""

        # Add type-specific metrics
        type_metrics = self._get_type_specific_metrics(qualified_field, field_type)
        if type_metrics:
            metrics = f"{metrics}, {type_metrics}"

        return f"SELECT {metrics} FROM {table_name}"

    def _generate_array_profiling_sql(
        self, field_path: str, field_type: str, table_name: str
    ) -> str:
        """Generate SQL for array/repeated fields"""
        qualified_field = f"`{field_path}`"
        array_element = "element"

        metrics = f"""COUNT(*) as total_count,
COUNTIF({qualified_field} IS NOT NULL) as non_null_count,
AVG(ARRAY_LENGTH({qualified_field})) as avg_array_length,
MIN(ARRAY_LENGTH({qualified_field})) as min_array_length,
MAX(ARRAY_LENGTH({qualified_field})) as max_array_length"""

        # Add element-level metrics if supported for the type
        type_metrics = self._get_type_specific_metrics(array_element, field_type)
        if type_metrics:
            metrics = f"""{metrics},
(SELECT AS STRUCT 
    {type_metrics}
 FROM UNNEST({qualified_field}) as {array_element}
) as element_stats"""

        return f"SELECT {metrics} FROM {table_name}"

    def _get_type_specific_metrics(self, field: str, field_type: str) -> Optional[str]:
        """Gets type-specific metrics SQL"""
        if field_type in {
            "INTEGER",
            "INT64",
            "FLOAT",
            "FLOAT64",
            "NUMERIC",
            "BIGNUMERIC",
        }:
            return f"""MIN({field}) as min_value,
MAX({field}) as max_value,
AVG({field}) as avg_value,
APPROX_QUANTILES({field}, 2)[OFFSET(1)] as median,
STDDEV({field}) as stddev"""
        elif field_type in {"STRING", "BYTES"}:
            return f"""COUNT(DISTINCT {field}) as distinct_count,
AVG(LENGTH({field})) as avg_length,
MIN(LENGTH({field})) as min_length,
MAX(LENGTH({field})) as max_length"""
        elif field_type in {"DATE", "DATETIME", "TIMESTAMP"}:
            return f"""MIN({field}) as min_value,
MAX({field}) as max_value"""
        return None


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
        self.complex_type_handler = BigQueryComplexTypeHandler()

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

        if len(partition_id) not in partition_range_map:
            raise ValueError(
                f"Invalid partition_id {partition_id}. Must be yearly/monthly/daily/hourly."
            )

        duration, fmt = partition_range_map[len(partition_id)]

        if not partition_datetime:
            partition_datetime = datetime.strptime(partition_id, fmt)
        else:
            partition_datetime = datetime.strptime(
                partition_datetime.strftime(fmt), fmt
            )

        upper_bound = partition_datetime + duration
        return partition_datetime, upper_bound

    def convert_bigquery_schema(self, bq_field: "SchemaField") -> SchemaFieldClass:
        """Convert BigQuery SchemaField to DataHub SchemaFieldClass"""
        return SchemaFieldClass(
            fieldPath=bq_field.name,
            type=self._convert_field_type(bq_field.field_type),
            nativeDataType=bq_field.field_type,
            description=bq_field.description if bq_field.description else None,
            nullable=(bq_field.mode != "REQUIRED"),
        )

    def _convert_field_type(self, bq_type: str) -> SchemaFieldDataTypeClass:
        """Convert BigQuery type to DataHub SchemaFieldDataTypeClass"""
        from datahub.metadata.schema_classes import (
            ArrayTypeClass,
            BooleanTypeClass,
            BytesTypeClass,
            DateTypeClass,
            NullTypeClass,
            NumberTypeClass,
            RecordTypeClass,
            StringTypeClass,
            TimeTypeClass,
        )

        type_mapping = {
            "BOOL": BooleanTypeClass,
            "BOOLEAN": BooleanTypeClass,
            "BYTES": BytesTypeClass,
            "DATE": DateTypeClass,
            "DATETIME": TimeTypeClass,
            "TIME": TimeTypeClass,
            "TIMESTAMP": TimeTypeClass,
            "FLOAT": NumberTypeClass,
            "FLOAT64": NumberTypeClass,
            "INTEGER": NumberTypeClass,
            "INT64": NumberTypeClass,
            "NUMERIC": NumberTypeClass,
            "BIGNUMERIC": NumberTypeClass,
            "STRING": StringTypeClass,
            "STRUCT": RecordTypeClass,
            "RECORD": RecordTypeClass,
        }

        if bq_type.startswith("ARRAY<"):
            # Handle array type
            return SchemaFieldDataTypeClass(type=ArrayTypeClass())

        type_class = type_mapping.get(bq_type, NullTypeClass)
        return SchemaFieldDataTypeClass(type=type_class())

    def _get_required_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> Optional[List[str]]:
        """Get partition filters for all required partition columns."""
        try:
            current_time = datetime.now(timezone.utc)
            partition_filters = []

            # Get required partition columns
            required_partition_columns = set()

            if table.partition_info:
                if isinstance(table.partition_info.fields, list):
                    required_partition_columns.update(table.partition_info.fields)

                if table.partition_info.columns:
                    required_partition_columns.update(
                        col.name for col in table.partition_info.columns if col
                    )

            if not required_partition_columns:
                if not table.external:
                    return None

                # Check external table partitioning
                query = f"""SELECT column_name, data_type
FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table.name}' 
AND is_partitioning_column = 'YES'"""

                client = self.config.get_bigquery_client()
                results = list(client.query(query))

                if not results:
                    return []

                required_partition_columns.update(row.column_name for row in results)

            # Get column types
            query = f"""SELECT column_name, data_type
FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table.name}'"""

            results = list(self.config.get_bigquery_client().query(query))
            column_types = {row.column_name: row.data_type for row in results}

            # Handle time-based vs other columns
            time_columns = {
                col
                for col in required_partition_columns
                if col.lower() in {"year", "month", "day", "hour"}
            }
            other_columns = required_partition_columns - time_columns

            # Time-based filters - always treat as integers unless column type suggests otherwise
            for col in time_columns:
                col_lower = col.lower()
                col_type = column_types.get(col, "INT64")

                try:
                    # First try to get value from query
                    query = f"""SELECT DISTINCT {col} as val
FROM `{project}.{schema}.{table.name}`
WHERE {col} IS NOT NULL
ORDER BY {col} DESC
LIMIT 1"""

                    results = list(self.config.get_bigquery_client().query(query))
                    if results and results[0].val is not None:
                        val = results[0].val
                    else:
                        # Fall back to current time if no value found
                        val = {
                            "year": current_time.year,
                            "month": current_time.month,
                            "day": current_time.day,
                            "hour": current_time.hour,
                        }[col_lower]

                    needs_quotes = col_type.upper() in {
                        "STRING",
                        "BYTES",
                        "TIMESTAMP",
                        "DATE",
                        "DATETIME",
                        "TIME",
                    }
                    filter_val = f"'{val}'" if needs_quotes else str(val)
                    partition_filters.append(f"`{col}` = {filter_val}")

                except Exception as e:
                    logger.error(
                        f"Error getting partition value for time column {col}: {e}"
                    )

            # Non-time filters
            for col in other_columns:
                try:
                    query = f"""SELECT DISTINCT {col} as val
FROM `{project}.{schema}.{table.name}`
WHERE {col} IS NOT NULL
ORDER BY {col} DESC
LIMIT 1"""

                    results = list(self.config.get_bigquery_client().query(query))

                    if not results or results[0].val is None:
                        logger.warning(f"No values found for partition column {col}")
                        continue

                    val = results[0].val
                    filter_val = val if isinstance(val, (int, float)) else f"'{val}'"
                    partition_filters.append(f"`{col}` = {filter_val}")

                except Exception as e:
                    logger.error(f"Error getting partition value for {col}: {e}")

            return partition_filters if partition_filters else None

        except Exception as e:
            logger.error(f"Error determining partition filters: {e}")
            return None

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

        # For external tables, add specific handling
        if bq_table.external:
            base_kwargs["is_external"] = "true"
            # Add any specific external table options needed

        partition_filters = self._get_required_partition_filters(
            bq_table, db_name, schema_name
        )

        if partition_filters is None:
            logger.warning(
                f"Could not construct partition filters for {bq_table.name}. "
                "This may cause partition elimination errors."
            )
            return base_kwargs

        # If no partition filters needed (e.g. some external tables), return base kwargs
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
        """Get profile request with complex type handling"""
        profile_request = super().get_profile_request(table, schema_name, db_name)
        if not profile_request:
            return None

        bq_table = cast(BigqueryTable, table)

        # Skip external tables if configured
        if bq_table.external and not self.config.profiling.profile_external_tables:
            self.report.report_warning(
                title="Profiling skipped for external table",
                message="profiling.profile_external_tables is disabled",
                context=profile_request.pretty_name,
            )
            return None

        # Handle partitioning
        partition_filters = self._get_required_partition_filters(
            bq_table, db_name, schema_name
        )

        if partition_filters is None:
            self.report.report_warning(
                title="Profile skipped for partitioned table",
                message="Could not construct partition filters - required for partition elimination",
                context=profile_request.pretty_name,
            )
            return None

        if not self.config.profiling.partition_profiling_enabled:
            logger.debug(
                f"Skipping {profile_request.pretty_name} - partition_profiling_enabled is False"
            )
            self.report.profiling_skipped_partition_profiling_disabled.append(
                profile_request.pretty_name
            )
            return None

        # Enhance with complex type handling
        try:
            # Get table schema
            table_ref = f"{db_name}.{schema_name}.{bq_table.name}"
            bq_table_obj = self.config.get_bigquery_client().get_table(table_ref)

            # Process each field
            query_dict = {}

            # Check nested fields by recursively building paths
            def process_field(field: SchemaField, parent_path: str = "") -> None:
                field_path = (
                    f"{parent_path}.{field.name}" if parent_path else field.name
                )

                # Generate profiling SQL for this field
                profile_sql = self.complex_type_handler.generate_profiling_sql(
                    field_path=field_path,
                    field_type=field.field_type,
                    table_name=table_ref,
                    is_repeated=(field.mode == "REPEATED"),
                )

                if profile_sql:
                    if partition_filters:
                        where_clause = " AND ".join(partition_filters)
                        profile_sql = f"{profile_sql} WHERE {where_clause}"

                    # Add to query_dict instead of profiling_queries
                    query_dict[field_path] = profile_sql

                # Recursively process nested fields if field is a STRUCT/RECORD
                if field.field_type in ("RECORD", "STRUCT") and hasattr(
                    field, "fields"
                ):
                    for nested_field in field.fields:
                        process_field(nested_field, field_path)

            # Process each field in the schema
            for bq_field in bq_table_obj.schema:
                process_field(bq_field)

            if not query_dict:
                logger.debug(
                    f"No profiling queries generated for {profile_request.pretty_name}"
                )
                return None

            # Set the query dictionary in the profile request
            profile_request.query_dict = query_dict
            return profile_request

        except Exception as e:
            self.report.report_warning(
                message="Failed to enhance profile request",
                context=profile_request.pretty_name,
                exc=e,
            )
            return None

    def get_workunits(
        self, project_id: str, tables: Dict[str, List[BigqueryTable]]
    ) -> Iterable[MetadataWorkUnit]:
        """Get profile workunits with complex type handling"""
        for dataset in tables:
            for table in tables[dataset]:
                table_id = BigqueryTableIdentifier(
                    project_id=project_id, dataset=dataset, table=table.name
                )

                profile_request = self.get_profile_request(table, dataset, project_id)

                if profile_request:
                    self.report.report_entity_profiled(profile_request.pretty_name)
                    yield from self.generate_profile_workunits(
                        [profile_request],
                        max_workers=self.config.profiling.max_workers,
                        platform=self.platform,
                        profiler_args=self.get_profile_args(),
                    )
                else:
                    logger.debug(f"Skipping profiling for {table_id}")

    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        """Get dataset name in BigQuery format."""
        return BigqueryTableIdentifier(
            project_id=db_name, dataset=schema_name, table=table_name
        ).get_table_name()
