"""BigQuery profiler with modular security and partition handling."""

import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple, cast

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery import (
    PartitionDiscovery,
)
from datahub.ingestion.source.bigquery_v2.profiling.query_executor import QueryExecutor

# Import our modular components
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    build_safe_table_reference,
    validate_and_filter_expressions,
    validate_bigquery_identifier,
)
from datahub.ingestion.source.sql.sql_generic import BaseTable
from datahub.ingestion.source.sql.sql_generic_profiler import (
    GenericProfiler,
    TableProfilerRequest,
)
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler

logger = logging.getLogger(__name__)


class BigqueryProfiler(GenericProfiler):
    """BigQuery profiler with partition-aware querying and security validation."""

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
        self.partition_discovery = PartitionDiscovery(config)
        self.query_executor = QueryExecutor(config)

    @staticmethod
    def get_partition_range_from_partition_id(
        partition_id: str, partition_datetime: Optional[datetime]
    ) -> Tuple[datetime, datetime]:
        return PartitionDiscovery.get_partition_range_from_partition_id(
            partition_id, partition_datetime
        )

    def get_batch_kwargs(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> dict:
        bq_table = cast(BigqueryTable, table)

        try:
            safe_project = validate_bigquery_identifier(db_name, "project")
            safe_schema = validate_bigquery_identifier(schema_name, "dataset")
            safe_table = validate_bigquery_identifier(bq_table.name, "table")
        except ValueError as e:
            logger.error(f"Invalid identifier in batch kwargs: {e}")
            raise

        base_kwargs = {
            "schema": db_name,  # <project>
            "table": f"{schema_name}.{table.name}",  # <dataset>.<table>
            "project": db_name,
            "dataset": schema_name,
            "table_name": bq_table.name,
        }

        if bq_table.external:
            base_kwargs["is_external"] = "true"

        partition_filters = self.partition_discovery.get_required_partition_filters(
            bq_table, db_name, schema_name, self.query_executor.execute_query_safely
        )

        if partition_filters is None:
            logger.warning(
                f"Could not construct partition filters for {bq_table.name}. "
                "This may cause partition elimination errors."
            )
            return base_kwargs

        if not partition_filters:
            return base_kwargs

        validated_filters = validate_and_filter_expressions(
            partition_filters, "batch kwargs"
        )

        if not validated_filters:
            logger.warning("No valid partition filters after validation")
            return base_kwargs

        partition_where = " AND ".join(validated_filters)
        logger.debug(f"Using partition filters: {partition_where}")

        safe_table_ref = f"{safe_project}.{safe_schema}.{safe_table}"

        if self.config.profiling.profiling_row_limit > 0:
            row_limit = max(1, int(self.config.profiling.profiling_row_limit))
            custom_sql = f"""SELECT * 
FROM {safe_table_ref}
WHERE {partition_where}
LIMIT {row_limit}"""
        else:
            custom_sql = f"""SELECT * 
FROM {safe_table_ref}
WHERE {partition_where}"""

        base_kwargs.update({"custom_sql": custom_sql, "partition_handling": "true"})

        return base_kwargs

    def get_profile_request(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> Optional[TableProfilerRequest]:
        profile_request = super().get_profile_request(table, schema_name, db_name)

        if not profile_request:
            return None

        bq_table = cast(BigqueryTable, table)

        # Check if profiling should be skipped due to table staleness
        if self._should_skip_profiling_due_to_staleness(bq_table):
            return None

        if bq_table.external and not self.config.profiling.profile_external_tables:
            self.report.report_warning(
                title="Profiling skipped for external table",
                message="profiling.profile_external_tables is disabled",
                context=profile_request.pretty_name,
            )
            return None

        # Check if partition profiling is enabled (applies to both regular and external tables)
        if not self.config.profiling.partition_profiling_enabled:
            logger.debug(
                f"{profile_request.pretty_name} is skipped because profiling.partition_profiling_enabled property is disabled"
            )
            self.report.profiling_skipped_partition_profiling_disabled.append(
                profile_request.pretty_name
            )
            return None

        # For external tables, defer expensive partition discovery to parallel phase
        if bq_table.external:
            # Store info needed for deferred partition discovery
            profile_request.needs_partition_discovery = True  # type: ignore[attr-defined]
            profile_request.bq_table = bq_table  # type: ignore[attr-defined]
            profile_request.db_name = db_name  # type: ignore[attr-defined]
            profile_request.schema_name = schema_name  # type: ignore[attr-defined]
        else:
            # For regular tables, do partition discovery now (usually fast)
            partition_filters = self.partition_discovery.get_required_partition_filters(
                bq_table, db_name, schema_name, self.query_executor.execute_query_safely
            )

            if partition_filters is None:
                self.report.report_warning(
                    title="Profile skipped for partitioned table",
                    message="Could not construct partition filters - required for partition elimination",
                    context=profile_request.pretty_name,
                )
                return None

            # Apply partition filters and windowing for regular tables
            if partition_filters:
                validated_filters = validate_and_filter_expressions(
                    partition_filters, "profile request"
                )

                if validated_filters:
                    # Apply partition date windowing if configured
                    windowed_filters = self._apply_partition_date_windowing(
                        validated_filters, bq_table
                    )

                    partition_where = " AND ".join(windowed_filters)
                    safe_table_ref = build_safe_table_reference(
                        db_name, schema_name, bq_table.name
                    )

                    custom_sql = f"""SELECT * 
FROM {safe_table_ref}
WHERE {partition_where}"""

                    logger.debug(
                        f"Using partition filters (with windowing): {partition_where}"
                    )
                    profile_request.batch_kwargs.update(
                        dict(custom_sql=custom_sql, partition_handling="true")
                    )

        return profile_request

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
                    logger.debug(
                        f"Skipping profiling of external table {project_id}.{dataset}.{table.name}"
                    )
                    continue

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

        yield from self.generate_profile_workunits_with_deferred_partitions(
            profile_requests,
            max_workers=self.config.profiling.max_workers,
            platform=self.platform,
            profiler_args=self.get_profile_args(),
        )

    def generate_profile_workunits_with_deferred_partitions(
        self,
        profile_requests: List[TableProfilerRequest],
        max_workers: int,
        platform: str,
        profiler_args: Dict,
    ) -> Iterable[MetadataWorkUnit]:
        """Generate profile workunits with deferred partition discovery for external tables."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def process_external_table_request(
            request: TableProfilerRequest,
        ) -> Optional[TableProfilerRequest]:
            """Process a single external table request with partition discovery in parallel."""
            if not hasattr(request, "needs_partition_discovery") or not getattr(
                request, "needs_partition_discovery", False
            ):
                return request

            try:
                # Do the expensive partition discovery in this parallel thread
                bq_table = request.bq_table  # type: ignore[attr-defined]
                db_name = request.db_name  # type: ignore[attr-defined]
                schema_name = request.schema_name  # type: ignore[attr-defined]

                partition_filters = (
                    self.partition_discovery.get_required_partition_filters(
                        bq_table,
                        db_name,
                        schema_name,
                        self.query_executor.execute_query_safely,
                    )
                )

                if partition_filters is None:
                    logger.warning(
                        f"Could not construct partition filters for external table {request.pretty_name} - skipping profiling"
                    )
                    return None

                # Apply partition filters and windowing for external tables
                if partition_filters:
                    validated_filters = validate_and_filter_expressions(
                        partition_filters, "external table profile request"
                    )

                    if validated_filters:
                        # Apply partition date windowing if configured
                        windowed_filters = self._apply_partition_date_windowing(
                            validated_filters, bq_table
                        )

                        partition_where = " AND ".join(windowed_filters)
                        safe_table_ref = build_safe_table_reference(
                            db_name, schema_name, bq_table.name
                        )

                        custom_sql = f"""SELECT * 
FROM {safe_table_ref}
WHERE {partition_where}"""

                        logger.debug(
                            f"Using partition filters for external table (with windowing): {partition_where}"
                        )
                        request.batch_kwargs.update(
                            dict(custom_sql=custom_sql, partition_handling="true")
                        )

                # Clean up temporary attributes
                delattr(request, "needs_partition_discovery")
                delattr(request, "bq_table")
                delattr(request, "db_name")
                delattr(request, "schema_name")

                return request

            except Exception as e:
                logger.error(
                    f"Error processing partition discovery for external table {request.pretty_name}: {e}"
                )
                return None

        # Separate external table requests that need parallel processing
        external_requests = [
            req for req in profile_requests if hasattr(req, "needs_partition_discovery")
        ]
        regular_requests = [
            req
            for req in profile_requests
            if not hasattr(req, "needs_partition_discovery")
        ]

        processed_requests = list(
            regular_requests
        )  # Regular requests are already processed

        if external_requests:
            logger.info(
                f"Processing partition discovery for {len(external_requests)} external table(s) in parallel"
            )

            # Process external table partition discovery in parallel
            with ThreadPoolExecutor(
                max_workers=min(max_workers, len(external_requests))
            ) as executor:
                future_to_request = {
                    executor.submit(process_external_table_request, req): req
                    for req in external_requests
                }

                for future in as_completed(future_to_request):
                    result = future.result()
                    if result is not None:
                        processed_requests.append(result)

        # Now run the regular profiling with all processed requests
        if processed_requests:
            yield from super().generate_profile_workunits(
                processed_requests,
                max_workers=max_workers,
                platform=platform,
                profiler_args=profiler_args,
            )

    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        return BigqueryTableIdentifier(
            project_id=db_name, dataset=schema_name, table=table_name
        ).get_table_name()

    def __str__(self) -> str:
        """String representation of the profiler."""
        return f"BigqueryProfiler(project={getattr(self.config, 'project_id', 'unknown')}, timeout={self.query_executor.get_effective_timeout()}s)"

    def __repr__(self) -> str:
        return f"BigqueryProfiler(config={self.config.__class__.__name__})"

    def _should_skip_profiling_due_to_staleness(self, table: BigqueryTable) -> bool:
        """
        Check if profiling should be skipped due to table staleness.

        Uses last_altered timestamp for both regular and external tables.
        BigQuery INFORMATION_SCHEMA maps last_modified_time to last_altered for both table types.

        Args:
            table: The BigQuery table object

        Returns:
            True if profiling should be skipped, False otherwise
        """
        if not self.config.profiling.skip_stale_tables:
            return False

        now = datetime.now(timezone.utc)
        threshold_date = now - timedelta(
            days=self.config.profiling.staleness_threshold_days
        )

        # Check last modification time
        last_modified = None

        # For both regular and external tables, BigQuery maps last_modified_time to last_altered
        # This works for both table types since BigQuery INFORMATION_SCHEMA provides
        # last_modified_time for both regular and external tables
        if table.last_altered:
            last_modified = table.last_altered
        else:
            # If no last_altered time available, default to not skipping (conservative approach)
            table_type = "external table" if table.external else "table"
            logger.debug(
                f"{table_type.title()} {table.name} has no last_altered time, will not skip profiling"
            )
            return False

        # Convert to timezone-aware datetime if needed
        if last_modified.tzinfo is None:
            last_modified = last_modified.replace(tzinfo=timezone.utc)

        is_stale = last_modified < threshold_date

        if is_stale:
            days_since_modified = (now - last_modified).days
            logger.info(
                f"Skipping profiling for stale table {table.name} - "
                f"last modified {days_since_modified} days ago ({last_modified.strftime('%Y-%m-%d')})"
            )
            return True

        logger.debug(
            f"Table {table.name} is fresh - last modified {last_modified.strftime('%Y-%m-%d')}, "
            f"will proceed with profiling"
        )
        return False

    def _apply_partition_date_windowing(
        self, partition_filters: List[str], table: BigqueryTable
    ) -> List[str]:
        """
        Apply partition date windowing to limit profiling to recent partitions.

        This adds additional date range filters to focus profiling on recent data,
        improving performance and relevance of profiling results.

        Args:
            partition_filters: List of existing partition filter expressions
            table: The BigQuery table being profiled

        Returns:
            List of partition filters with date windowing applied
        """
        if not self.config.profiling.partition_datetime_window_days:
            # Date windowing is disabled
            return partition_filters

        window_days = self.config.profiling.partition_datetime_window_days
        windowed_filters = partition_filters.copy()

        # Find date-like columns in the partition filters
        date_columns = self._extract_date_columns_from_filters(partition_filters)

        if not date_columns:
            # No date columns found, return original filters
            logger.debug(
                f"No date columns found in partition filters for {table.name}, skipping date windowing"
            )
            return partition_filters

        # Get the reference date from the partition filters or use current date
        reference_date = self._get_reference_date_from_filters(
            partition_filters, date_columns
        )
        if not reference_date:
            reference_date = datetime.now(timezone.utc).date()

        # Calculate the date window
        start_date = reference_date - timedelta(days=window_days)

        # Add date range filters for each date column
        for col_name in date_columns:
            # Use flexible date formatting based on column type
            start_date_str = self._format_date_for_bigquery_column(start_date, col_name)
            end_date_str = self._format_date_for_bigquery_column(
                reference_date, col_name
            )

            # Add range filter to limit the date window
            range_filter = (
                f"`{col_name}` >= {start_date_str} AND `{col_name}` <= {end_date_str}"
            )
            windowed_filters.append(range_filter)

        logger.debug(
            f"Applied {window_days}-day partition window for {table.name}: "
            f"{start_date.strftime('%Y-%m-%d')} to {reference_date.strftime('%Y-%m-%d')}"
        )

        return windowed_filters

    def _extract_date_columns_from_filters(
        self, partition_filters: List[str]
    ) -> List[str]:
        """Extract date-like column names from partition filter expressions."""
        date_columns = []

        # Common date column patterns
        date_column_patterns = [
            "date",
            "dt",
            "partition_date",
            "date_partition",
            "event_date",
            "created_date",
            "updated_date",
            "timestamp",
            "datetime",
            "time",
            "created_at",
            "modified_at",
            "updated_at",
            "event_time",
        ]

        for filter_expr in partition_filters:
            for pattern in date_column_patterns:
                if f"`{pattern}`" in filter_expr.lower():
                    if pattern not in date_columns:
                        date_columns.append(pattern)

        return date_columns

    def _get_reference_date_from_filters(
        self, partition_filters: List[str], date_columns: List[str]
    ) -> Optional[date]:
        """Extract the reference date from existing partition filters."""

        for filter_expr in partition_filters:
            for col_name in date_columns:
                # Look for date patterns like `date` = '2025-08-28'
                pattern = rf"`{col_name}`\s*=\s*'(\d{{4}}-\d{{2}}-\d{{2}})'"
                match = re.search(pattern, filter_expr, re.IGNORECASE)
                if match:
                    try:
                        date_str = match.group(1)
                        return datetime.strptime(date_str, "%Y-%m-%d").date()
                    except ValueError:
                        continue

        return None

    def _format_date_for_bigquery_column(self, date_obj: date, col_name: str) -> str:
        """
        Format a date for BigQuery based on the likely column type.

        Uses the most compatible format for different BigQuery partition column types:
        - DATE columns: '2025-08-28'
        - DATETIME/TIMESTAMP columns: DATE('2025-08-28') function for compatibility
        - Unknown columns: DATE('2025-08-28') as safest option

        Args:
            date_obj: Date to format
            col_name: Column name (used to infer type)

        Returns:
            Formatted date string suitable for BigQuery queries
        """
        date_str = date_obj.strftime("%Y-%m-%d")

        # For most date-like columns, use DATE() function for maximum compatibility
        # This works for DATE, DATETIME, and TIMESTAMP columns
        if self._is_likely_timestamp_column(col_name):
            # For timestamp columns, use TIMESTAMP() function with start/end of day
            if col_name.endswith("_start") or "start" in col_name.lower():
                return f"TIMESTAMP('{date_str} 00:00:00')"
            elif col_name.endswith("_end") or "end" in col_name.lower():
                return f"TIMESTAMP('{date_str} 23:59:59')"
            else:
                # Default to start of day for timestamp comparisons
                return f"TIMESTAMP('{date_str}')"
        elif self._is_likely_datetime_column(col_name):
            # For datetime columns, use DATETIME() function
            return f"DATETIME('{date_str}')"
        else:
            # For DATE columns and unknown types, use DATE() function (safest)
            return f"DATE('{date_str}')"

    def _is_likely_timestamp_column(self, col_name: str) -> bool:
        """Check if column name suggests it's a TIMESTAMP type."""
        timestamp_indicators = [
            "timestamp",
            "ts",
            "created_at",
            "updated_at",
            "modified_at",
            "event_time",
            "log_time",
            "ingested_at",
            "processed_at",
        ]
        return any(indicator in col_name.lower() for indicator in timestamp_indicators)

    def _is_likely_datetime_column(self, col_name: str) -> bool:
        """Check if column name suggests it's a DATETIME type."""
        datetime_indicators = [
            "datetime",
            "date_time",
            "event_datetime",
            "log_datetime",
        ]
        return any(indicator in col_name.lower() for indicator in datetime_indicators)
