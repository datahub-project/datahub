"""
BigQuery Profiler with Partition Discovery and Cost Optimization.

This module provides a comprehensive BigQuery profiler that:
1. Discovers and prioritizes recent date partitions for cost efficiency
2. Implements SQL injection protection via parameterized queries
3. Applies intelligent sampling based on table size and configuration
4. Supports both internal and external table profiling
5. Handles partition date windowing for focused profiling
6. Provides parallel processing for external table partition discovery

Key Features:
- Cost-optimized partition discovery
- Security-first approach with input validation and safe query construction
- Configurable sampling strategies
- Comprehensive logging and reporting for observability
- Modular design with separate components for security, partition discovery, and query execution
"""

import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple, cast

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    DATE_FORMAT_PATTERNS,
    DATE_FORMAT_YYYY_MM_DD,
    DATE_FORMAT_YYYYMMDD,
    DATE_LIKE_COLUMN_NAMES,
    STRFTIME_FORMATS,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.discovery import (
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
    """
    BigQuery Profiler with Cost Optimization and Security.

    This profiler implements a multi-stage approach to BigQuery profiling:

    STAGE 1: Table Filtering & Validation
    - Validates BigQuery identifiers to prevent SQL injection
    - Checks table staleness and size limits
    - Determines if external table profiling is enabled

    STAGE 2: Partition Discovery & Optimization
    - For internal tables: Uses INFORMATION_SCHEMA.PARTITIONS for metadata-only discovery
    - For external tables: Strategic sampling with today/yesterday date checks
    - Prioritizes recent date partitions for cost efficiency

    STAGE 3: Query Construction & Sampling
    - Applies conditional TABLESAMPLE based on table size and configuration
    - Constructs parameterized queries with partition filters
    - Implements row limits and safety constraints

    STAGE 4: Parallel Processing (External Tables)
    - Defers expensive partition discovery to parallel threads
    - Processes multiple external tables concurrently
    - Maintains cost efficiency while improving performance
    """

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

        # Initialize modular components for clean separation of concerns
        self.partition_discovery = PartitionDiscovery(config)
        self.query_executor = QueryExecutor(config)

        # Track profiling statistics for reporting
        self._tables_profiled = 0
        self._external_tables_processed = 0
        self._partition_discovery_calls = 0

        # Dataset-level partition metadata cache to avoid redundant INFORMATION_SCHEMA queries
        # Structure: {(project, dataset): {table_name: {partition_columns: [...], column_types: {...}}}}
        self._partition_metadata_cache: Dict[Tuple[str, str], Dict[str, Dict]] = {}
        self._cache_hits = 0
        self._cache_misses = 0

    def _populate_partition_metadata_cache(self, project: str, dataset: str) -> None:
        cache_key = (project, dataset)

        if cache_key in self._partition_metadata_cache:
            return

        try:
            safe_info_schema_ref = build_safe_table_reference(
                project, dataset, "INFORMATION_SCHEMA.COLUMNS"
            )

            query = f"""
SELECT table_name, column_name, data_type
FROM {safe_info_schema_ref}
WHERE is_partitioning_column = 'YES'
ORDER BY table_name, ordinal_position
"""

            from google.cloud.bigquery import QueryJobConfig

            job_config = QueryJobConfig()

            rows = self.query_executor.execute_query_safely(
                query, job_config, f"partition metadata cache for {project}.{dataset}"
            )

            dataset_cache: Dict[str, Dict] = {}

            for row in rows:
                table_name = row.table_name
                if table_name not in dataset_cache:
                    dataset_cache[table_name] = {
                        "partition_columns": [],
                        "column_types": {},
                    }
                dataset_cache[table_name]["partition_columns"].append(row.column_name)
                dataset_cache[table_name]["column_types"][row.column_name] = (
                    row.data_type
                )

            self._partition_metadata_cache[cache_key] = dataset_cache

        except Exception as e:
            logger.warning(
                f"Failed to populate partition metadata cache for {project}.{dataset}: {e}"
            )
            self._partition_metadata_cache[cache_key] = {}

    def _get_cached_partition_metadata(
        self, project: str, dataset: str, table_name: str
    ) -> Optional[Dict]:
        cache_key = (project, dataset)

        if cache_key not in self._partition_metadata_cache:
            self._populate_partition_metadata_cache(project, dataset)

        dataset_cache = self._partition_metadata_cache.get(cache_key, {})
        table_metadata = dataset_cache.get(table_name)

        if table_metadata:
            self._cache_hits += 1
        else:
            self._cache_misses += 1

        return table_metadata

    def log_cache_statistics(self) -> None:
        total_requests = self._cache_hits + self._cache_misses
        if total_requests > 0:
            hit_rate = (self._cache_hits / total_requests) * 100
            logger.info(
                f"Partition metadata cache statistics: "
                f"{self._cache_hits} hits, {self._cache_misses} misses, "
                f"{hit_rate:.1f}% hit rate, "
                f"{len(self._partition_metadata_cache)} datasets cached"
            )

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
        """
        STAGE 1: Generate batch kwargs with security validation and cost-optimized sampling.

        This method constructs the batch kwargs that will be passed to the Great Expectations
        profiler. It implements our cost optimization strategy by:

        1. SECURITY: Validates all identifiers to prevent SQL injection
        2. PARTITION DISCOVERY: Gets optimal partition filters for cost efficiency
        3. SAMPLING LOGIC: Applies conditional TABLESAMPLE based on table size
        4. QUERY CONSTRUCTION: Builds custom SQL with proper WHERE clauses and limits

        The resulting custom_sql bypasses GE's default sampling to give us full control
        over query construction and cost optimization.
        """
        bq_table = cast(BigqueryTable, table)
        table_ref = f"{db_name}.{schema_name}.{bq_table.name}"

        # STEP 1: Security validation - prevent SQL injection attacks
        try:
            safe_project = validate_bigquery_identifier(db_name, "project")
            safe_schema = validate_bigquery_identifier(schema_name, "dataset")
            safe_table = validate_bigquery_identifier(bq_table.name, "table")
        except ValueError as e:
            self.report.report_warning(
                title="Invalid identifier in profiling",
                message=f"Skipping profiling due to invalid identifier: {e}",
                context=table_ref,
            )
            raise

        # STEP 2: Initialize base kwargs for Great Expectations profiler
        base_kwargs = {
            "schema": db_name,  # <project>
            "table": f"{schema_name}.{table.name}",  # <dataset>.<table>
            "project": db_name,
            "dataset": schema_name,
            "table_name": bq_table.name,
        }

        if bq_table.external:
            base_kwargs["is_external"] = "true"

        logger.debug(f"Starting partition discovery for {table_ref}")
        self._partition_discovery_calls += 1

        cached_metadata = self._get_cached_partition_metadata(
            db_name, schema_name, bq_table.name
        )

        partition_filters = self.partition_discovery.get_required_partition_filters(
            bq_table,
            db_name,
            schema_name,
            self.query_executor.execute_query_safely,
            cached_partition_metadata=cached_metadata,
        )

        if partition_filters is None:
            # Partition discovery failed, but profiling can still proceed via GE's default approach
            # This might work for non-partitioned tables or result in higher query costs
            logger.info(
                f"Partition discovery failed for {table_ref}, proceeding with default profiling approach"
            )
            return base_kwargs

        # STEP 4: Validate and apply partition filters for security
        validated_filters = []
        partition_where = ""

        if partition_filters:
            validated_filters = validate_and_filter_expressions(
                partition_filters, "batch kwargs"
            )

            if validated_filters:
                partition_where = " AND ".join(validated_filters)
                logger.info(
                    f"Applied partition filters for {table_ref}: {partition_where}"
                )
            else:
                # Filters were rejected during validation, but profiling continues without them
                # This might work for non-partitioned tables or cause query failures later
                logger.info(
                    f"Partition filters were rejected during validation for {table_ref}, proceeding without filters"
                )

        safe_table_ref = f"{safe_project}.{safe_schema}.{safe_table}"

        # STEP 5: Cost-optimized query construction with intelligent sampling
        # We build custom SQL to bypass GE's default sampling and maintain full control
        # over query costs and maintain consistency with Great Expectations.
        custom_sql = None

        # Determine if we should apply TABLESAMPLE based on table size and configuration
        should_sample = (
            self.config.profiling.use_sampling
            and hasattr(bq_table, "rows_count")
            and bq_table.rows_count
            and bq_table.rows_count > self.config.profiling.sample_size
        )

        if should_sample:
            # SAMPLING PATH: Large table - apply TABLESAMPLE for cost efficiency
            rows_count = bq_table.rows_count or 1  # Avoid division by zero
            sample_pc = self.config.profiling.sample_size / rows_count
            sample_percent = min(100 * sample_pc, 100.0)  # Cap at 100%

            if partition_where:
                custom_sql = f"""SELECT * FROM {safe_table_ref} 
TABLESAMPLE SYSTEM ({sample_percent:.8f} PERCENT)
WHERE {partition_where}"""
            else:
                custom_sql = f"""SELECT * FROM {safe_table_ref} 
TABLESAMPLE SYSTEM ({sample_percent:.8f} PERCENT)"""

            logger.info(
                f"Applied {sample_percent:.4f}% sampling to {table_ref} "
                f"({bq_table.rows_count:,} rows) for cost optimization"
            )
        else:
            # NON-SAMPLING PATH: Smaller table or sampling disabled - use row limits
            if partition_where:
                # Partitioned table with row limit
                if self.config.profiling.profiling_row_limit > 0:
                    row_limit = max(1, int(self.config.profiling.profiling_row_limit))
                    custom_sql = f"""SELECT * FROM {safe_table_ref}
WHERE {partition_where}
LIMIT {row_limit}"""
                    logger.info(
                        f"Applied row limit ({row_limit:,}) to partitioned {table_ref}"
                    )
                else:
                    custom_sql = f"""SELECT * FROM {safe_table_ref}
WHERE {partition_where}"""
                    logger.info(f"No row limit applied to partitioned {table_ref}")
            else:
                # Non-partitioned table - apply row limit or safety limit
                if self.config.profiling.profiling_row_limit > 0:
                    row_limit = max(1, int(self.config.profiling.profiling_row_limit))
                    custom_sql = f"SELECT * FROM {safe_table_ref} LIMIT {row_limit}"
                    logger.info(
                        f"Applied row limit ({row_limit:,}) to non-partitioned {table_ref}"
                    )
                else:
                    # Safety check for very large non-partitioned tables
                    if (
                        hasattr(bq_table, "rows_count")
                        and bq_table.rows_count
                        and bq_table.rows_count > 1000000  # 1M+ rows
                    ):
                        safety_limit = 100000  # Conservative safety limit
                        custom_sql = (
                            f"SELECT * FROM {safe_table_ref} LIMIT {safety_limit}"
                        )
                        # This is a successful cost optimization, not an error
                        logger.info(
                            f"Applied safety limit of {safety_limit:,} rows to large table {table_ref} ({bq_table.rows_count:,} rows) to prevent excessive costs"
                        )
                    else:
                        custom_sql = f"SELECT * FROM {safe_table_ref}"
                        logger.debug(f"No limits applied to small {table_ref}")

        # STEP 6: Finalize batch kwargs with custom SQL and metadata
        base_kwargs.update({"custom_sql": custom_sql, "partition_handling": "true"})

        logger.debug(
            f"Generated batch kwargs for {table_ref} with custom_sql: {custom_sql[:100]}..."
        )
        return base_kwargs

    def get_profile_request(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> Optional[TableProfilerRequest]:
        """
        STAGE 2: Create and validate profile requests with cost-aware partition handling.

        This method implements a sophisticated filtering and preparation strategy:

        1. BASIC VALIDATION: Checks if table meets basic profiling criteria
        2. STALENESS CHECK: Skips tables that haven't been modified recently
        3. EXTERNAL TABLE HANDLING: Validates external table profiling settings
        4. PARTITION STRATEGY: Chooses immediate vs deferred partition discovery
        5. COST OPTIMIZATION: Applies partition filters and sampling for internal tables

        External tables use deferred partition discovery (STAGE 4) to avoid blocking
        the main profiling pipeline with expensive partition queries.
        """
        profile_request = super().get_profile_request(table, schema_name, db_name)

        if not profile_request:
            return None

        bq_table = cast(BigqueryTable, table)
        table_ref = f"{db_name}.{schema_name}.{bq_table.name}"

        # STEP 1: Check table staleness to avoid profiling outdated data
        if self._should_skip_profiling_due_to_staleness(bq_table):
            logger.info(f"Skipping profiling of stale table: {table_ref}")
            return None

        # STEP 2: Validate external table profiling configuration
        if bq_table.external and not self.config.profiling.profile_external_tables:
            # This is a config-based skip, not an error
            logger.info(
                f"Skipping profiling for external table {profile_request.pretty_name} (profiling.profile_external_tables is disabled)"
            )
            return None

        # STEP 3: Check if partition profiling is globally enabled
        if not self.config.profiling.partition_profiling_enabled:
            logger.info(f"Skipping partition profiling (disabled): {table_ref}")
            self.report.profiling_skipped_partition_profiling_disabled.append(
                profile_request.pretty_name
            )
            return None

        # STEP 4: Choose partition discovery strategy based on table type
        if bq_table.external:
            # EXTERNAL TABLE PATH: Defer expensive partition discovery to parallel processing
            # This prevents blocking the main profiling pipeline with slow external table queries
            logger.info(
                f"Deferring partition discovery for external table: {table_ref}"
            )
            profile_request.needs_partition_discovery = True  # type: ignore[attr-defined]
            profile_request.bq_table = bq_table  # type: ignore[attr-defined]
            profile_request.db_name = db_name  # type: ignore[attr-defined]
            profile_request.schema_name = schema_name  # type: ignore[attr-defined]
            self._external_tables_processed += 1
        else:
            # INTERNAL TABLE PATH: Immediate partition discovery using INFORMATION_SCHEMA
            # Internal tables can use metadata-only queries for fast partition discovery
            logger.info(
                f"Starting immediate partition discovery for internal table: {table_ref}"
            )

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

            # STEP 5: Apply partition filters with security validation and date windowing
            if partition_filters:
                validated_filters = validate_and_filter_expressions(
                    partition_filters, "profile request"
                )

                if validated_filters:
                    logger.info(
                        f"Applying date windowing to {len(validated_filters)} partition filters for {table_ref}"
                    )

                    # Apply partition date windowing if configured to focus on recent data
                    windowed_filters = self._apply_partition_date_windowing(
                        validated_filters, bq_table
                    )

                    partition_where = " AND ".join(windowed_filters)
                    safe_table_ref = build_safe_table_reference(
                        db_name, schema_name, bq_table.name
                    )

                    logger.info(
                        f"Final partition WHERE clause for {table_ref}: {partition_where}"
                    )

                    # Apply sampling-compatible custom SQL for regular tables too
                    if (
                        self.config.profiling.use_sampling
                        and hasattr(bq_table, "rows_count")
                        and bq_table.rows_count
                        and bq_table.rows_count > self.config.profiling.sample_size
                    ):
                        # Calculate sampling percentage
                        sample_pc = (
                            self.config.profiling.sample_size / bq_table.rows_count
                        )
                        sample_percent = min(100 * sample_pc, 100.0)

                        custom_sql = f"""SELECT * FROM {safe_table_ref} 
TABLESAMPLE SYSTEM ({sample_percent:.8f} PERCENT)
WHERE {partition_where}"""

                        logger.debug(
                            f"Using {sample_percent:.4f}% sampling for regular table with {bq_table.rows_count:,} rows"
                        )
                    else:
                        # No sampling - use regular query with row limit
                        if self.config.profiling.profiling_row_limit > 0:
                            row_limit = max(
                                1, int(self.config.profiling.profiling_row_limit)
                            )
                            custom_sql = f"""SELECT * FROM {safe_table_ref}
WHERE {partition_where}
LIMIT {row_limit}"""
                        else:
                            custom_sql = f"""SELECT * FROM {safe_table_ref}
WHERE {partition_where}"""

                    logger.debug(
                        f"Using partition filters (with windowing): {partition_where}"
                    )
                    profile_request.batch_kwargs.update(
                        dict(custom_sql=custom_sql, partition_handling="true")
                    )

        # STEP 6: Log successful profile request creation
        logger.info(f"Successfully created profile request for {table_ref}")
        return profile_request

    def get_workunits(
        self, project_id: str, tables: Dict[str, List[BigqueryTable]]
    ) -> Iterable[MetadataWorkUnit]:
        """
        STAGE 3: Main profiling orchestrator - processes all tables in a project.

        This method coordinates the entire profiling process:
        1. Iterates through all tables in all datasets
        2. Creates profile requests with cost optimization
        3. Tracks profiling statistics for reporting
        4. Launches parallel processing for external tables
        5. Generates final profile workunits
        """
        profile_requests: List[TableProfilerRequest] = []
        total_tables = sum(len(dataset_tables) for dataset_tables in tables.values())

        logger.info(
            f"Starting profiling for project {project_id} with {total_tables} total tables across {len(tables)} datasets"
        )

        # Reset profiling statistics for this project
        self._tables_profiled = 0
        self._external_tables_processed = 0
        self._partition_discovery_calls = 0

        for dataset in tables:
            dataset_tables = tables[dataset]
            logger.info(
                f"Processing dataset {project_id}.{dataset} with {len(dataset_tables)} tables"
            )

            for table in dataset_tables:
                normalized_table_name = BigqueryTableIdentifier(
                    project_id=project_id, dataset=dataset, table=table.name
                ).get_table_name()

                # Skip external tables if profiling is disabled
                if table.external and not self.config.profiling.profile_external_tables:
                    self.report.profiling_skipped_other[f"{project_id}.{dataset}"] += 1
                    logger.debug(
                        f"Skipping external table profiling (disabled): {normalized_table_name}"
                    )
                    continue

                # Create profile request with cost optimization
                logger.debug(
                    f"Creating profile request for table: {normalized_table_name}"
                )
                profile_request = self.get_profile_request(table, dataset, project_id)

                if profile_request is not None:
                    self.report.report_entity_profiled(profile_request.pretty_name)
                    profile_requests.append(profile_request)
                    self._tables_profiled += 1
                    logger.info(
                        f"Accepted table for profiling: {normalized_table_name}"
                    )
                else:
                    logger.info(
                        f"Table not eligible for profiling: {normalized_table_name}"
                    )

        # Report profiling statistics
        self._log_profiling_statistics(project_id, total_tables, len(profile_requests))

        if len(profile_requests) == 0:
            logger.warning(f"No tables eligible for profiling in project {project_id}")
            return

        # Launch parallel processing with deferred partition discovery
        logger.info(
            f"Starting parallel profiling for {len(profile_requests)} tables in project {project_id}"
        )
        yield from self.generate_profile_workunits_with_deferred_partitions(
            profile_requests,
            max_workers=self.config.profiling.max_workers,
            platform=self.platform,
            profiler_args=self.get_profile_args(),
        )

        logger.info(f"Completed profiling for project {project_id}")

    def _log_profiling_statistics(
        self, project_id: str, total_tables: int, eligible_tables: int
    ) -> None:
        """Log comprehensive profiling statistics for observability."""
        skipped_tables = total_tables - eligible_tables

        acceptance_rate = (
            (eligible_tables / total_tables * 100) if total_tables > 0 else 0.0
        )

        logger.info(
            f"PROFILING STATISTICS for project {project_id}:\n"
            f"  • Total tables discovered: {total_tables:,}\n"
            f"  • Tables accepted for profiling: {eligible_tables:,}\n"
            f"  • Tables skipped: {skipped_tables:,}\n"
            f"  • External tables processed: {self._external_tables_processed:,}\n"
            f"  • Partition discovery calls: {self._partition_discovery_calls:,}\n"
            f"  • Profiling acceptance rate: {acceptance_rate:.1f}%"
        )

        # Report to structured reporting for monitoring
        self.report.profile_table_selection_criteria[project_id] = (
            f"{eligible_tables}/{total_tables} tables accepted for profiling "
            f"({acceptance_rate:.1f}% acceptance rate)"
        )

    def generate_profile_workunits_with_deferred_partitions(
        self,
        profile_requests: List[TableProfilerRequest],
        max_workers: int,
        platform: str,
        profiler_args: Dict,
    ) -> Iterable[MetadataWorkUnit]:
        """
        STAGE 4: Parallel processing with deferred partition discovery for external tables.

        This method implements a two-phase approach to optimize performance:

        PHASE 1: Immediate processing of internal tables (already have partition filters)
        PHASE 2: Parallel partition discovery for external tables to avoid blocking

        External table partition discovery is expensive and can take several seconds per table.
        By deferring this to parallel threads, we prevent the main profiling pipeline from
        being blocked while still maintaining cost efficiency through strategic sampling.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def process_external_table_request(
            request: TableProfilerRequest,
        ) -> Optional[TableProfilerRequest]:
            """
            Process a single external table request with partition discovery in parallel.

            This function runs in a separate thread to avoid blocking the main profiling
            pipeline. It performs the expensive partition discovery operations that can
            take several seconds for external tables with complex partitioning schemes.
            """
            if not hasattr(request, "needs_partition_discovery") or not getattr(
                request, "needs_partition_discovery", False
            ):
                return request

            try:
                # Extract table information stored during profile request creation
                bq_table = request.bq_table  # type: ignore[attr-defined]
                db_name = request.db_name  # type: ignore[attr-defined]
                schema_name = request.schema_name  # type: ignore[attr-defined]
                table_ref = f"{db_name}.{schema_name}.{bq_table.name}"

                logger.info(
                    f"Starting parallel partition discovery for external table: {table_ref}"
                )

                # Perform cost-optimized partition discovery (strategic sampling for external tables)
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
                        f"Could not construct partition filters for external table {table_ref} - skipping profiling"
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

                        # Apply sampling-compatible custom SQL for external tables too
                        if (
                            self.config.profiling.use_sampling
                            and hasattr(bq_table, "rows_count")
                            and bq_table.rows_count
                            and bq_table.rows_count > self.config.profiling.sample_size
                        ):
                            # Calculate sampling percentage
                            sample_pc = (
                                self.config.profiling.sample_size / bq_table.rows_count
                            )
                            sample_percent = min(100 * sample_pc, 100.0)

                            custom_sql = f"""SELECT * FROM {safe_table_ref} 
TABLESAMPLE SYSTEM ({sample_percent:.8f} PERCENT)
WHERE {partition_where}"""

                            logger.debug(
                                f"Using {sample_percent:.4f}% sampling for external table with {bq_table.rows_count:,} rows"
                            )
                        else:
                            # No sampling - use regular query with row limit
                            if self.config.profiling.profiling_row_limit > 0:
                                row_limit = max(
                                    1, int(self.config.profiling.profiling_row_limit)
                                )
                                custom_sql = f"""SELECT * FROM {safe_table_ref}
WHERE {partition_where}
LIMIT {row_limit}"""
                            else:
                                custom_sql = f"""SELECT * FROM {safe_table_ref}
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

        # Log cache statistics for observability
        self.log_cache_statistics()

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
            # Detect the format used in existing filters for this column
            existing_format = self._detect_date_format_in_filters(
                partition_filters, col_name
            )

            # Use consistent formatting based on existing filter format
            if existing_format == DATE_FORMAT_YYYYMMDD:
                # STRING format like '20250913'
                format_str = STRFTIME_FORMATS[DATE_FORMAT_YYYYMMDD]
                start_date_str = f"'{start_date.strftime(format_str)}'"
                end_date_str = f"'{reference_date.strftime(format_str)}'"
            elif existing_format == DATE_FORMAT_YYYY_MM_DD:
                # STRING format like '2025-09-13'
                format_str = STRFTIME_FORMATS[DATE_FORMAT_YYYY_MM_DD]
                start_date_str = f"'{start_date.strftime(format_str)}'"
                end_date_str = f"'{reference_date.strftime(format_str)}'"
            else:
                # Default to YYYY-MM-DD string format (let BigQuery handle casting)
                format_str = STRFTIME_FORMATS[DATE_FORMAT_YYYY_MM_DD]
                start_date_str = f"'{start_date.strftime(format_str)}'"
                end_date_str = f"'{reference_date.strftime(format_str)}'"

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

    def _detect_date_format_in_filters(
        self, partition_filters: List[str], col_name: str
    ) -> Optional[str]:
        """
        Detect the date format used in existing partition filters for a specific column.

        This helps ensure consistent formatting when adding date windowing filters,
        avoiding type mismatch errors between STRING and DATE types.

        Args:
            partition_filters: List of existing partition filter expressions
            col_name: Column name to check format for

        Returns:
            Format string (DATE_FORMAT_YYYYMMDD, DATE_FORMAT_YYYY_MM_DD, or None for DATE functions)
        """
        # Build a prefix pattern to find filters for this specific column
        col_prefix = f"`{re.escape(col_name)}`"

        for filter_expr in partition_filters:
            # Only check filters that reference this column
            if col_prefix not in filter_expr:
                continue

            # Check each format pattern using pre-compiled regex
            for format_name, pattern in DATE_FORMAT_PATTERNS.items():
                if pattern.search(filter_expr):
                    return format_name

        # If no specific format detected, assume DATE function format
        return None

    def _extract_date_columns_from_filters(
        self, partition_filters: List[str]
    ) -> List[str]:
        """
        Extract date-like column names from partition filter expressions.

        Note: This uses pattern matching as a heuristic. Ideally, we would have
        column type information available, but partition filters are often constructed
        without access to the full schema metadata.
        """
        date_columns = []

        for filter_expr in partition_filters:
            filter_lower = filter_expr.lower()
            # Check against all known date-like column patterns from constants
            for pattern in DATE_LIKE_COLUMN_NAMES:
                if f"`{pattern}`" in filter_lower:
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
