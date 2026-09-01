import logging
from datetime import datetime
from typing import Callable, Dict, List, Optional, Set, Tuple

import sqlglot
from dateutil.relativedelta import relativedelta
from google.cloud.bigquery import QueryJobConfig, Row, ScalarQueryParameter
from sqlglot.expressions import (
    Anonymous,
    Date,
    DatetimeTrunc,
    Expression,
    Identifier,
    PartitionedByProperty,
    Property,
)

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
from datahub.ingestion.source.bigquery_v2.profiling import queries
from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    PARTITIONING_COLUMN_FLAG,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.info_schema import (
    InfoSchemaQueries,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.types import (
    CachedPartitionMetadata,
)
from datahub.ingestion.source.bigquery_v2.profiling.reporting import warn
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    build_safe_table_reference,
)

logger = logging.getLogger(__name__)


class PartitionDiscovery:
    def __init__(
        self, config: BigQueryV2Config, report: Optional[BigQueryV2Report] = None
    ):
        self.config = config
        self.report = report
        self.info_schema = InfoSchemaQueries(report)

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
            (delta, format_str) = partition_range_map[len(partition_id)]
            duration = delta
            if not partition_datetime:
                partition_datetime = datetime.strptime(partition_id, format_str)
            else:
                partition_datetime = datetime.strptime(
                    partition_datetime.strftime(format_str), format_str
                )
        else:
            raise ValueError(
                f"Invalid partition_id {partition_id}. It must be yearly/monthly/daily/hourly."
            )
        upper_bound_partition_datetime = partition_datetime + duration
        return partition_datetime, upper_bound_partition_datetime

    def get_partition_columns_from_info_schema(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Dict[str, str]:
        return self.info_schema.get_partition_columns_from_info_schema(
            table, project, schema, execute_query_func
        )

    def get_partition_columns_from_ddl(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Dict[str, str]:
        partition_cols_with_types: Dict[str, str] = {}

        if not table.ddl:
            return partition_cols_with_types

        try:
            parsed = sqlglot.parse_one(table.ddl, dialect="bigquery")

            partition_by_expr = None
            for prop in parsed.find_all(Property):
                if isinstance(prop, PartitionedByProperty):
                    partition_by_expr = prop.this
                    break

            if not partition_by_expr:
                logger.debug(
                    f"No PARTITION BY clause found in DDL for table {table.name}"
                )
                return partition_cols_with_types

            column_names = self._extract_column_names_from_sqlglot_partition(
                partition_by_expr
            )

            if not column_names:
                warn(
                    self.report,
                    logger,
                    title="Partition columns from DDL failed",
                    message="Found a PARTITION BY clause but could not extract any column "
                    "names from it; the table may be treated as unpartitioned and "
                    "full-scanned or skipped.",
                    context=f"{table.name}",
                )
                return partition_cols_with_types

            logger.debug(
                f"Extracted partition columns from DDL: {column_names} for table {table.name}"
            )

            return self._get_partition_column_types(
                table, project, schema, column_names, execute_query_func
            )

        except Exception as e:
            warn(
                self.report,
                logger,
                title="Partition columns from DDL failed",
                message="Could not parse the table DDL to find partition columns; the "
                "table may be treated as unpartitioned and full-scanned or skipped.",
                context=f"{table.name}: {e}",
            )

        return partition_cols_with_types

    def get_required_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        cached_partition_metadata: Optional[CachedPartitionMetadata] = None,
    ) -> Optional[List[str]]:
        """Return partition filter expressions needed to query this table.

        Returns [] for unpartitioned tables, a list of filter strings for partitioned
        tables, or None if filters are required but could not be determined (caller
        should skip the table to avoid full scans).

        Discovery strategy (in priority order):
        1. table.partition_info / table.schema fields (zero-cost, already fetched)
        2. Caller-supplied cached INFORMATION_SCHEMA.COLUMNS metadata
        3. Direct INFORMATION_SCHEMA.COLUMNS query
        4. Probe with a test query and parse the resulting partition-filter error
        5. max_partition_id from table metadata (zero-scan)
        6. INFORMATION_SCHEMA.PARTITIONS (internal tables only)
        7. Direct table query for latest date values (_find_real_partition_values)
        8. Sampling fallback (_get_partitions_with_sampling)
        """
        logger.info(
            f"Starting partition discovery for table {table.name} "
            f"(project={project}, dataset={schema})"
        )

        required_partition_columns = self._get_partition_columns_from_table_info(table)

        if not required_partition_columns and cached_partition_metadata:
            required_partition_columns = list(
                dict.fromkeys(cached_partition_metadata.get("partition_columns", []))
            )

        # Track whether the INFORMATION_SCHEMA.COLUMNS lookup was authoritative: an empty
        # result from a *successful* query means the table is genuinely unpartitioned,
        # whereas an empty result from a *failed* query means the state is unknown.
        schema_authoritative = True
        if not required_partition_columns:
            schema_columns, schema_authoritative = (
                self._get_partition_columns_from_schema(
                    table, project, schema, execute_query_func
                )
            )
            # Preserve the INFORMATION_SCHEMA.COLUMNS ordinal_position order (the query
            # orders by it) rather than sorting alphabetically: a composite partition key
            # is positional, so an alphabetical reorder would bind partition-id
            # components to the wrong columns downstream.
            required_partition_columns = schema_columns

        probe_error: Optional[str] = None
        if not required_partition_columns and not schema_authoritative:
            # Only probe when the authoritative COLUMNS lookup failed. A successful,
            # empty COLUMNS result is definitive, so re-probing would be wasted work.
            probed_columns, probe_error = self._probe_required_partition_columns(
                table, project, schema, execute_query_func, "partition detection"
            )
            required_partition_columns = sorted(probed_columns)

        if not required_partition_columns:
            if table.external:
                # External (e.g. hive-partitioned) tables often expose partition columns
                # only via DDL, which the schema and probe checks above don't read.
                return self._get_external_table_partition_filters(
                    table, project, schema, execute_query_func
                )
            if probe_error is not None:
                # COLUMNS lookup failed AND the probe errored: the partition state is
                # unknown, not confirmed-unpartitioned. Returning [] would trigger an
                # unfiltered/full scan that likely fails the same way, so preserve the
                # unknown state and skip the table instead.
                warn(
                    self.report,
                    logger,
                    title="Partition column detection failed",
                    message="Could not determine partition columns from "
                    "INFORMATION_SCHEMA or the fallback probe query; skipping the table "
                    "rather than risk an unfiltered scan",
                    context=f"{table.name}: probe error={probe_error}",
                )
                return None
            # Either the COLUMNS query authoritatively returned no partition columns, or
            # the probe ran cleanly without a partition-filter error: unpartitioned.
            logger.debug(f"No partition columns found for table {table.name}")
            return []

        column_types = self._get_partition_column_types(
            table,
            project,
            schema,
            list(required_partition_columns),
            execute_query_func,
            cached_metadata=cached_partition_metadata,
        )

        # A user-pinned profiling.partition_datetime takes priority over discovery: it
        # explicitly selects which partition to profile instead of the latest.
        datetime_override = self._get_partition_datetime_override_filters(
            table, required_partition_columns, column_types
        )
        if datetime_override is not None:
            return datetime_override

        filters_from_metadata = None
        if table.max_partition_id:
            filters_from_metadata = self._get_partition_filters_from_max_partition_id(
                table, list(required_partition_columns), column_types
            )
        if filters_from_metadata:
            logger.info(
                f"Zero-scan optimization: using max_partition_id from schema metadata for {table.name}"
            )
            return filters_from_metadata

        if not table.external:
            partition_filters = self._get_partition_filters_from_information_schema(
                table,
                project,
                schema,
                list(required_partition_columns),
                execute_query_func,
                column_types,
            )
            if partition_filters:
                return partition_filters
            else:
                logger.debug(
                    f"INFORMATION_SCHEMA approach failed for {table.name}, falling back to strategic dates"
                )

        partition_filters = self._find_real_partition_values(
            table,
            project,
            schema,
            list(required_partition_columns),
            execute_query_func,
            cached_metadata=cached_partition_metadata,
        )

        if partition_filters:
            return partition_filters

        sample_filters = self._get_partitions_with_sampling(
            table, project, schema, execute_query_func
        )
        if sample_filters:
            return sample_filters

        # No usable partition values. An unfiltered query would be rejected on a
        # require-filter table and is unbounded on an external table, so skip those;
        # other tables are profiled unfiltered (bounded by the row/size limit).
        requires_filter = bool(
            table.partition_info and table.partition_info.require_partition_filter
        )
        if requires_filter or table.external:
            logger.warning(
                f"Could not find valid partition values for table {table.name} "
                f"with required columns {required_partition_columns}. "
                f"Skipping profiling to avoid inaccurate results."
            )
            return None

        warn(
            self.report,
            logger,
            title="Profiled without a partition filter",
            message="No partition values could be discovered, but the table does not "
            "require a partition filter, so it is profiled without one (bounded by the "
            "profiling row/size limit). The profile describes the whole table rather "
            "than a single partition.",
            context=f"{table.name}: required columns {sorted(required_partition_columns)}",
        )
        return []

    def _get_partition_column_types(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        cached_metadata: Optional[CachedPartitionMetadata] = None,
    ) -> Dict[str, str]:
        # cached_metadata is passed down the discovery call chain rather than held as
        # instance state so a single PartitionDiscovery can be shared across the parallel
        # external-table threads without racing on it.
        if cached_metadata:
            cached_types = cached_metadata.get("column_types", {})
            if all(col in cached_types for col in partition_columns):
                return {col: cached_types[col] for col in partition_columns}

        return self.info_schema.get_partition_column_types(
            table, project, schema, partition_columns, execute_query_func
        )

    def _extract_column_names_from_sqlglot_partition(
        self, partition_expr: Expression
    ) -> List[str]:
        column_names = []

        try:
            # BigQuery PARTITION BY is always a single expression (a column, or a
            # function of one column), never a comma-separated list. Treat the whole
            # node as one expression and let the per-node handling below pull the column
            # out. Do NOT expand `.expressions`: on a function such as
            # RANGE_BUCKET(user_id, GENERATE_ARRAY(0, 100, 10)) or TIMESTAMP_TRUNC(ts, DAY)
            # those entries are the call's arguments, so walking them invents fake
            # partition columns (the unit, or a GENERATE_ARRAY bound like `0`).
            expressions = [partition_expr] if partition_expr else []

            for expr in expressions:
                column_name = None

                if isinstance(expr, (Date, DatetimeTrunc)):
                    if hasattr(expr, "this") and expr.this:
                        column_name = str(expr.this)

                elif isinstance(expr, Anonymous):
                    # Try to extract the first argument (usually the column)
                    if hasattr(expr, "expressions") and expr.expressions:
                        first_arg = expr.expressions[0]
                        if isinstance(first_arg, Identifier):
                            column_name = str(first_arg)
                        elif hasattr(first_arg, "this"):
                            column_name = str(first_arg.this)

                elif isinstance(expr, Identifier):
                    column_name = str(expr)

                elif hasattr(expr, "this") and expr.this:
                    if isinstance(expr.this, Identifier):
                        column_name = str(expr.this)
                    else:
                        # Try to extract column from nested expression
                        column_name = str(expr.this)

                if column_name:
                    column_name = column_name.strip().strip("`").strip('"').strip("'")
                    if column_name and column_name not in column_names:
                        column_names.append(column_name)
                        logger.debug(f"Extracted partition column: {column_name}")

        except Exception as e:
            logger.warning(
                f"Error extracting column names from sqlglot partition expression: {e}"
            )

        return column_names

    def _get_partition_columns_from_table_info(self, table: BigqueryTable) -> List[str]:
        # PartitionInfo.fields is ordered and its position is significant for composite
        # partitions, so preserve declared order while deduplicating rather than
        # collapsing to an unordered set (which could map a positional component to the
        # wrong value downstream).
        ordered: List[str] = []
        seen: Set[str] = set()

        if table.partition_info:
            names = list(table.partition_info.fields)
            if table.partition_info.columns is not None:
                names.extend(col.name for col in table.partition_info.columns)
            for name in names:
                if name not in seen:
                    seen.add(name)
                    ordered.append(name)

        return ordered

    def _probe_required_partition_columns(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        purpose: str,
    ) -> Tuple[Set[str], Optional[str]]:
        # Cheap COUNT(*) probe. This is only a *supplementary* detector: BigQuery raises
        # "requires filter over column(s) ..." only for tables with
        # require_partition_filter=TRUE, so a failure lets us parse the partition columns
        # from the error. A *successful* probe does NOT prove the table is unpartitioned —
        # a partitioned table with require_partition_filter=FALSE also succeeds. That is
        # why this runs only after the authoritative INFORMATION_SCHEMA.COLUMNS lookup
        # (which flags partition columns regardless of require_partition_filter); callers
        # must not treat probe success alone as definitive. Returns (columns, error):
        # empty columns + None error means "no require-filter error", not "unpartitioned".
        return set(), None

    def _get_partition_columns_from_schema(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Tuple[List[str], bool]:
        """Return (partition columns, authoritative).

        The columns are returned in INFORMATION_SCHEMA.COLUMNS ``ordinal_position`` order
        (the query orders by it) and de-duplicated preserving that order, because a
        composite partition key is positional — the caller maps partition-id components
        to columns by position, so the order must not be lost.

        ``authoritative`` is True only when the INFORMATION_SCHEMA.COLUMNS query executed
        successfully; an empty list with authoritative=True means the table is genuinely
        unpartitioned, whereas authoritative=False means the lookup failed and the state
        is unknown (the caller should fall back to a probe).
        """
        required_partition_columns: List[str] = []

        try:
            safe_info_schema_ref = build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.COLUMNS"
            )

            query = queries.PARTITION_COLUMN_NAMES.format(
                info_schema_ref=safe_info_schema_ref, flag=PARTITIONING_COLUMN_FLAG
            )

            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("table_name", "STRING", table.name)
                ]
            )

            query_results = execute_query_func(
                query, job_config, "partition columns from schema"
            )
            # dict.fromkeys de-duplicates while keeping the ordinal_position order.
            required_partition_columns = list(
                dict.fromkeys(row.column_name for row in query_results)
            )
            logger.debug(
                f"Found partition columns from schema: {required_partition_columns}"
            )
        except Exception as e:
            # Don't probe here: the coordinator runs the probe fallback exactly once when
            # this returns empty, so probing internally would double-probe on a COLUMNS
            # failure. Signal non-authoritative so the coordinator's single probe surfaces
            # the error state instead of treating the table as unpartitioned.
            logger.debug(f"Error querying partition columns from schema: {e}")
            return required_partition_columns, False

        return required_partition_columns, True

    def _get_partitions_with_sampling(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
        # Last resort when INFORMATION_SCHEMA and direct date queries both fail. Date
        # columns use ORDER BY date DESC (cheap); non-date tables use TABLESAMPLE SYSTEM.
        return None

    def _verify_partition_has_data(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> bool:
        return False

    def _get_external_table_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
        return None

    def _find_real_partition_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        required_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        cached_metadata: Optional[CachedPartitionMetadata] = None,
    ) -> Optional[List[str]]:
        return None

    def _get_partition_datetime_override_filters(
        self,
        table: BigqueryTable,
        required_columns: List[str],
        column_types: Dict[str, str],
    ) -> Optional[List[str]]:
        # profiling.partition_datetime pins a specific date/time partition to profile
        # (instead of the latest). It only makes sense for a single temporal partition
        # column; for a composite key or a non-temporal column it cannot be expressed, so
        # warn and let normal discovery run rather than silently ignoring the setting.
        return None

    def _get_partition_filters_from_max_partition_id(
        self,
        table: BigqueryTable,
        required_columns: List[str],
        column_types: Dict[str, str],
    ) -> Optional[List[str]]:
        return None

    def _get_partition_filters_from_information_schema(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        required_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        column_types: Dict[str, str],
    ) -> Optional[List[str]]:
        return self.info_schema.get_partition_filters_from_information_schema(
            table,
            project,
            schema,
            required_columns,
            execute_query_func,
            self._verify_partition_has_data,
            column_types,
        )
