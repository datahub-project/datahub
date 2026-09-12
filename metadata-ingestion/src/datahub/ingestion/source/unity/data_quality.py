import logging
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Iterable, Set

from datahub.emitter.mce_builder import make_ts_millis
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.assertion import (
    DataQualityAssertion,
    build_assertion_info_mcp,
    build_assertion_run_event_mcp,
    make_dq_assertion_urn,
)
from datahub.ingestion.source.unity.config import UnityCatalogDataQualityConfig
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import Table, TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport

logger = logging.getLogger(__name__)

# The monitor's null count for a column; completeness passes when it is zero.
COMPLETENESS_METRIC = "num_nulls"
COMPLETENESS_THRESHOLD = 0.0

# Keep fractional seconds so a window ending mid-second is not truncated below the
# query's upper bound (which would drop that window).
_TS_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

# `count` is a reserved word and collides with Row.count on the client, so it is
# backtick-quoted and aliased. `log_type = 'INPUT'` selects the monitored table
# itself (not the drift baseline); `column_name <> ':table'` drops the whole-table
# summary row so only per-column completeness is published.
_PROFILE_METRICS_QUERY = """\
SELECT window.end AS window_end, column_name, monitor_version,
       `count` AS row_count, num_nulls, percent_null
FROM {metrics_table}
WHERE log_type = 'INPUT' AND slice_key IS NULL AND column_name <> ':table'
  AND window.end >= TIMESTAMP '{start}' AND window.end <= TIMESTAMP '{end}'
"""


class UnityCatalogDataQualityExtractor:
    def __init__(
        self,
        config: UnityCatalogDataQualityConfig,
        report: UnityCatalogReport,
        proxy: UnityCatalogApiProxy,
        dataset_urn_builder: Callable[[TableReference], str],
        end_time: datetime,
    ) -> None:
        self.config = config
        self.report = report
        self.proxy = proxy
        self.dataset_urn_builder = dataset_urn_builder
        self.end_time = end_time
        # Assertion definitions are emitted once; run events are emitted per window.
        self._emitted_assertion_urns: Set[str] = set()

    def get_workunits(self, tables: Iterable[Table]) -> Iterable[MetadataWorkUnit]:
        if not self.proxy.data_quality_available():
            self.report.warning(
                title="Data quality assertions unavailable",
                message="The installed databricks-sdk does not expose the data "
                "quality API (requires databricks-sdk>=0.68.0). Skipping "
                "data-quality assertion extraction.",
            )
            return

        for table in tables:
            yield from self._process_table(table)

    def _process_table(self, table: Table) -> Iterable[MetadataWorkUnit]:
        if not table.table_id:
            return

        try:
            monitor = self.proxy.get_quality_monitor(table.table_id)
        except Exception as e:
            # NotFound (no monitor) is already degraded to None inside the proxy; an
            # exception here is a real API error (auth, rate limit, transient) and
            # must not be mistaken for an unmonitored table.
            self.report.num_quality_monitor_errors += 1
            self.report.warning(
                title="Failed to fetch data quality monitor",
                message="Could not determine whether the table has a data quality "
                "monitor.",
                context=table.ref.qualified_table_name,
                exc=e,
            )
            return

        if monitor is None:
            self.report.num_quality_tables_without_monitor += 1
            return

        profiling = monitor.data_profiling_config
        metrics_table = profiling.profile_metrics_table_name if profiling else None
        if not metrics_table:
            self.report.quality_monitors_missing_metrics.append(
                table.ref.qualified_table_name
            )
            return

        self.report.num_quality_monitors_found += 1
        dataset_urn = self.dataset_urn_builder(table.ref)

        try:
            rows = self.proxy.run_sql_query(self._build_query(metrics_table))
        except Exception as e:
            self.report.num_quality_metric_query_failures += 1
            self.report.warning(
                title="Failed to read data quality metrics",
                message="Could not query the monitor profile-metrics table.",
                context=table.ref.qualified_table_name,
                exc=e,
            )
            return

        for row in rows:
            yield from self._process_column(dataset_urn, row.asDict())

    def _process_column(
        self, dataset_urn: str, record: Dict[str, object]
    ) -> Iterable[MetadataWorkUnit]:
        column = str(record.get("column_name") or "")
        num_nulls = record.get(COMPLETENESS_METRIC)
        if not column or num_nulls is None:
            return
        if not self.config.column_pattern.allowed(column):
            return

        result = self._build_result(column, record, float(num_nulls))  # type: ignore[arg-type]
        assertion_urn = make_dq_assertion_urn(dataset_urn, result.column, result.metric)

        if assertion_urn not in self._emitted_assertion_urns:
            self._emitted_assertion_urns.add(assertion_urn)
            self.report.num_quality_assertions_emitted += 1
            yield build_assertion_info_mcp(
                result, assertion_urn, dataset_urn
            ).as_workunit()

        self.report.num_quality_run_events_emitted += 1
        yield build_assertion_run_event_mcp(
            result, assertion_urn, dataset_urn
        ).as_workunit()

    def _build_result(
        self,
        column: str,
        record: Dict[str, object],
        num_nulls: float,
    ) -> DataQualityAssertion:
        timestamp_millis = self._to_millis(record.get("window_end"))
        monitor_version = record.get("monitor_version")

        native: Dict[str, str] = {}
        for key in ("percent_null", "row_count", "monitor_version"):
            value = record.get(key)
            if value is not None:
                native[key] = str(value)

        return DataQualityAssertion(
            column=column,
            metric=COMPLETENESS_METRIC,
            threshold=COMPLETENESS_THRESHOLD,
            observed=num_nulls,
            passed=num_nulls <= COMPLETENESS_THRESHOLD,
            timestamp_millis=timestamp_millis,
            run_id=f"{monitor_version}:{timestamp_millis}",
            native_results=native,
        )

    def _build_query(self, metrics_table: str) -> str:
        quoted_table = ".".join(f"`{part}`" for part in metrics_table.split("."))
        start = self.end_time - timedelta(days=self.config.max_window_days)
        return _PROFILE_METRICS_QUERY.format(
            metrics_table=quoted_table,
            start=start.strftime(_TS_FORMAT),
            end=self.end_time.strftime(_TS_FORMAT),
        )

    def _to_millis(self, window_end: object) -> int:
        if isinstance(window_end, datetime):
            return make_ts_millis(window_end)
        return make_ts_millis(self.end_time.astimezone(timezone.utc))
