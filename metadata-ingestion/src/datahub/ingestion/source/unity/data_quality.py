import logging
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Iterable, Optional, Set

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

_TS_FORMAT = "%Y-%m-%d %H:%M:%S"

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
    """Publishes Databricks data-quality monitor results as DataHub assertions.

    For each ingested table that has a monitor, reads the profile-metrics table over
    SQL (windowed on the ingestion end_time) and emits one idempotent completeness
    assertion per monitored column, plus a per-window run result.
    """

    def __init__(
        self,
        config: UnityCatalogDataQualityConfig,
        report: UnityCatalogReport,
        proxy: UnityCatalogApiProxy,
        dataset_urn_builder: Callable[[TableReference], str],
        end_time: datetime,
        platform_instance: Optional[str],
        env: str,
    ) -> None:
        self.config = config
        self.report = report
        self.proxy = proxy
        self.dataset_urn_builder = dataset_urn_builder
        self.end_time = end_time
        self.platform_instance = platform_instance
        self.env = env
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

        monitor = self.proxy.get_quality_monitor(table.table_id)
        profiling = getattr(monitor, "data_profiling_config", None) if monitor else None
        metrics_table = (
            getattr(profiling, "profile_metrics_table_name", None)
            if profiling
            else None
        )
        if not metrics_table:
            self.report.num_quality_tables_without_monitor += 1
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
            yield from self._process_column(table.ref, dataset_urn, row.asDict())

    def _process_column(
        self, ref: TableReference, dataset_urn: str, record: Dict[str, object]
    ) -> Iterable[MetadataWorkUnit]:
        column = str(record.get("column_name") or "")
        num_nulls = record.get(COMPLETENESS_METRIC)
        if not column or num_nulls is None:
            return
        if not self.config.column_pattern.allowed(column):
            return

        result = self._build_result(ref, column, record, float(num_nulls))  # type: ignore[arg-type]
        assertion_urn = make_dq_assertion_urn(result, self.platform_instance, self.env)

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
        ref: TableReference,
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
            table_qualified_name=ref.qualified_table_name,
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
