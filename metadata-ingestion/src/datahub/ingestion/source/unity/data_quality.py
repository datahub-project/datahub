import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Iterable, List, Optional, Set

from datahub.emitter.mce_builder import make_ts_millis
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.assertion import (
    COMMIT_FRESHNESS_CHECK,
    DAILY_ROW_COUNT_CHECK,
    TOTAL_ROW_COUNT_CHECK,
    CheckRun,
    CheckSpec,
    build_assertion_info_mcp,
    build_assertion_run_event_mcp,
    make_dq_assertion_urn,
    map_status_to_result_type,
)
from datahub.ingestion.source.unity.config import UnityCatalogDataQualityConfig
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import Table, TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport

logger = logging.getLogger(__name__)

# Databricks anomaly-detection writes one row per scan per table to this metastore-wide
# system table, with a real verdict (status) per check. We read those verdicts rather
# than deriving our own from raw profile metrics.
RESULTS_TABLE = "system.data_quality_monitoring.table_results"

# Keep fractional seconds so a scan at mid-second is not truncated below the query's
# upper bound (which would drop that scan).
_TS_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

# The window bounds carry an explicit +00:00 offset. Without it Databricks parses a
# TIMESTAMP literal in the warehouse's session timezone, shifting the lookback window
# by the workspace offset (and dropping/including the wrong scans when the window is
# small).
_TS_LITERAL_FORMAT = _TS_FORMAT + "+00:00"

_QUERY_TEMPLATE = """\
SELECT {columns}
FROM {table}
WHERE event_time >= TIMESTAMP '{start}' AND event_time <= TIMESTAMP '{end}'
  AND table_id IN ({table_ids})
ORDER BY event_time
"""


def _as_utc(dt: datetime) -> datetime:
    # The SQL connector may hand back naive datetimes that denote UTC; attach the zone
    # so .timestamp() does not reinterpret them in the local timezone.
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_iso(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


@dataclass(frozen=True)
class _CheckColumns:
    """Binds a CheckSpec to its struct in the results table and the shape of its run
    event. Row-count checks report ``last_value`` as the aggregate and the predicted
    min/max band on nativeResults; the freshness check's values are timestamps, so they
    all go on nativeResults instead of actualAggValue."""

    spec: CheckSpec
    struct_path: str
    is_row_count: bool

    def select_columns(self) -> List[str]:
        prefix = self.spec.native_type
        path = self.struct_path
        columns = [
            f"{path}.status AS {prefix}_status",
            f"{path}.error_code AS {prefix}_error_code",
            f"{path}.last_value AS {prefix}_last_value",
        ]
        if self.is_row_count:
            columns.append(
                f"{path}.min_predicted_value AS {prefix}_min_predicted_value"
            )
            columns.append(
                f"{path}.max_predicted_value AS {prefix}_max_predicted_value"
            )
        else:
            columns.append(f"{path}.predicted_value AS {prefix}_predicted_value")
        return columns

    def build_run(
        self, record: Dict[str, object], timestamp_millis: int
    ) -> Optional[CheckRun]:
        prefix = self.spec.native_type
        status = record.get(f"{prefix}_status")
        error_code = record.get(f"{prefix}_error_code")
        result_type = map_status_to_result_type(
            str(status) if status is not None else None,
            str(error_code) if error_code is not None else None,
        )
        if result_type is None:
            # Training / Unknown / missing — not a pass or fail verdict.
            return None

        native: Dict[str, str] = {}
        if error_code is not None:
            native["error_code"] = str(error_code)

        last_value = record.get(f"{prefix}_last_value")
        actual_agg_value: Optional[float] = None
        if self.is_row_count:
            if last_value is not None:
                actual_agg_value = float(last_value)  # type: ignore[arg-type]
            for suffix in ("min_predicted_value", "max_predicted_value"):
                value = record.get(f"{prefix}_{suffix}")
                if value is not None:
                    native[suffix] = str(value)
        else:
            if last_value is not None:
                native["last_value"] = _to_iso(last_value)
            predicted = record.get(f"{prefix}_predicted_value")
            if predicted is not None:
                native["predicted_value"] = _to_iso(predicted)

        return CheckRun(
            result_type=result_type,
            timestamp_millis=timestamp_millis,
            run_id=str(timestamp_millis),
            actual_agg_value=actual_agg_value,
            native_results=native,
        )


_CHECKS: List[_CheckColumns] = [
    _CheckColumns(COMMIT_FRESHNESS_CHECK, "freshness.commit_freshness", False),
    _CheckColumns(DAILY_ROW_COUNT_CHECK, "completeness.daily_row_count", True),
    _CheckColumns(TOTAL_ROW_COUNT_CHECK, "completeness.total_row_count", True),
]


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
        # Assertion definitions are emitted once; run events are emitted per scan.
        self._emitted_assertion_urns: Set[str] = set()

    def get_workunits(self, tables: Iterable[Table]) -> Iterable[MetadataWorkUnit]:
        tables_by_id: Dict[str, Table] = {
            table.table_id: table for table in tables if table.table_id
        }
        if not tables_by_id:
            return

        try:
            rows = self.proxy.run_sql_query(self._build_query(list(tables_by_id)))
        except Exception as e:
            self.report.num_quality_query_failures += 1
            self.report.warning(
                title="Failed to read data quality monitoring results",
                message="Could not query the anomaly-detection results system table. "
                "The ingesting principal needs an explicit SELECT grant on "
                f"{RESULTS_TABLE} (account-admin only by default).",
                exc=e,
            )
            return

        for row in rows:
            yield from self._process_row(row.asDict(), tables_by_id)

    def _process_row(
        self, record: Dict[str, object], tables_by_id: Dict[str, Table]
    ) -> Iterable[MetadataWorkUnit]:
        table_id = record.get("table_id")
        table = tables_by_id.get(str(table_id)) if table_id is not None else None
        if table is None:
            return

        self.report.num_quality_scan_rows_read += 1
        timestamp_millis = self._to_millis(record.get("event_time"))
        dataset_urn = self.dataset_urn_builder(table.ref)

        for check in _CHECKS:
            run = check.build_run(record, timestamp_millis)
            if run is None:
                self.report.num_quality_checks_skipped += 1
                continue

            assertion_urn = make_dq_assertion_urn(dataset_urn, check.spec.key)
            if assertion_urn not in self._emitted_assertion_urns:
                self._emitted_assertion_urns.add(assertion_urn)
                self.report.num_quality_assertions_emitted += 1
                yield build_assertion_info_mcp(
                    check.spec, assertion_urn, dataset_urn
                ).as_workunit()

            self.report.num_quality_run_events_emitted += 1
            yield build_assertion_run_event_mcp(
                run, assertion_urn, dataset_urn
            ).as_workunit()

    def _build_query(self, table_ids: List[str]) -> str:
        columns = ["table_id", "event_time"]
        for check in _CHECKS:
            columns.extend(check.select_columns())
        # table_ids are Databricks-issued UUIDs; quote defensively regardless.
        id_list = ", ".join("'" + tid.replace("'", "''") + "'" for tid in table_ids)
        end = _as_utc(self.end_time)
        start = end - timedelta(days=self.config.max_window_days)
        return _QUERY_TEMPLATE.format(
            columns=",\n       ".join(columns),
            table=RESULTS_TABLE,
            start=start.strftime(_TS_LITERAL_FORMAT),
            end=end.strftime(_TS_LITERAL_FORMAT),
            table_ids=id_list,
        )

    def _to_millis(self, event_time: object) -> int:
        dt = _as_utc(event_time) if isinstance(event_time, datetime) else None
        if dt is None and isinstance(event_time, str):
            for fmt in (_TS_FORMAT, "%Y-%m-%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(event_time, fmt).replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    continue
        if dt is None:
            # event_time is a non-null TIMESTAMP in the system table, so this is
            # unexpected; stamp the ingestion end_time rather than dropping the run.
            logger.warning(
                "Unparseable data quality event_time %r; using ingestion end_time",
                event_time,
            )
            dt = _as_utc(self.end_time)
        return make_ts_millis(dt)
