import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

from datahub.emitter.mce_builder import make_ts_millis
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.assertion import (
    PipelineExpectationAssertion,
    build_expectation_info_mcp,
    build_expectation_run_event_mcp,
    make_expectation_assertion_urn,
)
from datahub.ingestion.source.unity.config import (
    UnityCatalogPipelineExpectationsConfig,
)
from datahub.ingestion.source.unity.identifier_helper import (
    split_databricks_identifier,
)
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport

logger = logging.getLogger(__name__)

_FLOW_PROGRESS = "flow_progress"


@dataclass
class _ExpectationTotals:
    # Databricks emits one flow_progress event per micro-batch; the official event-log
    # query sums passed/failed across a single update, so we do the same.
    passed: int = 0
    failed: int = 0
    # Whether any micro-batch in this update actually reported a failed_records count.
    # Databricks can abort an update (e.g. an `expect_or_fail` violation) without
    # writing counts, and a missing count must not be read as zero failures (a false
    # pass) — so an update with no count produces no run event instead.
    has_count: bool = False
    latest_ts_millis: int = 0
    # The expectation's action (ALLOW / DROP / FAIL) is constant across an update's
    # micro-batches; keep the first one we see. Often absent from the event payload.
    action: Optional[str] = None


@dataclass
class _WindowedExpectations:
    # update_id -> {(dataset name, expectation name): totals}. Updates are kept in the
    # order encountered (newest-first, as the event log returns them); dict preserves
    # insertion order. One run event is emitted per update so faster-than-ingestion
    # updates are not silently dropped.
    updates: Dict[str, Dict[Tuple[str, str], _ExpectationTotals]] = field(
        default_factory=dict
    )


class UnityCatalogPipelineExpectationsExtractor:
    def __init__(
        self,
        config: UnityCatalogPipelineExpectationsConfig,
        report: UnityCatalogReport,
        proxy: UnityCatalogApiProxy,
        dataset_urn_builder: Callable[[TableReference], str],
        metastore: Optional[str] = None,
        resolve_ingested_ref: Optional[
            Callable[[TableReference], Optional[TableReference]]
        ] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> None:
        self.config = config
        self.report = report
        self.proxy = proxy
        self.dataset_urn_builder = dataset_urn_builder
        # Bound extraction to the same [start_time, end_time] window as usage and
        # lineage. Events are newest-first, so once one older than start_time is seen
        # paging stops — this also caps REST calls on continuous pipelines whose single
        # long-running update would otherwise be paged to exhaustion. When unset (unit
        # tests), the window is open and every returned event is considered.
        self.start_time_millis = make_ts_millis(start_time) if start_time else None
        self.end_time_millis = make_ts_millis(end_time) if end_time else None
        # Metastore id embedded in dataset URNs when `include_metastore` is on; the
        # assertion URN must resolve to the same dataset the connector published.
        self.metastore = metastore
        # Pipeline expectations are discovered from the event log independently of
        # table ingestion, so a pipeline can reference datasets the recipe excluded
        # (or intermediate pipeline views). The source resolves an event-log dataset
        # to the canonical ingested TableReference (or None if it was never
        # published), which both filters out excluded datasets and gives us the
        # catalog's exact casing for the URN — Lakeflow can report a different case.
        self.resolve_ingested_ref = resolve_ingested_ref

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        try:
            pipelines = self.proxy.list_pipelines()
        except Exception as e:
            self.report.warning(
                title="Failed to list pipelines",
                message="Could not enumerate Lakeflow pipelines for expectation "
                "assertion extraction.",
                exc=e,
            )
            return

        for pipeline in pipelines:
            pipeline_id = pipeline.pipeline_id
            name = pipeline.name or ""
            if not pipeline_id or not self.config.pipeline_pattern.allowed(name):
                continue
            self.report.num_pipelines_scanned += 1
            yield from self._process_pipeline(pipeline_id, name)

    def _process_pipeline(
        self, pipeline_id: str, name: str
    ) -> Iterable[MetadataWorkUnit]:
        try:
            target = self.proxy.get_pipeline_target(pipeline_id)
        except Exception as e:
            # Resolving the target reads the pipeline spec; a permission error,
            # deletion, or an older SDK without `spec.schema` must skip this
            # pipeline, not abort extraction for the rest.
            self.report.warning(
                title="Failed to read pipeline target",
                message="Could not resolve the pipeline's Unity Catalog target for "
                "expectation assertion extraction.",
                context=name or pipeline_id,
                exc=e,
            )
            return
        if target is None:
            self.report.pipelines_without_uc_target.append(name or pipeline_id)
            return
        catalog, schema = target

        try:
            # get_pipeline_events pages lazily; aggregation consumes only back to the
            # window start, so the try must wrap the iteration, not just the call.
            aggregated = self._aggregate_window(
                self.proxy.get_pipeline_events(pipeline_id)
            )
        except Exception as e:
            # The event log is owner-only over SQL, but the REST events endpoint only
            # needs pipeline read access; a failure here is usually a permission or
            # transient API error, so surface it rather than silently emitting nothing.
            self.report.num_pipeline_event_errors += 1
            self.report.warning(
                title="Failed to read pipeline events",
                message="Could not read the pipeline event log for expectation "
                "metrics.",
                context=name or pipeline_id,
                exc=e,
            )
            return

        yield from self._emit_expectations(pipeline_id, catalog, schema, aggregated)

    def _aggregate_window(
        self, events: Iterable[Dict[str, object]]
    ) -> _WindowedExpectations:
        # Events come back newest-first. Aggregate every update whose events fall in the
        # [start_time, end_time] window, summing an update's per-micro-batch snapshots.
        # The first event older than start_time ends iteration (and, because the proxy
        # pages lazily, stops further REST calls); events newer than end_time are
        # skipped but paging continues back toward the window.
        result = _WindowedExpectations()
        for event in events:
            ts_millis = _parse_ts_millis(event.get("timestamp"))
            if (
                ts_millis is not None
                and self.start_time_millis is not None
                and ts_millis < self.start_time_millis
            ):
                # This event predates the window, so the update it belongs to extends
                # before start_time. Whatever we accumulated for that update is only a
                # partial slice of a longer-running (e.g. continuous) update whose single
                # update_id lives for its whole lifetime. Publishing that partial sum as
                # the update's verdict would misreport counts and, because the run is
                # keyed on update_id, let a later window overwrite an earlier failure with
                # a false pass. Drop it — an update's events are contiguous in time, so
                # only the one update active at the window boundary can straddle it.
                straddling = _extract_update_id(event)
                if straddling is not None and straddling in result.updates:
                    del result.updates[straddling]
                    self.report.num_pipeline_partial_updates_skipped += 1
                break
            if event.get("event_type") != _FLOW_PROGRESS:
                continue
            if (
                ts_millis is not None
                and self.end_time_millis is not None
                and ts_millis > self.end_time_millis
            ):
                continue
            expectations = _extract_expectations(event)
            if not expectations:
                continue
            update_id = _extract_update_id(event)
            if update_id is None:
                continue
            per_update = result.updates.setdefault(update_id, {})

            for item in expectations:
                dataset_name = str(item.get("dataset") or "")
                expectation = str(item.get("name") or "")
                if not dataset_name or not expectation:
                    continue
                totals = per_update.setdefault(
                    (dataset_name, expectation), _ExpectationTotals()
                )
                totals.passed += _as_int(item.get("passed_records"))
                failed = item.get("failed_records")
                if failed is not None:
                    totals.failed += _as_int(failed)
                    totals.has_count = True
                totals.latest_ts_millis = max(totals.latest_ts_millis, ts_millis or 0)
                action = item.get("action")
                if action is not None and totals.action is None:
                    totals.action = str(action)
        return result

    def _emit_expectations(
        self,
        pipeline_id: str,
        catalog: str,
        schema: str,
        aggregated: _WindowedExpectations,
    ) -> Iterable[MetadataWorkUnit]:
        # Regroup by expectation: one assertion carries the run of every windowed
        # update, keyed on the update id, so re-ingesting an overlapping window updates
        # the same runs instead of duplicating assertions.
        by_expectation: Dict[Tuple[str, str], List[Tuple[str, _ExpectationTotals]]] = {}
        for update_id, per_update in aggregated.updates.items():
            for key, totals in per_update.items():
                by_expectation.setdefault(key, []).append((update_id, totals))

        emitted_assertions: Set[str] = set()
        for (dataset_name, expectation), runs in by_expectation.items():
            self.report.num_pipeline_expectations_found += 1

            ref = _resolve_table_ref(dataset_name, catalog, schema, self.metastore)
            if self.resolve_ingested_ref is not None:
                # Build the URN from the ingested ref, not the event-log ref, so a
                # casing difference between Lakeflow and the Unity Catalog API can't
                # attach the assertion to a dataset that was never published.
                ingested_ref = self.resolve_ingested_ref(ref)
                if ingested_ref is None:
                    self.report.num_pipeline_expectation_datasets_filtered += 1
                    continue
                ref = ingested_ref

            # Drop updates that aborted without writing counts: a missing failed_records
            # is not a zero-failure pass, so recording one would be a false green.
            scored_runs = [(uid, t) for uid, t in runs if t.has_count]
            if not scored_runs:
                self.report.num_pipeline_expectations_without_counts += 1
                continue

            dataset_urn = self.dataset_urn_builder(ref)
            assertion_urn = make_expectation_assertion_urn(
                dataset_urn, pipeline_id, expectation
            )

            for update_id, totals in scored_runs:
                result = PipelineExpectationAssertion(
                    name=expectation,
                    pipeline_id=pipeline_id,
                    failed_records=totals.failed,
                    passed_records=totals.passed,
                    action=totals.action,
                    timestamp_millis=totals.latest_ts_millis
                    or make_ts_millis(datetime.now(timezone.utc)),
                    run_id=update_id,
                    native_results={
                        "passed_records": str(totals.passed),
                        "failed_records": str(totals.failed),
                    },
                )

                if assertion_urn not in emitted_assertions:
                    emitted_assertions.add(assertion_urn)
                    self.report.num_expectation_assertions_emitted += 1
                    yield build_expectation_info_mcp(
                        result, assertion_urn, dataset_urn
                    ).as_workunit()

                self.report.num_expectation_run_events_emitted += 1
                yield build_expectation_run_event_mcp(
                    result, assertion_urn, dataset_urn
                ).as_workunit()


def _resolve_table_ref(
    dataset_name: str, catalog: str, schema: str, metastore: Optional[str] = None
) -> TableReference:
    # Lakeflow reports the expectation's dataset fully-qualified (catalog.schema.table)
    # on Unity Catalog pipelines, but can report a bare or schema-qualified name on
    # older pipelines. Split on unquoted dots (identifiers may embed backtick-quoted
    # dots) and fall back to the pipeline target for any missing component; an
    # unbalanced-quote result degrades to treating the whole string as a table name.
    parts = split_databricks_identifier(dataset_name) or [dataset_name]
    if len(parts) >= 3:
        return TableReference(
            metastore=metastore, catalog=parts[-3], schema=parts[-2], table=parts[-1]
        )
    if len(parts) == 2:
        return TableReference(
            metastore=metastore, catalog=catalog, schema=parts[0], table=parts[1]
        )
    return TableReference(
        metastore=metastore, catalog=catalog, schema=schema, table=parts[0]
    )


def _extract_expectations(event: Dict[str, object]) -> List[Dict[str, object]]:
    details = event.get("details")
    if not isinstance(details, dict):
        return []
    flow_progress = details.get(_FLOW_PROGRESS)
    if not isinstance(flow_progress, dict):
        return []
    data_quality = flow_progress.get("data_quality")
    if not isinstance(data_quality, dict):
        return []
    expectations = data_quality.get("expectations")
    if not isinstance(expectations, list):
        return []
    return [item for item in expectations if isinstance(item, dict)]


def _extract_update_id(event: Dict[str, object]) -> Optional[str]:
    origin = event.get("origin")
    if isinstance(origin, dict):
        update_id = origin.get("update_id")
        if update_id is not None:
            return str(update_id)
    return None


def _parse_ts_millis(value: object) -> Optional[int]:
    # Returns None for a missing or unparseable timestamp so the caller can decide
    # (the run event falls back to "now", while window filtering simply keeps the
    # event rather than excluding it on an unknown time).
    if isinstance(value, str) and value:
        try:
            normalized = value.replace("Z", "+00:00")
            return make_ts_millis(datetime.fromisoformat(normalized))
        except ValueError:
            pass
    return None


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0
