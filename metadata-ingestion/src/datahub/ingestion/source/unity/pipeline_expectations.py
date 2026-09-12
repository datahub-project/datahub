import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional, Tuple

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
    latest_ts_millis: int = 0


@dataclass
class _PipelineExpectations:
    update_id: Optional[str] = None
    # Keyed by (expectation dataset name, expectation name).
    totals: Dict[Tuple[str, str], _ExpectationTotals] = field(default_factory=dict)


class UnityCatalogPipelineExpectationsExtractor:
    def __init__(
        self,
        config: UnityCatalogPipelineExpectationsConfig,
        report: UnityCatalogReport,
        proxy: UnityCatalogApiProxy,
        dataset_urn_builder: Callable[[TableReference], str],
    ) -> None:
        self.config = config
        self.report = report
        self.proxy = proxy
        self.dataset_urn_builder = dataset_urn_builder

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
        target = self.proxy.get_pipeline_target(pipeline_id)
        if target is None:
            self.report.pipelines_without_uc_target.append(name or pipeline_id)
            return
        catalog, schema = target

        try:
            events = self.proxy.get_pipeline_events(pipeline_id)
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

        aggregated = self._aggregate_latest_update(events)
        if aggregated.update_id is None:
            return

        for (dataset_name, expectation), totals in aggregated.totals.items():
            self.report.num_pipeline_expectations_found += 1
            yield from self._emit_expectation(
                pipeline_id,
                aggregated.update_id,
                catalog,
                schema,
                dataset_name,
                expectation,
                totals,
            )

    def _aggregate_latest_update(
        self, events: List[Dict[str, object]]
    ) -> _PipelineExpectations:
        # Events come back newest-first; the first expectation-bearing event fixes the
        # update we report, and we ignore expectations from earlier updates.
        result = _PipelineExpectations()
        for event in events:
            if event.get("event_type") != _FLOW_PROGRESS:
                continue
            expectations = _extract_expectations(event)
            if not expectations:
                continue
            update_id = _extract_update_id(event)
            if result.update_id is None:
                result.update_id = update_id
            if update_id != result.update_id:
                continue

            ts_millis = _parse_ts_millis(event.get("timestamp"))
            for item in expectations:
                dataset_name = str(item.get("dataset") or "")
                expectation = str(item.get("name") or "")
                if not dataset_name or not expectation:
                    continue
                totals = result.totals.setdefault(
                    (dataset_name, expectation), _ExpectationTotals()
                )
                totals.passed += _as_int(item.get("passed_records"))
                totals.failed += _as_int(item.get("failed_records"))
                totals.latest_ts_millis = max(totals.latest_ts_millis, ts_millis)
        return result

    def _emit_expectation(
        self,
        pipeline_id: str,
        update_id: str,
        catalog: str,
        schema: str,
        dataset_name: str,
        expectation: str,
        totals: _ExpectationTotals,
    ) -> Iterable[MetadataWorkUnit]:
        ref = TableReference(
            metastore=None, catalog=catalog, schema=schema, table=dataset_name
        )
        dataset_urn = self.dataset_urn_builder(ref)
        assertion_urn = make_expectation_assertion_urn(
            dataset_urn, pipeline_id, expectation
        )

        result = PipelineExpectationAssertion(
            name=expectation,
            pipeline_id=pipeline_id,
            failed_records=totals.failed,
            passed_records=totals.passed,
            timestamp_millis=totals.latest_ts_millis,
            run_id=update_id,
            native_results={
                "passed_records": str(totals.passed),
                "failed_records": str(totals.failed),
            },
        )

        self.report.num_expectation_assertions_emitted += 1
        yield build_expectation_info_mcp(
            result, assertion_urn, dataset_urn
        ).as_workunit()

        self.report.num_expectation_run_events_emitted += 1
        yield build_expectation_run_event_mcp(
            result, assertion_urn, dataset_urn
        ).as_workunit()


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


def _parse_ts_millis(value: object) -> int:
    if isinstance(value, str) and value:
        try:
            normalized = value.replace("Z", "+00:00")
            return make_ts_millis(datetime.fromisoformat(normalized))
        except ValueError:
            pass
    return make_ts_millis(datetime.now(timezone.utc))


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
