from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Dict, List, Optional, Set, cast

from datahub.emitter.mce_builder import make_dataset_urn, make_ts_millis
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.assertion import (
    make_expectation_assertion_urn,
)
from datahub.ingestion.source.unity.config import (
    UnityCatalogPipelineExpectationsConfig,
)
from datahub.ingestion.source.unity.pipeline_expectations import (
    UnityCatalogPipelineExpectationsExtractor,
)
from datahub.ingestion.source.unity.proxy_types import TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    DatasetAssertionScopeClass,
)

CATALOG = "cat"
SCHEMA = "sch"
TS = "2026-09-11T00:00:00.000Z"
TS_MILLIS = make_ts_millis(datetime(2026, 9, 11, tzinfo=timezone.utc))


def _dataset_urn(ref: TableReference) -> str:
    return make_dataset_urn("databricks", ref.qualified_table_name, "PROD")


def _expectation(
    name: str, dataset: str, passed: int, failed: int
) -> Dict[str, object]:
    return {
        "name": name,
        "dataset": dataset,
        "passed_records": passed,
        "failed_records": failed,
    }


def _event(
    update_id: str,
    expectations: List[Dict[str, object]],
    timestamp: str = TS,
    event_type: str = "flow_progress",
) -> Dict[str, object]:
    return {
        "event_type": event_type,
        "timestamp": timestamp,
        "origin": {"update_id": update_id},
        "details": {"flow_progress": {"data_quality": {"expectations": expectations}}},
    }


class _FakeProxy:
    def __init__(
        self,
        events: Optional[List[Dict[str, object]]] = None,
        target: Optional[tuple] = (CATALOG, SCHEMA),
        pipeline_name: str = "my_pipeline",
        list_error: Optional[Exception] = None,
        events_error: Optional[Exception] = None,
    ) -> None:
        self._events = events or []
        self._target = target
        self._pipeline = SimpleNamespace(pipeline_id="pid-1", name=pipeline_name)
        self._list_error = list_error
        self._events_error = events_error

    def list_pipelines(self) -> List[object]:
        if self._list_error is not None:
            raise self._list_error
        return [self._pipeline]

    def get_pipeline_target(self, pipeline_id: str) -> Optional[tuple]:
        return self._target

    def get_pipeline_events(self, pipeline_id: str) -> List[Dict[str, object]]:
        if self._events_error is not None:
            raise self._events_error
        return self._events


def _extractor(
    proxy: _FakeProxy, config: Optional[UnityCatalogPipelineExpectationsConfig] = None
) -> UnityCatalogPipelineExpectationsExtractor:
    return UnityCatalogPipelineExpectationsExtractor(
        config=config or UnityCatalogPipelineExpectationsConfig(enabled=True),
        report=UnityCatalogReport(),
        proxy=cast(object, proxy),  # type: ignore[arg-type]
        dataset_urn_builder=_dataset_urn,
    )


def _aspect_names(workunits: List[MetadataWorkUnit]) -> List[str]:
    names = []
    for wu in workunits:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        assert wu.metadata.aspectName is not None
        names.append(wu.metadata.aspectName)
    return names


def _run_events(workunits: List[MetadataWorkUnit]) -> List[AssertionRunEventClass]:
    events = []
    for wu in workunits:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        if isinstance(wu.metadata.aspect, AssertionRunEventClass):
            events.append(wu.metadata.aspect)
    return events


def _result_types(workunits: List[MetadataWorkUnit]) -> Set[str]:
    types: Set[str] = set()
    for event in _run_events(workunits):
        assert event.result is not None
        types.add(str(event.result.type))
    return types


def test_emits_assertion_and_run_event_per_expectation() -> None:
    events = [
        _event(
            "u1",
            [
                _expectation("valid_id", "orders", passed=100, failed=0),
                _expectation("non_null_amount", "orders", passed=90, failed=10),
            ],
        )
    ]
    extractor = _extractor(_FakeProxy(events=events))
    wus = list(extractor.get_workunits())

    names = _aspect_names(wus)
    assert names.count("assertionInfo") == 2
    assert names.count("assertionRunEvent") == 2
    # valid_id passes (0 failed), non_null_amount fails (10 failed).
    assert _result_types(wus) == {
        AssertionResultTypeClass.SUCCESS,
        AssertionResultTypeClass.FAILURE,
    }
    report = extractor.report
    assert report.num_pipelines_scanned == 1
    assert report.num_pipeline_expectations_found == 2
    assert report.num_expectation_assertions_emitted == 2
    assert report.num_expectation_run_events_emitted == 2


def test_info_mcp_is_dataset_row_scope_and_resolves_dataset_urn() -> None:
    events = [_event("u1", [_expectation("valid_id", "orders", 100, 0)])]
    wus = list(_extractor(_FakeProxy(events=events)).get_workunits())
    info = next(
        wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, AssertionInfoClass)
    )
    assert isinstance(info, AssertionInfoClass)
    assert info.customAssertion is not None
    assert info.customAssertion.scope == DatasetAssertionScopeClass.DATASET_ROWS
    expected_urn = _dataset_urn(
        TableReference(metastore=None, catalog=CATALOG, schema=SCHEMA, table="orders")
    )
    assert info.customAssertion.entity == expected_urn


def test_only_latest_update_is_aggregated() -> None:
    # Events come back newest-first; expectations from an older update are ignored.
    events = [
        _event("u2", [_expectation("valid_id", "orders", 200, 0)]),
        _event("u1", [_expectation("stale_check", "orders", 1, 1)]),
    ]
    wus = list(_extractor(_FakeProxy(events=events)).get_workunits())
    run_events = _run_events(wus)
    assert len(run_events) == 1
    assert run_events[0].runId == "u2"


def test_sums_records_across_flow_progress_events_in_update() -> None:
    # A single update emits per-micro-batch snapshots that must be summed.
    events = [
        _event("u1", [_expectation("non_null_amount", "orders", 3, 0)]),
        _event("u1", [_expectation("non_null_amount", "orders", 4, 1)]),
    ]
    wus = list(_extractor(_FakeProxy(events=events)).get_workunits())
    run_events = _run_events(wus)
    assert len(run_events) == 1
    assert run_events[0].result is not None
    # 1 failed record total -> FAILURE, actualAggValue == failed_records.
    assert run_events[0].result.type == AssertionResultTypeClass.FAILURE
    assert run_events[0].result.actualAggValue == 1.0
    assert run_events[0].timestampMillis == TS_MILLIS


def test_pipeline_without_uc_target_is_skipped() -> None:
    extractor = _extractor(_FakeProxy(events=[], target=None))
    assert list(extractor.get_workunits()) == []
    assert list(extractor.report.pipelines_without_uc_target) == ["my_pipeline"]


def test_event_read_error_is_reported() -> None:
    proxy = _FakeProxy(events_error=RuntimeError("permission denied"))
    extractor = _extractor(proxy)
    assert list(extractor.get_workunits()) == []
    assert extractor.report.num_pipeline_event_errors == 1


def test_list_pipelines_error_is_reported() -> None:
    proxy = _FakeProxy(list_error=RuntimeError("boom"))
    extractor = _extractor(proxy)
    assert list(extractor.get_workunits()) == []
    assert extractor.report.num_pipelines_scanned == 0


def test_pipeline_pattern_filters_pipelines() -> None:
    config = UnityCatalogPipelineExpectationsConfig(enabled=True)
    config.pipeline_pattern.deny.append("my_pipeline")
    events = [_event("u1", [_expectation("valid_id", "orders", 100, 0)])]
    extractor = _extractor(_FakeProxy(events=events), config)
    assert list(extractor.get_workunits()) == []
    assert extractor.report.num_pipelines_scanned == 0


def test_assertion_urn_is_deterministic_and_namespaced() -> None:
    dataset_urn = _dataset_urn(
        TableReference(metastore=None, catalog=CATALOG, schema=SCHEMA, table="orders")
    )
    other = make_dataset_urn("databricks", "cat.sch.other", "PROD")
    assert make_expectation_assertion_urn(
        dataset_urn, "pid-1", "valid_id"
    ) == make_expectation_assertion_urn(dataset_urn, "pid-1", "valid_id")
    assert make_expectation_assertion_urn(
        dataset_urn, "pid-1", "valid_id"
    ) != make_expectation_assertion_urn(dataset_urn, "pid-1", "other_check")
    assert make_expectation_assertion_urn(
        dataset_urn, "pid-1", "valid_id"
    ) != make_expectation_assertion_urn(dataset_urn, "pid-2", "valid_id")
    assert make_expectation_assertion_urn(
        dataset_urn, "pid-1", "valid_id"
    ) != make_expectation_assertion_urn(other, "pid-1", "valid_id")
