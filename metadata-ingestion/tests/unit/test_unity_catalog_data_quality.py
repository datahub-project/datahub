from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Dict, List, Optional, cast

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.assertion import (
    COMMIT_FRESHNESS_CHECK,
    DAILY_ROW_COUNT_CHECK,
    LAKEHOUSE_MONITOR_ASSERTION_TYPE,
    TOTAL_ROW_COUNT_CHECK,
    CheckRun,
    build_assertion_info_mcp,
    build_assertion_run_event_mcp,
    make_dq_assertion_urn,
    map_status_to_result_type,
)
from datahub.ingestion.source.unity.config import UnityCatalogDataQualityConfig
from datahub.ingestion.source.unity.data_quality import (
    RESULTS_TABLE,
    UnityCatalogDataQualityExtractor,
)
from datahub.ingestion.source.unity.proxy_types import Table, TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionStdOperatorClass,
    DatasetAssertionScopeClass,
)

FROZEN_END = datetime(2026, 9, 12, tzinfo=timezone.utc)
SCAN_TIME = datetime(2026, 9, 11, 6, tzinfo=timezone.utc)


def _ref() -> TableReference:
    return TableReference(metastore=None, catalog="cat", schema="sch", table="tbl")


def _dataset_urn(ref: TableReference) -> str:
    return make_dataset_urn("databricks", ref.qualified_table_name, "PROD")


def _table(table_id: Optional[str] = "table-uuid") -> Table:
    return cast(Table, SimpleNamespace(table_id=table_id, ref=_ref()))


class _FakeRow:
    def __init__(self, data: Dict[str, object]) -> None:
        self._data = data

    def asDict(self) -> Dict[str, object]:
        return self._data


class _FakeProxy:
    def __init__(
        self,
        rows: Optional[List[Dict[str, object]]] = None,
        query_error: Optional[Exception] = None,
    ) -> None:
        self._rows = rows or []
        self._query_error = query_error
        self.queries: List[str] = []

    def run_sql_query(self, query: str) -> List[_FakeRow]:
        self.queries.append(query)
        if self._query_error is not None:
            raise self._query_error
        return [_FakeRow(r) for r in self._rows]


def _extractor(proxy: _FakeProxy) -> UnityCatalogDataQualityExtractor:
    return UnityCatalogDataQualityExtractor(
        config=UnityCatalogDataQualityConfig(enabled=True),
        report=UnityCatalogReport(),
        proxy=proxy,  # type: ignore[arg-type]
        dataset_urn_builder=_dataset_urn,
        end_time=FROZEN_END,
    )


def _scan_row(
    *,
    table_id: str = "table-uuid",
    event_time: datetime = SCAN_TIME,
    commit_freshness_status: Optional[str] = "Healthy",
    daily_row_count_status: Optional[str] = "Unhealthy",
    total_row_count_status: Optional[str] = "Healthy",
    commit_freshness_error_code: Optional[str] = None,
) -> Dict[str, object]:
    return {
        "table_id": table_id,
        "event_time": event_time,
        "commit_freshness_status": commit_freshness_status,
        "commit_freshness_error_code": commit_freshness_error_code,
        "commit_freshness_last_value": datetime(2026, 9, 11, 5, tzinfo=timezone.utc),
        "commit_freshness_predicted_value": datetime(
            2026, 9, 11, 5, 30, tzinfo=timezone.utc
        ),
        "daily_row_count_status": daily_row_count_status,
        "daily_row_count_error_code": None,
        "daily_row_count_last_value": 500,
        "daily_row_count_min_predicted_value": 10,
        "daily_row_count_max_predicted_value": 1000,
        "total_row_count_status": total_row_count_status,
        "total_row_count_error_code": None,
        "total_row_count_last_value": 12000,
        "total_row_count_min_predicted_value": 11000,
        "total_row_count_max_predicted_value": 13000,
    }


def test_status_mapping_uses_databricks_verdict() -> None:
    assert (
        map_status_to_result_type("Healthy", None) == AssertionResultTypeClass.SUCCESS
    )
    assert (
        map_status_to_result_type("Unhealthy", None) == AssertionResultTypeClass.FAILURE
    )
    assert map_status_to_result_type("Error", None) == AssertionResultTypeClass.ERROR
    # Model-fit failure surfaces as an error_code, not a status.
    assert (
        map_status_to_result_type("Unhealthy", "FAILED_TO_FIT_MODEL")
        == AssertionResultTypeClass.ERROR
    )
    # Not a pass/fail verdict -> no run event.
    assert map_status_to_result_type("Training", None) is None
    assert map_status_to_result_type("Unknown", None) is None
    assert map_status_to_result_type(None, None) is None


def test_assertion_urn_is_per_dataset_and_per_check() -> None:
    dataset_urn = _dataset_urn(_ref())
    other = make_dataset_urn("databricks", "cat.sch.other", "PROD")
    freshness = make_dq_assertion_urn(dataset_urn, COMMIT_FRESHNESS_CHECK.key)
    # Idempotent: same identity -> same URN.
    assert freshness == make_dq_assertion_urn(dataset_urn, COMMIT_FRESHNESS_CHECK.key)
    # Each check is its own assertion.
    assert freshness != make_dq_assertion_urn(dataset_urn, DAILY_ROW_COUNT_CHECK.key)
    # Datasets (and so workspaces) cannot collide.
    assert freshness != make_dq_assertion_urn(other, COMMIT_FRESHNESS_CHECK.key)


def test_build_info_mcp_is_native_and_fixed_title() -> None:
    dataset_urn = _dataset_urn(_ref())
    urn = make_dq_assertion_urn(dataset_urn, DAILY_ROW_COUNT_CHECK.key)
    info = build_assertion_info_mcp(DAILY_ROW_COUNT_CHECK, urn, dataset_urn).aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.description == DAILY_ROW_COUNT_CHECK.title
    assert info.source is not None and info.source.type == "EXTERNAL"
    ca = info.customAssertion
    assert ca is not None
    assert ca.type == LAKEHOUSE_MONITOR_ASSERTION_TYPE
    assert ca.entity == dataset_urn
    assert ca.operator == AssertionStdOperatorClass._NATIVE_
    assert ca.nativeType == "daily_row_count"
    assert ca.scope == DatasetAssertionScopeClass.DATASET_ROWS
    # Predicted bounds change per scan, so they must not be on the definition.
    assert ca.parameters is None


def test_build_run_event_row_count_vs_freshness_shape() -> None:
    dataset_urn = _dataset_urn(_ref())
    urn = "urn:li:assertion:x"
    row_count = build_assertion_run_event_mcp(
        CheckRun(
            result_type=AssertionResultTypeClass.FAILURE,
            timestamp_millis=1,
            run_id="1",
            actual_agg_value=500.0,
            native_results={"min_predicted_value": "10", "max_predicted_value": "1000"},
        ),
        urn,
        dataset_urn,
    ).aspect
    assert isinstance(row_count, AssertionRunEventClass)
    assert row_count.result is not None
    assert row_count.result.actualAggValue == 500.0
    assert row_count.result.nativeResults == {
        "min_predicted_value": "10",
        "max_predicted_value": "1000",
    }

    freshness = build_assertion_run_event_mcp(
        CheckRun(
            result_type=AssertionResultTypeClass.SUCCESS,
            timestamp_millis=1,
            run_id="1",
            actual_agg_value=None,
            native_results={"last_value": "t0", "predicted_value": "t1"},
        ),
        urn,
        dataset_urn,
    ).aspect
    assert isinstance(freshness, AssertionRunEventClass)
    assert freshness.result is not None
    # Freshness values are timestamps -> nativeResults, never actualAggValue.
    assert freshness.result.actualAggValue is None
    assert "predicted_value" in (freshness.result.nativeResults or {})


def _by_assertion(
    workunits: List[MetadataWorkUnit],
) -> Dict[str, Dict[str, object]]:
    """Group emitted aspects by assertion URN -> {info, runs:[...]}"""
    grouped: Dict[str, Dict[str, object]] = {}
    for wu in workunits:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        urn = wu.metadata.entityUrn
        assert urn is not None
        entry = grouped.setdefault(urn, {"info": None, "runs": []})
        aspect = wu.metadata.aspect
        if isinstance(aspect, AssertionInfoClass):
            entry["info"] = aspect
        elif isinstance(aspect, AssertionRunEventClass):
            cast(List[AssertionRunEventClass], entry["runs"]).append(aspect)
    return grouped


def test_extractor_emits_three_checks_with_real_verdicts() -> None:
    proxy = _FakeProxy(rows=[_scan_row()])
    extractor = _extractor(proxy)
    wus = list(extractor.get_workunits([_table()]))
    grouped = _by_assertion(wus)

    dataset_urn = _dataset_urn(_ref())
    expected = [
        (COMMIT_FRESHNESS_CHECK, AssertionResultTypeClass.SUCCESS),
        (DAILY_ROW_COUNT_CHECK, AssertionResultTypeClass.FAILURE),
        (TOTAL_ROW_COUNT_CHECK, AssertionResultTypeClass.SUCCESS),
    ]
    for spec, result_type in expected:
        urn = make_dq_assertion_urn(dataset_urn, spec.key)
        entry = grouped[urn]
        assert entry["info"] is not None
        runs = cast(List[AssertionRunEventClass], entry["runs"])
        assert len(runs) == 1
        assert runs[0].result is not None
        assert runs[0].result.type == result_type

    report = extractor.report
    assert report.num_quality_scan_rows_read == 1
    assert report.num_quality_assertions_emitted == 3
    assert report.num_quality_run_events_emitted == 3


def test_extractor_skips_training_and_unknown() -> None:
    proxy = _FakeProxy(
        rows=[
            _scan_row(
                commit_freshness_status="Training",
                daily_row_count_status="Unknown",
                total_row_count_status="Healthy",
            )
        ]
    )
    extractor = _extractor(proxy)
    wus = list(extractor.get_workunits([_table()]))
    grouped = _by_assertion(wus)
    # Only the total_row_count check produced a verdict.
    assert len(grouped) == 1
    assert extractor.report.num_quality_run_events_emitted == 1
    assert extractor.report.num_quality_checks_skipped == 2


def test_extractor_definition_emitted_once_across_scans() -> None:
    rows = [
        _scan_row(),
        _scan_row(event_time=datetime(2026, 9, 11, 18, tzinfo=timezone.utc)),
    ]
    extractor = _extractor(_FakeProxy(rows=rows))
    wus = list(extractor.get_workunits([_table()]))

    info_count = 0
    run_ids = []
    for wu in wus:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        aspect = wu.metadata.aspect
        if isinstance(aspect, AssertionInfoClass):
            info_count += 1
        elif isinstance(aspect, AssertionRunEventClass):
            run_ids.append(aspect.runId)

    # 3 definitions total (not 6), 6 run events (3 checks x 2 scans).
    assert info_count == 3
    assert extractor.report.num_quality_run_events_emitted == 6
    # Each scan has its own run id (derived from event_time), so the two scans
    # append distinct runs rather than overwriting.
    assert len(set(run_ids)) == 2


def test_extractor_skips_rows_for_unknown_table() -> None:
    proxy = _FakeProxy(rows=[_scan_row(table_id="not-ingested")])
    extractor = _extractor(proxy)
    assert list(extractor.get_workunits([_table()])) == []
    assert extractor.report.num_quality_scan_rows_read == 0


def test_extractor_no_tables_skips_query() -> None:
    proxy = _FakeProxy(rows=[_scan_row()])
    extractor = _extractor(proxy)
    assert list(extractor.get_workunits([_table(table_id=None)])) == []
    assert proxy.queries == []


def test_extractor_reports_query_failure() -> None:
    proxy = _FakeProxy(query_error=RuntimeError("no SELECT grant"))
    extractor = _extractor(proxy)
    assert list(extractor.get_workunits([_table()])) == []
    assert extractor.report.num_quality_query_failures == 1


def test_query_targets_system_table_and_window() -> None:
    proxy = _FakeProxy(rows=[])
    extractor = _extractor(proxy)
    list(extractor.get_workunits([_table()]))
    query = proxy.queries[0]
    assert RESULTS_TABLE in query
    assert "'table-uuid'" in query
    assert "event_time >= TIMESTAMP '2026-09-05 00:00:00.000000'" in query
    assert "event_time <= TIMESTAMP '2026-09-12 00:00:00.000000'" in query
    assert "commit_freshness_status" in query
    assert "daily_row_count_status" in query
    assert "total_row_count_status" in query
    # Only the three per-check statuses are selected; the group rollups
    # (freshness.status, completeness.status) and the table-level status are
    # deliberately excluded so the same failure is not counted twice.
    assert query.count(".status AS") == 3
