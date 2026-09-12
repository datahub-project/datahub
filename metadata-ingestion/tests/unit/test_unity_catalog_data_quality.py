from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Dict, List, Optional, Set, cast

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.assertion import (
    COMPLETENESS_NATIVE_TYPE,
    LAKEHOUSE_MONITOR_ASSERTION_TYPE,
    DataQualityAssertion,
    build_assertion_info_mcp,
    build_assertion_run_event_mcp,
    make_dq_assertion_urn,
)
from datahub.ingestion.source.unity.config import UnityCatalogDataQualityConfig
from datahub.ingestion.source.unity.data_quality import UnityCatalogDataQualityExtractor
from datahub.ingestion.source.unity.proxy_types import Table, TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    DatasetAssertionScopeClass,
)

PROFILE_TABLE = "cat.sch.tbl_profile_metrics"

_UNSET = object()


def _ref() -> TableReference:
    return TableReference(metastore=None, catalog="cat", schema="sch", table="tbl")


def _dataset_urn(ref: TableReference) -> str:
    return make_dataset_urn("databricks", ref.qualified_table_name, "PROD")


def _result(column: str = "c1", num_nulls: float = 0.0) -> DataQualityAssertion:
    return DataQualityAssertion(
        column=column,
        metric="num_nulls",
        threshold=0.0,
        observed=num_nulls,
        passed=num_nulls <= 0.0,
        timestamp_millis=1,
        run_id="0:1",
    )


def _monitor(metrics_table: Optional[str] = PROFILE_TABLE) -> object:
    profiling = (
        SimpleNamespace(profile_metrics_table_name=metrics_table)
        if metrics_table is not None
        else None
    )
    return SimpleNamespace(data_profiling_config=profiling)


class _FakeRow:
    def __init__(self, data: Dict[str, object]) -> None:
        self._data = data

    def asDict(self) -> Dict[str, object]:
        return self._data


class _FakeProxy:
    def __init__(
        self,
        rows: Optional[List[Dict[str, object]]] = None,
        available: bool = True,
        monitor: object = _UNSET,
        monitor_error: Optional[Exception] = None,
        query_error: Optional[Exception] = None,
    ) -> None:
        self._rows = rows or []
        self._available = available
        self._monitor = _monitor() if monitor is _UNSET else monitor
        self._monitor_error = monitor_error
        self._query_error = query_error
        self.queries: List[str] = []

    def data_quality_available(self) -> bool:
        return self._available

    def get_quality_monitor(self, table_id: str) -> object:
        if self._monitor_error is not None:
            raise self._monitor_error
        return self._monitor

    def run_sql_query(self, query: str) -> List[_FakeRow]:
        self.queries.append(query)
        if self._query_error is not None:
            raise self._query_error
        return [_FakeRow(r) for r in self._rows]


def _table(table_id: Optional[str] = "table-uuid") -> Table:
    return cast(Table, SimpleNamespace(table_id=table_id, ref=_ref()))


def _extractor(
    proxy: _FakeProxy, config: Optional[UnityCatalogDataQualityConfig] = None
) -> UnityCatalogDataQualityExtractor:
    return UnityCatalogDataQualityExtractor(
        config=config or UnityCatalogDataQualityConfig(enabled=True),
        report=UnityCatalogReport(),
        proxy=proxy,  # type: ignore[arg-type]
        dataset_urn_builder=_dataset_urn,
        end_time=datetime(2026, 9, 12, tzinfo=timezone.utc),
    )


def _row(column: str, num_nulls: float) -> Dict[str, object]:
    return {
        "window_end": datetime(2026, 9, 11, tzinfo=timezone.utc),
        "column_name": column,
        "monitor_version": 0,
        "row_count": 100,
        "num_nulls": num_nulls,
        "percent_null": num_nulls,
    }


def test_assertion_urn_is_deterministic_and_namespaced_by_dataset() -> None:
    dataset_urn = _dataset_urn(_ref())
    other_dataset_urn = make_dataset_urn("databricks", "cat.sch.other", "PROD")
    # Same identity -> same URN; column, metric, and dataset each change it, so
    # assertions from different datasets/workspaces cannot collide.
    assert make_dq_assertion_urn(
        dataset_urn, "c1", "num_nulls"
    ) == make_dq_assertion_urn(dataset_urn, "c1", "num_nulls")
    assert make_dq_assertion_urn(
        dataset_urn, "c1", "num_nulls"
    ) != make_dq_assertion_urn(dataset_urn, "c2", "num_nulls")
    assert make_dq_assertion_urn(
        dataset_urn, "c1", "num_nulls"
    ) != make_dq_assertion_urn(other_dataset_urn, "c1", "num_nulls")


def test_build_info_mcp_sets_column_field_urn() -> None:
    ref = _ref()
    dataset_urn = _dataset_urn(ref)
    result = _result()
    urn = make_dq_assertion_urn(dataset_urn, result.column, result.metric)
    info = build_assertion_info_mcp(result, urn, dataset_urn).aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.customAssertion is not None
    ca = info.customAssertion
    assert ca.scope == DatasetAssertionScopeClass.DATASET_COLUMN
    assert ca.field is not None
    assert "c1" in ca.field
    assert info.source is not None and info.source.type == "EXTERNAL"
    # The assertions list renders the name from `description`, so it must carry the
    # column (the frontend doesn't fetch the structured scope/aggregation fields).
    assert info.description is not None
    assert "c1" in info.description
    assert ca.type == LAKEHOUSE_MONITOR_ASSERTION_TYPE
    assert ca.nativeType == COMPLETENESS_NATIVE_TYPE
    assert ca.aggregation == "NULL_COUNT"
    assert ca.operator == "EQUAL_TO"
    assert ca.parameters is not None and ca.parameters.value is not None
    assert ca.parameters.value.value == "0"


def _run_event_type(result: DataQualityAssertion) -> str:
    aspect = build_assertion_run_event_mcp(
        result, "urn:li:assertion:x", _dataset_urn(_ref())
    ).aspect
    assert isinstance(aspect, AssertionRunEventClass)
    assert aspect.result is not None
    return str(aspect.result.type)


def test_build_run_event_result_types() -> None:
    assert _run_event_type(_result(num_nulls=0.0)) == AssertionResultTypeClass.SUCCESS
    assert _run_event_type(_result(num_nulls=5.0)) == AssertionResultTypeClass.FAILURE


def _aspect_names(workunits: List[MetadataWorkUnit]) -> List[str]:
    names = []
    for wu in workunits:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        assert wu.metadata.aspectName is not None
        names.append(wu.metadata.aspectName)
    return names


def _run_event_types(workunits: List[MetadataWorkUnit]) -> Set[str]:
    types: Set[str] = set()
    for wu in workunits:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        aspect = wu.metadata.aspect
        if isinstance(aspect, AssertionRunEventClass):
            assert aspect.result is not None
            types.add(str(aspect.result.type))
    return types


def test_extractor_emits_assertion_and_run_event_per_column() -> None:
    proxy = _FakeProxy(rows=[_row("c1", 0.0), _row("c2", 10.0)])
    extractor = _extractor(proxy)
    wus = list(extractor.get_workunits([_table()]))
    names = _aspect_names(wus)
    assert names.count("assertionInfo") == 2  # one per column, emitted once
    assert names.count("assertionRunEvent") == 2
    report = extractor.report
    assert report.num_quality_monitors_found == 1
    assert report.num_quality_assertions_emitted == 2
    assert report.num_quality_run_events_emitted == 2
    # c1 passes (0 nulls), c2 fails (10 nulls > 0).
    assert _run_event_types(wus) == {
        AssertionResultTypeClass.SUCCESS,
        AssertionResultTypeClass.FAILURE,
    }


def test_extractor_column_pattern_filters_columns() -> None:
    config = UnityCatalogDataQualityConfig(enabled=True)
    config.column_pattern.deny.append("c2")
    proxy = _FakeProxy(rows=[_row("c1", 0.0), _row("c2", 0.0)])
    wus = list(_extractor(proxy, config).get_workunits([_table()]))
    assert _aspect_names(wus).count("assertionInfo") == 1


def test_extractor_skips_rows_without_null_metric() -> None:
    row = _row("c1", 0.0)
    row["num_nulls"] = None
    wus = list(_extractor(_FakeProxy(rows=[row])).get_workunits([_table()]))
    assert wus == []


def test_extractor_skips_when_api_unavailable() -> None:
    proxy = _FakeProxy(rows=[_row("c1", 0.0)], available=False)
    assert list(_extractor(proxy).get_workunits([_table()])) == []


def test_extractor_skips_table_without_id() -> None:
    proxy = _FakeProxy(rows=[_row("c1", 0.0)])
    assert list(_extractor(proxy).get_workunits([_table(table_id=None)])) == []


def test_extractor_counts_table_without_monitor() -> None:
    proxy = _FakeProxy(monitor=None)
    extractor = _extractor(proxy)
    assert list(extractor.get_workunits([_table()])) == []
    assert extractor.report.num_quality_tables_without_monitor == 1
    assert extractor.report.num_quality_monitors_found == 0


def test_extractor_reports_monitor_without_metrics_table() -> None:
    proxy = _FakeProxy(monitor=_monitor(metrics_table=None))
    extractor = _extractor(proxy)
    assert list(extractor.get_workunits([_table()])) == []
    assert list(extractor.report.quality_monitors_missing_metrics) == [
        _ref().qualified_table_name
    ]
    assert extractor.report.num_quality_monitors_found == 0


def test_extractor_reports_monitor_api_error() -> None:
    # A real API error must not be mistaken for an unmonitored table.
    proxy = _FakeProxy(monitor_error=RuntimeError("boom"))
    extractor = _extractor(proxy)
    assert list(extractor.get_workunits([_table()])) == []
    assert extractor.report.num_quality_monitor_errors == 1
    assert extractor.report.num_quality_tables_without_monitor == 0


def test_extractor_reports_metric_query_failure() -> None:
    # run_sql_query raises on failure, so a broken query is reported rather than
    # silently looking like a monitor with no rows.
    proxy = _FakeProxy(rows=[_row("c1", 0.0)], query_error=RuntimeError("no SELECT"))
    extractor = _extractor(proxy)
    assert list(extractor.get_workunits([_table()])) == []
    assert extractor.report.num_quality_metric_query_failures == 1
