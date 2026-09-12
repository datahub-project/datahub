from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Dict, List, Optional, Set, cast

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.assertion import (
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


def _ref() -> TableReference:
    return TableReference(metastore=None, catalog="cat", schema="sch", table="tbl")


def _dataset_urn(ref: TableReference) -> str:
    return make_dataset_urn("databricks", ref.qualified_table_name, "PROD")


def _result(column: str = "c1", num_nulls: float = 0.0) -> DataQualityAssertion:
    return DataQualityAssertion(
        table_qualified_name=_ref().qualified_table_name,
        column=column,
        metric="num_nulls",
        threshold=0.0,
        observed=num_nulls,
        passed=num_nulls <= 0.0,
        timestamp_millis=1,
        run_id="0:1",
    )


class _FakeRow:
    def __init__(self, data: Dict[str, object]) -> None:
        self._data = data

    def asDict(self) -> Dict[str, object]:
        return self._data


class _FakeProxy:
    def __init__(self, rows: List[Dict[str, object]], available: bool = True) -> None:
        self._rows = rows
        self._available = available
        self.queries: List[str] = []

    def data_quality_available(self) -> bool:
        return self._available

    def get_quality_monitor(self, table_id: str) -> Optional[object]:
        return SimpleNamespace(
            data_profiling_config=SimpleNamespace(
                profile_metrics_table_name=PROFILE_TABLE
            )
        )

    def run_sql_query(self, query: str) -> List[_FakeRow]:
        self.queries.append(query)
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
        platform_instance=None,
        env="PROD",
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


def test_assertion_urn_is_deterministic_and_stable_across_windows() -> None:
    # Same identity fields -> same URN even though the run window differs, so
    # re-ingesting a check is idempotent.
    a = _result(num_nulls=0.0)
    b = _result(num_nulls=0.0)
    b.timestamp_millis = 999
    assert make_dq_assertion_urn(a, None, "PROD") == make_dq_assertion_urn(
        b, None, "PROD"
    )
    assert make_dq_assertion_urn(a, None, "PROD") != make_dq_assertion_urn(
        _result(column="c2"), None, "PROD"
    )


def test_build_info_mcp_sets_column_field_urn() -> None:
    ref = _ref()
    dataset_urn = _dataset_urn(ref)
    result = _result()
    urn = make_dq_assertion_urn(result, None, "PROD")
    info = build_assertion_info_mcp(result, urn, dataset_urn).aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.customAssertion is not None
    assert info.customAssertion.scope == DatasetAssertionScopeClass.DATASET_COLUMN
    assert info.customAssertion.field is not None
    assert "c1" in info.customAssertion.field
    assert info.source is not None and info.source.type == "EXTERNAL"


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
