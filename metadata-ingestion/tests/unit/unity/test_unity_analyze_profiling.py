"""ANALYZE-based profiling: UnityCatalogApiProxy.get_table_stats over a mocked
WorkspaceClient (statement execution + tables.get), and the
UnityCatalogAnalyzeProfiler that turns the resulting TableProfile into a
DatasetProfile aspect."""

from typing import Callable, Dict, List, Optional
from unittest.mock import MagicMock

import pytest
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.catalog import ColumnInfo, TableInfo
from databricks.sdk.service.sql import ServiceError, ServiceErrorCode, StatementState

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.analyze_profiler import (
    UnityCatalogAnalyzeProfiler,
)
from datahub.ingestion.source.unity.config import UnityCatalogAnalyzeProfilerConfig
from datahub.ingestion.source.unity.hive_metastore_proxy import HiveMetastoreProxy
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import (
    ColumnProfile,
    TableProfile,
    TableReference,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.metadata.schema_classes import DatasetProfileClass
from tests.unit.unity.conftest import (
    WAREHOUSE_ID,
    statement_response,
    warning_messages,
)

pytestmark = pytest.mark.usefixtures("no_sleep")

REF = TableReference(metastore=None, catalog="main", schema="sales", table="orders")
OTHER_REF = TableReference(metastore=None, catalog="main", schema="ops", table="jobs")

STATS: Dict[str, str] = {
    "spark.sql.statistics.numRows": "100",
    "spark.sql.statistics.totalSize": "4096",
    "spark.sql.statistics.colStats.id.nullCount": "0",
    "spark.sql.statistics.colStats.id.distinctCount": "100",
    "spark.sql.statistics.colStats.id.min": "1",
    "spark.sql.statistics.colStats.id.max": "100",
}


def _table_info(properties: Dict[str, str]) -> TableInfo:
    return TableInfo(
        name="orders",
        columns=[ColumnInfo(name="id"), ColumnInfo(name="name")],
        properties=properties,
    )


@pytest.fixture
def analyze_ws(workspace_client: MagicMock) -> MagicMock:
    """A client whose ANALYZE succeeds and whose tables.get carries STATS."""
    workspace_client.statement_execution.execute_statement.return_value = (
        statement_response(StatementState.PENDING)
    )
    workspace_client.statement_execution.get_statement.return_value = (
        statement_response(StatementState.SUCCEEDED)
    )
    workspace_client.tables.get.return_value = _table_info(STATS)
    return workspace_client


def _statements(ws: MagicMock) -> List[str]:
    return [
        c.kwargs["statement"]
        for c in ws.statement_execution.execute_statement.call_args_list
    ]


def test_get_table_stats_runs_analyze_then_reads_table_properties(
    analyze_ws: MagicMock, api_proxy: Callable[..., UnityCatalogApiProxy]
) -> None:
    proxy = api_proxy(analyze_ws)

    profile = proxy.get_table_stats(
        REF, max_wait_secs=10, call_analyze=True, include_columns=True
    )

    assert profile is not None
    assert (profile.num_rows, profile.total_size, profile.num_columns) == (
        100,
        4096,
        2,
    )
    by_name = {c.name: c for c in profile.column_profiles}
    assert by_name["id"].distinct_count == 100
    assert by_name["id"].null_count == 0
    assert (by_name["id"].min, by_name["id"].max) == ("1", "100")
    # A column ANALYZE produced no stats for is present but falsy.
    assert not by_name["name"]

    assert _statements(analyze_ws) == [
        "ANALYZE TABLE sales.orders COMPUTE STATISTICS FOR ALL COLUMNS"
    ]
    kwargs = analyze_ws.statement_execution.execute_statement.call_args.kwargs
    assert (kwargs["catalog"], kwargs["warehouse_id"]) == ("main", WAREHOUSE_ID)
    analyze_ws.tables.get.assert_called_once_with("main.sales.orders")


def test_call_analyze_off_reads_the_last_computed_stats(
    analyze_ws: MagicMock, api_proxy: Callable[..., UnityCatalogApiProxy]
) -> None:
    proxy = api_proxy(analyze_ws)

    profile = proxy.get_table_stats(
        REF, max_wait_secs=10, call_analyze=False, include_columns=False
    )

    assert profile is not None
    assert profile.num_rows == 100
    assert profile.column_profiles == []
    analyze_ws.statement_execution.execute_statement.assert_not_called()
    analyze_ws.statement_execution.get_statement.assert_not_called()


def test_table_level_only_analyze_skips_column_statistics(
    analyze_ws: MagicMock, api_proxy: Callable[..., UnityCatalogApiProxy]
) -> None:
    proxy = api_proxy(analyze_ws)

    profile = proxy.get_table_stats(
        REF, max_wait_secs=10, call_analyze=True, include_columns=False
    )

    assert profile is not None
    assert profile.column_profiles == []
    assert _statements(analyze_ws) == ["ANALYZE TABLE sales.orders COMPUTE STATISTICS"]


def test_analyze_timeout_is_reported_and_yields_no_profile(
    analyze_ws: MagicMock, api_proxy: Callable[..., UnityCatalogApiProxy]
) -> None:
    analyze_ws.statement_execution.get_statement.return_value = statement_response(
        StatementState.RUNNING
    )
    proxy = api_proxy(analyze_ws)

    profile = proxy.get_table_stats(
        REF, max_wait_secs=3, call_analyze=True, include_columns=True
    )

    assert profile is None
    assert str(REF) in proxy.report.profile_table_timeouts
    analyze_ws.tables.get.assert_not_called()


def test_failed_analyze_is_reported_not_raised(
    analyze_ws: MagicMock, api_proxy: Callable[..., UnityCatalogApiProxy]
) -> None:
    analyze_ws.statement_execution.execute_statement.return_value = statement_response(
        StatementState.FAILED,
        error=ServiceError(
            error_code=ServiceErrorCode.BAD_REQUEST,
            message="[INSUFFICIENT_PERMISSIONS] no `SELECT` on orders",
        ),
    )
    proxy = api_proxy(analyze_ws)

    profile = proxy.get_table_stats(
        REF, max_wait_secs=10, call_analyze=True, include_columns=True
    )

    assert profile is None
    # profile_table_errors groups (table, error) samples under the error text.
    (samples,) = proxy.report.profile_table_errors.values()
    assert [ref for ref, _ in samples] == [str(REF)]
    analyze_ws.tables.get.assert_not_called()


def test_unsupported_column_type_retries_at_table_level(
    analyze_ws: MagicMock, api_proxy: Callable[..., UnityCatalogApiProxy]
) -> None:
    """ANALYZE ... FOR ALL COLUMNS fails on e.g. map columns; the proxy must
    fall back to table-level statistics instead of dropping the table."""
    analyze_ws.statement_execution.execute_statement.side_effect = [
        DatabricksError(
            "[UNSUPPORTED_FEATURE.ANALYZE_UNSUPPORTED_COLUMN_TYPE] map column",
            error_code="BAD_REQUEST",
        ),
        statement_response(StatementState.SUCCEEDED),
    ]
    proxy = api_proxy(analyze_ws)

    profile = proxy.get_table_stats(
        REF, max_wait_secs=10, call_analyze=True, include_columns=True
    )

    assert profile is not None
    assert profile.num_rows == 100
    assert profile.column_profiles == []
    assert _statements(analyze_ws) == [
        "ANALYZE TABLE sales.orders COMPUTE STATISTICS FOR ALL COLUMNS",
        "ANALYZE TABLE sales.orders COMPUTE STATISTICS",
    ]
    assert proxy.report.num_profile_failed_unsupported_column_type == 1


def test_unparseable_statistic_is_counted_and_dropped(
    analyze_ws: MagicMock, api_proxy: Callable[..., UnityCatalogApiProxy]
) -> None:
    analyze_ws.tables.get.return_value = _table_info(
        {**STATS, "spark.sql.statistics.numRows": "unknown"}
    )
    proxy = api_proxy(analyze_ws)

    profile = proxy.get_table_stats(
        REF, max_wait_secs=10, call_analyze=False, include_columns=False
    )

    assert profile is not None
    assert profile.num_rows is None
    assert profile.total_size == 4096
    assert proxy.report.num_profile_failed_int_casts == 1


def test_hive_metastore_tables_are_profiled_through_the_hive_proxy(
    analyze_ws: MagicMock, api_proxy: Callable[..., UnityCatalogApiProxy]
) -> None:
    hive = MagicMock(spec=HiveMetastoreProxy)
    expected = TableProfile(
        num_rows=7, num_columns=1, total_size=None, column_profiles=[]
    )
    hive.get_table_profile.return_value = expected
    proxy = api_proxy(analyze_ws, hive_metastore_proxy=hive)
    ref = TableReference(
        metastore=None, catalog="hive_metastore", schema="sales", table="legacy"
    )

    profile = proxy.get_table_stats(
        ref, max_wait_secs=10, call_analyze=False, include_columns=True
    )

    assert profile is expected
    hive.get_table_profile.assert_called_once_with(ref, True)
    analyze_ws.tables.get.assert_not_called()


def test_start_warehouse_returns_none_when_the_warehouse_is_missing(
    workspace_client: MagicMock, api_proxy: Callable[..., UnityCatalogApiProxy]
) -> None:
    workspace_client.warehouses.start.side_effect = DatabricksError("no such warehouse")

    assert api_proxy(workspace_client).start_warehouse() is None


# --- UnityCatalogAnalyzeProfiler -------------------------------------------


def _column(
    name: str,
    null_count: Optional[int] = None,
    distinct_count: Optional[int] = None,
    min_value: Optional[str] = None,
    max_value: Optional[str] = None,
) -> ColumnProfile:
    return ColumnProfile(
        name=name,
        null_count=null_count,
        distinct_count=distinct_count,
        min=min_value,
        max=max_value,
        version=None,
        avg_len=None,
        max_len=None,
    )


@pytest.fixture
def proxy() -> MagicMock:
    return MagicMock(spec=UnityCatalogApiProxy)


def _profiler(proxy: MagicMock, **config: object) -> UnityCatalogAnalyzeProfiler:
    return UnityCatalogAnalyzeProfiler(
        config=UnityCatalogAnalyzeProfilerConfig(enabled=True, max_workers=1, **config),
        report=UnityCatalogReport(),
        proxy=proxy,
        dataset_urn_builder=lambda ref: f"urn:li:dataset:{ref.qualified_table_name}",
    )


def _aspect(wu: MetadataWorkUnit) -> DatasetProfileClass:
    aspect = wu.get_aspect_of_type(DatasetProfileClass)
    assert aspect is not None
    return aspect


def test_analyze_profiler_emits_dataset_profile_with_field_proportions(
    proxy: MagicMock,
) -> None:
    """Proportions derive from num_rows; ANALYZE's approximate distinct counts
    can exceed the row count and must be capped at 1.0, and a column with no
    stats at all is left out of fieldProfiles."""
    proxy.get_table_stats.return_value = TableProfile(
        num_rows=200,
        num_columns=3,
        total_size=1024,
        column_profiles=[
            _column("id", null_count=0, distinct_count=200, min_value="1"),
            _column("note", null_count=50, distinct_count=400),
            _column("empty"),
        ],
    )
    profiler = _profiler(proxy, max_wait_secs=30)

    workunits = list(profiler.get_workunits([REF]))

    assert len(workunits) == 1
    assert workunits[0].get_urn() == "urn:li:dataset:main.sales.orders"
    aspect = _aspect(workunits[0])
    assert (aspect.rowCount, aspect.columnCount, aspect.sizeInBytes) == (
        200,
        3,
        1024,
    )
    assert aspect.fieldProfiles is not None
    fields = {f.fieldPath: f for f in aspect.fieldProfiles}
    assert set(fields) == {"id", "note"}
    assert (fields["id"].uniqueProportion, fields["id"].nullProportion) == (1.0, 0.0)
    assert fields["id"].min == "1"
    assert (fields["note"].uniqueProportion, fields["note"].nullProportion) == (
        1.0,
        0.25,
    )
    proxy.get_table_stats.assert_called_once_with(
        REF, max_wait_secs=30, call_analyze=True, include_columns=True
    )


def test_table_level_only_profile_carries_no_field_profiles(
    proxy: MagicMock,
) -> None:
    proxy.get_table_stats.return_value = TableProfile(
        num_rows=5, num_columns=2, total_size=10, column_profiles=[]
    )
    profiler = _profiler(proxy, profile_table_level_only=True, call_analyze=False)

    workunits = list(profiler.get_workunits([REF]))

    assert _aspect(workunits[0]).fieldProfiles is None
    assert proxy.get_table_stats.call_args.kwargs["include_columns"] is False
    assert proxy.get_table_stats.call_args.kwargs["call_analyze"] is False


def test_profiling_pattern_filters_tables_before_any_analyze(
    proxy: MagicMock,
) -> None:
    proxy.get_table_stats.return_value = TableProfile(
        num_rows=1, num_columns=1, total_size=1, column_profiles=[]
    )
    profiler = _profiler(proxy, pattern=AllowDenyPattern(deny=[r"^main\.sales\..*"]))

    workunits = list(profiler.get_workunits([REF, OTHER_REF]))

    assert [wu.get_urn() for wu in workunits] == ["urn:li:dataset:main.ops.jobs"]
    assert proxy.get_table_stats.call_args.args == (OTHER_REF,)


def test_empty_and_missing_profiles_are_recorded_not_emitted(
    proxy: MagicMock,
) -> None:
    """An all-None TableProfile is falsy and lands in profile_table_empty; a
    None result (timeout / failure already reported by the proxy) is silent."""
    proxy.get_table_stats.side_effect = [
        TableProfile(
            num_rows=None, num_columns=None, total_size=None, column_profiles=[]
        ),
        None,
    ]
    profiler = _profiler(proxy)

    workunits = list(profiler.get_workunits([REF, OTHER_REF]))

    assert workunits == []
    assert list(profiler.report.profile_table_empty) == [str(REF)]


def test_one_table_failing_does_not_stop_the_others(proxy: MagicMock) -> None:
    proxy.get_table_stats.side_effect = [
        RuntimeError("warehouse hiccup"),
        TableProfile(num_rows=1, num_columns=1, total_size=1, column_profiles=[]),
    ]
    profiler = _profiler(proxy)

    workunits = list(profiler.get_workunits([REF, OTHER_REF]))

    assert [wu.get_urn() for wu in workunits] == ["urn:li:dataset:main.ops.jobs"]
    assert warning_messages(profiler.report) == {
        "Unexpected error during profiling table"
    }
