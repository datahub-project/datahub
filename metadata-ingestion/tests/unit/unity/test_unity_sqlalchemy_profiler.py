"""The SQLAlchemy (GE) profiler's request building: which tables are profiled,
at what level, and how size / row count are read over the warehouse. The
SQLAlchemy engine is a fake answering DESCRIBE DETAIL and count(*)."""

from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Optional
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.service.catalog import DataSourceFormat, TableType

from datahub.ingestion.source.unity.profiler import (
    UnityCatalogProfiler,
    UnityCatalogSQLGenericTable,
)
from datahub.ingestion.source.unity.proxy_types import (
    Catalog,
    Column,
    Schema,
    Table,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport
from tests.unit.unity.conftest import (
    WAREHOUSE_ID,
    FakeSqlEngine,
    unity_config,
    warning_messages,
)

GIB = 2**30


def _table(
    name: str = "orders",
    data_source_format: DataSourceFormat = DataSourceFormat.DELTA,
    column_count: int = 2,
    updated_at: Optional[datetime] = None,
) -> Table:
    catalog = Catalog(
        id="main", name="main", comment=None, metastore=None, owner=None, type=None
    )
    schema = Schema(
        id="main.sales", name="sales", catalog=catalog, comment=None, owner=None
    )
    return Table(
        id=f"main.sales.{name}",
        name=name,
        comment=None,
        schema=schema,
        columns=[
            Column(
                id=f"col_{i}",
                name=f"col_{i}",
                comment=None,
                type_text="int",
                type_name=None,
                type_precision=None,
                type_scale=None,
                position=i,
                nullable=True,
            )
            for i in range(column_count)
        ],
        storage_location=None,
        data_source_format=data_source_format,
        table_type=TableType.MANAGED,
        owner=None,
        generation=None,
        created_at=None,
        created_by=None,
        updated_at=updated_at,
        updated_by=None,
        table_id=None,
        view_definition=None,
        properties={},
    )


@pytest.fixture(autouse=True)
def profiler_workspace_client(workspace_client: MagicMock) -> Iterator[MagicMock]:
    """The profiler resolves its SQL connection params from a WorkspaceClient
    at construction time."""
    with patch(
        "datahub.ingestion.source.unity.profiler.create_workspace_client",
        return_value=workspace_client,
    ):
        yield workspace_client


def _profiler(**profiling: Any) -> UnityCatalogProfiler:
    config = unity_config(
        warehouse_id=WAREHOUSE_ID,
        profiling={"method": "sqlalchemy", "enabled": True, **profiling},
    )
    assert config.is_sqlalchemy_profiling()
    return UnityCatalogProfiler(
        config,
        config.profiling,  # type: ignore[arg-type]
        UnityCatalogReport(),
    )


def test_delta_table_size_is_read_and_a_full_profile_requested() -> None:
    profiler = _profiler()
    engine = FakeSqlEngine(size_in_bytes=1024)

    request = profiler.get_unity_profile_request(
        UnityCatalogSQLGenericTable(_table()), engine.engine
    )

    assert request is not None
    assert request.pretty_name == "main.sales.orders"
    assert request.batch_kwargs == {"schema": "sales", "table": "orders"}
    assert not request.profile_table_level_only
    assert request.table.size_in_bytes == 1024
    assert engine.executed == ["DESCRIBE DETAIL `main`.`sales`.`orders`"]
    assert profiler.report.entities_profiled == 1


def test_oversized_delta_table_falls_back_to_table_level_with_a_row_count() -> None:
    """Above profile_table_size_limit (GiB) a column profile is too expensive;
    the request degrades to table level and fetches the row count instead."""
    profiler = _profiler(profile_table_size_limit=1)
    engine = FakeSqlEngine(size_in_bytes=3 * GIB, row_count=42)

    request = profiler.get_unity_profile_request(
        UnityCatalogSQLGenericTable(_table()), engine.engine
    )

    assert request is not None
    assert request.profile_table_level_only
    assert request.table.rows_count == 42
    assert any("count(*)" in sql for sql in engine.executed)
    assert profiler.report.num_profile_missing_row_count == 0


def test_non_delta_table_is_profiled_without_delta_only_queries() -> None:
    """DESCRIBE DETAIL only works on Delta tables, so other formats are profiled
    with size and row count unknown rather than failing the query."""
    profiler = _profiler(profile_table_level_only=True)
    engine = FakeSqlEngine()

    request = profiler.get_unity_profile_request(
        UnityCatalogSQLGenericTable(
            _table(data_source_format=DataSourceFormat.PARQUET)
        ),
        engine.engine,
    )

    assert request is not None
    assert request.table.size_in_bytes is None
    assert request.table.rows_count is None
    assert engine.executed == []
    assert profiler.report.num_profile_missing_size_in_bytes == 1
    assert profiler.report.num_profile_missing_row_count == 1


def test_size_lookup_failure_is_reported_and_profiling_continues() -> None:
    profiler = _profiler()
    engine = FakeSqlEngine(describe_error=RuntimeError("DESCRIBE DETAIL denied"))

    request = profiler.get_unity_profile_request(
        UnityCatalogSQLGenericTable(_table()), engine.engine
    )

    assert request is not None
    assert request.table.size_in_bytes is None
    assert warning_messages(profiler.report) == {"Failed to get table size"}


def test_row_count_failure_is_reported_and_table_level_profile_still_requested() -> (
    None
):
    profiler = _profiler(profile_table_level_only=True)
    engine = FakeSqlEngine(size_in_bytes=10, count_error=RuntimeError("count denied"))

    request = profiler.get_unity_profile_request(
        UnityCatalogSQLGenericTable(_table()), engine.engine
    )

    assert request is not None
    assert request.profile_table_level_only
    assert request.table.rows_count is None
    assert profiler.report.num_profile_missing_row_count == 1
    assert warning_messages(profiler.report) == {"Failed to get table row count"}


def test_empty_describe_and_count_results_leave_size_and_rows_unknown() -> None:
    profiler = _profiler(profile_table_level_only=True)
    engine = FakeSqlEngine(no_result_rows=True)

    request = profiler.get_unity_profile_request(
        UnityCatalogSQLGenericTable(_table()), engine.engine
    )

    assert request is not None
    assert request.table.size_in_bytes is None
    assert request.table.rows_count is None
    assert profiler.report.num_profile_missing_size_in_bytes == 1
    assert profiler.report.num_profile_missing_row_count == 1


def test_table_without_columns_is_dropped() -> None:
    profiler = _profiler(report_dropped_profiles=True)

    request = profiler.get_unity_profile_request(
        UnityCatalogSQLGenericTable(_table(column_count=0)),
        FakeSqlEngine(size_in_bytes=1).engine,
    )

    assert request is None
    assert "main.sales.orders" in profiler.report.filtered


def test_stale_table_is_skipped_entirely() -> None:
    """profile_if_updated_since_days drops tables not touched recently."""
    profiler = _profiler(profile_if_updated_since_days=1)
    stale = datetime.now(timezone.utc) - timedelta(days=10)

    request = profiler.get_unity_profile_request(
        UnityCatalogSQLGenericTable(_table(updated_at=stale)),
        FakeSqlEngine(size_in_bytes=1).engine,
    )

    assert request is None
    assert profiler.report.entities_profiled == 0


def test_get_workunits_only_forwards_tables_that_produced_requests() -> None:
    """Tables that produce no request are not handed to the GE generator, and
    with no requests at all the generator is not invoked."""
    profiler = _profiler(max_workers=2)
    engine = FakeSqlEngine(size_in_bytes=10)

    with (
        patch(
            "datahub.ingestion.source.unity.profiler.create_engine",
            return_value=engine.engine,
        ),
        patch.object(
            profiler, "generate_profile_workunits", autospec=True, return_value=[]
        ) as generate,
    ):
        assert (
            list(profiler.get_workunits([_table("a"), _table("b", column_count=0)]))
            == []
        )
        requests = generate.call_args.args[0]
        assert [r.pretty_name for r in requests] == ["main.sales.a"]

        generate.reset_mock()
        assert list(profiler.get_workunits([_table("c", column_count=0)])) == []
        generate.assert_not_called()
