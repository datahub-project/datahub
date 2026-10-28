from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.service.catalog import DataSourceFormat, TableType

from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
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

GIB = 2**30


class _Row:
    """The subset of a SQLAlchemy Row the profiler reads."""

    def __init__(self, **values: Any) -> None:
        self._values = values

    def _asdict(self) -> Dict[str, Any]:
        return dict(self._values)


class _FakeEngine:
    """A SQLAlchemy engine stand-in answering DESCRIBE DETAIL and count(*)."""

    def __init__(
        self,
        size_in_bytes: Optional[int] = None,
        row_count: Optional[int] = None,
        describe_error: Optional[Exception] = None,
    ) -> None:
        self.executed: List[str] = []
        self._size = size_in_bytes
        self._rows = row_count
        self._describe_error = describe_error
        self.engine = MagicMock()
        conn = self.engine.connect.return_value.__enter__.return_value
        conn.dialect.identifier_preparer.quote = lambda name: f"`{name}`"
        conn.execute.side_effect = self._execute

    def _execute(self, statement: Any) -> MagicMock:
        sql = str(statement)
        self.executed.append(sql)
        result = MagicMock()
        if sql.startswith("DESCRIBE DETAIL"):
            if self._describe_error:
                raise self._describe_error
            result.fetchone.return_value = _Row(sizeInBytes=self._size)
        else:
            result.fetchone.return_value = _Row(numRows=self._rows)
        return result


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


@pytest.fixture
def fake_workspace_client() -> Iterator[MagicMock]:
    ws = MagicMock()
    ws.config.host = "https://example.cloud.databricks.com"
    ws.config.warehouse_id = "wh1"
    with patch(
        "datahub.ingestion.source.unity.profiler.create_workspace_client",
        return_value=ws,
    ):
        yield ws


def _profiler(**profiling: Any) -> UnityCatalogProfiler:
    config = UnityCatalogSourceConfig.model_validate(
        {
            "workspace_url": "https://example.cloud.databricks.com",
            "token": "t",
            "warehouse_id": "wh1",
            "profiling": {"method": "sqlalchemy", "enabled": True, **profiling},
        }
    )
    assert config.is_sqlalchemy_profiling()
    return UnityCatalogProfiler(
        config,
        config.profiling,  # type: ignore[arg-type]
        UnityCatalogReport(),
    )


@pytest.mark.usefixtures("fake_workspace_client")
def test_delta_table_size_is_read_and_a_full_profile_requested() -> None:
    profiler = _profiler()
    engine = _FakeEngine(size_in_bytes=1024)

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


@pytest.mark.usefixtures("fake_workspace_client")
def test_oversized_delta_table_falls_back_to_table_level_with_a_row_count() -> None:
    profiler = _profiler(profile_table_size_limit=1)
    engine = _FakeEngine(size_in_bytes=3 * GIB, row_count=42)

    request = profiler.get_unity_profile_request(
        UnityCatalogSQLGenericTable(_table()), engine.engine
    )

    assert request is not None
    assert request.profile_table_level_only
    assert request.table.rows_count == 42
    assert any("count(*)" in sql for sql in engine.executed)
    assert profiler.report.num_profile_missing_row_count == 0


@pytest.mark.usefixtures("fake_workspace_client")
def test_non_delta_table_is_profiled_without_delta_only_queries() -> None:
    profiler = _profiler(profile_table_level_only=True)
    engine = _FakeEngine()

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


@pytest.mark.usefixtures("fake_workspace_client")
def test_size_lookup_failure_is_reported_and_profiling_continues() -> None:
    profiler = _profiler()
    engine = _FakeEngine(describe_error=RuntimeError("DESCRIBE DETAIL denied"))

    request = profiler.get_unity_profile_request(
        UnityCatalogSQLGenericTable(_table()), engine.engine
    )

    assert request is not None
    assert request.table.size_in_bytes is None
    assert len(profiler.report.warnings) == 1


@pytest.mark.usefixtures("fake_workspace_client")
def test_table_without_columns_is_dropped() -> None:
    profiler = _profiler(report_dropped_profiles=True)

    request = profiler.get_unity_profile_request(
        UnityCatalogSQLGenericTable(_table(column_count=0)),
        _FakeEngine(size_in_bytes=1).engine,
    )

    assert request is None
    assert "main.sales.orders" in profiler.report.filtered


@pytest.mark.usefixtures("fake_workspace_client")
def test_stale_table_is_skipped_entirely() -> None:
    profiler = _profiler(profile_if_updated_since_days=1)
    stale = datetime.now(timezone.utc) - timedelta(days=10)

    request = profiler.get_unity_profile_request(
        UnityCatalogSQLGenericTable(_table(updated_at=stale)),
        _FakeEngine(size_in_bytes=1).engine,
    )

    assert request is None
    assert profiler.report.entities_profiled == 0


@pytest.mark.usefixtures("fake_workspace_client")
def test_get_workunits_only_forwards_tables_that_produced_requests() -> None:
    profiler = _profiler(max_workers=2)
    engine = _FakeEngine(size_in_bytes=10)
    generate = MagicMock(return_value=[])

    with (
        patch(
            "datahub.ingestion.source.unity.profiler.create_engine",
            return_value=engine.engine,
        ),
        patch.object(profiler, "generate_profile_workunits", generate),
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
