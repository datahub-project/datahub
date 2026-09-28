from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.engine import make_url
from sqlalchemy.engine.default import DefaultDialect

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.vertica import VerticaConfig, VerticaSource
from datahub.ingestion.source.sql.vertica_inspector import (
    DataHubVerticaDialect,
    VerticaInspector,
)
from datahub.metadata.schema_classes import UpstreamLineageClass


class _FakeResult:
    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> List[Dict[str, Any]]:
        return self._rows

    def scalar(self) -> Optional[Any]:
        return next(iter(self._rows[0].values())) if self._rows else None


class _FakeConnection:
    """Answers catalog queries from canned rows keyed on a SQL substring."""

    def __init__(self, rows_by_table: Dict[str, List[Dict[str, Any]]]) -> None:
        self._rows_by_table = rows_by_table
        self.executed: List[Tuple[str, Dict[str, Any]]] = []

    def execute(
        self, statement: Any, params: Optional[Dict[str, Any]] = None
    ) -> _FakeResult:
        sql = str(statement)
        self.executed.append((sql, params or {}))
        for table, rows in self._rows_by_table.items():
            if table in sql:
                schema = (params or {}).get("schema")
                return _FakeResult(
                    [row for row in rows if row.get("_schema", schema) == schema]
                )
        return _FakeResult([])


def _inspector_with(fake: _FakeConnection) -> VerticaInspector:
    inspector = VerticaInspector(inspect(create_engine("sqlite://")))

    @contextmanager
    def _fake_connection() -> Iterator[_FakeConnection]:
        yield fake

    inspector._reflection_connection = _fake_connection  # type: ignore[method-assign,assignment]
    return inspector


def test_reflection_connection_reuses_bound_connection():
    """Vertica reflection reuses the inspector's bound connection (no per-query checkout).

    get_inspectors() binds the inspector to a live Connection, so the Vertica-specific
    reflection methods must reuse it rather than opening a new connection each call —
    and must not close the borrowed connection.
    """
    engine = create_engine("sqlite://")
    conn = engine.connect()
    inspector = VerticaInspector(inspect(conn))

    with inspector._reflection_connection() as reused:
        assert reused is conn
    assert not conn.closed  # borrowed connection must stay open
    conn.close()


def test_reflection_connection_falls_back_to_short_lived_connection():
    """If bound to an Engine, open and close a short-lived connection instead."""
    engine = create_engine("sqlite://")
    inspector = VerticaInspector(inspect(engine))

    with inspector._reflection_connection() as opened:
        assert not opened.closed
    assert opened.closed  # short-lived connection is closed on exit


def test_vertica_uri_https():
    config = VerticaConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:5433",
            "database": "db",
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "vertica+vertica_python://user:password@host:5433/db"
    )


def test_view_lineage_is_database_qualified():
    fake = _FakeConnection(
        {
            "v_catalog.view_tables": [
                {
                    "database_name": "vmart",
                    "table_name": "sales_view",
                    "table_schema": "public",
                    "reference_table_name": "customer_dimension",
                    "reference_table_schema": "public",
                },
                {
                    "database_name": "vmart",
                    "table_name": "sales_view",
                    "table_schema": "public",
                    "reference_table_name": "store_sales_fact",
                    "reference_table_schema": "store",
                },
            ]
        }
    )
    inspector = _inspector_with(fake)

    assert inspector._populate_view_lineage("sales_view", "public") == {
        "vmart.public.sales_view": [
            ("vmart.public.customer_dimension", "[]", "[]"),
            ("vmart.store.store_sales_fact", "[]", "[]"),
        ]
    }

    source = VerticaSource(
        VerticaConfig.model_validate({"host_port": "host:5433", "database": "vmart"}),
        PipelineContext(run_id="vertica-test"),
    )
    dataset_name = source.get_identifier(
        schema="public", entity="sales_view", inspector=inspector
    )
    lineage = source._get_upstream_lineage_info(
        f"urn:li:dataset:(urn:li:dataPlatform:vertica,{dataset_name},PROD)",
        inspector,
        "sales_view",
        "public",
    )
    assert isinstance(lineage, UpstreamLineageClass)
    assert [upstream.dataset for upstream in lineage.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:vertica,vmart.public.customer_dimension,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:vertica,vmart.store.store_sales_fact,PROD)",
    ]


def test_schema_wide_queries_run_once_per_schema():
    fake = _FakeConnection(
        {
            "v_catalog.tables": [
                {"_schema": "public", "table_name": "a", "owner_name": "alice"},
                {"_schema": "public", "table_name": "b", "owner_name": "bob"},
                {"_schema": "store", "table_name": "c", "owner_name": "carol"},
            ]
        }
    )
    inspector = _inspector_with(fake)

    assert inspector.get_table_owner("a", "public") == "alice"
    assert inspector.get_table_owner("b", "public") == "bob"
    assert len(fake.executed) == 1

    assert inspector.get_table_owner("c", "store") == "carol"
    assert len(fake.executed) == 2
    assert [params["schema"] for _, params in fake.executed] == ["public", "store"]


def test_projection_properties_query_once_per_schema():
    fake = _FakeConnection({})
    inspector = _inspector_with(fake)

    inspector.get_projection_comment("p1", "public")
    queries_for_first_projection = len(fake.executed)
    inspector.get_projection_comment("p2", "public")

    assert queries_for_first_projection > 0
    assert len(fake.executed) == queries_for_first_projection


def test_vertica_scheme_resolves_to_datahub_dialect():
    assert (
        make_url("vertica+vertica_python://user@host:5433/db").get_dialect()
        is DataHubVerticaDialect
    )


@pytest.mark.parametrize(
    "method_name",
    sorted(name for name in dir(PGDialect) if name.startswith("get_multi_")),
)
def test_multi_reflection_avoids_pg_catalog(method_name: str) -> None:
    # PGDialect's get_multi_* query pg_catalog, which Vertica lacks; Table
    # autoload must go through DefaultDialect's per-table fallbacks instead.
    assert getattr(DataHubVerticaDialect, method_name) is getattr(
        DefaultDialect, method_name
    )


def test_view_definition_reads_vertica_catalog():
    fake = _FakeConnection({"v_catalog.views": [{"view_definition": "SELECT 1"}]})

    definition = DataHubVerticaDialect().get_view_definition(
        fake,  # type: ignore[arg-type]
        "Sales_View",
        schema="Public",
    )

    assert definition == "SELECT 1"
    ((sql, params),) = fake.executed
    assert "pg_catalog" not in sql
    assert params == {"schema": "public", "view": "sales_view"}
