"""The shared `sql` command: the base owns the fetch-one-extra convention, so a
driver adapter cannot silently make every result look complete.
"""

from typing import Any, List

import pytest

from datahub.ingestion.agent.probe_methods import (
    ProbeMethodSpec,
    _enforce_gates,
    _iter_specs,
)
from datahub.ingestion.agent.sql_gate import SqlScopeError
from datahub.ingestion.agent.sql_passthrough import (
    CatalogRows,
    SqlCatalogPassthrough,
    rows_from_mappings,
)

CATALOG_QUERY = "SELECT table_name FROM information_schema.tables"


class _Provider(SqlCatalogPassthrough):
    """A driver adapter that honours the limit it is handed, as the contract asks."""

    sql_dialect = "postgres"

    def __init__(self, available: int = 100) -> None:
        self.available = available
        self.asked_for: List[int] = []

    def execute_catalog_query(self, query: str, limit: int) -> CatalogRows:
        self.asked_for.append(limit)
        return CatalogRows(
            columns=["table_name"],
            rows=[[f"t{i}"] for i in range(min(self.available, limit))],
        )

    def __exit__(self, *exc: object) -> None:
        return None


def _sql_spec(owner: type) -> ProbeMethodSpec:
    spec = getattr(getattr(owner, "sql", None), "__probe_command__", None)
    assert isinstance(spec, ProbeMethodSpec)
    return spec


def test_the_adapter_is_asked_for_one_more_row_than_the_caller_wanted():
    # The whole reason this is shared. `truncated` is computed by comparing rows
    # returned against the limit, so an adapter fetching exactly `limit` would report
    # truncated=false for a result set that was cut short -- and an agent would
    # conclude it had seen every table in the catalog.
    provider = _Provider()
    provider.sql(CATALOG_QUERY, limit=10)
    assert provider.asked_for == [11]


def test_more_rows_than_the_limit_is_reported_as_truncated():
    out = _Provider(available=100).sql(CATALOG_QUERY, limit=10)
    assert out["row_count"] == 10
    assert out["truncated"] is True


def test_exactly_the_limit_is_not_truncated():
    # The boundary the +1 exists to resolve: 10 available, 10 asked for.
    out = _Provider(available=10).sql(CATALOG_QUERY, limit=10)
    assert out["row_count"] == 10
    assert out["truncated"] is False


def test_fewer_rows_than_the_limit_is_not_truncated():
    out = _Provider(available=3).sql(CATALOG_QUERY, limit=10)
    assert out["rows"] == [["t0"], ["t1"], ["t2"]]
    assert out["truncated"] is False


def test_the_inherited_command_is_discovered_and_gated():
    provider = _Provider()
    assert "sql" in dict(_iter_specs(_Provider))
    # The gate reads sql_dialect off the instance, so the mixing class's dialect
    # decides which grammar the query is parsed against.
    with pytest.raises(SqlScopeError):
        _enforce_gates(_sql_spec(_Provider), provider, {"query": "SELECT * FROM t"})
    _enforce_gates(_sql_spec(_Provider), provider, {"query": CATALOG_QUERY})


def test_a_provider_that_forgets_the_adapter_says_so():
    class Forgetful(SqlCatalogPassthrough):
        sql_dialect = "postgres"

    with pytest.raises(NotImplementedError, match="execute_catalog_query"):
        Forgetful().sql(CATALOG_QUERY)


def test_mappings_take_their_column_order_from_the_first_record():
    rows = rows_from_mappings([{"b": 2, "a": 1}, {"a": 10, "b": 20}])
    assert rows.columns == ["b", "a"]
    # Read through the first record's order, so a driver that varies key order
    # between rows cannot shear the result set into the wrong columns.
    assert rows.rows == [[2, 1], [20, 10]]


def test_mappings_with_no_records_yield_no_columns():
    rows = rows_from_mappings([])
    assert rows.columns == []
    assert rows.rows == []


def test_a_missing_key_in_a_later_record_reads_as_null_not_a_shift():
    rows = rows_from_mappings([{"a": 1, "b": 2}, {"a": 3}])
    assert rows.rows == [[1, 2], [3, None]]


@pytest.mark.parametrize(
    "provider_module,provider_name",
    [
        (
            "datahub.ingestion.source.snowflake.snowflake_probe",
            "SnowflakeMetadataProbe",
        ),
        (
            "datahub.ingestion.source.bigquery_v2.bigquery_probe",
            "BigQueryMetadataProbe",
        ),
        ("datahub.ingestion.source.sql.sqlalchemy_probe", "SqlAlchemyMetadataProbe"),
    ],
)
def test_every_warehouse_probe_supplies_only_its_driver_adapter(
    provider_module: str, provider_name: str
) -> None:
    # Each of these had its own copy of the sql method. What remains connector-side
    # is execute_catalog_query and nothing else about the command.
    import importlib

    cls: Any = getattr(importlib.import_module(provider_module), provider_name)
    assert issubclass(cls, SqlCatalogPassthrough)
    assert "execute_catalog_query" in vars(cls)
    assert "sql" not in vars(cls)
    # And each still declares the dialect the gate parses against.
    assert hasattr(cls, "sql_dialect")
