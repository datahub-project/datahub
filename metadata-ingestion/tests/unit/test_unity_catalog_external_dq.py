from typing import Any, Dict, Iterator, List, Sequence, Tuple
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.source.external_dq.contract import LogicalType
from datahub.ingestion.source.unity.external_dq import (
    UnityDatasetLocator,
    UnityExternalDQReader,
)
from datahub.ingestion.source.unity.identifier_helper import quote_databricks_identifier
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport


class _Row(dict):
    def asDict(self) -> Dict[str, Any]:
        return dict(self)


class FakeProxy:
    def __init__(self) -> None:
        self.queries: List[Tuple[str, Sequence[Any]]] = []

    def describe_table_columns(
        self, catalog: str, schema: str, table: str
    ) -> List[Tuple[str, str, int]]:
        return [("rule_id", "string", 1), ("updated_at", "timestamp", 2)]

    def iter_sql_rows(self, query: str, params: Sequence[Any] = ()) -> Iterator[_Row]:
        self.queries.append((query, params))
        yield _Row(rule_id="r1", updated_at=1)


def test_quote_escapes_backticks() -> None:
    assert quote_databricks_identifier("we`ird") == "`we``ird`"


def test_reader_selects_named_columns_and_epoch_millis() -> None:
    proxy = FakeProxy()
    reader = UnityExternalDQReader(proxy)  # type: ignore[arg-type]
    columns = [
        ("rule_id", LogicalType.STRING),
        ("updated_at", LogicalType.TIMESTAMP),
        ("Extra", None),
    ]
    rows = list(reader.read_rules("main.gov.`dq.rules`", columns))
    query, _ = proxy.queries[0]
    assert "FROM `main`.`gov`.`dq.rules`" in query
    assert "unix_millis(`updated_at`) AS `updated_at`" in query
    assert "`Extra` AS `extra`" in query
    assert rows == [{"rule_id": "r1", "updated_at": 1}]


def test_reader_filters_and_orders_results() -> None:
    proxy = FakeProxy()
    reader = UnityExternalDQReader(proxy)  # type: ignore[arg-type]
    list(
        reader.read_results("main.gov.dq_results", [("run_id", LogicalType.STRING)], 42)
    )
    query, params = proxy.queries[0]
    assert "WHERE `executed_at` >= timestamp_millis(%s)" in query
    assert query.rstrip().endswith("ORDER BY `executed_at`, `run_id`")
    assert list(params) == [42]


def test_reader_describe_maps_physical_columns() -> None:
    reader = UnityExternalDQReader(FakeProxy())  # type: ignore[arg-type]
    described = reader.describe("main.gov.dq_rules")
    assert [(c.name, c.data_type, c.position) for c in described] == [
        ("rule_id", "string", 1),
        ("updated_at", "timestamp", 2),
    ]


def test_locator_is_case_insensitive() -> None:
    ref = TableReference(metastore=None, catalog="main", schema="sales", table="orders")
    locator = UnityDatasetLocator(
        [ref], lambda r: make_dataset_urn("databricks", str(r))
    )
    assert locator.dataset_urn(["Main", "SALES", "Orders"]) == make_dataset_urn(
        "databricks", "main.sales.orders"
    )
    assert locator.dataset_urn(["main", "sales"]) is None
    assert locator.dataset_urn(["main", "sales", "missing"]) is None


def test_locator_fixes_column_casing_from_ingested_schema() -> None:
    urn = make_dataset_urn("databricks", "main.sales.orders")
    locator = UnityDatasetLocator([], str, field_names=lambda u: ["OrderId", "amount"])
    assert locator.field_urn(urn, "orderid").endswith(",OrderId)")
    # Unknown columns are passed through so the rule is still published.
    assert locator.field_urn(urn, "not_a_column").endswith(",not_a_column)")


@patch("datahub.ingestion.source.unity.proxy.connect")
def test_iter_sql_rows_raises_instead_of_swallowing(mock_connect: MagicMock) -> None:
    mock_connect.return_value.cursor.return_value.execute.side_effect = RuntimeError(
        "boom"
    )
    client = MagicMock()
    client.config.host = "https://test.databricks.com"
    client.config.token = "t"
    client.config.warehouse_id = "wh"
    proxy = UnityCatalogApiProxy(workspace_client=client, report=UnityCatalogReport())
    with pytest.raises(RuntimeError, match="boom"):
        list(proxy.iter_sql_rows("SELECT 1"))
