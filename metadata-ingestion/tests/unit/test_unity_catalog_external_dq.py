from typing import Any, Dict, Iterator, List, Sequence, Tuple
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.external_dq.contract import (
    RESULTS_COLUMNS,
    RULES_COLUMNS,
    LogicalType,
)
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.external_dq import (
    UnityDatasetLocator,
    UnityExternalDQReader,
)
from datahub.ingestion.source.unity.identifier_helper import quote_databricks_identifier
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.unity.source import UnityCatalogSource
from datahub.metadata.schema_classes import AssertionInfoClass, AssertionRunEventClass
from tests.unit.external_dq._fixtures import result_raw, rule_raw


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


_DBX = {
    LogicalType.STRING: "string",
    LogicalType.BOOLEAN: "boolean",
    LogicalType.INT64: "bigint",
    LogicalType.FLOAT64: "double",
    LogicalType.TIMESTAMP: "timestamp",
    LogicalType.ARRAY_STRING: "array<string>",
}
_BASE = {
    "token": "t",
    "workspace_url": "https://test.databricks.com",
    "include_hive_metastore": False,
}
_DQ = {
    "enabled": True,
    "rules_table": "main.governance.dq_rules",
    "results_table": "main.governance.dq_results",
}


def test_config_requires_warehouse_and_three_part_names() -> None:
    with pytest.raises(ValueError, match="warehouse_id"):
        UnityCatalogSourceConfig.model_validate({**_BASE, "external_dq": _DQ})
    with pytest.raises(ValueError, match="catalog.schema.table"):
        UnityCatalogSourceConfig.model_validate(
            {
                **_BASE,
                "warehouse_id": "wh",
                "external_dq": {**_DQ, "rules_table": "dq_rules"},
            }
        )


class ContractProxy:
    def describe_table_columns(
        self, catalog: str, schema: str, table: str
    ) -> List[Tuple[str, str, int]]:
        contract = RULES_COLUMNS if table == "dq_rules" else RESULTS_COLUMNS
        return [(c.name, _DBX[c.logical_type], i + 1) for i, c in enumerate(contract)]

    def iter_sql_rows(self, query: str, params: Sequence[Any] = ()) -> Iterator[_Row]:
        row = rule_raw() if "dq_rules" in query else result_raw()
        yield _Row(row)


def test_source_emits_external_dq_assertions_for_ingested_tables() -> None:
    config = UnityCatalogSourceConfig.model_validate(
        {**_BASE, "warehouse_id": "wh", "external_dq": _DQ}
    )
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        source = UnityCatalogSource(PipelineContext(run_id="test"), config)
    source.table_refs = {
        TableReference(metastore=None, catalog="main", schema="sales", table="orders")
    }
    source.unity_catalog_api_proxy = ContractProxy()  # type: ignore[assignment]
    workunits = list(source._get_external_dq_workunits())
    aspects = [wu.metadata.aspect for wu in workunits]  # type: ignore[union-attr]
    info = next(a for a in aspects if isinstance(a, AssertionInfoClass))
    assert info.customAssertion is not None
    assert info.customAssertion.entity == source.gen_dataset_urn(
        next(iter(source.table_refs))
    )
    assert info.customAssertion.type == "Databricks Data Quality"
    assert any(isinstance(a, AssertionRunEventClass) for a in aspects)
    assert source.report.external_dq.run_events_emitted == 1
