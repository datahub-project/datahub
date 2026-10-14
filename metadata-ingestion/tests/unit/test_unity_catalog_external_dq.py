from typing import Any, Iterator, List, Sequence, Tuple
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.external_dq.contract import (
    RESULTS_COLUMNS,
    RULES_COLUMNS,
    LogicalType,
)
from datahub.ingestion.source.external_dq.validate import PhysicalColumn
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
from tests.unit.external_dq._fixtures import (
    SqlRow,
    databricks_columns,
    result_raw,
    rule_raw,
)


class FakeProxy:
    def __init__(self) -> None:
        self.queries: List[Tuple[str, Sequence[Any]]] = []
        self.rows = [SqlRow(rule_id="r1", updated_at=1)]

    def describe_table_columns(
        self, catalog: str, schema: str, table: str
    ) -> List[PhysicalColumn]:
        return [
            PhysicalColumn("rule_id", "string", 1),
            PhysicalColumn("updated_at", "timestamp", 2),
        ]

    def _execute_sql_query_streaming(
        self,
        query: str,
        params: Sequence[Any] = (),
        batch_size: int = 10000,
        *,
        raise_on_error: bool = False,
    ) -> Iterator[SqlRow]:
        assert raise_on_error, "the DQ reader must not swallow query failures"
        self.queries.append((query, params))
        yield from self.rows


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


def test_reader_converts_numpy_arrays_to_lists() -> None:
    # databricks-sql materializes rows through pandas, so ARRAY columns arrive as
    # numpy arrays; the contract layer only accepts lists.
    proxy = FakeProxy()
    proxy.rows = [SqlRow(rule_id="r1", dataset_path=np.array(["main", "s", "t"]))]
    reader = UnityExternalDQReader(proxy)  # type: ignore[arg-type]
    [row] = reader.read_rules("main.gov.dq_rules", [("rule_id", LogicalType.STRING)])
    assert type(row["dataset_path"]) is list and row["dataset_path"] == [
        "main",
        "s",
        "t",
    ]


def test_reader_counts_results_before_boundary() -> None:
    proxy = FakeProxy()
    proxy.rows = [SqlRow(n=7)]
    reader = UnityExternalDQReader(proxy)  # type: ignore[arg-type]
    assert reader.count_results_before("main.gov.dq_results", 42) == 7
    query, params = proxy.queries[0]
    assert query == (
        "SELECT count(*) AS n FROM `main`.`gov`.`dq_results` "
        "WHERE `executed_at` < timestamp_millis(%s)"
    )
    assert list(params) == [42]


def test_reader_describe_splits_the_table_name() -> None:
    proxy = FakeProxy()
    proxy.describe_table_columns = MagicMock(return_value=[])  # type: ignore[method-assign]
    UnityExternalDQReader(proxy).describe("main.gov.`dq.rules`")  # type: ignore[arg-type]
    proxy.describe_table_columns.assert_called_once_with("main", "gov", "dq.rules")


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


def _proxy(warehouse_id: Any = "wh") -> UnityCatalogApiProxy:
    client = MagicMock()
    client.config.host = "https://test.databricks.com"
    client.config.token = "t"
    client.config.warehouse_id = warehouse_id
    return UnityCatalogApiProxy(workspace_client=client, report=UnityCatalogReport())


@patch("datahub.ingestion.source.unity.proxy.connect")
def test_streaming_query_raises_when_asked(mock_connect: MagicMock) -> None:
    mock_connect.return_value.cursor.return_value.execute.side_effect = RuntimeError(
        "boom"
    )
    proxy = _proxy()
    # Default: reported, not raised (usage/lineage callers rely on this).
    assert list(proxy._execute_sql_query_streaming("SELECT 1")) == []
    with pytest.raises(RuntimeError, match="boom"):
        list(proxy._execute_sql_query_streaming("SELECT 1", raise_on_error=True))


def test_streaming_query_without_warehouse_raises_when_asked() -> None:
    with pytest.raises(RuntimeError, match="warehouse_id"):
        list(
            _proxy(warehouse_id=None)._execute_sql_query_streaming(
                "SELECT 1", raise_on_error=True
            )
        )


@patch("datahub.ingestion.source.unity.proxy.connect")
def test_describe_table_columns_returns_physical_columns(
    mock_connect: MagicMock,
) -> None:
    cursor = mock_connect.return_value.cursor.return_value
    cursor.fetchmany.side_effect = [
        [SqlRow(column_name="rule_id", full_data_type="string", ordinal_position=0)],
        [],
    ]
    assert _proxy().describe_table_columns("main", "gov", "dq_rules") == [
        PhysicalColumn("rule_id", "string", 0)
    ]


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
    ) -> List[PhysicalColumn]:
        return databricks_columns(
            RULES_COLUMNS if table == "dq_rules" else RESULTS_COLUMNS
        )

    def _execute_sql_query_streaming(
        self,
        query: str,
        params: Sequence[Any] = (),
        batch_size: int = 10000,
        *,
        raise_on_error: bool = False,
    ) -> Iterator[SqlRow]:
        if "count(*)" in query:
            yield SqlRow(n=0)
            return
        row = rule_raw() if "dq_rules" in query else result_raw()
        yield SqlRow(row)


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
