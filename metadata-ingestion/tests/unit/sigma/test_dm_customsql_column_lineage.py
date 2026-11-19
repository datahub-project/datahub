"""Column lineage for Data Model elements built from custom SQL.

Fixtures follow Sigma's documented shapes: ``/columns`` entries carry an opaque
``columnId``, a display ``name`` and a ``formula`` that references the SQL output
as ``[Custom SQL/<output column name>]``; ``/lineage`` carries a ``customSQL``
entry with ``connectionId`` and ``definition``.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sigma.config import SigmaSourceConfig
from datahub.ingestion.source.sigma.connection_registry import (
    SigmaConnectionRecord,
    SigmaConnectionRegistry,
)
from datahub.ingestion.source.sigma.data_classes import (
    CustomSqlEntry,
    SigmaDataModel,
    SigmaDataModelColumn,
    SigmaDataModelElement,
)
from datahub.ingestion.source.sigma.sigma import SigmaSource
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI
from datahub.metadata.schema_classes import (
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    UpstreamLineageClass,
)

_CONN = SigmaConnectionRecord(
    connection_id="conn-1",
    name="Warehouse",
    sigma_type="snowflake",
    datahub_platform="snowflake",
    is_mappable=True,
)
_ORDERS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)"
_CUSTOMERS = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.customers,PROD)"
)
_SCHEMAS: Dict[str, List[str]] = {
    _ORDERS: ["order_id", "customer_id", "amount"],
    _CUSTOMERS: ["customer_id", "name"],
}
_CSQL_SOURCE = "csql-1"


def _schema(cols: List[str]) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="t",
        platform="urn:li:dataPlatform:snowflake",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath=c,
                type=SchemaFieldDataTypeClass(NumberTypeClass()),
                nativeDataType="NUMBER",
            )
            for c in cols
        ],
    )


def _get_entities(
    entity_name: str, urns: List[str], aspects: List[str], **kwargs: object
) -> Dict[str, Dict[str, Tuple[SchemaMetadataClass, None]]]:
    """Serves SchemaMetadata the way DataHubGraph.get_entities does."""
    return {
        urn: {"schemaMetadata": (_schema(_SCHEMAS[urn]), None)}
        for urn in urns
        if urn in _SCHEMAS
    }


def _make_source(with_graph: bool = True, **config: object) -> SigmaSource:
    ctx = PipelineContext(run_id="test")
    if with_graph:
        graph = MagicMock(spec=DataHubGraph)
        graph.get_entities.side_effect = _get_entities
        ctx.graph = graph
    with (
        patch.object(SigmaAPI, "_generate_token"),
        patch.object(SigmaSource, "_build_connection_registry") as mock_reg,
    ):
        mock_reg.return_value = SigmaConnectionRegistry(
            by_id={_CONN.connection_id: _CONN}
        )
        return SigmaSource(
            config=SigmaSourceConfig.model_validate(
                {"client_id": "id", "client_secret": "secret", **config}
            ),
            ctx=ctx,
        )


def _data_model(
    sql: str,
    columns: List[Tuple[str, str]],
    element_name: str = "Orders",
    extra_elements: Optional[List[SigmaDataModelElement]] = None,
    extra_source_ids: Optional[List[str]] = None,
) -> SigmaDataModel:
    """One custom SQL element whose columns are (display name, formula) pairs."""
    element = SigmaDataModelElement(
        elementId="el-1",
        name=element_name,
        type="table",
        source_ids=[_CSQL_SOURCE, *(extra_source_ids or [])],
    )
    # Attached after construction, as _assemble_data_model does with /columns rows.
    element.columns = [
        SigmaDataModelColumn.model_validate(
            {
                # Sigma's custom SQL columnIds are opaque.
                "columnId": f"Xq{i}aZ9pLm",
                "name": name,
                "formula": formula,
                "elementId": "el-1",
            }
        )
        for i, (name, formula) in enumerate(columns)
    ]
    return SigmaDataModel(
        dataModelId="dm-1",
        name="My Model",
        createdAt=datetime(2024, 1, 1, tzinfo=timezone.utc),
        updatedAt=datetime(2024, 1, 2, tzinfo=timezone.utc),
        urlId="dm-url",
        elements=[element, *(extra_elements or [])],
        custom_sql_by_name={
            _CSQL_SOURCE: CustomSqlEntry(
                name=_CSQL_SOURCE,
                type="customSQL",
                connectionId=_CONN.connection_id,
                definition=sql,
            )
        },
    )


def _lineage(
    source: SigmaSource, dm: SigmaDataModel
) -> Tuple[str, Optional[UpstreamLineageClass]]:
    """Run element emission; return the element's final lineage."""
    urns = {
        el.elementId: source._gen_data_model_element_urn(dm, el) for el in dm.elements
    }
    element_urn = urns["el-1"]
    workunits = list(source._gen_data_model_workunit(dm, urns))
    found: Optional[UpstreamLineageClass] = None
    for wu in workunits:
        mcp = wu.metadata
        if (
            isinstance(mcp, MetadataChangeProposalWrapper)
            and mcp.entityUrn == element_urn
            and isinstance(mcp.aspect, UpstreamLineageClass)
        ):
            found = mcp.aspect
    return element_urn, found


def _edges(
    element_urn: str, lineage: Optional[UpstreamLineageClass]
) -> Dict[str, Set[str]]:
    """Sigma column name -> upstream schemaField URNs."""
    assert lineage is not None
    prefix = f"urn:li:schemaField:({element_urn},"
    out: Dict[str, Set[str]] = {}
    for fgl in lineage.fineGrainedLineages or []:
        for ds in fgl.downstreams or []:
            assert ds.startswith(prefix), f"downstream {ds} not on element"
            out.setdefault(ds[len(prefix) : -1], set()).update(fgl.upstreams or [])
    return out


def _col(table: str, col: str) -> str:
    return make_schema_field_urn(table, col)


def test_element_named_differently_from_custom_sql_prefix_gets_lineage_on_its_columns() -> (
    None
):
    source = _make_source()
    element_urn, lineage = _lineage(
        source,
        _data_model(
            "SELECT ORDER_ID, AMOUNT FROM MY_DB.MY_SCHEMA.ORDERS",
            [("Order Id", "[Custom SQL/ORDER_ID]"), ("Amount", "[Custom SQL/AMOUNT]")],
        ),
    )
    assert _edges(element_urn, lineage) == {
        "Order Id": {_col(_ORDERS, "order_id")},
        "Amount": {_col(_ORDERS, "amount")},
    }
    assert source.reporter.dm_customsql_column_lineage_emitted == 1


def test_unqualified_columns_in_join_resolve_through_upstream_schemas() -> None:
    source = _make_source()
    element_urn, lineage = _lineage(
        source,
        _data_model(
            "SELECT ORDER_ID, NAME FROM MY_DB.MY_SCHEMA.ORDERS O "
            "JOIN MY_DB.MY_SCHEMA.CUSTOMERS C ON O.CUSTOMER_ID = C.CUSTOMER_ID",
            [("Order Id", "[Custom SQL/ORDER_ID]"), ("Name", "[Custom SQL/NAME]")],
        ),
    )
    assert _edges(element_urn, lineage) == {
        "Order Id": {_col(_ORDERS, "order_id")},
        "Name": {_col(_CUSTOMERS, "name")},
    }


def test_sigma_platform_instance_is_not_applied_to_warehouse_tables() -> None:
    source = _make_source(platform_instance="sigma_prod")
    element_urn, lineage = _lineage(
        source,
        _data_model(
            "SELECT ORDER_ID, NAME FROM MY_DB.MY_SCHEMA.ORDERS O "
            "JOIN MY_DB.MY_SCHEMA.CUSTOMERS C ON O.CUSTOMER_ID = C.CUSTOMER_ID",
            [("Order Id", "[Custom SQL/ORDER_ID]"), ("Name", "[Custom SQL/NAME]")],
        ),
    )
    assert _edges(element_urn, lineage) == {
        "Order Id": {_col(_ORDERS, "order_id")},
        "Name": {_col(_CUSTOMERS, "name")},
    }


def test_connection_override_sets_instance_env_and_default_namespace() -> None:
    source = _make_source(
        platform_instance="sigma_prod",
        connection_to_platform_map={
            _CONN.connection_id: {
                "platform_instance": "wh",
                "env": "DEV",
                "default_database": "my_db",
                "default_schema": "my_schema",
            }
        },
    )
    _, lineage = _lineage(
        source,
        _data_model(
            "SELECT ORDER_ID FROM ORDERS", [("Order Id", "[Custom SQL/ORDER_ID]")]
        ),
    )
    assert lineage is not None
    assert [u.dataset for u in lineage.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,wh.my_db.my_schema.orders,DEV)"
    ]


def test_select_star_join_resolves_through_upstream_schemas() -> None:
    source = _make_source()
    element_urn, lineage = _lineage(
        source,
        _data_model(
            "SELECT O.*, C.NAME FROM MY_DB.MY_SCHEMA.ORDERS O "
            "JOIN MY_DB.MY_SCHEMA.CUSTOMERS C ON O.CUSTOMER_ID = C.CUSTOMER_ID",
            [("Amount", "[Custom SQL/AMOUNT]"), ("Name", "[Custom SQL/NAME]")],
        ),
    )
    assert _edges(element_urn, lineage) == {
        "Amount": {_col(_ORDERS, "amount")},
        "Name": {_col(_CUSTOMERS, "name")},
    }


def test_calculated_columns_inherit_upstreams_of_the_sql_columns_they_use() -> None:
    source = _make_source()
    element_urn, lineage = _lineage(
        source,
        _data_model(
            "SELECT ORDER_ID, CUSTOMER_ID, AMOUNT FROM MY_DB.MY_SCHEMA.ORDERS",
            [
                ("Amount", "[Custom SQL/AMOUNT]"),
                ("Doubled", "[Amount] * 2"),
                ("Key", "Concat([Custom SQL/ORDER_ID], [Custom SQL/CUSTOMER_ID])"),
                ("Constant", "1"),
            ],
        ),
    )
    assert _edges(element_urn, lineage) == {
        "Amount": {_col(_ORDERS, "amount")},
        "Doubled": {_col(_ORDERS, "amount")},
        "Key": {_col(_ORDERS, "order_id"), _col(_ORDERS, "customer_id")},
    }


def test_quoted_mixed_case_alias_matches_its_formula_reference() -> None:
    source = _make_source()
    element_urn, lineage = _lineage(
        source,
        _data_model(
            'SELECT ORDER_ID AS "orderId" FROM MY_DB.MY_SCHEMA.ORDERS',
            [("Order", "[Custom SQL/orderId]")],
        ),
    )
    assert _edges(element_urn, lineage) == {"Order": {_col(_ORDERS, "order_id")}}


def test_duplicate_output_names_get_no_edge_and_are_reported() -> None:
    source = _make_source()
    element_urn, lineage = _lineage(
        source,
        _data_model(
            "SELECT O.CUSTOMER_ID, C.CUSTOMER_ID, O.AMOUNT FROM MY_DB.MY_SCHEMA.ORDERS O "
            "JOIN MY_DB.MY_SCHEMA.CUSTOMERS C ON O.CUSTOMER_ID = C.CUSTOMER_ID",
            [
                ("Customer Id", "[Custom SQL/CUSTOMER_ID]"),
                ("Amount", "[Custom SQL/AMOUNT]"),
            ],
        ),
    )
    assert _edges(element_urn, lineage) == {"Amount": {_col(_ORDERS, "amount")}}
    assert source.reporter.dm_customsql_columns_unresolved == 1


def test_unaliased_aggregate_gets_no_edge_and_is_reported() -> None:
    source = _make_source()
    element_urn, lineage = _lineage(
        source,
        _data_model(
            "SELECT CUSTOMER_ID, SUM(AMOUNT) FROM MY_DB.MY_SCHEMA.ORDERS GROUP BY 1",
            [
                ("Customer Id", "[Custom SQL/CUSTOMER_ID]"),
                ("Total", "[Custom SQL/SUM(AMOUNT)]"),
            ],
        ),
    )
    assert _edges(element_urn, lineage) == {
        "Customer Id": {_col(_ORDERS, "customer_id")}
    }
    assert source.reporter.dm_customsql_columns_unresolved == 1


def test_uppercase_opaque_column_id_is_not_read_as_a_sql_column() -> None:
    source = _make_source()
    dm = _data_model(
        "SELECT ORDER_ID FROM MY_DB.MY_SCHEMA.ORDERS",
        [("Order Id", "[Custom SQL/ORDER_ID]")],
    )
    # A random columnId can be all uppercase; it names no SQL column.
    dm.elements[0].columns[0].columnId = "KD62PQK5AL"
    element_urn, lineage = _lineage(source, dm)
    assert _edges(element_urn, lineage) == {"Order Id": {_col(_ORDERS, "order_id")}}


def test_without_upstream_schemas_select_star_emits_table_lineage_and_reports_columns() -> (
    None
):
    source = _make_source(with_graph=False)
    element_urn, lineage = _lineage(
        source,
        _data_model(
            "SELECT * FROM MY_DB.MY_SCHEMA.ORDERS",
            [("Order Id", "[Custom SQL/ORDER_ID]"), ("Amount", "[Custom SQL/AMOUNT]")],
        ),
    )
    assert lineage is not None
    assert [u.dataset for u in lineage.upstreams] == [_ORDERS]
    assert not lineage.fineGrainedLineages
    assert source.reporter.dm_customsql_column_lineage_emitted == 0
    assert source.reporter.dm_customsql_columns_unresolved == 2
    assert any(
        "Sigma custom SQL column lineage" in (w.title or "")
        for w in source.reporter.warnings
    )


def test_custom_sql_refs_are_not_resolved_against_a_sibling_element_of_that_name() -> (
    None
):
    sibling = SigmaDataModelElement(elementId="el-2", name="Custom SQL", type="table")
    sibling.columns = [
        SigmaDataModelColumn.model_validate(
            {
                "columnId": "Pq7rT2vWx0",
                "name": "ORDER_ID",
                "formula": "[Other/ORDER_ID]",
                "elementId": "el-2",
            }
        )
    ]
    source = _make_source()
    element_urn, lineage = _lineage(
        source,
        _data_model(
            "SELECT ORDER_ID FROM MY_DB.MY_SCHEMA.ORDERS",
            [("Order Id", "[Custom SQL/ORDER_ID]")],
            extra_elements=[sibling],
            extra_source_ids=["el-2"],
        ),
    )
    assert _edges(element_urn, lineage) == {"Order Id": {_col(_ORDERS, "order_id")}}


def test_cte_case_and_union_columns_trace_to_every_contributing_column() -> None:
    source = _make_source()
    element_urn, lineage = _lineage(
        source,
        _data_model(
            "WITH A AS (SELECT ORDER_ID, CASE WHEN AMOUNT > 0 THEN AMOUNT END AS AMT "
            "FROM MY_DB.MY_SCHEMA.ORDERS) "
            "SELECT ORDER_ID AS ID, AMT FROM A "
            "UNION ALL SELECT CUSTOMER_ID, 0 FROM MY_DB.MY_SCHEMA.CUSTOMERS",
            [("Id", "[Custom SQL/ID]"), ("Amt", "[Custom SQL/AMT]")],
        ),
    )
    assert _edges(element_urn, lineage) == {
        "Id": {_col(_ORDERS, "order_id"), _col(_CUSTOMERS, "customer_id")},
        "Amt": {_col(_ORDERS, "amount")},
    }


def test_sql_with_sigma_templating_is_reported_without_lineage() -> None:
    source = _make_source()
    _, lineage = _lineage(
        source,
        _data_model(
            "SELECT ORDER_ID FROM {{#identifier [db]}}.MY_SCHEMA.ORDERS",
            [("Order Id", "[Custom SQL/ORDER_ID]")],
        ),
    )
    assert lineage is None
    assert source.reporter.dm_customsql_parse_failed == 1


@pytest.mark.parametrize(
    "connection_id, definition",
    [
        pytest.param(_CONN.connection_id, "", id="no-definition"),
        pytest.param("conn-unknown", "SELECT 1", id="unknown-connection"),
        pytest.param("", "SELECT 1", id="no-connection"),
    ],
)
def test_unparseable_custom_sql_source_is_skipped(
    connection_id: str, definition: str
) -> None:
    source = _make_source()
    dm = _data_model("unused", [("Order Id", "[Custom SQL/ORDER_ID]")])
    dm.custom_sql_by_name[_CSQL_SOURCE] = CustomSqlEntry(
        name=_CSQL_SOURCE,
        type="customSQL",
        connectionId=connection_id,
        definition=definition,
    )
    _, lineage = _lineage(source, dm)
    assert lineage is None
    assert source.reporter.dm_customsql_skipped == 1


def test_element_with_two_custom_sql_sources_is_skipped() -> None:
    source = _make_source()
    dm = _data_model(
        "SELECT ORDER_ID FROM MY_DB.MY_SCHEMA.ORDERS",
        [("Order Id", "[Custom SQL/ORDER_ID]")],
        extra_source_ids=["csql-2"],
    )
    dm.custom_sql_by_name["csql-2"] = CustomSqlEntry(
        name="csql-2",
        type="customSQL",
        connectionId=_CONN.connection_id,
        definition="SELECT NAME FROM MY_DB.MY_SCHEMA.CUSTOMERS",
    )
    _, lineage = _lineage(source, dm)
    assert lineage is None
    assert source.reporter.dm_customsql_skipped == 1


def test_custom_sql_lineage_merges_with_lineage_to_a_sibling_element() -> None:
    sibling = SigmaDataModelElement(elementId="el-2", name="Base", type="table")
    sibling.columns = [
        SigmaDataModelColumn.model_validate(
            {
                "columnId": "Bq3rT2vWx9",
                "name": "Region",
                "formula": "[Other/Region]",
                "elementId": "el-2",
            }
        )
    ]
    source = _make_source()
    dm = _data_model(
        "SELECT ORDER_ID FROM MY_DB.MY_SCHEMA.ORDERS",
        [("Order Id", "[Custom SQL/ORDER_ID]"), ("Region", "[Base/Region]")],
        extra_elements=[sibling],
        extra_source_ids=["el-2"],
    )
    element_urn, lineage = _lineage(source, dm)
    sibling_urn = source._gen_data_model_element_urn(dm, sibling)
    assert lineage is not None
    assert {u.dataset for u in lineage.upstreams} == {_ORDERS, sibling_urn}
    assert _edges(element_urn, lineage) == {
        "Order Id": {_col(_ORDERS, "order_id")},
        "Region": {_col(sibling_urn, "Region")},
    }
    assert source.reporter.dm_customsql_columns_unresolved == 0


def test_without_upstream_schemas_unqualified_join_columns_are_reported() -> None:
    source = _make_source(with_graph=False)
    element_urn, lineage = _lineage(
        source,
        _data_model(
            "SELECT ORDER_ID, NAME FROM MY_DB.MY_SCHEMA.ORDERS O "
            "JOIN MY_DB.MY_SCHEMA.CUSTOMERS C ON O.CUSTOMER_ID = C.CUSTOMER_ID",
            [("Order Id", "[Custom SQL/ORDER_ID]"), ("Name", "[Custom SQL/NAME]")],
        ),
    )
    assert lineage is not None
    assert {u.dataset for u in lineage.upstreams} == {_ORDERS, _CUSTOMERS}
    assert not lineage.fineGrainedLineages
    assert source.reporter.dm_customsql_columns_unresolved == 2


def test_sql_reading_another_sigma_element_is_reported() -> None:
    source = _make_source()
    element_urn, lineage = _lineage(
        source,
        _data_model(
            "SELECT O.ORDER_ID, E.REGION FROM MY_DB.MY_SCHEMA.ORDERS O "
            "JOIN sigma_element('Regions') E ON O.ORDER_ID = E.ORDER_ID",
            [("Order Id", "[Custom SQL/ORDER_ID]")],
        ),
    )
    assert any(
        "reads other Sigma elements" in (i.title or "") for i in source.reporter.infos
    )


def test_a_column_reading_an_unresolvable_sql_column_gets_no_edge_nor_do_its_dependents() -> (
    None
):
    source = _make_source()
    element_urn, lineage = _lineage(
        source,
        _data_model(
            "SELECT ORDER_ID, CUSTOMER_ID, SUM(AMOUNT) FROM MY_DB.MY_SCHEMA.ORDERS GROUP BY 1, 2",
            [
                ("Order Id", "[Custom SQL/ORDER_ID]"),
                ("Key", "Concat([Custom SQL/CUSTOMER_ID], [Custom SQL/SUM(AMOUNT)])"),
                ("Key Twice", "Concat([Key], [Key])"),
            ],
        ),
    )
    assert _edges(element_urn, lineage) == {"Order Id": {_col(_ORDERS, "order_id")}}
    assert source.reporter.dm_customsql_columns_unresolved == 2


def test_with_one_joined_schema_missing_no_column_gets_a_partial_edge() -> None:
    source = _make_source()
    element_urn, lineage = _lineage(
        source,
        _data_model(
            "SELECT COALESCE(AMOUNT, BONUS) AS X FROM MY_DB.MY_SCHEMA.ORDERS O "
            "JOIN MY_DB.MY_SCHEMA.PROMOS P ON O.ORDER_ID = P.ORDER_ID",
            [("X", "[Custom SQL/X]")],
        ),
    )
    assert lineage is not None
    assert not lineage.fineGrainedLineages
    assert source.reporter.dm_customsql_columns_unresolved == 1


def test_a_column_read_from_another_sigma_element_is_reported() -> None:
    source = _make_source()
    element_urn, lineage = _lineage(
        source,
        _data_model(
            "SELECT O.ORDER_ID, E.REGION FROM MY_DB.MY_SCHEMA.ORDERS O "
            "JOIN sigma_element('Regions') E ON O.ORDER_ID = E.ORDER_ID",
            [("Order Id", "[Custom SQL/ORDER_ID]"), ("Region", "[Custom SQL/REGION]")],
        ),
    )
    assert _edges(element_urn, lineage) == {}
    assert source.reporter.dm_customsql_columns_unresolved == 2


def test_custom_sql_refs_of_an_element_with_an_ambiguous_sql_source_map_nowhere() -> (
    None
):
    sibling = SigmaDataModelElement(elementId="el-2", name="Custom SQL", type="table")
    sibling.columns = [
        SigmaDataModelColumn.model_validate(
            {
                "columnId": "Pq7rT2vWx0",
                "name": "ORDER_ID",
                "formula": "[Other/ORDER_ID]",
                "elementId": "el-2",
            }
        )
    ]
    source = _make_source()
    dm = _data_model(
        "unused",
        [("Order Id", "[Custom SQL/ORDER_ID]")],
        extra_elements=[sibling],
        extra_source_ids=["el-2"],
    )
    # Two /lineage customSQL entries shared this name, so neither was kept.
    dm.custom_sql_by_name = {}
    dm.ambiguous_custom_sql_names = {_CSQL_SOURCE}
    element_urn, lineage = _lineage(source, dm)
    assert _edges(element_urn, lineage) == {}
