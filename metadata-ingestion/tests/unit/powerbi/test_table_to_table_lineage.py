"""Tests for M-Query table-to-table lineage (ING-1905).

Bridge-backed: each M-Query expression is parsed via the JS bridge, mirroring
tests/unit/test_ast_utils.py. No static fixtures.
"""

import datahub.ingestion.source.powerbi.m_query.parser as parser
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    ResolvePlatformInstanceFromDatasetTypeMapping,
)
from datahub.ingestion.source.powerbi.m_query._bridge import (
    NodeIdMap,
    _clear_bridge,
    get_bridge,
)
from datahub.ingestion.source.powerbi.m_query.resolver import (
    resolve_to_table_references,
)
from datahub.ingestion.source.powerbi.powerbi import Mapper
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    PowerBIDataset,
    Table,
)


def _parse(expression: str) -> NodeIdMap:
    _clear_bridge()
    node_map = get_bridge().parse(expression)
    _clear_bridge()
    return node_map


def _config() -> PowerBiDashboardSourceConfig:
    return PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
    )


def _dataset_with_tables(tables: list) -> PowerBIDataset:
    dataset = PowerBIDataset(
        id="d1",
        name="ds",
        description="",
        webUrl=None,
        workspace_id="w1",
        workspace_name="w",
        parameters={},
        tables=tables,
        tags=[],
    )
    for table in tables:
        table.dataset = dataset
    return dataset


def _mapper(config: PowerBiDashboardSourceConfig) -> Mapper:
    return Mapper(
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
        reporter=PowerBiDashboardSourceReport(),
        dataplatform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
    )


def test_bare_sibling_reference() -> None:
    node_map = _parse("let\n    Source = DimDate\nin\n    Source")
    assert resolve_to_table_references(node_map) == ["DimDate"]


def test_quoted_sibling_reference() -> None:
    node_map = _parse('let\n    source = #"tbl_PayrollHistory"\nin\n    source')
    assert resolve_to_table_references(node_map) == ["tbl_PayrollHistory"]


def test_table_combine_multiple_references() -> None:
    node_map = _parse(
        "let\n"
        "    Source = Table.Combine({tblDivEthnTitle, tblDivGenTitle})\n"
        "in\n"
        "    Source"
    )
    assert resolve_to_table_references(node_map) == [
        "tblDivEthnTitle",
        "tblDivGenTitle",
    ]


def test_bare_reference_without_let() -> None:
    node_map = _parse("DimDate")
    assert resolve_to_table_references(node_map) == ["DimDate"]


def test_nested_join_captures_both_tables() -> None:
    # Table.NestedJoin / Table.Join reference two sibling tables across their
    # arguments; both must be captured, not just the first (left) one.
    node_map = _parse(
        'let Source = Table.NestedJoin(tblOrders, {"id"}, tblCustomers, {"id"},'
        ' "cust", JoinKind.LeftOuter) in Source'
    )
    refs = resolve_to_table_references(node_map)
    assert "tblOrders" in refs
    assert "tblCustomers" in refs


def test_local_variables_are_not_references() -> None:
    node_map = _parse("let\n    a = 1,\n    b = a + 1\nin\n    b")
    assert resolve_to_table_references(node_map) == []


def test_genuine_data_source_has_no_table_references() -> None:
    node_map = _parse(
        "let\n"
        '    Source = Snowflake.Databases("acct.snowflakecomputing.com","WH"),\n'
        '    db = Source{[Name="DB",Kind="Database"]}[Data],\n'
        '    sch = db{[Name="PUBLIC",Kind="Schema"]}[Data],\n'
        '    tbl = sch{[Name="ORDERS",Kind="Table"]}[Data]\n'
        "in\n"
        "    tbl"
    )
    assert resolve_to_table_references(node_map) == []


def test_unsupported_function_call_without_let_is_not_a_reference() -> None:
    # An unsupported source expressed as a bare function call must not be
    # mistaken for a sibling-table reference (neither the function name nor its
    # arguments).
    node_map = _parse("LOAD_DATA(Source)")
    assert resolve_to_table_references(node_map) == []


def test_get_upstream_tables_captures_sibling_reference() -> None:
    config = _config()
    table = Table(
        name="New Names",
        full_name="d1.New Names",
        expression='let\n    Source = #"factNewNames"\nin\n    Source',
    )
    lineages = parser.get_upstream_tables(
        table=table,
        reporter=PowerBiDashboardSourceReport(),
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
    )
    assert len(lineages) == 1
    assert lineages[0].powerbi_table_upstreams == ["factNewNames"]
    assert lineages[0].upstreams == []


def test_sibling_reference_resolves_to_upstream_urn() -> None:
    config = _config()
    child = Table(name="New Names", full_name="d1.New Names")
    sibling = Table(name="factNewNames", full_name="d1.factNewNames")
    _dataset_with_tables([child, sibling])

    # Case-insensitive match against the sibling table name.
    urns = _mapper(config)._table_reference_upstreams(["FACTNEWNAMES"], child)

    assert len(urns) == 1
    assert urns[0].startswith("urn:li:dataset:(urn:li:dataPlatform:powerbi,")
    assert "d1.factnewnames" in urns[0].lower()


def test_unmatched_and_self_references_are_dropped() -> None:
    config = _config()
    child = Table(name="New Names", full_name="d1.New Names")
    sibling = Table(name="factNewNames", full_name="d1.factNewNames")
    _dataset_with_tables([child, sibling])
    mapper = _mapper(config)

    # Name that matches no sibling table.
    assert mapper._table_reference_upstreams(["does_not_exist"], child) == []
    # A table referencing itself must not produce a self-loop.
    assert mapper._table_reference_upstreams(["New Names"], child) == []
