"""Tests for M-Query table-to-table lineage.

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


def test_nested_let_outer_variable_is_not_a_reference() -> None:
    # A variable bound in an outer `let` must not be captured as a sibling table
    # when the walk descends into a nested `let`.
    node_map = _parse("let tblX = 1,\n    outer = let y = tblX in y\nin\n    outer")
    assert resolve_to_table_references(node_map) == []


def test_join_with_shared_ancestor_has_no_circular_warning(caplog) -> None:
    import logging

    node_map = _parse(
        'let Base = SomeSiblingTbl,'
        ' Joined = Table.NestedJoin(Base, {"k"}, Base, {"k"}, "n", JoinKind.Inner)'
        " in Joined"
    )
    with caplog.at_level(logging.WARNING):
        refs = resolve_to_table_references(node_map)

    # The shared ancestor `Base` is walked once per join argument; that must not
    # be mistaken for a circular reference.
    assert not any("Circular reference" in rec.message for rec in caplog.records)
    assert refs == ["SomeSiblingTbl"]


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


def test_sibling_reference_alongside_data_source() -> None:
    # An M-Query that combines an external source with a sibling-table reference
    # must surface both — the sibling ref is not skipped just because a
    # recognized data-access function is present.
    config = _config()
    table = Table(
        name="Combined",
        full_name="d1.Combined",
        expression=(
            'let Source = Sql.Database("srv", "db"),'
            " Combined = Table.Combine({Source, SiblingTable}) in Combined"
        ),
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
    all_refs = [name for lin in lineages for name in lin.powerbi_table_upstreams]
    assert "SiblingTable" in all_refs


def test_get_upstream_tables_captures_dax_calculated_table_reference() -> None:
    # A DAX calculated-table expression fails M-Query parsing and is routed to the
    # DAX extractor, which surfaces the referenced sibling table.
    config = _config()
    table = Table(
        name="FMS Summary",
        full_name="d1.FMS Summary",
        expression="summarize('FMS Lookup', 'FMS Lookup'[FMSID])",
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
    assert lineages[0].powerbi_table_upstreams == ["FMS Lookup"]
    assert lineages[0].upstreams == []


def test_dax_table_name_containing_let_substring() -> None:
    # "Outlet" contains the substring "let" but is not the M-Query `let` keyword;
    # the DAX expression must still be routed to the DAX extractor.
    config = _config()
    table = Table(
        name="Outlet Summary",
        full_name="d1.Outlet Summary",
        expression="summarize('Outlet', 'Outlet'[Region])",
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
    assert lineages[0].powerbi_table_upstreams == ["Outlet"]


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


def test_emitted_and_dropped_references_are_reported() -> None:
    config = _config()
    child = Table(name="Child", full_name="d1.Child")
    sibling = Table(name="Sib", full_name="d1.Sib")
    _dataset_with_tables([child, sibling])
    reporter = PowerBiDashboardSourceReport()
    mapper = Mapper(
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
        reporter=reporter,
        dataplatform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
    )

    urns = mapper._table_reference_upstreams(["Sib", "Ghost"], child)

    assert len(urns) == 1
    assert reporter.m_query_table_to_table_lineage == 1
    assert any(
        "d1.Child -> d1.Sib" in sample
        for sample in reporter.m_query_table_to_table_lineage_samples
    )
    assert reporter.m_query_table_to_table_unmatched == 1
    assert any(
        "d1.Child -> Ghost" in sample
        for sample in reporter.m_query_table_to_table_unmatched_samples
    )


def test_stray_reference_does_not_inflate_resolver_success() -> None:
    # An unsupported source whose only unresolved identifier is not a real sibling
    # table must count as no-lineage, not success.
    config = _config()
    child = Table(
        name="X",
        full_name="d1.X",
        expression="let Source = NotASiblingTable in Source",
    )
    _dataset_with_tables([child])
    reporter = PowerBiDashboardSourceReport()

    parser.get_upstream_tables(
        table=child,
        reporter=reporter,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
    )

    assert reporter.m_query_resolver_successes == 0
    assert reporter.m_query_resolver_no_lineage == 1


def test_let_bearing_parse_failure_is_not_treated_as_dax() -> None:
    # A genuine M-Query (contains `let`) that fails to parse must be reported as
    # a parse failure, not silently reinterpreted as DAX — M record access
    # `id[Field]` is lexically identical to DAX `Table[Column]`.
    config = _config()
    reporter = PowerBiDashboardSourceReport()
    table = Table(name="x", full_name="d1.x", expression="let Source = Foo[Bar] in")
    lineages = parser.get_upstream_tables(
        table=table,
        reporter=reporter,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(config),
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
    )
    assert lineages == []
    assert reporter.m_query_parse_unknown_errors == 1
    assert reporter.m_query_dax_table_lineage == 0
