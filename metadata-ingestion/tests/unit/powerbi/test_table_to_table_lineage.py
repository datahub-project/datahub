"""Tests for M-Query table-to-table lineage.

Bridge-backed: each M-Query expression is parsed via the JS bridge, mirroring
tests/unit/test_ast_utils.py. No static fixtures.
"""

import logging
import time
from unittest.mock import MagicMock

import pytest

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
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamLineageClass,
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


def test_join_with_shared_ancestor_has_no_circular_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    node_map = _parse(
        "let Base = SomeSiblingTbl,"
        ' Joined = Table.NestedJoin(Base, {"k"}, Base, {"k"}, "n", JoinKind.Inner)'
        " in Joined"
    )
    with caplog.at_level(logging.WARNING):
        refs = resolve_to_table_references(node_map)

    # The shared ancestor `Base` is walked once per join argument; that must not
    # be mistaken for a circular reference.
    assert not any("Circular reference" in rec.message for rec in caplog.records)
    assert refs == ["SomeSiblingTbl"]


def test_lambda_parameter_is_not_a_reference() -> None:
    # Function parameters are unresolved identifiers but not sibling tables, and
    # people name them exactly like dimension tables (Country, Region, Date).
    node_map = _parse("let A = List.Transform({1,2}, (Country) => Country) in A")
    assert resolve_to_table_references(node_map) == []


def test_quoted_dotted_table_name_is_a_reference() -> None:
    # The dotted-name filter exists to drop M library/enum references
    # (QuoteStyle.Csv). Those are never #"..."-quoted, so a quoted dotted name is
    # a real table (dim.Date, Sales.Orders are common in Power BI).
    node_map = _parse('let A = #"My.Table" in A')
    assert resolve_to_table_references(node_map) == ["My.Table"]


def test_merge_chain_resolves_in_linear_time() -> None:
    # Regression guard: collecting table references must not re-walk shared
    # subtrees per argument. A merge chain where each step joins two earlier
    # steps (an ordinary Power Query shape) previously blew up exponentially
    # (~1.6^n), hanging ingestion on real 30-80 step queries.
    steps = ["S0 = SrcTbl", "S1 = SrcTbl2"]
    for i in range(2, 30):
        steps.append(
            f'S{i} = Table.NestedJoin(S{i - 1}, {{"k"}}, S{i - 2}, {{"k"}},'
            f' "n{i}", JoinKind.Inner)'
        )
    node_map = _parse("let " + ", ".join(steps) + " in S29")

    start = time.perf_counter()
    refs = resolve_to_table_references(node_map)
    elapsed = time.perf_counter() - start

    # Linear resolution is ~1ms; the exponential version took >30s at this size.
    assert elapsed < 5.0, f"table-reference walk took {elapsed:.1f}s"
    assert refs == ["SrcTbl", "SrcTbl2"]


def test_query_parameter_is_not_a_reference() -> None:
    # M parameters are unresolved identifiers but not sibling tables; when the
    # parameter map is provided they must be excluded.
    node_map = _parse(
        "let Source = Web.Contents(BaseUrl), D = Json.Document(Source) in D"
    )
    assert resolve_to_table_references(node_map) == ["BaseUrl"]
    assert (
        resolve_to_table_references(node_map, parameters={"BaseUrl": "http://x"}) == []
    )


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
    config = _config()  # convert_urns_to_lowercase defaults to False
    child = Table(name="New Names", full_name="d1.New Names")
    sibling = Table(name="factNewNames", full_name="d1.factNewNames")
    _dataset_with_tables([child, sibling])

    # Case-insensitive sibling match; the emitted URN preserves the sibling's
    # actual casing when convert_urns_to_lowercase is off.
    urns = _mapper(config)._table_reference_upstreams(["FACTNEWNAMES"], child)

    assert urns == ["urn:li:dataset:(urn:li:dataPlatform:powerbi,d1.factNewNames,PROD)"]


def test_sibling_reference_urn_lowercased_when_configured() -> None:
    config = _config()
    config.convert_urns_to_lowercase = True
    child = Table(name="New Names", full_name="d1.New Names")
    sibling = Table(name="factNewNames", full_name="d1.factNewNames")
    _dataset_with_tables([child, sibling])

    urns = _mapper(config)._table_reference_upstreams(["factNewNames"], child)

    assert urns == ["urn:li:dataset:(urn:li:dataPlatform:powerbi,d1.factnewnames,PROD)"]


def test_table_reference_upstreams_warns_when_no_dataset() -> None:
    config = _config()
    orphan = Table(name="Orphan", full_name="d1.Orphan")  # dataset stays None
    reporter = PowerBiDashboardSourceReport()
    mapper = Mapper(
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
        reporter=reporter,
        dataplatform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
    )

    assert mapper._table_reference_upstreams(["Something"], orphan) == []
    assert list(reporter.warnings)


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


def test_table_to_table_lineage_can_be_disabled() -> None:
    config = _config()
    config.extract_table_to_table_lineage = False
    child = Table(
        name="New Names",
        full_name="d1.New Names",
        expression='let\n    Source = #"factNewNames"\nin\n    Source',
    )
    sibling = Table(name="factNewNames", full_name="d1.factNewNames")
    _dataset_with_tables([child, sibling])

    lineages = parser.get_upstream_tables(
        table=child,
        reporter=PowerBiDashboardSourceReport(),
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
    )
    assert all(not lin.powerbi_table_upstreams for lin in lineages)


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
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
    )
    assert lineages == []
    assert reporter.m_query_parse_unknown_errors == 1
    assert reporter.m_query_dax_table_lineage == 0


@pytest.mark.parametrize(
    "expression",
    [
        "Table.Combine({DimDate, DimProduct}",  # missing ')'
        'Sql.Database("h","d"',  # missing ')'
        "let Source = Foo[Bar] in",  # missing output expression
    ],
)
def test_malformed_m_query_without_let_is_not_treated_as_dax(expression: str) -> None:
    # `let` is sufficient but not necessary for M-Query: a malformed M expression
    # with no `let` must be reported as a parse failure, never mined for DAX
    # references (which would fabricate lineage from an unparseable expression).
    config = _config()
    reporter = PowerBiDashboardSourceReport()
    table = Table(name="x", full_name="d1.x", expression=expression)

    lineages = parser.get_upstream_tables(
        table=table,
        reporter=reporter,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
    )

    assert lineages == []
    assert reporter.m_query_dax_table_lineage == 0
    assert reporter.m_query_parse_unknown_errors == 1


def test_extract_lineage_emits_transformed_upstream_edge() -> None:
    # End-to-end through the mapper: a sibling reference must become an
    # UpstreamClass of type TRANSFORMED pointing at the sibling's dataset URN.
    config = _config()
    child = Table(
        name="Summary",
        full_name="d1.Summary",
        expression='let\n    Source = #"Base"\nin\n    Source',
    )
    base = Table(name="Base", full_name="d1.Base")
    _dataset_with_tables([child, base])

    ds_urn = "urn:li:dataset:(urn:li:dataPlatform:powerbi,d1.Summary,PROD)"
    mcps = _mapper(config).extract_lineage(child, ds_urn, MagicMock())

    upstream_aspects = [
        mcp.aspect for mcp in mcps if isinstance(mcp.aspect, UpstreamLineageClass)
    ]
    assert len(upstream_aspects) == 1
    edges = upstream_aspects[0].upstreams
    assert any(
        edge.type == DatasetLineageTypeClass.TRANSFORMED
        and edge.dataset == "urn:li:dataset:(urn:li:dataPlatform:powerbi,d1.Base,PROD)"
        for edge in edges
    )
