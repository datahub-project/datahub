"""Tests for PowerBI table-to-table lineage (ING-1905)."""

from typing import List

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    ResolvePlatformInstanceFromDatasetTypeMapping,
)
from datahub.ingestion.source.powerbi.m_query import dax_resolver, parser
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    PowerBIDataset,
    Table,
)


def _make_config() -> PowerBiDashboardSourceConfig:
    return PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
    )


def _make_table_with_siblings(
    expression: str,
    table_name: str,
    sibling_names: List[str],
) -> Table:
    """Create a Table whose expression references sibling tables."""
    subject = Table(name=table_name, full_name=f"Workspace.Dataset.{table_name}")
    subject.expression = expression
    siblings = [
        Table(name=n, full_name=f"Workspace.Dataset.{n}") for n in sibling_names
    ]
    dataset = PowerBIDataset(
        id="dataset-id",
        name="Dataset",
        description="",
        webUrl=None,
        workspace_id="workspace-id",
        workspace_name="Workspace",
        parameters={},
        tables=[subject] + siblings,
        tags=[],
    )
    subject.dataset = dataset
    for s in siblings:
        s.dataset = dataset
    return subject


def _upstream_tables(
    expression: str, table_name: str, sibling_names: List[str]
) -> List[str]:
    """Run get_upstream_tables() and return all powerbi_table_upstreams."""
    table = _make_table_with_siblings(expression, table_name, sibling_names)
    config = _make_config()
    lineages = parser.get_upstream_tables(
        table=table,
        reporter=PowerBiDashboardSourceReport(),
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
        parameters={},
    )
    result = []
    for lineage in lineages:
        result.extend(lineage.powerbi_table_upstreams)
    return result


def test_bare_identifier_references_sibling_table():
    """Bare identifier expression DimDate → references sibling table DimDate."""
    table = _make_table_with_siblings("DimDate", "CalcTable", ["DimDate", "OtherTable"])
    config = _make_config()
    reporter = PowerBiDashboardSourceReport()
    lineages = parser.get_upstream_tables(
        table=table,
        reporter=reporter,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
        parameters={},
    )
    refs = [r for lin in lineages for r in lin.powerbi_table_upstreams]
    assert refs == ["DimDate"]
    tel = reporter.table_expression_lineage_stats
    assert tel.m_query_m_ast_parsed == 1
    assert tel.m_query_lineage_extracted == 1, (
        "Expected a resolver path to find the sibling reference"
    )
    assert tel.dax_calculated_table_extractions == 0
    assert tel.dax_calculated_table_sibling_refs_found == 0


def test_quoted_let_identifier_references_sibling_table():
    """let source=#"tbl_PayrollHistory" in source → references tbl_PayrollHistory."""
    expr = 'let\n    source = #"tbl_PayrollHistory"\nin\n    source'
    refs = _upstream_tables(expr, "CalcTable", ["tbl_PayrollHistory"])
    assert refs == ["tbl_PayrollHistory"]


def test_table_combine_references_multiple_siblings():
    """Table.Combine({tblA, tblB}) → references both sibling tables."""
    expr = (
        "let\n"
        "    Source = Table.Combine({tblA, tblB}),\n"
        '    #"Filtered Rows" = Table.SelectRows(Source, each [Active] = true)\n'
        "in\n"
        '    #"Filtered Rows"'
    )
    refs = _upstream_tables(expr, "CalcTable", ["tblA", "tblB"])
    assert sorted(refs) == ["tblA", "tblB"]


def test_external_source_expression_unchanged():
    """M-Query with Sql.Database should produce upstreams, not powerbi_table_upstreams."""
    expr = (
        "let\n"
        '    Source = Sql.Database("myserver", "mydb"),\n'
        '    dbo_orders = Source{[Schema="dbo", Item="orders"]}[Data]\n'
        "in\n"
        "    dbo_orders"
    )
    table = _make_table_with_siblings(expr, "OrdersTable", ["OtherTable"])
    config = _make_config()
    lineages = parser.get_upstream_tables(
        table=table,
        reporter=PowerBiDashboardSourceReport(),
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
        parameters={},
    )
    pbi_refs = [ref for lin in lineages for ref in lin.powerbi_table_upstreams]
    ext_upstreams = [u for lin in lineages for u in lin.upstreams]
    assert pbi_refs == [], "External source should not produce powerbi_table_upstreams"
    assert len(ext_upstreams) >= 1, "External source should produce upstreams"


def test_dax_summarize_references_sibling_table():
    """DAX summarize expression references 'FMS Lookup' sibling table."""
    expr = "summarize('FMS Lookup','FMS Lookup'[FMSID])"
    table = _make_table_with_siblings(expr, "Summary", ["FMS Lookup", "OtherTable"])
    config = _make_config()
    reporter = PowerBiDashboardSourceReport()
    lineages = parser.get_upstream_tables(
        table=table,
        reporter=reporter,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
        parameters={},
    )
    refs = [r for lin in lineages for r in lin.powerbi_table_upstreams]
    assert refs == ["FMS Lookup"]
    tel = reporter.table_expression_lineage_stats
    assert tel.m_query_non_m_expression == 1
    assert tel.m_query_m_ast_parsed == 0
    assert tel.m_query_lineage_extracted == 1
    assert tel.dax_calculated_table_extractions == 1
    assert tel.dax_calculated_table_sibling_refs_found == 1


def test_dax_builtin_does_not_match_absent_sibling():
    """CALENDAR(...) should not emit sibling refs when Sales is unrelated.

    The M bridge may parse this expression successfully; we still expect no
    powerbi_table_upstreams when nothing resolves to the sibling set.
    """
    expr = "CALENDAR(DATE(2020,1,1), DATE(2025,1,1))"
    table = _make_table_with_siblings(expr, "DateTable", ["Sales"])
    config = _make_config()
    reporter = PowerBiDashboardSourceReport()
    lineages = parser.get_upstream_tables(
        table=table,
        reporter=reporter,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
        parameters={},
    )
    assert [r for lin in lineages for r in lin.powerbi_table_upstreams] == []
    tel = reporter.table_expression_lineage_stats
    assert tel.m_query_no_lineage == 1
    assert tel.m_query_lineage_extracted == 0
    assert tel.dax_calculated_table_extractions == 0
    assert tel.dax_calculated_table_sibling_refs_found == 0


def test_dax_column_lineage_related_lookup():
    """RELATED(Customers[Name]) → upstream ColumnRef for Customers.Name."""
    cll = dax_resolver.extract_dax_column_lineage(
        column_name="CustomerName",
        expression="RELATED(Customers[Name])",
        table_urn="urn:li:dataset:(urn:li:dataPlatform:powerbi,ws.ds.Orders,PROD)",
        sibling_table_urns={
            "customers": "urn:li:dataset:(urn:li:dataPlatform:powerbi,ws.ds.Customers,PROD)"
        },
    )
    assert len(cll) == 1
    assert cll[0].downstream.column == "CustomerName"
    assert len(cll[0].upstreams) == 1
    assert cll[0].upstreams[0].column == "Name"
    assert "Customers" in cll[0].upstreams[0].table


def test_dax_column_lineage_unknown_table_ignored():
    """Reference to a table not in sibling_table_urns produces no lineage."""
    cll = dax_resolver.extract_dax_column_lineage(
        column_name="Col",
        expression="RELATED(UnknownTable[Field])",
        table_urn="urn:li:dataset:(urn:li:dataPlatform:powerbi,ws.ds.Orders,PROD)",
        sibling_table_urns={},
    )
    assert cll == []


def test_dax_column_lineage_intra_table_measure_ignored():
    """[Total Sales] with no table prefix should produce no upstream refs."""
    cll = dax_resolver.extract_dax_column_lineage(
        column_name="Derived",
        expression="[Total Sales] * 1.1",
        table_urn="urn:li:dataset:(urn:li:dataPlatform:powerbi,ws.ds.Orders,PROD)",
        sibling_table_urns={
            "orders": "urn:li:dataset:(urn:li:dataPlatform:powerbi,ws.ds.Orders,PROD)"
        },
    )
    assert cll == []
