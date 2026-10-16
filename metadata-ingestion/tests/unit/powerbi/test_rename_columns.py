from typing import Dict, List

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    ResolvePlatformInstanceFromDatasetTypeMapping,
)
from datahub.ingestion.source.powerbi.m_query import parser as mquery_parser
from datahub.ingestion.source.powerbi.m_query.data_classes import Lineage
from datahub.ingestion.source.powerbi.m_query.pattern_handler import (
    apply_table_rename_columns,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Column,
    Table,
)
from datahub.metadata.schema_classes import StringTypeClass
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
)

_UPSTREAM = (
    "urn:li:dataset:(urn:li:dataPlatform:mssql,sales_db.gold.dim_customers,PROD)"
)


def _column(name: str) -> Column:
    return Column(
        name=name,
        dataType="String",
        isHidden=False,
        datahubDataType=StringTypeClass(),
    )


def _config() -> PowerBiDashboardSourceConfig:
    return PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
    )


def _upstream_columns(expression: str, columns: List[str]) -> Dict[str, List[str]]:
    config = _config()
    lineages = mquery_parser.get_upstream_tables(
        table=Table(
            name="dim_customers",
            full_name="ws.ds.dim_customers",
            expression=expression,
            columns=[_column(name) for name in columns],
        ),
        reporter=PowerBiDashboardSourceReport(),
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
    )
    assert len(lineages) == 1
    assert lineages[0].upstreams[0].urn == _UPSTREAM
    return {
        info.downstream.column: [ref.column for ref in info.upstreams]
        for info in lineages[0].column_lineage
    }


def test_rename_columns_map_powerbi_fields_back_to_source() -> None:
    expression = """\
let
    Source = Sql.Database("warehouse.example", "sales_db"),
    gold_dim_customers = Source{[Schema="gold",Item="dim_customers"]}[Data],
    #"Renamed Columns" = Table.RenameColumns(gold_dim_customers, {{"CustomerAccount", "Customer Account"}, {"CustomerName", "Customer Name"}}, MissingField.Ignore),
    #"Renamed Columns1" = Table.RenameColumns(#"Renamed Columns", {{"Customer Name", "Customer Full Name"}}),
    #"Changed Type" = Table.TransformColumnTypes(#"Renamed Columns1", {{"Customer Account", type text}})
in
    #"Changed Type"
"""
    assert _upstream_columns(
        expression, ["Customer Account", "Customer Full Name", "City"]
    ) == {
        "Customer Account": ["CustomerAccount"],
        "Customer Full Name": ["CustomerName"],
        "City": ["City"],
    }


def test_rename_columns_in_one_call_apply_together() -> None:
    expression = """\
let
    Source = Sql.Database("warehouse.example", "sales_db"),
    gold_dim_customers = Source{[Schema="gold",Item="dim_customers"]}[Data],
    #"Renamed Columns" = Table.RenameColumns(gold_dim_customers, {{"A", "B"}, {"B", "A"}})
in
    #"Renamed Columns"
"""
    assert _upstream_columns(expression, ["A", "B"]) == {
        "A": ["B"],
        "B": ["A"],
    }


def test_rename_columns_without_pairs_keeps_field_names() -> None:
    expression = """\
let
    Source = Sql.Database("warehouse.example", "sales_db"),
    gold_dim_customers = Source{[Schema="gold",Item="dim_customers"]}[Data],
    #"Renamed Columns" = Table.RenameColumns(gold_dim_customers)
in
    #"Renamed Columns"
"""
    assert _upstream_columns(expression, ["City"]) == {"City": ["City"]}


def test_apply_renames_sql_output_to_powerbi_field() -> None:
    lineage = Lineage(
        upstreams=[],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(column="CustomerAccount"),
                upstreams=[ColumnRef(table=_UPSTREAM, column="CustomerAccount")],
            )
        ],
    )
    result = apply_table_rename_columns(
        lineage,
        ((("CustomerAccount", "Customer Account"),),),
        [_column("Customer Account")],
    )
    assert result.column_lineage[0].downstream.column == "Customer Account"
    assert result.column_lineage[0].upstreams[0].column == "CustomerAccount"


def test_apply_renames_is_noop_without_steps() -> None:
    lineage = Lineage(upstreams=[], column_lineage=[])
    assert apply_table_rename_columns(lineage, (), None) is lineage
