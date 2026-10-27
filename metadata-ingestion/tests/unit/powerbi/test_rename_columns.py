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
from datahub.ingestion.source.powerbi.m_query.data_classes import (
    ColumnRename,
    ColumnRenameStep,
    Lineage,
)
from datahub.ingestion.source.powerbi.m_query.pattern_handler import (
    OdbcLineage,
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


def _upstream_columns(
    expression: str, columns: List[str], upstream: str = _UPSTREAM
) -> Dict[str, List[str]]:
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
    assert lineages[0].upstreams[0].urn == upstream
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


def test_rename_columns_single_pair_form() -> None:
    expression = """\
let
    Source = Sql.Database("warehouse.example", "sales_db"),
    gold_dim_customers = Source{[Schema="gold",Item="dim_customers"]}[Data],
    #"Renamed Columns" = Table.RenameColumns(gold_dim_customers, {"CustomerAccount", "Customer Account"})
in
    #"Renamed Columns"
"""
    assert _upstream_columns(expression, ["Customer Account", "City"]) == {
        "Customer Account": ["CustomerAccount"],
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


def test_rename_columns_around_table_combine_maps_each_source() -> None:
    expression = """\
let
    Source = Sql.Database("warehouse.example", "sales_db"),
    customers = Source{[Schema="gold",Item="dim_customers"]}[Data],
    orders = Source{[Schema="gold",Item="dim_orders"]}[Data],
    combined = Table.Combine({customers, orders}),
    #"Renamed Columns" = Table.RenameColumns(combined, {{"CustomerAccount", "Customer Account"}})
in
    #"Renamed Columns"
"""
    config = _config()
    lineages = mquery_parser.get_upstream_tables(
        table=Table(
            name="dim_customers",
            full_name="ws.ds.dim_customers",
            expression=expression,
            columns=[_column("Customer Account")],
        ),
        reporter=PowerBiDashboardSourceReport(),
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
    )
    mapped = {
        lineage.upstreams[0].urn: {
            info.downstream.column: [ref.column for ref in info.upstreams]
            for info in lineage.column_lineage
        }
        for lineage in lineages
    }
    orders = "urn:li:dataset:(urn:li:dataPlatform:mssql,sales_db.gold.dim_orders,PROD)"
    assert mapped == {
        _UPSTREAM: {"Customer Account": ["CustomerAccount"]},
        orders: {"Customer Account": ["CustomerAccount"]},
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


def test_native_sql_output_renamed_to_powerbi_field() -> None:
    expression = """\
let
    Source = Sql.Database("warehouse.example", "sales_db", [Query="select CustomerAccount, City from gold.dim_customers"]),
    #"Renamed Columns" = Table.RenameColumns(Source, {{"CustomerAccount", "Customer Account"}})
in
    #"Renamed Columns"
"""
    # Without a schema, sqlglot lowercases MSSQL identifiers on both sides.
    assert _upstream_columns(expression, ["Customer Account", "City"]) == {
        "Customer Account": ["customeraccount"],
        "City": ["city"],
    }


def test_native_sql_rename_matches_folded_output_names() -> None:
    # Snowflake folds unquoted identifiers, so sqlglot's output names are not in
    # the casing Power BI's rename step uses.
    expression = """\
let
    Source = Value.NativeQuery(Snowflake.Databases("acme.snowflakecomputing.com", "COMPUTE_WH"){[Name="DB"]}[Data], "select customer_account, city from db.sch.customers", null, [EnableFolding=true]),
    #"Renamed Columns" = Table.RenameColumns(Source, {{"CUSTOMER_ACCOUNT", "Customer Account"}})
in
    #"Renamed Columns"
"""
    assert _upstream_columns(
        expression,
        ["Customer Account", "CITY"],
        upstream="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.customers,PROD)",
    ) == {
        "Customer Account": ["customer_account"],
        "CITY": ["city"],
    }


def test_native_sql_rename_swap_is_not_snapped_to_old_names() -> None:
    # The case remap would otherwise pin each output to the field of the same
    # name, which after the swap is the other column.
    expression = """\
let
    Source = Sql.Database("warehouse.example", "sales_db", [Query="select Name, City from gold.dim_customers"]),
    #"Renamed Columns" = Table.RenameColumns(Source, {{"Name", "City"}, {"City", "Name"}})
in
    #"Renamed Columns"
"""
    assert _upstream_columns(expression, ["City", "Name"]) == {
        "City": ["name"],
        "Name": ["city"],
    }


def test_sql_lineage_is_not_renamed_again_after_urn_transform() -> None:
    lineage = Lineage(
        upstreams=[],
        downstream_names_are_powerbi_fields=False,
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(column="Town"),
                upstreams=[ColumnRef(table=_UPSTREAM, column="City")],
            )
        ],
    )
    transformed = OdbcLineage._transform_lineage_urns(lineage, lambda urn: urn)
    result = apply_table_rename_columns(
        transformed,
        [
            ColumnRenameStep(
                renames=[ColumnRename(source_name="City", renamed_name="Town")]
            )
        ],
    )
    assert result.column_lineage[0].downstream.column == "Town"
    assert result.column_lineage[0].upstreams[0].column == "City"


def test_apply_renames_is_noop_without_steps() -> None:
    lineage = Lineage(upstreams=[], column_lineage=[])
    assert apply_table_rename_columns(lineage, []) is lineage
