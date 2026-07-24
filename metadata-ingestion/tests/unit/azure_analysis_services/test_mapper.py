from typing import Dict

from datahub.emitter import mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.azure_analysis_services.config import (
    AzureAnalysisServicesConfig,
)
from datahub.ingestion.source.azure_analysis_services.lineage import AasLineageExtractor
from datahub.ingestion.source.azure_analysis_services.mapper import AasMapper
from datahub.ingestion.source.azure_analysis_services.models import (
    AasMeasure,
    AasPartition,
    AasRelationship,
    AasTable,
    AasTabularModel,
)
from datahub.ingestion.source.azure_analysis_services.report import (
    AzureAnalysisServicesReport,
)
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.schema_classes import (
    SchemaMetadataClass,
    SubTypesClass,
    ViewPropertiesClass,
)

_SERVER = "asazure://westeurope.asazure.windows.net/myserver"


def _mapper(**config_overrides: object) -> AasMapper:
    payload: Dict[str, object] = {
        "server": _SERVER,
        "tenant_id": "t",
        "client_id": "c",
        "client_secret": "s",
    }
    payload.update(config_overrides)
    config = AzureAnalysisServicesConfig.model_validate(payload)
    report = AzureAnalysisServicesReport()
    ctx = PipelineContext(run_id="t")
    lineage = AasLineageExtractor(config, report, ctx)
    return AasMapper(
        config=config,
        report=report,
        ctx=ctx,
        server_name="myserver",
        lineage_extractor=lineage,
    )


def test_subtypes_query_table_is_view() -> None:
    mapper = _mapper()
    table = AasTable(
        name="Sales",
        partitions=[
            AasPartition(name="p", query_definition="let Source = 1 in Source")
        ],
    )
    assert mapper._table_subtypes(table) == [
        DatasetSubTypes.TABLE,
        DatasetSubTypes.VIEW,
    ]


def test_subtypes_calculated_table() -> None:
    mapper = _mapper()
    table = AasTable(
        name="SalesSummary",
        is_calculated=True,
        partitions=[AasPartition(name="c", query_definition="SUMMARIZE(Sales)")],
    )
    assert mapper._table_subtypes(table) == [
        DatasetSubTypes.TABLE,
        DatasetSubTypes.CALCULATED_TABLE,
    ]


def test_subtypes_import_table_without_expression() -> None:
    mapper = _mapper()
    table = AasTable(name="Static")
    assert mapper._table_subtypes(table) == [DatasetSubTypes.TABLE]


def test_view_properties_language() -> None:
    mapper = _mapper()
    query_table = AasTable(
        name="Sales",
        partitions=[
            AasPartition(name="p", query_definition="let Source = 1 in Source")
        ],
    )
    vp = mapper._table_view_properties(query_table)
    assert isinstance(vp, ViewPropertiesClass)
    assert vp.viewLanguage == "M"

    calc_table = AasTable(
        name="SalesSummary",
        is_calculated=True,
        partitions=[AasPartition(name="c", query_definition="SUMMARIZE(Sales)")],
    )
    calc_vp = mapper._table_view_properties(calc_table)
    assert calc_vp is not None
    assert calc_vp.viewLanguage == "DAX"


def test_powerbi_urn_alignment() -> None:
    aas_mapper = _mapper()
    assert (
        aas_mapper._table_dataset_name("Sales Model", "My Table")
        == "Sales Model.My Table"
    )

    pbi_mapper = _mapper(platform="powerbi")
    # Power BI's form_full_table_name replaces spaces with underscores.
    assert (
        pbi_mapper._table_dataset_name("Sales Model", "My Table")
        == "Sales_Model.My_Table"
    )


def test_powerbi_workspace_prefix_alignment() -> None:
    prefixed = _mapper(platform="powerbi", include_workspace_name_in_dataset_urn=True)
    # server_name="myserver" stands in for the workspace segment; Power BI
    # lowercases it and swaps spaces, matching workspace-prefixed Power BI URNs.
    assert (
        prefixed._table_dataset_name("Sales Model", "My Table")
        == "myserver.Sales_Model.My_Table"
    )

    # The prefix only applies in Power BI-aligned mode.
    aas_prefixed = _mapper(include_workspace_name_in_dataset_urn=True)
    assert (
        aas_prefixed._table_dataset_name("Sales Model", "My Table")
        == "Sales Model.My Table"
    )


def test_foreign_keys() -> None:
    mapper = _mapper()
    model = AasTabularModel(
        catalog="SalesModel",
        name="Sales Model",
        relationships=[
            AasRelationship(
                from_table="Sales",
                from_column="ProductKey",
                to_table="Product",
                to_column="ProductKey",
            )
        ],
    )
    sales = AasTable(name="Sales")
    sales_urn = builder.make_dataset_urn("azure-analysis-services", "SalesModel.Sales")
    product_urn = builder.make_dataset_urn(
        "azure-analysis-services", "SalesModel.Product"
    )
    urn_by_table = {"sales": sales_urn, "product": product_urn}

    keys = mapper._foreign_keys(model, sales, sales_urn, urn_by_table)
    assert len(keys) == 1
    assert keys[0].foreignDataset == product_urn
    assert keys[0].sourceFields == [
        builder.make_schema_field_urn(sales_urn, "ProductKey")
    ]
    assert keys[0].foreignFields == [
        builder.make_schema_field_urn(product_urn, "ProductKey")
    ]


def test_foreign_keys_missing_target_dropped() -> None:
    mapper = _mapper()
    model = AasTabularModel(
        catalog="SalesModel",
        name="Sales Model",
        relationships=[
            AasRelationship(
                from_table="Sales",
                from_column="ProductKey",
                to_table="Missing",
                to_column="Key",
            )
        ],
    )
    sales = AasTable(name="Sales")
    sales_urn = builder.make_dataset_urn("azure-analysis-services", "SalesModel.Sales")
    # The target table is absent from the URN index, so no constraint is emitted.
    keys = mapper._foreign_keys(model, sales, sales_urn, {"sales": sales_urn})
    assert keys == []


def test_cube_dataset_field_naming_and_dual_subtypes() -> None:
    mapper = _mapper()
    model = AasTabularModel(
        catalog="SalesModel",
        name="Sales Model",
        tables=[
            AasTable(
                name="Sales",
                measures=[
                    AasMeasure(name="Total Sales", expression="SUM(Sales[Amount])")
                ],
            )
        ],
    )
    aspects = [
        getattr(wu.metadata, "aspect", None)
        for wu in mapper._map_cube_dataset(model, mapper._model_key("SalesModel"))
    ]

    subtypes = next(a for a in aspects if isinstance(a, SubTypesClass))
    assert subtypes.typeNames == [
        DatasetSubTypes.CUBE,
        DatasetSubTypes.SEMANTIC_MODEL,
    ]

    schema = next(a for a in aspects if isinstance(a, SchemaMetadataClass))
    # Measures are re-emitted on the cube dataset qualified as <table>.<measure>.
    assert [f.fieldPath for f in schema.fields] == ["Sales.Total Sales"]


def test_map_table_swallows_and_continues() -> None:
    mapper = _mapper()
    model = AasTabularModel(
        catalog="SalesModel",
        name="Sales Model",
        tables=[AasTable(name="Bad"), AasTable(name="Good")],
    )

    original = mapper.lineage_extractor.extract_upstream_for_table

    def selective(table, dataset_urn):
        if table.name == "Bad":
            raise RuntimeError("mapping boom")
        return original(table, dataset_urn)

    mapper.lineage_extractor.extract_upstream_for_table = selective  # type: ignore[method-assign]

    urns = {wu.get_urn() for wu in mapper.map_model(model)}
    # The healthy table still emits despite the sibling failing.
    assert mapper._table_dataset_urn("SalesModel", "Good") in urns
    assert mapper.report.tables_skipped == 1
