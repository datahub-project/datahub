from typing import Dict, Generator, List, cast

import pytest

from datahub.emitter import mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.azure_analysis_services import constants
from datahub.ingestion.source.azure_analysis_services.config import (
    AzureAnalysisServicesConfig,
)
from datahub.ingestion.source.azure_analysis_services.lineage import AasLineageExtractor
from datahub.ingestion.source.azure_analysis_services.mapper import AasMapper
from datahub.ingestion.source.azure_analysis_services.models import (
    AasCalcDependency,
    AasColumn,
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
    ContainerPropertiesClass,
    DateTypeClass,
    ERModelRelationshipCardinalityClass,
    GlobalTagsClass,
    MetricInfoClass,
    MetricUpstreamsClass,
    NullTypeClass,
    NumberTypeClass,
    SemanticFieldAnnotationClass,
    SemanticFieldTypeClass,
    SemanticModelInfoClass,
    SemanticModelKeyClass,
    SemanticModelPropertiesClass,
    SubTypesClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import MetricUrn, SemanticModelUrn

_SERVER = "asazure://westeurope.asazure.windows.net/myserver"
_POWERBI_SERVER = "powerbi://api.powerbi.com/v1.0/myorg/salesws"


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


def test_convert_urns_to_lowercase_folds_dataset_urns() -> None:
    mapper = _mapper(convert_urns_to_lowercase=True)
    assert mapper._table_dataset_urn("SalesModel", "Sales") == builder.make_dataset_urn(
        "azure-analysis-services", "salesmodel.sales"
    )


def test_semantic_model_lists_tables_and_replaces_cube_dataset() -> None:
    mapper = _mapper(table_pattern={"deny": ["Ignored"]})
    model = AasTabularModel(
        catalog="SalesModel",
        name="Model",
        description="Sales semantic model",
        definition='{"name":"SalesModel"}',
        tables=[
            AasTable(
                name="Sales",
                measures=[
                    AasMeasure(name="Total Sales", expression="SUM(Sales[Amount])")
                ],
            ),
            AasTable(name="Product"),
            AasTable(name="Ignored"),
        ],
        relationships=[
            AasRelationship(
                from_table="sales",
                from_column="ProductKey",
                to_table="Product",
                to_column="ProductKey",
            ),
            AasRelationship(
                from_table="Sales",
                from_column="InactiveKey",
                to_table="Product",
                to_column="ProductKey",
                is_active=False,
            ),
            AasRelationship(
                from_table="Sales",
                from_column="IgnoredKey",
                to_table="Ignored",
                to_column="Key",
            ),
        ],
    )

    aspects = [
        (wu.get_urn(), getattr(wu.metadata, "aspect", None))
        for wu in mapper.map_model(model)
    ]
    model_urn = str(
        SemanticModelUrn(
            "urn:li:dataPlatform:azure-analysis-services", "myserver", "SalesModel"
        )
    )
    assert any(
        u == model_urn and isinstance(a, SemanticModelKeyClass) for u, a in aspects
    )
    info = next(
        a
        for u, a in aspects
        if u == model_urn and isinstance(a, SemanticModelInfoClass)
    )
    assert info.name == "SalesModel"
    assert info.description == "Sales semantic model"
    assert info.nativeDefinition == '{"name":"SalesModel"}'
    assert info.relationships is not None
    assert len(info.relationships) == 1
    relationship = info.relationships[0]
    assert relationship.from_ == "Sales"
    assert relationship.to == "Product"
    assert relationship.cardinality == ERModelRelationshipCardinalityClass.N_ONE

    sales_urn = mapper._table_dataset_urn("SalesModel", "Sales")
    membership = next(
        a
        for u, a in aspects
        if u == sales_urn and isinstance(a, SemanticModelPropertiesClass)
    )
    assert membership.alias == "Sales"
    assert membership.semanticModel == model_urn

    ignored_urn = mapper._table_dataset_urn("SalesModel", "Ignored")
    assert not any(u == ignored_urn for u, _ in aspects)

    sales_subtypes = next(
        a for u, a in aspects if u == sales_urn and isinstance(a, SubTypesClass)
    )
    assert sales_subtypes.typeNames[0] == DatasetSubTypes.SEMANTIC_MODEL_DATASET
    catalog_dataset = builder.make_dataset_urn("azure-analysis-services", "SalesModel")
    assert not any(u == catalog_dataset for u, _ in aspects)


def test_visible_columns_are_dimensions_and_measures_are_metrics() -> None:
    mapper = _mapper(table_pattern={"deny": ["Ignored"]})
    model = AasTabularModel(
        catalog="SalesModel",
        name="Model",
        tables=[
            AasTable(
                name="Sales",
                columns=[
                    AasColumn(
                        name="Region",
                        data_type=constants.TomDataType.STRING,
                        datahub_data_type=NullTypeClass(),
                    ),
                    AasColumn(
                        name="OrderDate",
                        data_type=constants.TomDataType.DATETIME,
                        datahub_data_type=DateTypeClass(),
                    ),
                    AasColumn(
                        name="InternalKey",
                        data_type=constants.TomDataType.INT64,
                        datahub_data_type=NumberTypeClass(),
                        is_hidden=True,
                    ),
                ],
                measures=[
                    AasMeasure(
                        name="Total Sales",
                        description="Sum of sales",
                        expression="SUM(Sales[Amount])",
                    ),
                    AasMeasure(name="Margin (%)", expression="1", is_hidden=True),
                    AasMeasure(
                        name="Ratio", expression="DIVIDE([Total Sales], Product[Cost])"
                    ),
                ],
            ),
            AasTable(name="Product"),
            AasTable(
                name="Hidden",
                is_hidden=True,
                columns=[
                    AasColumn(
                        name="Secret",
                        data_type=constants.TomDataType.STRING,
                        datahub_data_type=NullTypeClass(),
                    )
                ],
                measures=[AasMeasure(name="Not Exposed", expression="1")],
            ),
            AasTable(
                name="Ignored",
                measures=[AasMeasure(name="Ignored Measure", expression="1")],
            ),
        ],
        calc_dependencies=[
            AasCalcDependency(
                object_type="MEASURE",
                table="Sales",
                object_name="Ratio",
                referenced_object_type="COLUMN",
                referenced_table="Product",
                referenced_object="Cost",
            )
        ],
    )

    aspects = [
        (wu.get_urn(), getattr(wu.metadata, "aspect", None))
        for wu in mapper.map_model(model)
    ]

    sales_urn = mapper._table_dataset_urn("SalesModel", "Sales")
    product_urn = mapper._table_dataset_urn("SalesModel", "Product")
    annotations = {
        u: a for u, a in aspects if isinstance(a, SemanticFieldAnnotationClass)
    }
    assert set(annotations) == {
        builder.make_schema_field_urn(sales_urn, "Region"),
        builder.make_schema_field_urn(sales_urn, "OrderDate"),
    }
    order_date = annotations[builder.make_schema_field_urn(sales_urn, "OrderDate")]
    assert order_date.type == SemanticFieldTypeClass.DIMENSION
    assert order_date.dimension is not None and order_date.dimension.isTime

    model_urn = mapper._semantic_model_urn("SalesModel")

    def metric_urn(metric_id: str) -> str:
        return str(
            MetricUrn(
                "urn:li:dataPlatform:azure-analysis-services",
                "myserver.SalesModel",
                metric_id,
            )
        )

    assert {u for u, a in aspects if isinstance(a, MetricInfoClass)} == {
        metric_urn("Total Sales"),
        metric_urn("Ratio"),
    }
    total_urn = metric_urn("Total Sales")
    info = next(
        a for u, a in aspects if u == total_urn and isinstance(a, MetricInfoClass)
    )
    assert info.semanticModel == model_urn
    assert info.expression is not None
    assert info.expression.dialects[0].expression == "SUM(Sales[Amount])"

    def upstreams(metric_id: str) -> List[str]:
        aspect = next(
            a
            for u, a in aspects
            if u == metric_urn(metric_id) and isinstance(a, MetricUpstreamsClass)
        )
        return [edge.destinationUrn for edge in aspect.datasetUpstreams]

    assert upstreams("Total Sales") == [sales_urn]
    assert upstreams("Ratio") == [sales_urn, product_urn]
    assert not any(u == metric_urn("Margin _%_") for u, _ in aspects)
    assert not any(u == metric_urn("Not Exposed") for u, _ in aspects)


def test_ingest_hidden_metrics_tags_only_the_hidden_ones() -> None:
    mapper = _mapper(
        ingest_hidden_metrics={
            "enabled": True,
            "tag_hidden_metrics": True,
            "hidden_metric_tag": "Internal",
        },
    )
    model = AasTabularModel(
        catalog="SalesModel",
        name="Model",
        tables=[
            AasTable(
                name="Sales",
                measures=[
                    AasMeasure(name="Total Sales", expression="1"),
                    AasMeasure(name="Margin", expression="1", is_hidden=True),
                ],
            ),
            AasTable(
                name="Hidden",
                is_hidden=True,
                measures=[AasMeasure(name="Not Exposed", expression="1")],
            ),
        ],
    )
    aspects = [
        (wu.get_urn(), getattr(wu.metadata, "aspect", None))
        for wu in mapper.map_model(model)
    ]

    def metric_urn(metric_id: str) -> str:
        return str(
            MetricUrn(
                "urn:li:dataPlatform:azure-analysis-services",
                "myserver.SalesModel",
                metric_id,
            )
        )

    def tags(metric_id: str) -> List[str]:
        aspect = next(
            (
                a
                for u, a in aspects
                if u == metric_urn(metric_id) and isinstance(a, GlobalTagsClass)
            ),
            None,
        )
        return [tag.tag for tag in aspect.tags] if aspect is not None else []

    assert tags("Total Sales") == []
    assert tags("Margin") == [builder.make_tag_urn("Internal")]
    assert tags("Not Exposed") == [builder.make_tag_urn("Internal")]


def test_semantic_model_urn_follows_lowercase_and_platform_instance() -> None:
    mapper = _mapper(
        convert_urns_to_lowercase=True, platform_instance="finance_instance"
    )
    assert mapper._semantic_model_urn("SalesModel") == str(
        SemanticModelUrn(
            "urn:li:dataPlatform:azure-analysis-services",
            "finance_instance.myserver",
            "salesmodel",
        )
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
        DatasetSubTypes.SEMANTIC_MODEL_DATASET,
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
        DatasetSubTypes.SEMANTIC_MODEL_DATASET,
        DatasetSubTypes.CALCULATED_TABLE,
    ]


def test_subtypes_import_table_without_expression() -> None:
    mapper = _mapper()
    table = AasTable(name="Static")
    assert mapper._table_subtypes(table) == [DatasetSubTypes.SEMANTIC_MODEL_DATASET]


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


def test_view_properties_multiple_partitions_valid_m() -> None:
    mapper = _mapper()
    table = AasTable(
        name="Sales",
        partitions=[
            AasPartition(name="2023", query_definition="let Source = 2023 in Source"),
            AasPartition(name="2024", query_definition="let Source = 2024 in Source"),
        ],
    )
    vp = mapper._table_view_properties(table)
    assert isinstance(vp, ViewPropertiesClass)
    assert vp.viewLanguage == "M"
    # Both partition definitions must be preserved (no logic dropped)...
    assert "let Source = 2023 in Source" in vp.viewLogic
    assert "let Source = 2024 in Source" in vp.viewLogic
    # ...wrapped as a single valid M record literal, not two concatenated
    # `let ... in` blocks (which is not valid M).
    assert vp.viewLogic.startswith("[")
    assert vp.viewLogic.rstrip().endswith("]")
    assert '#"2023"' in vp.viewLogic
    assert '#"2024"' in vp.viewLogic


def test_view_properties_single_partition_emitted_as_is() -> None:
    mapper = _mapper()
    table = AasTable(
        name="Sales",
        partitions=[
            AasPartition(name="p", query_definition="let Source = 1 in Source")
        ],
    )
    vp = mapper._table_view_properties(table)
    assert isinstance(vp, ViewPropertiesClass)
    # A single partition is not record-wrapped — its M is stored verbatim.
    assert vp.viewLogic == "let Source = 1 in Source"


def test_map_model_skips_case_duplicate_and_filtered_tables() -> None:
    mapper = _mapper(table_pattern={"deny": ["Ignored"]})
    model = AasTabularModel(
        catalog="SalesModel",
        name="Sales Model",
        tables=[
            AasTable(name="Sales"),
            # Collides case-insensitively with "Sales" — must not be re-emitted.
            AasTable(name="SALES"),
            # Filtered out by table_pattern — must not be emitted at all.
            AasTable(name="Ignored"),
        ],
    )
    urns = [wu.get_urn() for wu in mapper.map_model(model)]
    sales_urn = mapper._table_dataset_urn("SalesModel", "Sales")
    ignored_urn = mapper._table_dataset_urn("SalesModel", "Ignored")
    # The case-collision is emitted exactly once, under the first table's URN.
    dataset_urns = [u for u in urns if u == sales_urn]
    assert dataset_urns, urns
    assert ignored_urn not in urns
    assert list(mapper.report.filtered_tables) == ["Ignored"]


def test_powerbi_urn_alignment() -> None:
    aas_mapper = _mapper()
    assert (
        aas_mapper._table_dataset_name("Sales Model", "My Table")
        == "Sales Model.My Table"
    )

    pbi_mapper = _mapper(platform="powerbi", server=_POWERBI_SERVER)
    # Power BI's form_full_table_name replaces spaces with underscores.
    assert (
        pbi_mapper._table_dataset_name("Sales Model", "My Table")
        == "Sales_Model.My_Table"
    )


def test_powerbi_workspace_prefix_alignment() -> None:
    prefixed = _mapper(
        platform="powerbi",
        server=_POWERBI_SERVER,
        include_workspace_name_in_dataset_urn=True,
    )
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


def test_cube_metadata_and_table_count_respect_table_pattern() -> None:
    mapper = _mapper(table_pattern={"deny": ["Ignored"]})
    model = AasTabularModel(
        catalog="SalesModel",
        name="Sales Model",
        tables=[
            AasTable(
                name="Sales",
                measures=[AasMeasure(name="Total", expression="SUM(Sales[Amount])")],
            ),
            AasTable(
                name="Ignored",
                measures=[AasMeasure(name="Hidden", expression="SUM(Ignored[X])")],
            ),
        ],
    )

    # table_count must reflect the filtered set, not every model table.
    props = mapper._model_container_properties(model)
    assert props[constants.PROP_TABLE_COUNT] == "1"


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


def test_map_model_does_not_swallow_consumer_errors() -> None:
    # A sink/pipeline error thrown back into the generator while consuming a
    # table's work units must propagate — not be recorded as a table-mapping
    # failure (which would also drop the rest of that table's work units).
    mapper = _mapper()
    model = AasTabularModel(
        catalog="SalesModel",
        name="Sales Model",
        tables=[AasTable(name="Sales")],
    )
    sales_urn = mapper._table_dataset_urn("SalesModel", "Sales")

    gen = cast(Generator[MetadataWorkUnit, None, None], mapper.map_model(model))
    for wu in gen:
        if wu.get_urn() == sales_urn:
            # Simulate the consumer (a sink) raising while pulling this work unit.
            with pytest.raises(RuntimeError, match="sink boom"):
                gen.throw(RuntimeError("sink boom"))
            break
    else:
        raise AssertionError("table work unit was never yielded")

    assert mapper.report.tables_skipped == 0


def test_model_named_by_catalog_and_dax_edges_get_dataset_upstreams() -> None:
    mapper = _mapper()
    model = AasTabularModel(
        catalog="SalesModel",
        name="Model",
        tables=[
            AasTable(
                name="Sales",
                columns=[AasColumn(name="Amount", datahub_data_type=NullTypeClass())],
            ),
            AasTable(
                name="SalesSummary",
                columns=[
                    AasColumn(name="TotalAmount", datahub_data_type=NullTypeClass())
                ],
            ),
        ],
        calc_dependencies=[
            AasCalcDependency(
                object_type="CALC_COLUMN",
                table="SalesSummary",
                object_name="TotalAmount",
                referenced_object_type="COLUMN",
                referenced_table="Sales",
                referenced_object="Amount",
            )
        ],
    )
    aspects = [
        (wu.metadata.entityUrn, wu.metadata.aspect)
        for wu in mapper.map_model(model)
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
    ]

    # TMSCHEMA_MODEL.Name is usually "Model"; the catalog names the model.
    container_names = [
        a.name for _, a in aspects if isinstance(a, ContainerPropertiesClass)
    ]
    assert container_names == ["myserver", "SalesModel"]
    model_urn = mapper._semantic_model_urn("SalesModel")
    [model_info] = [
        a
        for u, a in aspects
        if u == model_urn and isinstance(a, SemanticModelInfoClass)
    ]
    assert model_info.name == "SalesModel"

    summary_urn = mapper._table_dataset_urn("SalesModel", "SalesSummary")
    [lineage] = [
        a
        for u, a in aspects
        if u == summary_urn and isinstance(a, UpstreamLineageClass)
    ]
    assert [u.dataset for u in lineage.upstreams] == [
        mapper._table_dataset_urn("SalesModel", "Sales")
    ]
