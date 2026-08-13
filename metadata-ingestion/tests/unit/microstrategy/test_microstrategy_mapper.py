import json
from typing import Dict, Iterable, Optional, Type, TypeVar

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import (
    DERIVED_TAG_URN,
    DIMENSION_TAG_URN,
    MEASURE_TAG_URN,
    TEMPORAL_TAG_URN,
)
from datahub.ingestion.source.microstrategy.lineage import WarehouseLineageContext
from datahub.ingestion.source.microstrategy.mapper import MicroStrategyMapper
from datahub.ingestion.source.microstrategy.models import (
    DashboardDefinition,
    DatasetObject,
    FolderPart,
    MetricEnrichment,
    MicroStrategyObject,
    PredefinedFolderResolution,
    ReportDefinition,
    extract_folder_parts,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    ContainerClass,
    ContainerPropertiesClass,
    DashboardInfoClass,
    DatasetPropertiesClass,
    InputFieldsClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    UpstreamLineageClass,
    _Aspect,
)

T = TypeVar("T", bound=_Aspect)


def _aspect(workunits: Iterable[MetadataWorkUnit], aspect_type: Type[T]) -> T:
    for workunit in workunits:
        aspect = workunit.get_aspect_of_type(aspect_type)
        if aspect:
            return aspect
    raise AssertionError(f"Aspect {aspect_type} not found")


def _maybe_aspect(
    workunits: Iterable[MetadataWorkUnit], aspect_type: Type[T]
) -> Optional[T]:
    for workunit in workunits:
        aspect = workunit.get_aspect_of_type(aspect_type)
        if aspect:
            return aspect
    return None


def _container_names(workunits: Iterable[MetadataWorkUnit]) -> Dict[str, str]:
    names: Dict[str, str] = {}
    for workunit in workunits:
        properties = workunit.get_aspect_of_type(ContainerPropertiesClass)
        if properties:
            names[workunit.get_urn()] = properties.name
    return names


def _tag_urns(field: SchemaFieldClass) -> set[str]:
    if not field.globalTags:
        return set()
    return {tag.tag for tag in field.globalTags.tags}


def _definition() -> DashboardDefinition:
    return DashboardDefinition.from_api_response(
        object_id="dash-1",
        object_name="Executive Sales",
        description="Sales dossier",
        response={
            "result": {
                "definition": {
                    "datasets": [
                        {
                            "id": "ds-1",
                            "name": "Sales Cube",
                            "availableObjects": {
                                "metrics": [
                                    {
                                        "id": "metric-1",
                                        "name": "Revenue",
                                        "dataType": "double",
                                    }
                                ],
                                "attributes": [
                                    {
                                        "id": "attr-1",
                                        "name": "Order Date",
                                        "forms": [
                                            {
                                                "id": "form-1",
                                                "name": "ID",
                                                "dataType": "date",
                                                "baseFormCategory": "DATE",
                                            }
                                        ],
                                    }
                                ],
                            },
                        }
                    ],
                    "chapters": [
                        {
                            "pages": [
                                {
                                    "visualizations": [
                                        {
                                            "key": "viz-1",
                                            "name": "Revenue Trend",
                                            "type": "line",
                                            "datasets": ["ds-1"],
                                        }
                                    ]
                                }
                            ]
                        }
                    ],
                }
            }
        },
    )


def _mapper(
    emit_dashboard_dataset_edges: bool = False,
    extract_warehouse_lineage: bool = False,
    extract_report_sql_lineage: bool = False,
    metric_glossary_term_mapping: Optional[Dict[str, str]] = None,
    folder_pattern: Optional[Dict[str, object]] = None,
    extract_metric_formula_lineage: bool = False,
) -> MicroStrategyMapper:
    config = MicroStrategyConfig.model_validate(
        {
            "base_url": "https://mstr.example.com/MicroStrategyLibrary",
            "platform_instance": "prod",
            "emit_dashboard_dataset_edges": emit_dashboard_dataset_edges,
            "extract_warehouse_lineage": extract_warehouse_lineage,
            "extract_report_sql_lineage": extract_report_sql_lineage,
            "metric_glossary_term_mapping": metric_glossary_term_mapping or {},
            "extract_metric_formula_lineage": extract_metric_formula_lineage,
            **({"folder_pattern": folder_pattern} if folder_pattern else {}),
        }
    )
    return MicroStrategyMapper(config, MicroStrategyReport())


def test_schema_fields_tag_metrics_attributes_and_temporal_forms() -> None:
    mapper = _mapper()
    dashboard = _definition()
    parent_key = mapper.project_key("project-1")

    workunits = list(
        mapper.gen_dataset_workunits(
            "project-1",
            dashboard,
            dashboard.datasets[0],
            parent_key,
        )
    )
    schema = _aspect(workunits, SchemaMetadataClass)
    fields = {field.fieldPath: field for field in schema.fields}

    assert _tag_urns(fields["Revenue"]) == {MEASURE_TAG_URN}
    assert _tag_urns(fields["Order Date"]) == {DIMENSION_TAG_URN, TEMPORAL_TAG_URN}
    assert '"microstrategyObjectType": "metric"' in (fields["Revenue"].jsonProps or "")
    assert '"microstrategyObjectType": "attribute"' in (
        fields["Order Date"].jsonProps or ""
    )


def test_chart_inputs_are_dataset_lineage() -> None:
    mapper = _mapper()
    dashboard = _definition()
    visualization = dashboard.visualizations[0]

    workunits = list(
        mapper.gen_chart_workunits(
            "project-1",
            dashboard,
            visualization,
            mapper.project_key("project-1"),
        )
    )
    chart_info = _aspect(workunits, ChartInfoClass)
    expected_dataset_urn = mapper.lineage.dataset_urn(
        "project-1", dashboard.id, dashboard.datasets[0]
    )

    assert chart_info.inputs == [expected_dataset_urn]
    assert chart_info.inputEdges is not None
    assert [edge.destinationUrn for edge in chart_info.inputEdges] == [
        expected_dataset_urn
    ]
    assert chart_info.customProperties["microstrategyInputDatasetCount"] == "1"
    assert chart_info.customProperties["microstrategyObjectIdCount"] == "0"


def test_chart_input_fields_reference_metric_and_attribute_fields() -> None:
    mapper = _mapper()
    dashboard = _definition()
    visualization = dashboard.visualizations[0]
    visualization.object_ids = ["metric-1", "attr-1"]
    expected_dataset_urn = mapper.lineage.dataset_urn(
        "project-1", dashboard.id, dashboard.datasets[0]
    )

    workunits = list(
        mapper.gen_chart_workunits(
            "project-1",
            dashboard,
            visualization,
            mapper.project_key("project-1"),
        )
    )
    input_fields = _aspect(workunits, InputFieldsClass)

    schema_fields = [field.schemaField for field in input_fields.fields]
    assert all(field is not None for field in schema_fields)

    assert sorted(field.fieldPath for field in schema_fields if field is not None) == [
        "Order Date",
        "Revenue",
    ]
    assert sorted(field.schemaFieldUrn for field in input_fields.fields) == [
        f"urn:li:schemaField:({expected_dataset_urn},Order Date)",
        f"urn:li:schemaField:({expected_dataset_urn},Revenue)",
    ]


def test_report_chart_uses_report_source_dataset_inputs_and_fields() -> None:
    mapper = _mapper()
    report_object = MicroStrategyObject.model_validate(
        {
            "id": "report-1",
            "name": "Sales Report",
            "type": "3",
            "owner": {"username": "metadata_reader"},
        }
    )
    report_definition = ReportDefinition.from_api_response(
        object_id="report-1",
        object_name="Sales Report",
        response={
            "result": {
                "definition": {
                    "dataSource": {"id": "cube-1", "name": "Sales Cube"},
                    "availableObjects": [
                        {"id": "metric-1", "name": "Revenue", "type": "metric"},
                        {
                            "id": "attr-1",
                            "name": "Region",
                            "type": "attribute",
                        },
                    ],
                }
            }
        },
    )
    source_dataset = DatasetObject.model_validate(
        {
            "id": report_definition.source_id,
            "name": report_definition.source_name,
            "availableObjects": report_definition.available_objects,
        }
    )
    dataset_urn = mapper.lineage.dataset_urn(
        "project-1",
        report_object.id,
        source_dataset,
    )

    dataset_properties = _aspect(
        mapper.gen_report_source_dataset_workunits(
            "project-1",
            report_object,
            source_dataset,
            mapper.project_key("project-1"),
        ),
        DatasetPropertiesClass,
    )
    workunits = list(
        mapper.gen_report_workunits(
            "project-1",
            report_object,
            report_definition,
            source_dataset,
            mapper.project_key("project-1"),
        )
    )
    chart_info = _aspect(workunits, ChartInfoClass)
    input_fields = _aspect(workunits, InputFieldsClass)
    schema_fields = [field.schemaField for field in input_fields.fields]

    assert dataset_properties.customProperties["microstrategyReportId"] == "report-1"
    assert chart_info.inputs == [dataset_urn]
    assert chart_info.inputEdges is not None
    assert [edge.destinationUrn for edge in chart_info.inputEdges] == [dataset_urn]
    assert chart_info.customProperties["microstrategyReportSourceId"] == "cube-1"
    assert all(field is not None for field in schema_fields)
    assert sorted(field.fieldPath for field in schema_fields if field is not None) == [
        "Region",
        "Revenue",
    ]
    assert sorted(field.schemaFieldUrn for field in input_fields.fields) == [
        f"urn:li:schemaField:({dataset_urn},Region)",
        f"urn:li:schemaField:({dataset_urn},Revenue)",
    ]


def test_chart_inputs_are_inferred_from_runtime_visualization_objects() -> None:
    mapper = _mapper()
    dashboard = DashboardDefinition.from_api_response(
        object_id="dash-1",
        object_name="Executive Sales",
        response={
            "datasets": [
                {
                    "id": "ds-yesterday",
                    "name": "Retail Sales Yesterday",
                    "availableObjects": [
                        {"id": "metric-1", "name": "Revenue", "type": "metric"}
                    ],
                },
                {
                    "id": "ds-wtd",
                    "name": "Retail Sales WTD",
                    "availableObjects": [
                        {"id": "metric-1", "name": "Revenue", "type": "metric"}
                    ],
                },
            ],
            "chapters": [
                {
                    "pages": [
                        {
                            "visualizations": [
                                {
                                    "key": "viz-1",
                                    "name": "Revenue Yesterday",
                                    "runtimeDefinition": {
                                        "definition": {
                                            "columns": [
                                                {
                                                    "id": "00000000000000000000000000000000",
                                                    "name": "Metrics",
                                                    "type": "templateMetrics",
                                                    "elements": [
                                                        {
                                                            "id": "metric-1",
                                                            "name": "Revenue",
                                                            "type": "metric",
                                                        }
                                                    ],
                                                }
                                            ]
                                        }
                                    },
                                }
                            ]
                        }
                    ]
                }
            ],
        },
    )

    workunits = list(
        mapper.gen_chart_workunits(
            "project-1",
            dashboard,
            dashboard.visualizations[0],
            mapper.project_key("project-1"),
        )
    )
    chart_info = _aspect(workunits, ChartInfoClass)

    assert chart_info.inputs == [
        mapper.lineage.dataset_urn("project-1", dashboard.id, dashboard.datasets[0])
    ]


def test_dashboard_dataset_edges_are_disabled_by_default() -> None:
    mapper = _mapper()
    dashboard = _definition()
    dashboard_object = MicroStrategyObject.model_validate(
        {"id": dashboard.id, "name": dashboard.name}
    )

    workunits = list(
        mapper.gen_dashboard_workunits(
            "project-1",
            dashboard_object,
            dashboard,
            mapper.project_key("project-1"),
        )
    )
    dashboard_info = _aspect(workunits, DashboardInfoClass)

    assert dashboard_info.datasetEdges is None
    assert dashboard_info.chartEdges is not None
    assert [edge.destinationUrn for edge in dashboard_info.chartEdges] == [
        mapper.chart_urn("project-1", dashboard.id, dashboard.visualizations[0].key)
    ]


def test_dashboard_info_can_include_report_chart_dependency_edges() -> None:
    mapper = _mapper()
    dashboard = _definition()
    dashboard_object = MicroStrategyObject.model_validate(
        {"id": dashboard.id, "name": dashboard.name}
    )
    report_urn = mapper.report_urn("project-1", "report-1")

    dashboard_info = _aspect(
        mapper.gen_dashboard_workunits(
            "project-1",
            dashboard_object,
            dashboard,
            mapper.project_key("project-1"),
            extra_chart_urns=[report_urn],
        ),
        DashboardInfoClass,
    )

    assert dashboard_info.chartEdges is not None
    assert [edge.destinationUrn for edge in dashboard_info.chartEdges] == [
        mapper.chart_urn("project-1", dashboard.id, dashboard.visualizations[0].key),
        report_urn,
    ]
    assert dashboard_info.datasetEdges is None


def test_dashboard_dataset_edges_are_opt_in() -> None:
    mapper = _mapper(emit_dashboard_dataset_edges=True)
    dashboard = _definition()
    dashboard_object = MicroStrategyObject.model_validate(
        {"id": dashboard.id, "name": dashboard.name}
    )

    workunits = list(
        mapper.gen_dashboard_workunits(
            "project-1",
            dashboard_object,
            dashboard,
            mapper.project_key("project-1"),
        )
    )
    dashboard_info = _aspect(workunits, DashboardInfoClass)

    assert dashboard_info.datasetEdges is not None
    assert [edge.destinationUrn for edge in dashboard_info.datasetEdges] == [
        mapper.lineage.dataset_urn("project-1", dashboard.id, dashboard.datasets[0])
    ]


def test_chart_and_dashboard_descriptions_default_to_empty_string() -> None:
    mapper = _mapper()
    dashboard = DashboardDefinition.from_api_response(
        object_id="dash-1",
        object_name="Executive Sales",
        response={
            "datasets": [{"id": "ds-1", "name": "Sales Cube"}],
            "chapters": [
                {
                    "pages": [
                        {
                            "visualizations": [
                                {
                                    "key": "viz-1",
                                    "name": "Revenue Trend",
                                    "datasets": ["ds-1"],
                                }
                            ]
                        }
                    ]
                }
            ],
        },
    )
    dashboard_object = MicroStrategyObject.model_validate(
        {"id": dashboard.id, "name": dashboard.name}
    )
    parent_key = mapper.project_key("project-1")

    chart_info = _aspect(
        mapper.gen_chart_workunits(
            "project-1", dashboard, dashboard.visualizations[0], parent_key
        ),
        ChartInfoClass,
    )
    dashboard_info = _aspect(
        mapper.gen_dashboard_workunits(
            "project-1", dashboard_object, dashboard, parent_key
        ),
        DashboardInfoClass,
    )

    assert chart_info.description == ""
    assert dashboard_info.description == ""


def test_dataset_properties_include_source_warehouse_when_present() -> None:
    mapper = _mapper()
    dashboard = DashboardDefinition.from_api_response(
        object_id="dash-1",
        object_name="Executive Sales",
        response={
            "datasets": [
                {
                    "id": "ds-1",
                    "name": "Sales Cube",
                    "sourceWarehouse": {
                        "id": "source-1",
                        "name": "Enterprise Warehouse",
                        "datasourceType": "normal",
                        "database": {
                            "type": "snow_flake",
                            "version": "snowflake_1x",
                        },
                        "dbms": {"name": "Snowflake"},
                    },
                }
            ]
        },
    )

    dataset_properties = _aspect(
        mapper.gen_dataset_workunits(
            "project-1",
            dashboard,
            dashboard.datasets[0],
            mapper.project_key("project-1"),
        ),
        DatasetPropertiesClass,
    )

    assert (
        dataset_properties.customProperties["microstrategySourceWarehouseId"]
        == "source-1"
    )
    assert (
        dataset_properties.customProperties["microstrategySourceType"] == "snow_flake"
    )
    assert dataset_properties.customProperties["microstrategyDbmsName"] == "Snowflake"


def test_dashboard_info_includes_lifecycle_metadata() -> None:
    mapper = _mapper()
    dashboard = _definition()
    dashboard_object = MicroStrategyObject.model_validate(
        {
            "id": dashboard.id,
            "name": dashboard.name,
            "type": "55",
            "subtype": "14081",
            "owner": {"username": "metadata_reader"},
            "dateCreated": "2026-06-01T10:00:00.000+0000",
            "dateModified": "2026-06-02T11:30:00.000+0000",
        }
    )

    dashboard_info = _aspect(
        mapper.gen_dashboard_workunits(
            "project-1",
            dashboard_object,
            dashboard,
            mapper.project_key("project-1"),
        ),
        DashboardInfoClass,
    )

    assert dashboard_info.customProperties["microstrategyObjectType"] == "55"
    assert dashboard_info.customProperties["microstrategyObjectSubtype"] == "14081"
    assert dashboard_info.customProperties["microstrategyOwner"] == "metadata_reader"
    assert (
        dashboard_info.customProperties["microstrategyDateCreated"]
        == "2026-06-01T10:00:00.000+0000"
    )
    assert dashboard_info.lastModified is not None
    assert dashboard_info.lastModified.created is not None
    assert (
        dashboard_info.lastModified.created.actor == "urn:li:corpuser:metadata_reader"
    )
    assert dashboard_info.lastModified.lastModified is not None
    assert (
        dashboard_info.lastModified.lastModified.time
        > dashboard_info.lastModified.created.time
    )


def test_dataset_properties_include_semantic_counts() -> None:
    mapper = _mapper()
    dashboard = _definition()

    dataset_properties = _aspect(
        mapper.gen_dataset_workunits(
            "project-1",
            dashboard,
            dashboard.datasets[0],
            mapper.project_key("project-1"),
        ),
        DatasetPropertiesClass,
    )

    assert dataset_properties.customProperties["microstrategyMetricCount"] == "1"
    assert dataset_properties.customProperties["microstrategyAttributeCount"] == "1"
    assert dataset_properties.customProperties["microstrategyAttributeFormCount"] == "1"
    assert dataset_properties.customProperties["microstrategySchemaFieldCount"] == "2"
    assert dataset_properties.customProperties["microstrategyObjectIdCount"] == "2"


def test_dataset_workunits_skip_coarse_warehouse_upstream_lineage_by_default() -> None:
    mapper = _mapper()
    dashboard = _definition()
    dashboard.datasets[0].warehouse_upstream_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.orders.fact_orders,PROD)",
    ]

    workunits = list(
        mapper.gen_dataset_workunits(
            "project-1",
            dashboard,
            dashboard.datasets[0],
            mapper.project_key("project-1"),
        )
    )

    assert _maybe_aspect(workunits, UpstreamLineageClass) is None
    dataset_properties = _aspect(workunits, DatasetPropertiesClass)
    assert (
        "microstrategyWarehouseUpstreamCount" not in dataset_properties.customProperties
    )
    assert (
        "microstrategyWarehouseUpstreamPlatforms"
        not in dataset_properties.customProperties
    )


def test_dataset_workunits_include_warehouse_upstream_lineage_when_enabled() -> None:
    mapper = _mapper(extract_warehouse_lineage=True)
    dashboard = _definition()
    dashboard.datasets[0].warehouse_upstream_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.orders.fact_orders,PROD)",
    ]

    workunits = list(
        mapper.gen_dataset_workunits(
            "project-1",
            dashboard,
            dashboard.datasets[0],
            mapper.project_key("project-1"),
        )
    )

    upstream_lineage = _aspect(workunits, UpstreamLineageClass)
    dataset_properties = _aspect(workunits, DatasetPropertiesClass)

    assert sorted(upstream.dataset for upstream in upstream_lineage.upstreams) == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.orders.fact_orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)",
    ]
    assert (
        dataset_properties.customProperties["microstrategyWarehouseUpstreamCount"]
        == "2"
    )
    assert (
        dataset_properties.customProperties["microstrategyWarehouseUpstreamPlatforms"]
        == '["snowflake"]'
    )


def test_chart_with_unresolved_inputs_emits_empty_input_fields() -> None:
    mapper = _mapper()
    dashboard = _definition()
    visualization = dashboard.visualizations[0]
    # No binding, no object references: inputs and input fields unresolved.
    visualization.datasets = []
    visualization.object_ids = []
    dashboard.datasets.append(
        DatasetObject.model_validate({"id": "ds-2", "name": "Second Cube"})
    )

    workunits = list(
        mapper.gen_chart_workunits(
            "project-1",
            dashboard,
            visualization,
            mapper.project_key("project-1"),
        )
    )

    chart_info = _aspect(workunits, ChartInfoClass)
    input_fields = _aspect(workunits, InputFieldsClass)

    # Empty aspects are emitted so a previous run's lineage cannot linger.
    assert chart_info.inputs == []
    assert input_fields.fields == []


def test_dataset_upstreams_restricted_to_field_lineage_tables_when_available() -> None:
    mapper = _mapper(extract_warehouse_lineage=True)
    dashboard = _definition()
    dataset = dashboard.datasets[0]
    fact_table = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)"
    )
    dim_table = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.dims.org_hier,PROD)"
    )
    calendar_table = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.dims.mcal_day,PROD)"
    )
    # SQL view referenced all three, but only the fact table contributes fields.
    dataset.warehouse_upstream_urns = [fact_table, dim_table, calendar_table]
    dataset.field_warehouse_upstreams = {
        "Revenue": [f"urn:li:schemaField:({fact_table},net_sales_amt)"],
    }

    workunits = list(
        mapper.gen_dataset_workunits(
            "project-1",
            dashboard,
            dataset,
            mapper.project_key("project-1"),
        )
    )

    upstream_lineage = _aspect(workunits, UpstreamLineageClass)
    assert [upstream.dataset for upstream in upstream_lineage.upstreams] == [fact_table]
    assert upstream_lineage.fineGrainedLineages
    # The two filter-only tables were pruned and accounted for.
    assert mapper.report.warehouse_upstreams_pruned_by_field_evidence == 2
    assert mapper.report.warehouse_lineage_edges == 1


def test_report_source_dataset_warehouse_lineage_is_opt_in() -> None:
    report_object = MicroStrategyObject.model_validate(
        {"id": "report-1", "name": "Sales Report", "type": "3"}
    )
    source_dataset = DatasetObject.model_validate(
        {"id": "cube-1", "name": "Sales Cube"}
    )
    source_dataset.warehouse_upstream_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)"
    ]
    parent_key = _mapper().project_key("project-1")

    default_workunits = list(
        _mapper().gen_report_source_dataset_workunits(
            "project-1",
            report_object,
            source_dataset,
            parent_key,
        )
    )
    enabled_workunits = list(
        _mapper(extract_report_sql_lineage=True).gen_report_source_dataset_workunits(
            "project-1",
            report_object,
            source_dataset,
            parent_key,
        )
    )

    assert _maybe_aspect(default_workunits, UpstreamLineageClass) is None
    assert _aspect(enabled_workunits, UpstreamLineageClass).upstreams[0].dataset == (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)"
    )


def test_report_source_dataset_includes_model_fine_grained_lineage() -> None:
    mapper = _mapper()
    report_object = MicroStrategyObject.model_validate(
        {"id": "report-1", "name": "Sales Report", "type": "3"}
    )
    source_dataset = DatasetObject.model_validate(
        {
            "id": "cube-1",
            "name": "Sales Cube",
            "availableObjects": {
                "metrics": [{"id": "metric-1", "name": "Revenue", "type": "metric"}]
            },
        }
    )
    upstream_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)"
    )
    source_dataset.field_warehouse_upstreams = {
        "Revenue": [f"urn:li:schemaField:({upstream_dataset_urn},net_sales_amt)"]
    }

    upstream_lineage = _aspect(
        mapper.gen_report_source_dataset_workunits(
            "project-1",
            report_object,
            source_dataset,
            mapper.project_key("project-1"),
        ),
        UpstreamLineageClass,
    )

    assert [upstream.dataset for upstream in upstream_lineage.upstreams] == [
        upstream_dataset_urn
    ]
    fine_grained_lineages = upstream_lineage.fineGrainedLineages or []
    assert len(fine_grained_lineages) == 1
    assert fine_grained_lineages[0].downstreams == [
        (
            "urn:li:schemaField:"
            "(urn:li:dataset:(urn:li:dataPlatform:microstrategy,"
            "prod.project-1.report-1.cube-1,PROD),Revenue)"
        )
    ]


def test_dataset_workunits_include_model_fine_grained_lineage() -> None:
    mapper = _mapper()
    dashboard = _definition()
    dashboard.datasets[0].metric_enrichments["METRIC-1"] = MetricEnrichment(
        fact_ids=["fact-1"]
    )
    upstream_dataset_urn = (
        "urn:li:dataset:"
        "(urn:li:dataPlatform:snowflake,sales_db.orders.fact_orders,PROD)"
    )
    dashboard.datasets[0].warehouse_upstream_urns = [upstream_dataset_urn]
    index = mapper.lineage.model_lineage_index_from_tables(
        [
            {
                "physicalTable": {
                    "namespace": "SALES_DB",
                    "tablePrefix": "ORDERS",
                    "tableName": "fact_orders",
                },
                "facts": [
                    {
                        "information": {"objectId": "fact-1"},
                        "expression": {"text": "net_sales_amt"},
                    }
                ],
                "attributes": [
                    {
                        "information": {"objectId": "attr-1"},
                        "forms": [
                            {
                                "name": "ID",
                                "expression": {"text": "order_date_id"},
                            }
                        ],
                    }
                ],
            }
        ],
        WarehouseLineageContext(
            platform="snowflake",
            env="PROD",
            database="SALES_DB",
            schema="SALES",
        ),
    )
    mapper.attach_model_lineage(dashboard, index)

    workunits = list(
        mapper.gen_dataset_workunits(
            "project-1",
            dashboard,
            dashboard.datasets[0],
            mapper.project_key("project-1"),
        )
    )
    upstream_lineage = _aspect(workunits, UpstreamLineageClass)
    dataset_properties = _aspect(workunits, DatasetPropertiesClass)

    assert [upstream.dataset for upstream in upstream_lineage.upstreams] == [
        upstream_dataset_urn
    ]
    fine_grained_lineages = upstream_lineage.fineGrainedLineages or []
    assert len(fine_grained_lineages) == 2
    downstreams = sorted(
        downstream
        for lineage in fine_grained_lineages
        for downstream in (lineage.downstreams or [])
    )
    microstrategy_dataset_urn = (
        "urn:li:dataset:"
        "(urn:li:dataPlatform:microstrategy,prod.project-1.dash-1.ds-1,PROD)"
    )
    assert downstreams == [
        f"urn:li:schemaField:({microstrategy_dataset_urn},Order Date)",
        f"urn:li:schemaField:({microstrategy_dataset_urn},Revenue)",
    ]
    assert (
        "microstrategyWarehouseUpstreamCount" not in dataset_properties.customProperties
    )
    assert (
        dataset_properties.customProperties["microstrategyModelLineageFieldCount"]
        == "2"
    )


def test_dataset_workunits_skip_model_lineage_without_dataset_sql_context() -> None:
    mapper = _mapper()
    dashboard = _definition()
    dashboard.datasets[0].metric_enrichments["METRIC-1"] = MetricEnrichment(
        fact_ids=["fact-1"]
    )
    index = mapper.lineage.model_lineage_index_from_tables(
        [
            {
                "physicalTable": {
                    "namespace": "SALES_DB",
                    "tablePrefix": "ORDERS",
                    "tableName": "fact_orders",
                },
                "facts": [
                    {
                        "information": {"objectId": "fact-1"},
                        "expression": {"text": "net_sales_amt"},
                    }
                ],
            }
        ],
        WarehouseLineageContext(
            platform="snowflake",
            env="PROD",
            database="SALES_DB",
            schema="SALES",
        ),
    )
    mapper.attach_model_lineage(dashboard, index)

    workunits = list(
        mapper.gen_dataset_workunits(
            "project-1",
            dashboard,
            dashboard.datasets[0],
            mapper.project_key("project-1"),
        )
    )

    assert _maybe_aspect(workunits, UpstreamLineageClass) is None


def test_dashboard_properties_include_direct_dependency_summary() -> None:
    mapper = _mapper()
    dashboard = _definition()
    dashboard.dependencies = [
        MicroStrategyObject.model_validate(
            {"id": "metric-1", "name": "Revenue", "type": 4, "subtype": 1024}
        ),
        MicroStrategyObject.model_validate(
            {"id": "attr-1", "name": "Order Date", "type": 12, "subtype": 3072}
        ),
    ]
    dashboard_object = MicroStrategyObject.model_validate(
        {"id": dashboard.id, "name": dashboard.name}
    )

    dashboard_info = _aspect(
        mapper.gen_dashboard_workunits(
            "project-1",
            dashboard_object,
            dashboard,
            mapper.project_key("project-1"),
        ),
        DashboardInfoClass,
    )

    assert dashboard_info.customProperties["microstrategyDirectDependencyCount"] == "2"
    assert (
        '"4": 1'
        in dashboard_info.customProperties["microstrategyDirectDependencyTypeCounts"]
    )
    assert (
        "metric-1" in dashboard_info.customProperties["microstrategyDirectDependencies"]
    )


def test_metric_expression_is_preserved_in_field_json_props() -> None:
    mapper = _mapper()
    dashboard = _definition()
    dashboard.datasets[0].metric_enrichments["METRIC-1"] = MetricEnrichment(
        expression_text="Sum(Revenue)",
        expression_tokens='[{"id": "fact-1", "name": "Revenue Fact", "type": "fact"}]',
        fact_ids=["FACT-1"],
    )

    schema = _aspect(
        mapper.gen_dataset_workunits(
            "project-1",
            dashboard,
            dashboard.datasets[0],
            mapper.project_key("project-1"),
        ),
        SchemaMetadataClass,
    )
    revenue = next(field for field in schema.fields if field.fieldPath == "Revenue")

    assert "microstrategyMetricExpressionText" in (revenue.jsonProps or "")
    assert "Revenue Fact" in (revenue.jsonProps or "")
    # The formula also renders in the field description as a markdown code
    # block, after any human-authored description.
    assert revenue.description is not None
    assert revenue.description.endswith("```\nSum(Revenue)\n```")
    assert "MicroStrategy expression" not in revenue.description


def test_metric_description_without_expression_is_unchanged() -> None:
    mapper = _mapper()
    dashboard = _definition()

    schema = _aspect(
        mapper.gen_dataset_workunits(
            "project-1",
            dashboard,
            dashboard.datasets[0],
            mapper.project_key("project-1"),
        ),
        SchemaMetadataClass,
    )
    revenue = next(field for field in schema.fields if field.fieldPath == "Revenue")

    assert "```" not in (revenue.description or "")


def test_extract_folder_parts_from_search_result_payloads() -> None:
    # getAncestors=true responses carry the folder path as an ancestors array.
    assert extract_folder_parts(
        {
            "ancestors": [
                {"id": "f-1", "name": "Public Objects"},
                {"id": "f-2", "name": "Reports"},
            ]
        }
    ) == [
        FolderPart(name="Public Objects", id="f-1"),
        FolderPart(name="Reports", id="f-2"),
    ]
    assert extract_folder_parts({"location": "/Shared Reports/Finance"}) == [
        FolderPart(name="Shared Reports"),
        FolderPart(name="Finance"),
    ]
    assert extract_folder_parts({"folder": {"path": "/A/B"}}) == [
        FolderPart(name="A"),
        FolderPart(name="B"),
    ]
    assert extract_folder_parts({"name": "no folder info"}) == []
    # Malformed ancestors fall back to location.
    assert extract_folder_parts(
        {"ancestors": ["not-a-dict"], "location": "/Fallback"}
    ) == [FolderPart(name="Fallback")]


def test_gen_folder_containers_chain_and_deepest_folder_parent() -> None:
    mapper = _mapper()
    dashboard_object = MicroStrategyObject.model_validate(
        {"id": "dash-1", "name": "Sales Dashboard", "location": "/A/B"}
    )
    project_urn = mapper.project_key("project-1").as_urn()
    folder_a_key = mapper.folder_key("project-1", "A")
    folder_ab_key = mapper.folder_key("project-1", "A/B")

    workunits = list(mapper.gen_folder_containers("project-1", dashboard_object))
    parents: Dict[str, str] = {}
    for workunit in workunits:
        container = workunit.get_aspect_of_type(ContainerClass)
        if container:
            parents[workunit.get_urn()] = container.container

    assert parents[folder_a_key.as_urn()] == project_urn
    assert parents[folder_ab_key.as_urn()] == folder_a_key.as_urn()
    assert (
        mapper.folder_container_for_dashboard("project-1", dashboard_object)
        == folder_ab_key
    )


def test_gen_folder_containers_without_folder_labels_uses_raw_name() -> None:
    # No folder_labels passed (the pre-feature/opt-out case): raw metadata names
    # are used untouched, even though the ancestor id is present and would match.
    mapper = _mapper()
    dashboard_object = MicroStrategyObject.model_validate(
        {
            "id": "dash-1",
            "name": "Sales Dashboard",
            "ancestors": [{"id": "reports-folder-id", "name": "Reports"}],
        }
    )
    raw_key = mapper.folder_key("project-1", "Reports")

    workunits = list(mapper.gen_folder_containers("project-1", dashboard_object))
    names = _container_names(workunits)

    assert names[raw_key.as_urn()] == "Reports"
    assert (
        mapper.folder_container_for_dashboard("project-1", dashboard_object) == raw_key
    )


def test_gen_folder_containers_resolves_predefined_folder_label_everywhere() -> None:
    # The resolved label must feed pattern matching, container identity, AND
    # display name identically -- not just the display name.
    mapper = _mapper(folder_pattern={"deny": ["^Reports$"]})
    dashboard_object = MicroStrategyObject.model_validate(
        {
            "id": "dash-1",
            "name": "Sales Dashboard",
            "ancestors": [
                {"id": "public-objects-id", "name": "Public Objects"},
                {"id": "reports-folder-id", "name": "Reports"},
            ],
        }
    )
    # Keys are normalized ids; the ancestor payload's casing must not matter.
    folder_labels = PredefinedFolderResolution(
        labels={"REPORTS-FOLDER-ID": "Shared Reports"}, hidden_ids=set()
    )
    resolved_key = mapper.folder_key("project-1", "Public Objects/Shared Reports")
    raw_key = mapper.folder_key("project-1", "Public Objects/Reports")

    workunits = list(
        mapper.gen_folder_containers(
            "project-1", dashboard_object, predefined_folders=folder_labels
        )
    )
    names = _container_names(workunits)

    # Container identity uses the resolved name, not the raw one.
    assert resolved_key.as_urn() in names
    assert raw_key.as_urn() not in names
    assert names[resolved_key.as_urn()] == "Shared Reports"
    # folder_pattern matched against the resolved name ("Shared Reports"), so the
    # deny rule targeting the raw name ("Reports") no longer applies.
    assert (
        mapper.folder_container_for_dashboard(
            "project-1", dashboard_object, predefined_folders=folder_labels
        )
        == resolved_key
    )


def test_metric_glossary_term_mapping_attaches_term_to_schema_field() -> None:
    mapper = _mapper(
        metric_glossary_term_mapping={"Revenue": "urn:li:glossaryTerm:Revenue"}
    )
    dashboard = _definition()

    schema = _aspect(
        mapper.gen_dataset_workunits(
            "project-1",
            dashboard,
            dashboard.datasets[0],
            mapper.project_key("project-1"),
        ),
        SchemaMetadataClass,
    )
    revenue = next(field for field in schema.fields if field.fieldPath == "Revenue")

    assert revenue.glossaryTerms is not None
    assert [term.urn for term in revenue.glossaryTerms.terms] == [
        "urn:li:glossaryTerm:Revenue"
    ]


def _compound_grid_definition() -> DashboardDefinition:
    """Single-page compound grid: two family datasets plus a combined one,
    column groups named by family, one derived metric per group. The combined
    group's name never matches its dataset, so it binds by elimination."""
    return DashboardDefinition.from_api_response(
        object_id="dash-grid",
        object_name="Sales Overview",
        response={
            "definition": {
                "datasets": [
                    {
                        "id": "ds-store",
                        "name": "STORE SALES WTD",
                        "availableObjects": {
                            "metrics": [
                                {"id": "M100", "name": "Net Amount"},
                                {"id": "M200", "name": "Store Plan Amt"},
                            ],
                            "attributes": [
                                {
                                    "id": "A100",
                                    "name": "Region Number",
                                    "forms": [{"id": "F100", "name": "NUMBER"}],
                                }
                            ],
                        },
                    },
                    {
                        "id": "ds-web",
                        "name": "WEB SALES WTD",
                        "availableObjects": {
                            "metrics": [
                                {"id": "M100", "name": "Net Amount"},
                                {"id": "M300", "name": "Web Plan Amt"},
                            ],
                            "attributes": [
                                {
                                    "id": "A100",
                                    "name": "Region Number",
                                    "forms": [{"id": "F100", "name": "NUMBER"}],
                                }
                            ],
                        },
                    },
                    {
                        "id": "ds-combined",
                        "name": "COMBINED TOTALS WTD",
                        "availableObjects": {
                            "metrics": [
                                {"id": "M100", "name": "Net Amount"},
                                {"id": "M200", "name": "Store Plan Amt"},
                                {"id": "M300", "name": "Web Plan Amt"},
                            ],
                            "attributes": [
                                {
                                    "id": "A100",
                                    "name": "Region Number",
                                    "forms": [{"id": "F100", "name": "NUMBER"}],
                                }
                            ],
                        },
                    },
                ],
                "chapters": [
                    {
                        "key": "ch-1",
                        "name": "SALES",
                        "pages": [
                            {
                                "key": "pg-wtd",
                                "name": "WTD",
                                "visualizations": [
                                    {
                                        "key": "viz-wtd",
                                        "name": "Sales WTD",
                                        "visualizationType": "compound_grid",
                                        "runtimeDefinition": {
                                            "definition": {
                                                "grid": {
                                                    "columnSets": [
                                                        {
                                                            "key": "cs-store",
                                                            "name": "STORE",
                                                            "columns": [
                                                                {
                                                                    "type": "templateMetrics",
                                                                    "elements": [
                                                                        {
                                                                            "type": "metric",
                                                                            "id": "M100",
                                                                            "name": "Net Amount",
                                                                        },
                                                                        {
                                                                            "type": "metric",
                                                                            "id": "M200",
                                                                            "name": "Store Plan Amt",
                                                                        },
                                                                        {
                                                                            "type": "metric",
                                                                            "id": "D100",
                                                                            "name": "Pct To Plan",
                                                                            "derived": True,
                                                                            "dataType": "double",
                                                                        },
                                                                    ],
                                                                }
                                                            ],
                                                        },
                                                        {
                                                            "key": "cs-web",
                                                            "name": "WEB",
                                                            "columns": [
                                                                {
                                                                    "type": "templateMetrics",
                                                                    "elements": [
                                                                        {
                                                                            "type": "metric",
                                                                            "id": "M100",
                                                                            "name": "Net Amount",
                                                                        },
                                                                        {
                                                                            "type": "metric",
                                                                            "id": "M300",
                                                                            "name": "Web Plan Amt",
                                                                        },
                                                                        {
                                                                            "type": "metric",
                                                                            "id": "D200",
                                                                            "name": "Pct To Plan",
                                                                            "derived": True,
                                                                            "dataType": "double",
                                                                        },
                                                                    ],
                                                                }
                                                            ],
                                                        },
                                                        {
                                                            "key": "cs-total",
                                                            "name": "TOTAL SALES",
                                                            "columns": [
                                                                {
                                                                    "type": "templateMetrics",
                                                                    "elements": [
                                                                        {
                                                                            "type": "metric",
                                                                            "id": "M100",
                                                                            "name": "Net Amount",
                                                                        },
                                                                        {
                                                                            "type": "metric",
                                                                            "id": "D300",
                                                                            "name": "Pct To Plan",
                                                                            "derived": True,
                                                                            "dataType": "double",
                                                                        },
                                                                    ],
                                                                }
                                                            ],
                                                        },
                                                    ]
                                                }
                                            }
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        },
    )


def test_derived_metrics_become_tagged_schema_fields() -> None:
    mapper = _mapper()
    dashboard = _compound_grid_definition()
    mapper.attach_derived_metrics(dashboard)
    parent_key = mapper.project_key("project-1")

    store = next(ds for ds in dashboard.datasets if ds.id == "ds-store")
    combined = next(ds for ds in dashboard.datasets if ds.id == "ds-combined")
    assert list(store.derived_metrics) == ["D100"]
    assert list(combined.derived_metrics) == ["D300"]

    workunits = list(
        mapper.gen_dataset_workunits("project-1", dashboard, store, parent_key)
    )
    schema = _aspect(workunits, SchemaMetadataClass)
    fields = {field.fieldPath: field for field in schema.fields}

    derived_field = fields["Pct To Plan"]
    assert _tag_urns(derived_field) == {MEASURE_TAG_URN, DERIVED_TAG_URN}
    assert derived_field.description is not None
    assert "'STORE' column group" in derived_field.description
    assert "'Sales WTD'" in derived_field.description
    derived_props = json.loads(derived_field.jsonProps or "{}")
    assert derived_props["microstrategyObjectType"] == "derivedMetric"
    assert derived_props["microstrategyColumnGroup"] == "STORE"
    assert derived_props["microstrategyObjectId"] == "D100"
    # Catalog members of a bound group carry the group name too.
    catalog_props = json.loads(fields["Store Plan Amt"].jsonProps or "{}")
    assert catalog_props["microstrategyColumnGroup"] == "STORE"
    assert mapper.report.derived_metric_fields_scanned == 1


def test_derived_metric_never_shadows_catalog_object() -> None:
    mapper = _mapper()
    dashboard = _compound_grid_definition()
    # Mark a real catalog metric as derived in the grid payload: the catalog
    # field must win and no derived spec may be attached for that id.
    visualization = dashboard.visualizations[0]
    for column_set in visualization.column_sets:
        for metric in column_set.metrics:
            if metric.id == "M200":
                metric.derived = True
    mapper.attach_derived_metrics(dashboard)

    store = next(ds for ds in dashboard.datasets if ds.id == "ds-store")
    assert "M200" not in store.derived_metrics


def test_chart_input_fields_include_derived_metrics() -> None:
    mapper = _mapper()
    dashboard = _compound_grid_definition()
    mapper.attach_derived_metrics(dashboard)
    parent_key = mapper.project_key("project-1")

    workunits = list(
        mapper.gen_chart_workunits(
            "project-1", dashboard, dashboard.visualizations[0], parent_key
        )
    )
    chart_info = _aspect(workunits, ChartInfoClass)
    assert len(chart_info.inputs or []) == 3

    input_fields = _aspect(workunits, InputFieldsClass)
    field_paths = {
        input_field.schemaField.fieldPath
        for input_field in input_fields.fields
        if input_field.schemaField
    }
    assert "Pct To Plan" in field_paths

    # Shared catalog metrics legitimately repeat once per column group; each
    # entry is stamped with its group and source dataset so the repeats are
    # tellable apart in the chart's field list.
    shared_descriptions = {
        input_field.schemaField.description
        for input_field in input_fields.fields
        if input_field.schemaField and input_field.schemaField.fieldPath == "Net Amount"
    }
    assert len(shared_descriptions) == 3
    assert any(
        description and description.startswith("**STORE** — STORE SALES WTD")
        for description in shared_descriptions
    )
    assert any(
        description and description.startswith("**TOTAL SALES** — COMBINED TOTALS WTD")
        for description in shared_descriptions
    )

    column_groups = {
        prop: value for prop, value in (chart_info.customProperties or {}).items()
    }
    groups = json.loads(column_groups["microstrategyColumnGroups"])
    assert groups["STORE"]["dataset"] == "STORE SALES WTD"
    assert groups["TOTAL SALES"]["dataset"] == "COMBINED TOTALS WTD"
    assert "Pct To Plan" in groups["WEB"]["metrics"]


def test_metric_formula_lineage_is_opt_in_and_emits_sibling_edges() -> None:
    def build_dashboard() -> DashboardDefinition:
        dashboard = DashboardDefinition.from_api_response(
            object_id="dash-1",
            object_name="Growth",
            response={
                "definition": {
                    "datasets": [
                        {
                            "id": "ds-1",
                            "name": "Growth Cube",
                            "availableObjects": {
                                "metrics": [
                                    {"id": "M1", "name": "Net Amount"},
                                    {"id": "M2", "name": "Net Amount LY"},
                                    {"id": "M3", "name": "Growth Pct"},
                                ],
                                "attributes": [],
                            },
                        }
                    ],
                }
            },
        )
        dashboard.datasets[0].metric_enrichments["M3"] = MetricEnrichment(
            expression_text=(
                "(({Net Amount} - {Net Amount LY}) / Abs({Net Amount LY})) "
                "* 100 + {Net Amount PY}"
            )
        )
        return dashboard

    disabled_mapper = _mapper()
    dashboard = build_dashboard()
    workunits = list(
        disabled_mapper.gen_dataset_workunits(
            "project-1",
            dashboard,
            dashboard.datasets[0],
            disabled_mapper.project_key("project-1"),
        )
    )
    assert _maybe_aspect(workunits, UpstreamLineageClass) is None

    enabled_mapper = _mapper(extract_metric_formula_lineage=True)
    dashboard = build_dashboard()
    workunits = list(
        enabled_mapper.gen_dataset_workunits(
            "project-1",
            dashboard,
            dashboard.datasets[0],
            enabled_mapper.project_key("project-1"),
        )
    )
    upstream_lineage = _aspect(workunits, UpstreamLineageClass)
    assert upstream_lineage.upstreams == []
    fine_grained = upstream_lineage.fineGrainedLineages or []
    assert len(fine_grained) == 1
    assert fine_grained[0].downstreams is not None
    assert fine_grained[0].downstreams[0].endswith("Growth Pct)")
    assert fine_grained[0].upstreams is not None
    assert {urn.split(",")[-1] for urn in fine_grained[0].upstreams} == {
        "Net Amount)",
        "Net Amount LY)",
    }
    # {Net Amount PY} resolves to no sibling field and is counted, not guessed.
    assert enabled_mapper.report.metric_formula_refs_unresolved == 1
    assert enabled_mapper.report.metric_formula_lineage_edges == 1


def _three_family_dashboard(
    visualizations: "list[dict]",  # raw viz dicts placed on one page
) -> DashboardDefinition:
    def dataset(dataset_id: str, name: str, plan_metric_id: str) -> dict:
        return {
            "id": dataset_id,
            "name": name,
            "availableObjects": {
                "metrics": [
                    {"id": "M100", "name": "Net Amount"},
                    {"id": plan_metric_id, "name": f"{name} Plan"},
                ],
                "attributes": [],
            },
        }

    return DashboardDefinition.from_api_response(
        object_id="dash-fam",
        object_name="Families",
        response={
            "definition": {
                "datasets": [
                    dataset("ds-alpha", "ALPHA SALES", "M201"),
                    dataset("ds-beta", "BETA SALES", "M202"),
                    dataset("ds-gamma", "GAMMA SALES", "M203"),
                ],
                "chapters": [
                    {
                        "key": "ch-1",
                        "name": "MAIN",
                        "pages": [
                            {
                                "key": "pg-1",
                                "name": "SUMMARY",
                                "visualizations": visualizations,
                            }
                        ],
                    }
                ],
            }
        },
    )


def _grid_viz(key: str, name: str, column_sets: "list[dict]") -> dict:
    return {
        "key": key,
        "name": name,
        "visualizationType": "compound_grid",
        "runtimeDefinition": {"definition": {"grid": {"columnSets": column_sets}}},
    }


def _metrics_column(elements: "list[dict]") -> "list[dict]":
    return [{"type": "templateMetrics", "elements": elements}]


def test_derived_metric_falls_back_to_the_visualizations_only_bound_dataset() -> None:
    # "ALPHA" binds by name; "ZZZ" matches nothing and elimination cannot fire
    # (3 page datasets vs 2 groups) — its derived metric attaches through the
    # visualization's single bound dataset instead of being dropped.
    mapper = _mapper()
    dashboard = _three_family_dashboard(
        [
            _grid_viz(
                "viz-1",
                "Family Overview",
                [
                    {
                        "key": "cs-alpha",
                        "name": "ALPHA",
                        "columns": _metrics_column(
                            [
                                {"type": "metric", "id": "M201", "name": "ALPHA Plan"},
                            ]
                        ),
                    },
                    {
                        "key": "cs-zzz",
                        "name": "ZZZ",
                        "columns": _metrics_column(
                            [
                                {
                                    "type": "metric",
                                    "id": "D900",
                                    "name": "Mystery Pct",
                                    "derived": True,
                                },
                            ]
                        ),
                    },
                ],
            )
        ]
    )
    mapper.attach_derived_metrics(dashboard)

    alpha = next(ds for ds in dashboard.datasets if ds.id == "ds-alpha")
    assert list(alpha.derived_metrics) == ["D900"]
    assert mapper.report.derived_metrics_unattached == 0


def test_derived_metric_without_any_binding_is_counted_not_dropped() -> None:
    mapper = _mapper()
    dashboard = _three_family_dashboard(
        [
            _grid_viz(
                "viz-1",
                "Family Overview",
                [
                    {
                        "key": "cs-zzz",
                        "name": "ZZZ",
                        "columns": _metrics_column(
                            [
                                {
                                    "type": "metric",
                                    "id": "D900",
                                    "name": "Mystery Pct",
                                    "derived": True,
                                },
                            ]
                        ),
                    },
                ],
            )
        ]
    )
    mapper.attach_derived_metrics(dashboard)

    assert all(not ds.derived_metrics for ds in dashboard.datasets)
    assert mapper.report.derived_metrics_unattached == 1


def test_derived_metric_without_id_is_counted_not_dropped() -> None:
    mapper = _mapper()
    dashboard = _three_family_dashboard(
        [
            _grid_viz(
                "viz-1",
                "Family Overview",
                [
                    {
                        "key": "cs-alpha",
                        "name": "ALPHA",
                        "columns": _metrics_column(
                            [
                                {
                                    "type": "metric",
                                    "name": "No Id Pct",
                                    "derived": True,
                                },
                            ]
                        ),
                    },
                ],
            )
        ]
    )
    mapper.attach_derived_metrics(dashboard)

    assert all(not ds.derived_metrics for ds in dashboard.datasets)
    assert mapper.report.derived_metrics_unattached == 1


def test_duplicate_derived_id_across_visualizations_keeps_first_provenance() -> None:
    column_sets = [
        {
            "key": "cs-alpha",
            "name": "ALPHA",
            "columns": _metrics_column(
                [
                    {"type": "metric", "id": "M201", "name": "ALPHA Plan"},
                    {
                        "type": "metric",
                        "id": "D900",
                        "name": "Alpha Pct",
                        "derived": True,
                    },
                ]
            ),
        }
    ]
    mapper = _mapper()
    dashboard = _three_family_dashboard(
        [
            _grid_viz("viz-1", "First Viz", column_sets),
            _grid_viz("viz-2", "Second Viz", column_sets),
        ]
    )
    mapper.attach_derived_metrics(dashboard)

    alpha = next(ds for ds in dashboard.datasets if ds.id == "ds-alpha")
    assert list(alpha.derived_metrics) == ["D900"]
    assert alpha.derived_metrics["D900"].source_visualization_name == "First Viz"


def test_static_only_column_sets_bind_by_name_without_derived_fields() -> None:
    mapper = _mapper()
    dashboard = _three_family_dashboard(
        [
            {
                "key": "viz-1",
                "name": "Family Overview",
                "visualizationType": "compound_grid",
                "columnSets": [{"key": "cs-alpha", "name": "ALPHA"}],
            }
        ]
    )
    mapper.attach_derived_metrics(dashboard)
    assert all(not ds.derived_metrics for ds in dashboard.datasets)

    inputs = mapper.lineage.visualization_inputs(
        "project-1", dashboard, dashboard.visualizations[0]
    )
    assert len(inputs) == 1
    assert "ds-alpha" in inputs[0]


def test_gen_folder_containers_hides_system_folders() -> None:
    # The project root folder and "Public Objects" never appear in Strategy
    # Web; children re-parent to the nearest kept ancestor.
    mapper = _mapper()
    dashboard_object = MicroStrategyObject.model_validate(
        {
            "id": "dash-1",
            "name": "Sales Dashboard",
            "ancestors": [
                {"id": "project-root-id", "name": "Analytics"},
                {"id": "public-objects-id", "name": "Public Objects"},
                {"id": "reports-folder-id", "name": "Reports"},
                {"id": "finance-folder-id", "name": "Finance"},
            ],
        }
    )
    resolution = PredefinedFolderResolution(
        labels={"REPORTS-FOLDER-ID": "Shared Reports"},
        hidden_ids={"PROJECT-ROOT-ID", "PUBLIC-OBJECTS-ID"},
    )

    workunits = list(
        mapper.gen_folder_containers(
            "project-1", dashboard_object, predefined_folders=resolution
        )
    )
    names = set(_container_names(workunits).values())
    assert names == {"Shared Reports", "Finance"}

    parent_key = mapper.folder_container_for_dashboard(
        "project-1", dashboard_object, predefined_folders=resolution
    )
    assert parent_key == mapper.folder_key("project-1", "Shared Reports/Finance")
