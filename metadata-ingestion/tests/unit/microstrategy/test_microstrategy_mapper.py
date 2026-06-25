from typing import Iterable, Type, TypeVar

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import (
    DIMENSION_TAG_URN,
    MEASURE_TAG_URN,
    TEMPORAL_TAG_URN,
)
from datahub.ingestion.source.microstrategy.mapper import MicroStrategyMapper
from datahub.ingestion.source.microstrategy.models import (
    DashboardDefinition,
    MSTRObject,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    DashboardInfoClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
    UpstreamLineageClass,
)

T = TypeVar("T")


def _aspect(workunits: Iterable[MetadataWorkUnit], aspect_type: Type[T]) -> T:
    for workunit in workunits:
        aspect = workunit.get_aspect_of_type(aspect_type)
        if aspect:
            return aspect
    raise AssertionError(f"Aspect {aspect_type} not found")


def _tag_urns(field) -> set[str]:
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
) -> MicroStrategyMapper:
    config = MicroStrategyConfig.model_validate(
        {
            "base_url": "https://mstr.example.com/MicroStrategyLibrary",
            "platform_instance": "prod",
            "emit_dashboard_dataset_edges": emit_dashboard_dataset_edges,
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
    assert '"microstrategyObjectType": "metric"' in fields["Revenue"].jsonProps
    assert '"microstrategyObjectType": "attribute"' in fields["Order Date"].jsonProps


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
    dashboard_object = MSTRObject.model_validate(
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


def test_dashboard_dataset_edges_are_opt_in() -> None:
    mapper = _mapper(emit_dashboard_dataset_edges=True)
    dashboard = _definition()
    dashboard_object = MSTRObject.model_validate(
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
    dashboard_object = MSTRObject.model_validate(
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
    assert dataset_properties.customProperties["microstrategySourceType"] == "snow_flake"
    assert dataset_properties.customProperties["microstrategyDbmsName"] == "Snowflake"


def test_dataset_workunits_include_warehouse_upstream_lineage() -> None:
    mapper = _mapper()
    dashboard = _definition()
    dashboard.datasets[0].warehouse_upstream_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,SALES_DB.SALES.fact_sales,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,SALES_DB.ORDERS.fact_orders,PROD)",
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
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,SALES_DB.ORDERS.fact_orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,SALES_DB.SALES.fact_sales,PROD)",
    ]
    assert (
        dataset_properties.customProperties["microstrategyWarehouseUpstreamCount"]
        == "2"
    )
    assert (
        dataset_properties.customProperties["microstrategyWarehouseUpstreamPlatforms"]
        == '["snowflake"]'
    )


def test_dashboard_properties_include_direct_dependency_summary() -> None:
    mapper = _mapper()
    dashboard = _definition()
    dashboard.dependencies = [
        MSTRObject.model_validate(
            {"id": "metric-1", "name": "Revenue", "type": 4, "subtype": 1024}
        ),
        MSTRObject.model_validate(
            {"id": "attr-1", "name": "Order Date", "type": 12, "subtype": 3072}
        ),
    ]
    dashboard_object = MSTRObject.model_validate(
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
    assert '"4": 1' in dashboard_info.customProperties[
        "microstrategyDirectDependencyTypeCounts"
    ]
    assert "metric-1" in dashboard_info.customProperties[
        "microstrategyDirectDependencies"
    ]


def test_metric_expression_is_preserved_in_field_json_props() -> None:
    mapper = _mapper()
    dashboard = _definition()
    metric = dashboard.datasets[0].available_objects["metrics"][0]
    metric["modelExpression"] = {
        "text": "Sum(Revenue)",
        "tokens": '[{"id": "fact-1", "name": "Revenue Fact", "type": "fact"}]',
    }

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

    assert "microstrategyMetricExpressionText" in revenue.jsonProps
    assert "Revenue Fact" in revenue.jsonProps
