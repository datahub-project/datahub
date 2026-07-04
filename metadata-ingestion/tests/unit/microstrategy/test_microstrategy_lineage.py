from typing import Any, Dict, List
from unittest import mock

from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.lineage import (
    MicroStrategyLineageExtractor,
    WarehouseLineageContext,
    bind_visualizations_by_derived_objects,
    datahub_platform_for_source_type,
    extract_field_names_from_expression,
    metric_fact_ids_from_model,
    metric_metric_ids_from_model,
    qualify_table_name,
    unique_derived_object_owners,
    warehouse_context_from_datasource,
    warehouse_context_from_datasources,
)
from datahub.ingestion.source.microstrategy.models import (
    DashboardDefinition,
    Datasource,
    DatasourceReference,
    Visualization,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport


def _extractor() -> MicroStrategyLineageExtractor:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )
    return MicroStrategyLineageExtractor(config, MicroStrategyReport())


def test_maps_microstrategy_source_type_to_datahub_platform() -> None:
    assert datahub_platform_for_source_type("snow_flake") == "snowflake"
    assert datahub_platform_for_source_type("SQL Server") == "mssql"
    assert datahub_platform_for_source_type("postgre_sql") == "postgres"


def test_warehouse_context_from_datasource_reference() -> None:
    datasource = DatasourceReference.model_validate(
        {
            "id": "source-1",
            "name": "Sales Warehouse",
            "database": {
                "type": "snow_flake",
                "name": "SALES_DB",
                "schema": "SALES",
            },
        }
    )

    assert warehouse_context_from_datasource(datasource, "PROD") == (
        WarehouseLineageContext(
            platform="snowflake",
            env="PROD",
            database="SALES_DB",
            schema="SALES",
        )
    )


def test_warehouse_context_from_datasource_applies_platform_instance_map() -> None:
    datasource = DatasourceReference.model_validate(
        {
            "id": "source-1",
            "name": "Sales Warehouse",
            "database": {
                "type": "snow_flake",
                "name": "SALES_DB",
                "schema": "SALES",
            },
        }
    )

    context = warehouse_context_from_datasource(
        datasource,
        "PROD",
        platform_instance_map={"snowflake": "prod_wh"},
    )

    assert context is not None
    assert context.platform_instance == "prod_wh"


def test_warehouse_context_from_datasources_requires_unique_context() -> None:
    datasources = [
        Datasource.model_validate(
            {
                "id": "source-1",
                "name": "Sales Warehouse",
                "database": {
                    "type": "snow_flake",
                    "name": "SALES_DB",
                    "schema": "SALES",
                },
            }
        ),
        Datasource.model_validate(
            {
                "id": "source-2",
                "name": "Orders Warehouse",
                "database": {
                    "type": "snow_flake",
                    "name": "ORDERS_DB",
                    "schema": "ORDERS",
                },
            }
        ),
    ]

    assert warehouse_context_from_datasources(datasources, "PROD") is None


def test_qualify_table_name_uses_connection_database_and_schema() -> None:
    assert (
        qualify_table_name("fact_sales", database="SALES_DB", schema="SALES")
        == "SALES_DB.SALES.fact_sales"
    )
    assert (
        qualify_table_name("ORDERS.fact_orders", database="SALES_DB", schema="SALES")
        == "SALES_DB.ORDERS.fact_orders"
    )
    assert (
        qualify_table_name(
            "SALES_DB.SALES.fact_sales", database="OTHER_DB", schema="ORDERS"
        )
        == "SALES_DB.SALES.fact_sales"
    )


def test_warehouse_upstream_urns_from_sql() -> None:
    extractor = _extractor()
    context = WarehouseLineageContext(
        platform="snowflake",
        env="PROD",
        database="SALES_DB",
        schema="SALES",
    )

    upstreams = extractor.warehouse_upstream_urns_from_sql(
        "select * from fact_sales join ORDERS.fact_orders on 1 = 1",
        context,
    )

    assert upstreams == [
        "urn:li:dataset:"
        "(urn:li:dataPlatform:snowflake,sales_db.orders.fact_orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)",
    ]


def test_warehouse_upstream_urns_from_sql_reports_parse_failures() -> None:
    extractor = _extractor()
    context = WarehouseLineageContext(
        platform="snowflake",
        env="PROD",
        database="SALES_DB",
        schema="SALES",
    )

    with mock.patch(
        "datahub.sql_parsing.sqlglot_lineage.create_lineage_sql_parsed_result",
        side_effect=RuntimeError("parser exploded"),
    ):
        upstreams = extractor.warehouse_upstream_urns_from_sql(
            "select * from fact_sales",
            context,
        )

    assert upstreams == []
    assert len(extractor.report.sql_parse_failures) == 1
    assert len(extractor.report.warnings) == 1
    # Warning context must stay small — never the full SQL script.
    warning_contexts = "".join(
        "".join(warning.context or []) for warning in extractor.report.warnings
    )
    assert len(warning_contexts) < 500


def test_warehouse_upstream_urns_from_multipass_sql_excludes_intermediates() -> None:
    extractor = _extractor()
    context = WarehouseLineageContext(
        platform="snowflake",
        env="PROD",
        database="RETAIL_DB",
        schema="RETAIL",
    )

    # MicroStrategy multi-pass SQL: statements concatenated with blank lines,
    # no semicolons; later passes read the volatile tables created earlier.
    multipass_sql = (
        "CREATE VOLATILE  TABLE T08Q9Z6Q3SP000 AS \n"
        'select a11.REGION_NUMBER from "XRBIA_DM"."W_RTL_SLS_IT_LC_DY_A" a11\n'
        "group by a11.REGION_NUMBER\n"
        "\n"
        "CREATE VOLATILE  TABLE TAWFLXJ4PSP001 AS \n"
        "select * from T08Q9Z6Q3SP000\n"
        "\n"
        "select b.REGION_NUMBER from TAWFLXJ4PSP001 b\n"
        '  join "XRBIA_DM"."DIM_ITEM_CV" c on b.k = c.k'
    )

    upstreams = extractor.warehouse_upstream_urns_from_sql(multipass_sql, context)

    assert upstreams == [
        "urn:li:dataset:"
        "(urn:li:dataPlatform:snowflake,retail_db.xrbia_dm.dim_item_cv,PROD)",
        "urn:li:dataset:"
        "(urn:li:dataPlatform:snowflake,retail_db.xrbia_dm.w_rtl_sls_it_lc_dy_a,PROD)",
    ]
    assert not extractor.report.warnings


def test_warehouse_upstream_urns_skips_non_lineage_trailers() -> None:
    extractor = _extractor()
    context = WarehouseLineageContext(
        platform="snowflake",
        env="PROD",
        database="SALES_DB",
        schema="SALES",
    )

    # Real MicroStrategy SQL views end with DROPs and non-SQL commentary.
    sql = (
        "select * from fact_sales\n"
        "\n"
        "drop table TN2619RILMD004\n"
        "\n"
        "[Analytical engine calculation steps:\n"
        "\t1.  Calculate WJXBFS1]\n"
        "\n"
        "with parameters:\n\t1"
    )

    upstreams = extractor.warehouse_upstream_urns_from_sql(sql, context)

    assert upstreams == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)"
    ]
    assert len(extractor.report.sql_parse_failures) == 0
    assert not extractor.report.warnings


def test_warehouse_upstream_urns_drops_only_script_stays_silent() -> None:
    extractor = _extractor()
    context = WarehouseLineageContext(
        platform="snowflake",
        env="PROD",
        database="SALES_DB",
        schema="SALES",
    )

    sql = "drop table T1SP000\n\ndrop table T2SP001\n\n[Analytical engine steps]"

    upstreams = extractor.warehouse_upstream_urns_from_sql(sql, context)

    assert upstreams == []
    assert len(extractor.report.sql_parse_failures) == 0
    assert not extractor.report.warnings


def test_warehouse_upstream_urns_keeps_cte_named_parameters() -> None:
    extractor = _extractor()
    context = WarehouseLineageContext(
        platform="snowflake",
        env="PROD",
        database="SALES_DB",
        schema="SALES",
    )

    # A legitimate CTE named "parameters" must not match the trailer skip
    # (the real MicroStrategy trailer is "with parameters:").
    sql = "with parameters as (select * from fact_sales) select * from parameters"

    upstreams = extractor.warehouse_upstream_urns_from_sql(sql, context)

    assert upstreams == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)"
    ]


def test_warehouse_upstream_urns_keeps_self_referencing_write_reads() -> None:
    extractor = _extractor()
    context = WarehouseLineageContext(
        platform="snowflake",
        env="PROD",
        database="SALES_DB",
        schema="SALES",
    )

    # Only CREATE targets are subtracted: a persistent table written by an
    # INSERT in one pass and read by a later pass remains a genuine upstream
    # (unlike volatile CTAS intermediates).
    sql = (
        "insert into fact_rollup select * from fact_sales\n"
        "\n"
        "select * from fact_rollup join dim_org on 1=1"
    )

    upstreams = extractor.warehouse_upstream_urns_from_sql(sql, context)

    assert upstreams == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.dim_org,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_rollup,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)",
    ]


def test_bind_visualizations_ignores_shared_catalog_ids() -> None:
    shared_id = "C" * 32
    derived_id = "A" * 32
    dashboard = DashboardDefinition.model_validate(
        {
            "id": "dash-1",
            "name": "Dash",
            "datasets": [
                {
                    "id": "ds-a",
                    "name": "A",
                    "availableObjects": {"metrics": [{"id": shared_id}]},
                },
                {
                    "id": "ds-b",
                    "name": "B",
                    "availableObjects": {"metrics": [{"id": shared_id}]},
                },
            ],
            "visualizations": [
                # References only the shared-catalog object: must stay unbound
                # even though ds-a's derived expressions also reference it.
                {
                    "key": "viz-shared",
                    "name": "Shared Only",
                    "metrics": [{"id": shared_id, "type": "metric"}],
                },
                {
                    "key": "viz-derived",
                    "name": "Derived",
                    "metrics": [{"id": derived_id, "type": "metric"}],
                },
            ],
        }
    )
    owner_by_derived_id = {shared_id: "ds-a", derived_id: "ds-a"}

    bound = bind_visualizations_by_derived_objects(dashboard, owner_by_derived_id)

    assert bound == 1
    assert dashboard.visualizations[0].datasets == []
    assert dashboard.visualizations[1].datasets == ["ds-a"]


def test_warehouse_upstream_urns_partial_parse_failure_keeps_good_statements() -> None:
    extractor = _extractor()
    context = WarehouseLineageContext(
        platform="snowflake",
        env="PROD",
        database="SALES_DB",
        schema="SALES",
    )

    sql = "SET ANSI_NULLS ON\n\nselect * from fact_sales"

    upstreams = extractor.warehouse_upstream_urns_from_sql(sql, context)

    assert upstreams == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)"
    ]
    # The unsupported SET statement is counted but must not raise the loud
    # warning because lineage was still extracted.
    assert len(extractor.report.sql_parse_failures) == 1
    assert not extractor.report.warnings


def _inference_dashboard(dataset_names: List[str]) -> "DashboardDefinition":
    # All datasets share one object catalog (metric-1), mirroring dashboards
    # built from one cube per time period.
    return DashboardDefinition.model_validate(
        {
            "id": "dash-1",
            "name": "Salon Sales To Plan",
            "datasets": [
                {
                    "id": f"ds-{index}",
                    "name": name,
                    "availableObjects": {"metrics": [{"id": "metric-1"}]},
                }
                for index, name in enumerate(dataset_names)
            ],
        }
    )


def _grid_visualization(name: str) -> "Visualization":
    return Visualization.model_validate(
        {
            "key": "viz-1",
            "name": name,
            "visualizationType": "grid",
            "metrics": [{"id": "metric-1", "type": "metric"}],
        }
    )


def test_inference_matching_all_datasets_is_treated_as_unresolved() -> None:
    extractor = _extractor()
    dashboard = _inference_dashboard(
        [
            "Retail Sales Yesterday",
            "Service Sales Yesterday",
            "Total Sales Yesterday",
        ]
    )
    visualization = _grid_visualization("Salon Sales Yesterday")

    inputs = extractor.visualization_inputs("project-1", dashboard, visualization)

    # All three datasets share objects and name tokens with the visualization,
    # so the inference has no discriminating signal — no edges, counted as
    # unresolved and attributed to the ambiguity guard.
    assert inputs == []
    assert extractor.report.unresolved_visualizations == 1
    assert extractor.report.visualizations_suppressed_ambiguous == 1


def test_inference_matching_both_datasets_of_a_pair_is_kept() -> None:
    extractor = _extractor()
    dashboard = _inference_dashboard(
        ["Retail Sales Yesterday", "Service Sales Yesterday"]
    )
    visualization = _grid_visualization("Salon Sales Yesterday")

    inputs = extractor.visualization_inputs("project-1", dashboard, visualization)

    # Two-dataset dashboards are exempt from the ambiguity guard: a
    # visualization combining both datasets is plausible.
    assert len(inputs) == 2
    assert extractor.report.unresolved_visualizations == 0


def test_inference_matching_a_strict_subset_is_kept() -> None:
    extractor = _extractor()
    dashboard = _inference_dashboard(["Retail Sales Yesterday", "Inventory Snapshot"])
    visualization = _grid_visualization("Retail Sales Yesterday Trend")

    inputs = extractor.visualization_inputs("project-1", dashboard, visualization)

    assert inputs == [
        extractor.dataset_urn("project-1", "dash-1", dashboard.datasets[0])
    ]
    assert extractor.report.unresolved_visualizations == 0


def test_unique_derived_object_owners_maps_ids_and_drops_shared() -> None:
    model_document: Dict[str, Any] = {
        "datasets": [
            {
                "information": {"objectId": "DS-A", "name": "Retail WTD"},
                "derivedMetrics": [
                    {
                        "information": {"objectId": "A" * 32},
                        "embeddedObjects": [{"id": "B" * 32}],
                    }
                ],
                "derivedAttributes": [
                    {"information": {"objectId": "C" * 32}},
                    # Defined in both datasets -> no signal.
                    {"information": {"objectId": "D" * 32}},
                ],
            },
            {
                "information": {"objectId": "DS-B", "name": "Service WTD"},
                "derivedAttributes": [
                    {"information": {"objectId": "D" * 32}},
                    {
                        "information": {"objectId": "E" * 32},
                        # Expression-referenced shared-catalog object: must NOT
                        # be attributed to DS-B.
                        "forms": [
                            {
                                "expressions": [
                                    {
                                        "expression": {
                                            "tree": {"target": {"objectId": "F" * 32}}
                                        }
                                    }
                                ]
                            }
                        ],
                    },
                ],
            },
        ]
    }

    owners = unique_derived_object_owners(model_document)

    assert owners == {
        "A" * 32: "DS-A",
        "B" * 32: "DS-A",
        "C" * 32: "DS-A",
        "E" * 32: "DS-B",
    }
    assert "F" * 32 not in owners


def test_extract_field_names_from_model_expression() -> None:
    assert extract_field_names_from_expression("net_sales_amt") == ["net_sales_amt"]
    assert extract_field_names_from_expression(
        "(net_sales_amt + tax_amt) - discount_amt"
    ) == ["discount_amt", "net_sales_amt", "tax_amt"]


def test_extract_field_names_excludes_function_names() -> None:
    assert extract_field_names_from_expression("SUM(QTY_SOLD * UNIT_PRICE)") == [
        "QTY_SOLD",
        "UNIT_PRICE",
    ]


def test_metric_fact_ids_from_model_accepts_nested_and_top_level_tokens() -> None:
    nested_model: Dict[str, Any] = {
        "expression": {
            "tokens": [{"target": {"objectId": "fact-1", "subType": "fact"}}]
        }
    }
    top_level_model: Dict[str, Any] = {
        "expression": {"tokens": [{"objectId": "fact-1", "subType": "fact"}]}
    }

    assert metric_fact_ids_from_model(nested_model) == ["FACT-1"]
    assert metric_fact_ids_from_model(top_level_model) == ["FACT-1"]


def test_metric_metric_ids_from_model_accepts_nested_and_top_level_tokens() -> None:
    nested_model: Dict[str, Any] = {
        "expression": {
            "tokens": [{"target": {"objectId": "metric-2", "subType": "metric"}}]
        }
    }
    top_level_model: Dict[str, Any] = {
        "expression": {"tokens": [{"objectId": "metric-2", "subType": "metric"}]}
    }

    assert metric_metric_ids_from_model(nested_model) == ["METRIC-2"]
    assert metric_metric_ids_from_model(top_level_model) == ["METRIC-2"]


def test_physical_table_uses_context_database_over_mstr_namespace() -> None:
    extractor = _extractor()
    # The JDBC connection provides the real warehouse database; MicroStrategy's
    # table "namespace" is a logical namespace (dedup-suffixed) and must lose.
    context = WarehouseLineageContext(
        platform="snowflake",
        env="PROD",
        database="P_MER_EDW_DB",
        schema="XRBIA_DM",
    )

    index = extractor.model_lineage_index_from_tables(
        [
            {
                "physicalTable": {
                    "namespace": "XRBIA_DM_1",
                    "tablePrefix": "XRBIA_DM.",
                    "tableName": "W_RTL_SLS_IT_LC_DY_A",
                },
                "facts": [
                    {
                        "information": {"objectId": "fact-1"},
                        # Real modeling responses carry the warehouse's
                        # canonical upper case.
                        "expression": {"text": "NET_SLS_QTY"},
                    }
                ],
                "attributes": [],
            }
        ],
        context,
    )

    # Field urns must be lowercased to anchor on the warehouse connector's
    # lowercase schema fieldPaths; schemaField urns match case-sensitively.
    upstreams = index.fact_field_urns(["fact-1"])
    assert upstreams == [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,"
        "p_mer_edw_db.xrbia_dm.w_rtl_sls_it_lc_dy_a,PROD),net_sls_qty)"
    ]


def test_convert_urns_to_lowercase_false_preserves_warehouse_case() -> None:
    config = MicroStrategyConfig.model_validate(
        {
            "base_url": "https://mstr.example.com/MicroStrategyLibrary",
            "convert_urns_to_lowercase": False,
        }
    )
    extractor = MicroStrategyLineageExtractor(config, MicroStrategyReport())
    context = WarehouseLineageContext(
        platform="snowflake",
        env="PROD",
        database="P_MER_EDW_DB",
        schema="XRBIA_DM",
    )

    index = extractor.model_lineage_index_from_tables(
        [
            {
                "physicalTable": {
                    "namespace": "XRBIA_DM_1",
                    "tablePrefix": "XRBIA_DM.",
                    "tableName": "W_RTL_SLS_IT_LC_DY_A",
                },
                "facts": [
                    {
                        "information": {"objectId": "fact-1"},
                        "expression": {"text": "NET_SLS_QTY"},
                    }
                ],
                "attributes": [],
            }
        ],
        context,
    )

    assert index.fact_field_urns(["fact-1"]) == [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,"
        "P_MER_EDW_DB.XRBIA_DM.W_RTL_SLS_IT_LC_DY_A,PROD),NET_SLS_QTY)"
    ]


def test_model_lineage_index_maps_facts_and_attribute_forms_to_fields() -> None:
    extractor = _extractor()
    context = WarehouseLineageContext(
        platform="snowflake",
        env="PROD",
        database="SALES_DB",
        schema="SALES",
    )

    index = extractor.model_lineage_index_from_tables(
        [
            {
                "physicalTable": {
                    "namespace": "SALES_DB",
                    "tablePrefix": "ORDERS.",
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
        context,
    )

    upstream_dataset_urn = (
        "urn:li:dataset:"
        "(urn:li:dataPlatform:snowflake,sales_db.orders.fact_orders,PROD)"
    )
    assert index.fact_field_urns(["fact-1"]) == [
        f"urn:li:schemaField:({upstream_dataset_urn},net_sales_amt)"
    ]
    assert index.attribute_field_urns("attr-1", "ID") == [
        f"urn:li:schemaField:({upstream_dataset_urn},order_date_id)"
    ]
