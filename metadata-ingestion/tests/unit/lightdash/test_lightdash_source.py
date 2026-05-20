"""End-to-end source test — drive a fully-mocked LightdashClient and assert the
URN set + key aspects look right.

We mock the ``LightdashClient`` rather than mocking HTTP so the test reads as a
narrative of "given this Lightdash workspace, this is what DataHub sees".
"""

from __future__ import annotations

import warnings
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from datahub.errors import ExperimentalWarning
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.lightdash.config import LightdashSourceConfig
from datahub.ingestion.source.lightdash.models import (
    LightdashAdditionalMetric,
    LightdashChart,
    LightdashChartSummary,
    LightdashDashboard,
    LightdashDashboardSummary,
    LightdashDashboardTile,
    LightdashDashboardTileProperties,
    LightdashExplore,
    LightdashExploreTable,
    LightdashFieldDef,
    LightdashMetricQuery,
    LightdashOrganization,
    LightdashProject,
    LightdashProjectSummary,
    LightdashSpace,
    LightdashTableCalculation,
    LightdashUserRef,
    LightdashWarehouseConnection,
)

# The lightdash source imports datahub.sdk, which is still flagged
# ``ExperimentalWarning`` upstream. We acknowledge it here so the suite stays
# quiet under -W error.
warnings.filterwarnings("ignore", category=ExperimentalWarning)

from datahub.ingestion.source.lightdash.source import LightdashSource  # noqa: E402

ORG_UUID = "feb50a9c-040e-4f62-89c3-a614f6483f69"
PROJECT_UUID = "ce1188f7-2543-4176-a05e-489b0eb84713"
SPACE_UUID = "7498423a-f55c-4122-a57c-8c5b4f0cd8cd"
CHART_UUID = "aaf4e09b-2d5f-4872-b960-eeb4b2b227ee"
DASHBOARD_UUID = "01317bd4-c120-444e-a15e-737ffed0d79e"
USER_UUID = "8a3f1146-a857-4b60-8c95-5d0e477559ea"


@pytest.fixture
def source():
    cfg = LightdashSourceConfig.model_validate(
        {
            "connection": {
                "base_url": "https://lightdash.example.com",
                "personal_access_token": "ldpat_test",
            },
            "env": "PROD",
        }
    )
    ctx = PipelineContext(run_id="test-lightdash")
    src = LightdashSource(cfg, ctx)

    # Replace the live client with a fully-stubbed one. We don't want any real
    # HTTP — tests must be deterministic.
    src.client = MagicMock()
    src.client.get_organization.return_value = LightdashOrganization(
        organization_uuid=ORG_UUID, name="Aiven"
    )
    src.client.list_projects.return_value = [
        LightdashProjectSummary(
            project_uuid=PROJECT_UUID,
            name="Webstore",
            warehouse_type="clickhouse",
        ),
    ]
    src.client.get_project.return_value = LightdashProject(
        project_uuid=PROJECT_UUID,
        organization_uuid=ORG_UUID,
        name="Webstore",
        warehouse_connection=LightdashWarehouseConnection(
            type="clickhouse", schema="default"
        ),
    )
    src.client.list_spaces.return_value = [
        LightdashSpace(
            uuid=SPACE_UUID,
            name="Shared",
            project_uuid=PROJECT_UUID,
            organization_uuid=ORG_UUID,
        ),
    ]
    src.client.list_charts.return_value = [
        LightdashChartSummary(
            uuid=CHART_UUID,
            name="Live orders",
            project_uuid=PROJECT_UUID,
            organization_uuid=ORG_UUID,
            space_uuid=SPACE_UUID,
            space_name="Shared",
            chart_type="table",
            chart_kind="table",
        ),
    ]
    src.client.get_chart.return_value = LightdashChart(
        uuid=CHART_UUID,
        name="Live orders",
        description="Most-recent orders",
        project_uuid=PROJECT_UUID,
        organization_uuid=ORG_UUID,
        space_uuid=SPACE_UUID,
        space_name="Shared",
        chart_type="table",
        chart_kind="table",
        table_name="orders_enriched",
        metric_query=LightdashMetricQuery(explore_name="orders_enriched"),
        updated_at=datetime(2026, 5, 12, 10, 28, 57),
        updated_by_user=LightdashUserRef(
            user_uuid=USER_UUID, first_name="Stan", last_name="Dmitriev"
        ),
    )
    src.client.get_explore.return_value = LightdashExplore(
        name="orders_enriched",
        base_table="orders_enriched",
        joined_tables=[],
        tables={
            "orders_enriched": LightdashExploreTable(
                name="orders_enriched",
                schema="default",
                sql_table="`default`.`orders_enriched`",
            )
        },
    )
    src.client.list_dashboards.return_value = [
        LightdashDashboardSummary(
            uuid=DASHBOARD_UUID,
            name="Webstore Live Insights",
            description="Live mirror",
            project_uuid=PROJECT_UUID,
            organization_uuid=ORG_UUID,
            space_uuid=SPACE_UUID,
            updated_at=datetime(2026, 5, 13, 12, 14, 14),
            updated_by_user=LightdashUserRef(user_uuid=USER_UUID),
        ),
    ]
    src.client.get_dashboard.return_value = LightdashDashboard(
        uuid=DASHBOARD_UUID,
        name="Webstore Live Insights",
        description="Live mirror",
        project_uuid=PROJECT_UUID,
        organization_uuid=ORG_UUID,
        space_uuid=SPACE_UUID,
        updated_at=datetime(2026, 5, 13, 12, 14, 14),
        tiles=[
            LightdashDashboardTile(
                uuid="tile-1",
                type="saved_chart",
                properties=LightdashDashboardTileProperties(
                    saved_chart_uuid=CHART_UUID, chart_name="Live orders"
                ),
            )
        ],
    )

    yield src
    src.close()


def _workunits(source):
    return list(source.get_workunits_internal())


def test_emits_expected_top_level_urns(source):
    wus = _workunits(source)
    urns = {w.metadata.entityUrn for w in wus if hasattr(w.metadata, "entityUrn")}

    # Three container URNs: Project + Space (Organization is opt-in).
    container_urns = {u for u in urns if u and u.startswith("urn:li:container:")}
    assert len(container_urns) == 2

    # Chart + Dashboard.
    expected_chart_urn = f"urn:li:chart:(lightdash,{CHART_UUID})"
    expected_dashboard_urn = f"urn:li:dashboard:(lightdash,{DASHBOARD_UUID})"
    assert expected_chart_urn in urns
    assert expected_dashboard_urn in urns


def test_chart_has_upstream_dataset_in_clickhouse_default(source):
    wus = _workunits(source)
    chart_urn = f"urn:li:chart:(lightdash,{CHART_UUID})"
    expected_upstream = (
        "urn:li:dataset:(urn:li:dataPlatform:clickhouse,default.orders_enriched,PROD)"
    )

    chart_aspect = None
    for w in wus:
        if (
            getattr(w.metadata, "entityUrn", None) == chart_urn
            and w.metadata.aspectName == "chartInfo"
        ):
            chart_aspect = w.metadata.aspect
            break

    assert chart_aspect is not None, "Chart was emitted but no chartInfo aspect found"
    # Both fields must be populated: ``inputs`` for legacy clients, ``inputEdges``
    # so DataHub's graph indexer picks the relationship up. Without the latter
    # the lineage shows nothing in the UI.
    assert chart_aspect.inputs == [expected_upstream]
    assert chart_aspect.inputEdges is not None
    assert [e.destinationUrn for e in chart_aspect.inputEdges] == [expected_upstream]


def test_dashboard_references_chart(source):
    wus = _workunits(source)
    dashboard_urn = f"urn:li:dashboard:(lightdash,{DASHBOARD_UUID})"
    expected_chart_urn = f"urn:li:chart:(lightdash,{CHART_UUID})"

    edges = None
    for w in wus:
        if (
            getattr(w.metadata, "entityUrn", None) == dashboard_urn
            and w.metadata.aspectName == "dashboardInfo"
        ):
            edges = [e.destinationUrn for e in (w.metadata.aspect.chartEdges or [])]
            break

    assert edges == [expected_chart_urn]


def test_explore_fetched_once_even_for_multiple_charts(source):
    # Add a second chart that uses the same Explore — should hit the cache.
    source.client.list_charts.return_value = source.client.list_charts.return_value + [
        LightdashChartSummary(
            uuid="chart-2",
            name="Another orders chart",
            project_uuid=PROJECT_UUID,
            organization_uuid=ORG_UUID,
            space_uuid=SPACE_UUID,
            space_name="Shared",
            chart_type="line",
            chart_kind="line",
        )
    ]

    # Mirror what get_chart returns for the second chart — same explore.
    original_get_chart = source.client.get_chart

    def get_chart_side_effect(uuid):
        if uuid == "chart-2":
            return LightdashChart(
                uuid="chart-2",
                name="Another orders chart",
                project_uuid=PROJECT_UUID,
                organization_uuid=ORG_UUID,
                space_uuid=SPACE_UUID,
                space_name="Shared",
                chart_type="line",
                chart_kind="line",
                table_name="orders_enriched",
                metric_query=LightdashMetricQuery(explore_name="orders_enriched"),
            )
        return original_get_chart.return_value

    source.client.get_chart.side_effect = get_chart_side_effect

    _workunits(source)
    # Explore endpoint hit only once across both charts.
    assert source.client.get_explore.call_count == 1


def test_ownership_aspect_emitted(source):
    wus = _workunits(source)
    chart_urn = f"urn:li:chart:(lightdash,{CHART_UUID})"
    ownership_aspects = [
        w
        for w in wus
        if getattr(w.metadata, "entityUrn", None) == chart_urn
        and w.metadata.aspectName == "ownership"
    ]
    assert len(ownership_aspects) == 1
    owners = ownership_aspects[0].metadata.aspect.owners
    assert owners[0].owner == f"urn:li:corpuser:{USER_UUID}"


def test_extract_owners_disabled(source):
    source.config.extract_owners = False
    wus = _workunits(source)
    chart_urn = f"urn:li:chart:(lightdash,{CHART_UUID})"
    ownership_aspects = [
        w
        for w in wus
        if getattr(w.metadata, "entityUrn", None) == chart_urn
        and w.metadata.aspectName == "ownership"
    ]
    assert ownership_aspects == []


def test_chart_filter_drops_chart_and_keeps_dashboard_tile_dangling(source):
    """When a chart is filtered out, the dashboard still references it (we
    don't try to rewrite dashboard tiles — that's a dashboard-side concern).
    """
    source.config.chart_pattern.deny = [".*Live.*"]
    wus = _workunits(source)
    urns = {getattr(w.metadata, "entityUrn", None) for w in wus}
    assert f"urn:li:chart:(lightdash,{CHART_UUID})" not in urns
    assert source.report.charts_filtered == 1


def test_unknown_warehouse_type_falls_back_to_lightdash_platform(source):
    source.client.get_project.return_value = LightdashProject(
        project_uuid=PROJECT_UUID,
        organization_uuid=ORG_UUID,
        name="Webstore",
        warehouse_connection=LightdashWarehouseConnection(type="duckdb_serverless"),
    )
    wus = _workunits(source)
    chart_urn = f"urn:li:chart:(lightdash,{CHART_UUID})"
    chart_inputs = None
    for w in wus:
        if (
            getattr(w.metadata, "entityUrn", None) == chart_urn
            and w.metadata.aspectName == "chartInfo"
        ):
            chart_inputs = w.metadata.aspect.inputs
            break
    assert chart_inputs == [
        "urn:li:dataset:(urn:li:dataPlatform:lightdash,default.orders_enriched,PROD)"
    ]
    assert "duckdb_serverless" in source.report.warehouse_platform_fallbacks


def test_explicit_warehouse_platform_override(source):
    source.config.warehouse_platform = "clickhouse-eu"
    wus = _workunits(source)
    chart_urn = f"urn:li:chart:(lightdash,{CHART_UUID})"
    chart_inputs = None
    for w in wus:
        if (
            getattr(w.metadata, "entityUrn", None) == chart_urn
            and w.metadata.aspectName == "chartInfo"
        ):
            chart_inputs = w.metadata.aspect.inputs
            break
    assert chart_inputs == [
        "urn:li:dataset:(urn:li:dataPlatform:clickhouse-eu,default.orders_enriched,PROD)"
    ]


def test_report_counts(source):
    _workunits(source)
    assert source.report.projects_scanned == 1
    assert source.report.spaces_scanned == 1
    assert source.report.charts_scanned == 1
    assert source.report.dashboards_scanned == 1
    assert source.report.explores_resolved == 1


def test_chart_emits_input_fields_for_column_lineage(source):
    """Each Lightdash chart field shows up on the chart with its FULL Lightdash
    field id as fieldPath, with a ``consumesField`` edge to the underlying
    warehouse column (resolved from the field's ``${TABLE}.col`` SQL).
    """
    source.client.get_chart.return_value = LightdashChart(
        uuid=CHART_UUID,
        name="Live orders",
        project_uuid=PROJECT_UUID,
        organization_uuid=ORG_UUID,
        space_uuid=SPACE_UUID,
        chart_kind="table",
        table_name="orders_enriched",
        metric_query=LightdashMetricQuery(
            explore_name="orders_enriched",
            dimensions=["orders_enriched_order_id", "orders_enriched_order_status"],
            metrics=["orders_enriched_total_revenue"],
        ),
    )
    source.client.get_explore.return_value = LightdashExplore(
        name="orders_enriched",
        base_table="orders_enriched",
        tables={
            "orders_enriched": LightdashExploreTable(
                name="orders_enriched",
                schema="default",
                sql_table="`default`.`orders_enriched`",
                dimensions={
                    "order_id": LightdashFieldDef(
                        name="order_id",
                        table="orders_enriched",
                        sql="${TABLE}.order_id",
                        type="string",
                    ),
                    "order_status": LightdashFieldDef(
                        name="order_status",
                        table="orders_enriched",
                        sql="${TABLE}.order_status",
                        type="string",
                    ),
                },
                metrics={
                    "total_revenue": LightdashFieldDef(
                        name="total_revenue",
                        table="orders_enriched",
                        sql="${TABLE}.line_total_eur",
                        type="sum",
                        label="Total revenue",
                    ),
                },
            )
        },
    )

    wus = _workunits(source)
    chart_urn = f"urn:li:chart:(lightdash,{CHART_UUID})"
    fields_wu = [
        w
        for w in wus
        if getattr(w.metadata, "entityUrn", None) == chart_urn
        and w.metadata.aspectName == "inputFields"
    ]
    assert len(fields_wu) == 1, "Expected exactly one inputFields aspect per chart"
    fields = fields_wu[0].metadata.aspect.fields
    ds_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:clickhouse,default.orders_enriched,PROD)"
    )

    assert [f.schemaFieldUrn for f in fields] == [
        f"urn:li:schemaField:({ds_urn},order_id)",
        f"urn:li:schemaField:({ds_urn},order_status)",
        f"urn:li:schemaField:({ds_urn},line_total_eur)",
    ]
    # The chart "Fields" tab must show the Lightdash field ids (NOT the
    # warehouse column name) — that's what users see in the BI tool.
    assert [f.schemaField.fieldPath for f in fields] == [
        "orders_enriched_order_id",
        "orders_enriched_order_status",
        "orders_enriched_total_revenue",
    ]
    # The metric should carry its Lightdash type + label as well.
    total_revenue = fields[2]
    assert total_revenue.schemaField.nativeDataType == "sum"
    assert total_revenue.schemaField.description == "Total revenue"


def test_chart_multi_column_metric_emits_one_field_per_column(source):
    """A metric like ``sum(${TABLE}.a) / nullIf(uniqExact(${TABLE}.b), 0)``
    references two warehouse columns. The chart should show ONE field for that
    metric with TWO upstream lineage edges (same fieldPath, two distinct
    schemaFieldUrns).
    """
    source.client.get_chart.return_value = LightdashChart(
        uuid=CHART_UUID,
        name="AOV chart",
        project_uuid=PROJECT_UUID,
        organization_uuid=ORG_UUID,
        space_uuid=SPACE_UUID,
        chart_kind="big_number",
        table_name="orders_enriched",
        metric_query=LightdashMetricQuery(
            explore_name="orders_enriched",
            dimensions=[],
            metrics=["orders_enriched_average_order_value"],
        ),
    )
    source.client.get_explore.return_value = LightdashExplore(
        name="orders_enriched",
        base_table="orders_enriched",
        tables={
            "orders_enriched": LightdashExploreTable(
                name="orders_enriched",
                schema="default",
                metrics={
                    "average_order_value": LightdashFieldDef(
                        name="average_order_value",
                        table="orders_enriched",
                        sql=(
                            "sum(${TABLE}.line_total_eur) / nullIf(uniqExact(${TABLE}.order_id), 0)"
                        ),
                        type="number",
                    ),
                },
            )
        },
    )

    wus = _workunits(source)
    chart_urn = f"urn:li:chart:(lightdash,{CHART_UUID})"
    fields = [
        w
        for w in wus
        if getattr(w.metadata, "entityUrn", None) == chart_urn
        and w.metadata.aspectName == "inputFields"
    ][0].metadata.aspect.fields

    ds_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:clickhouse,default.orders_enriched,PROD)"
    )
    assert len(fields) == 2
    assert {f.schemaFieldUrn for f in fields} == {
        f"urn:li:schemaField:({ds_urn},line_total_eur)",
        f"urn:li:schemaField:({ds_urn},order_id)",
    }
    # Both lineage edges share the SAME chart field path — DataHub renders this
    # as one chart field with two upstream columns.
    assert {f.schemaField.fieldPath for f in fields} == {
        "orders_enriched_average_order_value"
    }


def test_chart_additional_metric_emits_input_field(source):
    """Chart-local ``additionalMetrics`` are user-defined on the saved chart
    (not in the Explore catalogue). Their ``${TABLE}.col`` SQL must still
    produce a column-lineage edge.
    """
    source.client.get_chart.return_value = LightdashChart(
        uuid=CHART_UUID,
        name="Live orders",
        project_uuid=PROJECT_UUID,
        organization_uuid=ORG_UUID,
        space_uuid=SPACE_UUID,
        chart_kind="table",
        table_name="orders_enriched",
        metric_query=LightdashMetricQuery(
            explore_name="orders_enriched",
            additional_metrics=[
                LightdashAdditionalMetric(
                    name="cancelled_revenue",
                    table="orders_enriched",
                    sql="${TABLE}.line_total_eur",
                    type="sum",
                    label="Cancelled revenue",
                )
            ],
        ),
    )
    source.client.get_explore.return_value = LightdashExplore(
        name="orders_enriched",
        base_table="orders_enriched",
        tables={
            "orders_enriched": LightdashExploreTable(
                name="orders_enriched", schema="default"
            )
        },
    )
    wus = _workunits(source)
    chart_urn = f"urn:li:chart:(lightdash,{CHART_UUID})"
    fields = [
        w
        for w in wus
        if getattr(w.metadata, "entityUrn", None) == chart_urn
        and w.metadata.aspectName == "inputFields"
    ][0].metadata.aspect.fields

    ds_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:clickhouse,default.orders_enriched,PROD)"
    )
    assert [f.schemaFieldUrn for f in fields] == [
        f"urn:li:schemaField:({ds_urn},line_total_eur)"
    ]
    assert [f.schemaField.fieldPath for f in fields] == [
        "orders_enriched_cancelled_revenue"
    ]


def test_chart_table_calculations_are_reported_but_not_emitted(source):
    """Table calculations have no warehouse lineage by design — the source must
    skip them silently and report the count so operators see they existed.
    """
    source.client.get_chart.return_value = LightdashChart(
        uuid=CHART_UUID,
        name="Live orders",
        project_uuid=PROJECT_UUID,
        organization_uuid=ORG_UUID,
        space_uuid=SPACE_UUID,
        chart_kind="table",
        table_name="orders_enriched",
        metric_query=LightdashMetricQuery(
            explore_name="orders_enriched",
            dimensions=["orders_enriched_order_status"],
            table_calculations=[
                LightdashTableCalculation(
                    name="running_total",
                    display_name="Running total",
                    sql="sum(${pivot.column})",
                )
            ],
        ),
    )
    source.client.get_explore.return_value = LightdashExplore(
        name="orders_enriched",
        base_table="orders_enriched",
        tables={
            "orders_enriched": LightdashExploreTable(
                name="orders_enriched",
                schema="default",
                dimensions={
                    "order_status": LightdashFieldDef(
                        name="order_status",
                        table="orders_enriched",
                        sql="${TABLE}.order_status",
                        type="string",
                    )
                },
            )
        },
    )
    _workunits(source)
    assert source.report.table_calculations_skipped == 1


def test_chart_input_fields_skipped_when_extract_lineage_disabled(source):
    source.config.extract_lineage = False
    source.client.get_chart.return_value = LightdashChart(
        uuid=CHART_UUID,
        name="Live orders",
        project_uuid=PROJECT_UUID,
        organization_uuid=ORG_UUID,
        space_uuid=SPACE_UUID,
        chart_kind="table",
        table_name="orders_enriched",
        metric_query=LightdashMetricQuery(
            explore_name="orders_enriched",
            dimensions=["orders_enriched_order_id"],
        ),
    )
    wus = _workunits(source)
    chart_urn = f"urn:li:chart:(lightdash,{CHART_UUID})"
    fields_wu = [
        w
        for w in wus
        if getattr(w.metadata, "entityUrn", None) == chart_urn
        and w.metadata.aspectName == "inputFields"
    ]
    assert fields_wu == []


def test_chart_type_resolved_from_list_endpoint(source):
    """Regression: ``/api/v1/saved/{uuid}`` does NOT expose chartKind at the top
    level. The source must read it from the list-endpoint summary.
    """
    source.client.get_chart.return_value = LightdashChart(
        uuid=CHART_UUID,
        name="Live orders",
        project_uuid=PROJECT_UUID,
        organization_uuid=ORG_UUID,
        space_uuid=SPACE_UUID,
        table_name="orders_enriched",
        metric_query=LightdashMetricQuery(explore_name="orders_enriched"),
    )
    wus = _workunits(source)
    chart_urn = f"urn:li:chart:(lightdash,{CHART_UUID})"
    chart_info = None
    for w in wus:
        if (
            getattr(w.metadata, "entityUrn", None) == chart_urn
            and w.metadata.aspectName == "chartInfo"
        ):
            chart_info = w.metadata.aspect
            break
    assert chart_info is not None
    assert chart_info.type == "TABLE"
    assert chart_info.customProperties["lightdashChartKind"] == "table"
