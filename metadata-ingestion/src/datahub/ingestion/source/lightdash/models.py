"""Pydantic v2 response models for the subset of the Lightdash REST API we read.

All Lightdash responses are wrapped as ``{"status": "ok", "results": ...}``; the
client unwraps ``results`` before handing the payload to these models. Every model
sets ``extra="ignore"`` so the connector keeps working when Lightdash adds new
fields on minor upgrades.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class _LightdashModel(BaseModel):
    """Common base — be lenient about unknown fields, allow alias-style population."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class LightdashUserRef(_LightdashModel):
    """User reference embedded on entities (``updatedByUser`` / ``createdByUser``).

    Older Lightdash builds don't expose ``email`` on this nested object — owner
    resolution falls back to ``userUuid`` when missing.
    """

    user_uuid: str = Field(alias="userUuid")
    first_name: Optional[str] = Field(default=None, alias="firstName")
    last_name: Optional[str] = Field(default=None, alias="lastName")
    email: Optional[str] = None

    @property
    def display_name(self) -> str:
        parts = [p for p in (self.first_name, self.last_name) if p]
        return " ".join(parts) if parts else self.user_uuid


class LightdashOrganization(_LightdashModel):
    organization_uuid: str = Field(alias="organizationUuid")
    name: str
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")


class LightdashProjectSummary(_LightdashModel):
    """Item shape returned by ``GET /api/v1/org/projects``."""

    project_uuid: str = Field(alias="projectUuid")
    name: str
    type: Optional[str] = None
    warehouse_type: Optional[str] = Field(default=None, alias="warehouseType")
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")
    created_by_user_uuid: Optional[str] = Field(default=None, alias="createdByUserUuid")
    created_by_user_name: Optional[str] = Field(default=None, alias="createdByUserName")


class LightdashWarehouseConnection(_LightdashModel):
    type: str
    schema_: Optional[str] = Field(default=None, alias="schema")
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None  # only on some warehouse types (snowflake/bigquery)


class LightdashProject(_LightdashModel):
    """Detailed shape returned by ``GET /api/v1/projects/{uuid}``."""

    project_uuid: str = Field(alias="projectUuid")
    organization_uuid: str = Field(alias="organizationUuid")
    name: str
    type: Optional[str] = None
    warehouse_connection: Optional[LightdashWarehouseConnection] = Field(
        default=None, alias="warehouseConnection"
    )


class LightdashSpace(_LightdashModel):
    uuid: str
    name: str
    project_uuid: str = Field(alias="projectUuid")
    organization_uuid: str = Field(alias="organizationUuid")
    slug: Optional[str] = None
    path: Optional[str] = None
    parent_space_uuid: Optional[str] = Field(default=None, alias="parentSpaceUuid")
    chart_count: Optional[Any] = Field(
        default=None, alias="chartCount"
    )  # API returns str
    dashboard_count: Optional[Any] = Field(default=None, alias="dashboardCount")


class LightdashChartSummary(_LightdashModel):
    """Item shape returned by ``GET /api/v1/projects/{uuid}/charts``.

    Note: this listing endpoint does NOT include ``tableName``; the source uses
    :class:`LightdashChart` (the saved-chart detail) to resolve upstream lineage.
    """

    uuid: str
    name: str
    description: Optional[str] = None
    project_uuid: str = Field(alias="projectUuid")
    organization_uuid: Optional[str] = Field(default=None, alias="organizationUuid")
    space_uuid: Optional[str] = Field(default=None, alias="spaceUuid")
    space_name: Optional[str] = Field(default=None, alias="spaceName")
    dashboard_uuid: Optional[str] = Field(default=None, alias="dashboardUuid")
    chart_type: Optional[str] = Field(default=None, alias="chartType")
    chart_kind: Optional[str] = Field(default=None, alias="chartKind")
    slug: Optional[str] = None
    updated_at: Optional[datetime] = Field(default=None, alias="updatedAt")
    updated_by_user: Optional[LightdashUserRef] = Field(
        default=None, alias="updatedByUser"
    )


class LightdashTableCalculation(_LightdashModel):
    """Ad-hoc, post-query calculation defined on the saved chart itself.

    Table calculations are computed in the BI layer from the query result set —
    they don't map back to a single warehouse column, so the source emits them
    as chart fields without a column-level lineage edge.
    """

    name: str
    display_name: Optional[str] = Field(default=None, alias="displayName")
    sql: Optional[str] = None
    type: Optional[str] = None


class LightdashCustomDimensionBin(_LightdashModel):
    """``bin``-style customDimension — buckets values from a real warehouse column.

    Lightdash exposes the bucketed column via ``dimensionId`` (an explore field
    id). The source uses that to wire chart-field-→-warehouse-column lineage.
    """

    name: str
    id: Optional[str] = None
    table: Optional[str] = None
    dimension_id: Optional[str] = Field(default=None, alias="dimensionId")
    bin_type: Optional[str] = Field(default=None, alias="binType")


class LightdashCustomSqlDimension(_LightdashModel):
    """``sql``-style customDimension — a bare SQL expression on the chart.

    Like a table calculation but compiled into the warehouse query. The source
    parses ``${TABLE}.<col>`` references out of ``sql`` for lineage.
    """

    name: str
    id: Optional[str] = None
    table: Optional[str] = None
    sql: Optional[str] = None


class LightdashAdditionalMetric(_LightdashModel):
    """Chart-local metric the user defines on top of an explore field.

    Lightdash compiles ``sql`` (always a ``${TABLE}.<col>``-style reference)
    against the warehouse, so it has clear column lineage — the source emits
    one InputField per ``${TABLE}.col`` extracted from ``sql``.
    """

    name: str
    table: Optional[str] = None
    sql: Optional[str] = None
    type: Optional[str] = None
    label: Optional[str] = None
    base_dimension_name: Optional[str] = Field(default=None, alias="baseDimensionName")


class LightdashMetricQuery(_LightdashModel):
    """The chart's compiled query.

    ``dimensions`` and ``metrics`` are field-ID lists in the form
    ``<table>_<fieldName>`` (e.g. ``orders_enriched_order_id``). The source
    strips the table prefix and looks the suffix up in the matching Explore
    table's ``dimensions`` / ``metrics`` to resolve the underlying warehouse
    column for chart-level field lineage.

    ``additionalMetrics`` and ``customDimensions`` are chart-local field
    definitions (no entry exists in the Explore for them) but they still
    reference warehouse columns through ``${TABLE}.col`` SQL fragments. The
    source surfaces them as chart fields and resolves their lineage from the
    embedded SQL. ``tableCalculations`` are post-query calculations with no
    warehouse-column lineage; the source still surfaces them as chart fields
    (without an upstream edge) so users see the full Lightdash field set in
    DataHub.
    """

    explore_name: Optional[str] = Field(default=None, alias="exploreName")
    dimensions: list[str] = Field(default_factory=list)
    metrics: list[str] = Field(default_factory=list)
    table_calculations: list[LightdashTableCalculation] = Field(
        default_factory=list, alias="tableCalculations"
    )
    additional_metrics: list[LightdashAdditionalMetric] = Field(
        default_factory=list, alias="additionalMetrics"
    )
    # customDimensions is a heterogeneous list (bin vs sql); we accept the raw
    # dicts and convert per-row at the call site.
    custom_dimensions: list[dict[str, Any]] = Field(
        default_factory=list, alias="customDimensions"
    )


class LightdashChart(LightdashChartSummary):
    """Detailed shape returned by ``GET /api/v1/saved/{uuid}``.

    Adds the warehouse-facing ``table_name`` and the ``metric_query.explore_name``
    that the source uses to look up join graph + sqlTable values from the
    matching Explore.
    """

    table_name: Optional[str] = Field(default=None, alias="tableName")
    metric_query: Optional[LightdashMetricQuery] = Field(
        default=None, alias="metricQuery"
    )


class LightdashDashboardSummary(_LightdashModel):
    uuid: str
    name: str
    description: Optional[str] = None
    project_uuid: str = Field(alias="projectUuid")
    organization_uuid: Optional[str] = Field(default=None, alias="organizationUuid")
    space_uuid: Optional[str] = Field(default=None, alias="spaceUuid")
    updated_at: Optional[datetime] = Field(default=None, alias="updatedAt")
    updated_by_user: Optional[LightdashUserRef] = Field(
        default=None, alias="updatedByUser"
    )


class LightdashDashboardTileProperties(_LightdashModel):
    saved_chart_uuid: Optional[str] = Field(default=None, alias="savedChartUuid")
    chart_name: Optional[str] = Field(default=None, alias="chartName")
    chart_slug: Optional[str] = Field(default=None, alias="chartSlug")


class LightdashDashboardTile(_LightdashModel):
    uuid: str
    type: str  # "saved_chart" / "markdown" / "loom"
    properties: Optional[LightdashDashboardTileProperties] = None


class LightdashDashboard(LightdashDashboardSummary):
    tiles: list[LightdashDashboardTile] = Field(default_factory=list)


class LightdashFieldDef(_LightdashModel):
    """One Lightdash dimension or metric.

    ``name`` is the field identifier (e.g. ``order_id``); ``table`` matches a key
    in ``Explore.tables``; ``sql`` is the SQL fragment Lightdash compiles into the
    query — usually ``${TABLE}.<column>`` for plain dimensions and the source
    column for metrics. The source extracts the underlying warehouse column from
    either ``name`` (when it equals the column) or ``sql`` (when an alias is in
    play).
    """

    name: str
    table: Optional[str] = None
    sql: Optional[str] = None
    type: Optional[str] = None
    label: Optional[str] = None
    field_type: Optional[str] = Field(default=None, alias="fieldType")


class LightdashExploreTable(_LightdashModel):
    """One entry of ``Explore.tables`` — the canonical place to read schema + sqlTable.

    ``sql_table`` is the raw, quoted warehouse path Lightdash compiles into SQL
    (e.g. ``\\`default\\`.\\`orders_enriched\\``). The source extracts the
    bare ``schema.table`` form for URN construction. The ``dimensions`` /
    ``metrics`` maps capture per-field metadata so the source can emit
    column-level chart lineage (``inputFields`` aspect on the Chart).
    """

    name: str
    schema_: Optional[str] = Field(default=None, alias="schema")
    database: Optional[str] = None
    sql_table: Optional[str] = Field(default=None, alias="sqlTable")
    dimensions: dict[str, LightdashFieldDef] = Field(default_factory=dict)
    metrics: dict[str, LightdashFieldDef] = Field(default_factory=dict)


class LightdashExplore(_LightdashModel):
    """Full Explore as returned by ``GET /api/v1/projects/{uuid}/explores/{name}``.

    The ``tables`` map holds the base table plus any joined tables — keyed by
    table name. The source emits one upstream Dataset URN per entry, giving
    charts proper lineage even when the Explore joins multiple dbt models.
    """

    name: str
    label: Optional[str] = None
    base_table: Optional[str] = Field(default=None, alias="baseTable")
    target_database: Optional[str] = Field(default=None, alias="targetDatabase")
    tables: dict[str, LightdashExploreTable] = Field(default_factory=dict)
    joined_tables: list[dict[str, Any]] = Field(
        default_factory=list, alias="joinedTables"
    )
