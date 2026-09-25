from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class HexConnection:
    """A Hex data connection resolved to its DataHub platform.

    Built once at the source boundary by HexSource._resolve_connections.
    `platform=None` means lineage will be skipped for cells referencing this
    connection — the display name is still available for the document builder.

    `default_database` and `default_schema` map to the slots `sqlglot_lineage`
    uses for unqualified table refs. The terminology is DataHub-side, not
    platform-native: for BigQuery `default_database` is the project ID; for
    Trino/Databricks it's the catalog; for Snowflake/Postgres/Redshift/MSSQL
    it's the database. For 2-part platforms (mysql/mariadb/clickhouse) only
    `default_schema` is populated — `default_database` stays None to avoid
    producing a wrong 3-part URN.
    """

    name: str
    platform: Optional[str]
    platform_instance: Optional[str] = None
    default_database: Optional[str] = None
    default_schema: Optional[str] = None


@dataclass
class SqlCell:
    """A SQL cell from the /v1/cells REST API."""

    cell_id: str
    cell_label: Optional[str]
    sql_source: str
    # None → in-memory transform, no external DB
    data_connection_id: Optional[str]


@dataclass
class ExploreCell:
    """A visualisation cell from the /v1/cells REST API."""

    cell_id: str
    cell_label: Optional[str]
    # dataframe reference not exposed by REST (only in YAML export)
    dataframe: Optional[str]
    chart_type: Optional[str]


@dataclass
class RunRecord:
    """Latest run from /v1/projects/{id}/runs."""

    run_id: str
    status: str
    start_time: datetime
    elapsed_seconds: Optional[float] = None


@dataclass
class Status:
    name: str


@dataclass
class Category:
    name: str
    description: Optional[str] = None


@dataclass
class Collection:
    name: str


@dataclass(frozen=True)
class Owner:
    email: str


@dataclass
class Analytics:
    appviews_all_time: Optional[int]
    appviews_last_7_days: Optional[int]
    appviews_last_14_days: Optional[int]
    appviews_last_30_days: Optional[int]
    last_viewed_at: Optional[datetime]


@dataclass
class _HexItemBase:
    """Shared fields between Project and Component."""

    id: str
    title: str
    description: Optional[str]
    last_edited_at: Optional[datetime] = None
    last_published_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    status: Optional[Status] = None
    categories: Optional[List[Category]] = None  # TODO: emit category description!
    collections: Optional[List[Collection]] = None
    creator: Optional[Owner] = None
    owner: Optional[Owner] = None
    analytics: Optional[Analytics] = None
    # Dataset URN strings resolved by the lineage builder. For Project, these
    # are stored in DashboardInfo.datasetEdges; for Component, in ChartInfo.inputs.
    upstream_datasets: List[str] = field(default_factory=list)
    # SchemaField URN strings for column-level lineage (InputFieldsClass).
    # Populated only when a graph-backed SchemaResolver is available.
    input_fields: List[str] = field(default_factory=list)


@dataclass
class Project(_HexItemBase):
    # Component IDs imported by this project (populated via export API).
    # Stored in DashboardInfo.charts as Chart URNs.
    used_component_ids: List[str] = field(default_factory=list)
    latest_run: Optional[RunRecord] = None


@dataclass
class Component(_HexItemBase):
    pass
