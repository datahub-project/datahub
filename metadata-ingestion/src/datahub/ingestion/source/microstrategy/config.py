from typing import Annotated, Dict, Literal, Optional, Union

from pydantic import Field, SecretStr, field_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel, HiddenFromDocs
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.microstrategy.constants import MICROSTRATEGY_PLATFORM
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities import config_clean


class MicroStrategyGuestAuth(ConfigModel):
    type: Literal["guest"] = "guest"


class MicroStrategyPasswordAuth(ConfigModel):
    type: Literal["password"] = "password"
    username: str = Field(description="MicroStrategy username.")
    password: SecretStr = Field(description="MicroStrategy password.")

    @field_validator("username", mode="after")
    @classmethod
    def username_not_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("username must not be empty")
        return value


MicroStrategyAuthConfig = Annotated[
    Union[MicroStrategyGuestAuth, MicroStrategyPasswordAuth],
    Field(discriminator="type"),
]


class MicroStrategyConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    platform: HiddenFromDocs[str] = Field(default=MICROSTRATEGY_PLATFORM)

    base_url: str = Field(
        description=(
            "MicroStrategy Library base URL, for example "
            "`https://your-company.example.com/MicroStrategyLibrary`."
        )
    )
    auth: MicroStrategyAuthConfig = Field(
        default_factory=MicroStrategyGuestAuth,
        description=(
            "Authentication mode. Use `type: guest` for public demo-style access "
            "or `type: password` with username/password for authenticated tenants."
        ),
    )
    verify_ssl: bool = Field(
        default=True,
        description="Whether to verify SSL certificates for MicroStrategy API calls.",
    )
    timeout_seconds: int = Field(
        default=30,
        gt=0,
        description="HTTP request timeout in seconds.",
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        description="Maximum retry attempts for transient API failures.",
    )
    page_size: int = Field(
        default=100,
        gt=0,
        description="Number of objects requested per paginated metadata search call.",
    )

    project_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter MicroStrategy projects by name.",
    )
    dashboard_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter MicroStrategy dossiers/dashboards by name.",
    )
    report_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter MicroStrategy reports by name.",
    )
    folder_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter folder containers by name.",
    )

    include_hidden: bool = Field(
        default=False,
        description="Whether to include hidden MicroStrategy objects when APIs support it.",
    )
    extract_dashboards: bool = Field(
        default=True,
        description="Whether to extract dossiers/documents as DataHub dashboards.",
    )
    extract_charts: bool = Field(
        default=True,
        description="Whether to extract visualizations as DataHub charts.",
    )
    extract_reports: bool = Field(
        default=False,
        description=(
            "Whether to extract MicroStrategy reports as DataHub charts. Disabled "
            "by default because reports can be numerous and are independent from "
            "dossier visualization extraction."
        ),
    )
    extract_report_definitions: bool = Field(
        default=True,
        description=(
            "Whether to fetch report definitions for source dataset, metric, and "
            "attribute details when `extract_reports` is enabled."
        ),
    )
    extract_cubes: bool = Field(
        default=True,
        description="Whether to extract embedded dashboard datasets as DataHub datasets.",
    )
    extract_lineage: bool = Field(
        default=True,
        description="Whether to emit dataset-to-chart lineage when resolved from definitions.",
    )
    extract_visualization_details: bool = Field(
        default=True,
        description=(
            "Whether to execute dashboards and fetch per-visualization runtime "
            "definitions to resolve dataset-to-visualization lineage when the "
            "static dashboard definition does not include dataset IDs."
        ),
    )
    extract_source_warehouses: bool = Field(
        default=True,
        description=(
            "Whether to call the MicroStrategy datasource management APIs to "
            "discover project source warehouse names, source types, database "
            "versions, DBMS names, and connection metadata."
        ),
    )
    extract_dashboard_dependencies: bool = Field(
        default=True,
        description=(
            "Whether to call metadata search lineage APIs for direct dashboard "
            "components such as metrics, attributes, filters, and functions."
        ),
    )
    extract_metric_expressions: bool = Field(
        default=True,
        description=(
            "Whether to fetch metric model definitions with expression tokens "
            "and attach expression metadata to metric schema fields when the "
            "MicroStrategy principal has access."
        ),
    )
    extract_model_lineage: bool = Field(
        default=True,
        description=(
            "Whether to attempt modeling/table API access needed for logical "
            "table and source warehouse lineage. If privileges are missing, "
            "the connector reports the failure and continues."
        ),
    )
    extract_warehouse_lineage: bool = Field(
        default=False,
        description=(
            "Whether to execute dashboard/dossier SQL-view APIs and emit upstream "
            "coarse table-level lineage from MicroStrategy datasets to source "
            "warehouse datasets parsed from SQL. Disabled by default because this "
            "is not field-level metric, attribute, or fact lineage. The connector "
            "discovers the warehouse platform from MicroStrategy datasource metadata "
            "and does not store raw SQL."
        ),
    )
    extract_report_sql_lineage: bool = Field(
        default=False,
        description=(
            "Whether to execute report SQL-view APIs and emit coarse table-level "
            "lineage from report source datasets to source warehouse datasets. "
            "Disabled by default for the same reason as `extract_warehouse_lineage`."
        ),
    )
    warehouse_lineage_sql_timeout_seconds: int = Field(
        default=180,
        gt=0,
        description=(
            "HTTP timeout in seconds for SQL-view APIs. These calls can be slower "
            "than metadata definition APIs because MicroStrategy must create and "
            "resolve a dashboard instance."
        ),
    )
    emit_dashboard_dataset_edges: bool = Field(
        default=False,
        description=(
            "Emit DashboardInfo.datasetEdges as a fallback. Disabled by default "
            "because BI dashboards with many datasets make lineage views noisy."
        ),
    )
    tag_measures_and_dimensions: bool = Field(
        default=True,
        description=(
            "Tag metric fields as Measure, attribute fields as Dimension, and "
            "date/time attribute forms as Temporal."
        ),
    )
    ingest_owner: bool = Field(
        default=True,
        description="Whether to map API owner fields to DataHub ownership aspects.",
    )
    prefer_embedded_dataset_schema: bool = Field(
        default=True,
        description=(
            "Use datasets embedded in dossier/document definitions as the primary "
            "schema source. This is the most reliable path for dashboard datasets."
        ),
    )

    metric_glossary_term_mapping: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Optional explicit mapping from MicroStrategy metric ID or name to "
            "DataHub glossary term URN."
        ),
    )
    attribute_glossary_term_mapping: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Optional explicit mapping from MicroStrategy attribute/form ID or name "
            "to DataHub glossary term URN."
        ),
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion config with stale entity removal support.",
    )

    @field_validator("base_url", mode="after")
    @classmethod
    def normalize_base_url(cls, value: str) -> str:
        value = config_clean.remove_trailing_slashes(value)
        if value.endswith("/api"):
            value = value[: -len("/api")]
        return value
