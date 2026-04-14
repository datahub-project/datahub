import logging
from typing import Optional

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities import config_clean

logger = logging.getLogger(__name__)


class MicroStrategyConnectionConfig(ConfigModel):
    """Connection configuration for MicroStrategy REST API."""

    base_url: str = Field(
        description=(
            "Base URL for MicroStrategy REST API (e.g., 'https://demo.microstrategy.com/MicroStrategyLibrary'). "
            "Should be the URL to the MicroStrategy Library web application. "
            "Do not include '/api' suffix - it will be appended automatically."
        )
    )

    username: Optional[str] = Field(
        default=None,
        description=(
            "MicroStrategy username for authentication. "
            "For demo instances, you can use anonymous access by setting use_anonymous=True and leaving this blank. "
            "For production, use a service account with metadata read permissions."
        ),
    )

    password: Optional[SecretStr] = Field(
        default=None,
        description=(
            "Password for MicroStrategy authentication. "
            "Required if username is provided. "
            "Stored securely and not logged."
        ),
    )

    use_anonymous: bool = Field(
        default=False,
        description=(
            "Use anonymous guest access (for demo instances). "
            "When enabled, username/password are not required. "
            "Only works on MicroStrategy instances configured for guest access."
        ),
    )

    timeout_seconds: int = Field(
        default=30,
        description="HTTP request timeout in seconds. Increase for slow networks or large metadata responses.",
    )

    max_retries: int = Field(
        default=3,
        description="Maximum number of retry attempts for failed API requests. Uses exponential backoff.",
    )

    @field_validator("base_url", mode="after")  # type: ignore[misc]  # pydantic v2 decorator stacking with ConfigModel metaclass
    @classmethod
    def validate_base_url(cls, v: str) -> str:
        """Validate and normalize base URL."""
        if not v.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return config_clean.remove_trailing_slashes(v)

    @model_validator(mode="after")  # type: ignore[misc]  # pydantic v2 decorator stacking with ConfigModel metaclass
    def validate_auth_config(self) -> "MicroStrategyConnectionConfig":
        """Validate authentication configuration consistency."""
        has_password = bool(self.password and self.password.get_secret_value())
        # Case 1: Anonymous mode - should not have credentials
        if self.use_anonymous:
            if self.username or has_password:
                raise ValueError(
                    "When use_anonymous=True, username and password should not be provided. "
                    "Choose either anonymous access OR credentials, not both."
                )

        # Case 2: Credential mode - must have both username and password
        else:
            if not self.username or not has_password:
                raise ValueError(
                    "When use_anonymous=False, both username and password are required. "
                    "Either provide credentials or set use_anonymous=True."
                )

        return self


class MicroStrategyConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """Configuration for MicroStrategy source."""

    connection: MicroStrategyConnectionConfig = Field(
        description="Connection settings for MicroStrategy REST API"
    )

    # Filtering patterns
    project_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex patterns to filter projects. "
            "Example: {'allow': ['^Production.*'], 'deny': ['^Test.*']}. "
            "Only matching projects will be ingested."
        ),
    )

    folder_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter folders within projects.",
    )

    dashboard_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex patterns to filter dashboards (dossiers). "
            "Applies to dashboard names. "
            "Use to exclude personal or test dashboards."
        ),
    )

    report_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter reports.",
    )

    cube_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex patterns to filter Intelligent Cubes by name after listing. "
            "Reduces per-cube API calls (schema, warehouse lineage) but not the cube search itself."
        ),
    )

    # Scope toggles — set to false to skip entire MicroStrategy API paths (fewer calls)
    include_folders: bool = Field(
        default=True,
        description=(
            "When false, do not call GET .../projects/{id}/folders or emit folder containers."
        ),
    )

    include_dashboards: bool = Field(
        default=True,
        description=(
            "When false, skip dashboard (dossier) search and processing—largest saver on demo envs."
        ),
    )

    include_reports: bool = Field(
        default=True,
        description="When false, skip report search and chart emission.",
    )

    include_cubes: bool = Field(
        default=True,
        description=(
            "When false, skip emitting cube datasets. "
            "Cube search still runs when include_lineage and include_reports are true "
            "(needed for report→cube lineage resolution)."
        ),
    )

    include_datasets: bool = Field(
        default=True,
        description=(
            "When false, skip GET .../projects/{id}/datasets and Library dataset emission. "
            "Dataset registry is still populated when include_lineage and include_reports are true."
        ),
    )

    # Feature flags
    include_lineage: bool = Field(
        default=True,
        description=(
            "Extract lineage between dashboards/reports and cubes/datasets. "
            "Shows data flow from cubes to visualizations. "
            "Critical for impact analysis."
        ),
    )

    include_ownership: bool = Field(
        default=True,
        description=(
            "Extract ownership information (creators, owners). "
            "Automatically links dashboards/reports to their creators. "
            "Enabled by default."
        ),
    )

    include_cube_schema: bool = Field(
        default=True,
        description=(
            "Extract schema (attributes, metrics) from Intelligent Cubes. "
            "Required for column-level lineage. "
            "Enabled by default."
        ),
    )

    include_cube_view_sql: bool = Field(
        default=True,
        description=(
            "When true, call the cube sqlView API and emit Dataset ViewProperties "
            "(SQL view definition) for each Intelligent Cube when SQL is returned."
        ),
    )

    include_field_formulas: bool = Field(
        default=True,
        description=(
            "When true, fetch attribute and metric expressions from "
            "GET /api/model/attributes/{id} and GET /api/model/metrics/{id} "
            "and surface them as the field description in inputFields. "
            "Attribute expressions show the physical column name and backing tables; "
            "metric expressions show the aggregation formula (e.g. 'Sum(NET_SLS_RTL_AMT)'). "
            "Adds one API call per unique attribute/metric. Enabled by default."
        ),
    )

    include_report_definitions: bool = Field(
        default=False,
        description=(
            "When true, call GET /api/v2/reports/{id} for each matched report to fetch its "
            "full definition. This enables: (1) report→cube lineage via dataSource.id, "
            "(2) report schema (attributes and metrics used), "
            "(3) enriched customProperties (filter presence, prompt count). "
            "OFF by default — 43,991 reports × 1 extra API call each is expensive. "
            "Enable only for scoped ingestion runs (e.g. with report_pattern filters) "
            "or when cube→report lineage and report schema are required."
        ),
    )

    include_column_lineage: bool = Field(
        default=True,
        description=(
            "Extract column-level lineage from cube SQL. "
            "Requires include_cube_schema, include_lineage, and include_warehouse_lineage to be true. "
            "The warehouse platform is detected automatically. "
            "Optionally set warehouse_lineage_database or warehouse_lineage_schema "
            "to qualify bare table names."
        ),
    )

    include_unloaded_projects: bool = Field(
        default=False,
        description=(
            "Ingest projects that are not loaded on IServer (status != 0). "
            "When false (default), only projects with status 0 (loaded) are ingested, "
            "matching typical REST behavior and avoiding ERR001 / unloaded-project failures. "
            "Set to true to restore pre-change behavior or for special environments."
        ),
    )

    cube_search_object_type: int = Field(
        default=776,
        description=(
            "Object type code passed to /api/searches/results when listing cubes. "
            "776 is used by MicroStrategy Library search for cube-style objects; "
            "override if your server expects a different type."
        ),
    )

    include_warehouse_lineage: bool = Field(
        default=False,
        description=(
            "When true with include_lineage, emit UpstreamLineage from physical warehouse "
            "tables to cubes, reports, and documents. "
            "The source platform is detected automatically from GET /api/datasources; "
            "if that endpoint returns 403, it is inferred from SQL quoting style. "
            "No manual platform configuration is required."
        ),
    )

    convert_lineage_urns_to_lowercase: bool = Field(
        default=True,
        description=(
            "When true, lowercase upstream table URNs so they match warehouse-ingested "
            "assets regardless of casing (e.g. Snowflake, BigQuery). "
            "When a DataHub graph connection is available (datahub_api configured), "
            "the connector first tries to resolve the correct casing by looking up "
            "the URN in DataHub; this flag is the fallback when the graph is unavailable "
            "or the entity has not been ingested yet."
        ),
    )

    warehouse_lineage_database: Optional[str] = Field(
        default=None,
        description=(
            "Override for the database/catalog name prepended to upstream table URNs. "
            "Normally auto-detected from the datasource JDBC/ODBC connection string "
            "(e.g. ``db=P_MER_EDW_DB`` for Snowflake). Only set this if auto-detection "
            "fails or you need to override to a different database."
        ),
    )

    warehouse_lineage_schema: Optional[str] = Field(
        default=None,
        description=(
            "Override for the schema name prepended to bare (unqualified) table names. "
            "Normally auto-detected from the datasource connection string. "
            "Only set this if auto-detection fails or you need to override."
        ),
    )

    preflight_dashboard_exists: bool = Field(
        default=False,
        description=(
            "Before fetching each dashboard definition, call GET /api/objects/{id}?type=55. "
            "Skips the dashboard on 404. Increases API traffic; use when troubleshooting missing objects."
        ),
    )

    max_workers: int = Field(
        default=4,
        ge=1,
        description=(
            "Maximum number of threads for parallel API calls. Used for: "
            "cube metadata prefetch (sqlView + schema), dashboard/report definition "
            "prefetch, field formula cache warming, and warehouse lineage SQL view "
            "retrieval. Set to 1 to disable parallelism (serial fetch, easier to debug). "
            "Values above 10 risk hitting MicroStrategy REST API rate limits."
        ),
    )

    # Stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description=(
            "Automatically remove deleted dashboards/reports from DataHub. "
            "Maintains catalog accuracy when objects are deleted in MicroStrategy. "
            "Recommended for production environments."
        ),
    )
