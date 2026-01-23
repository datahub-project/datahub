import logging
from typing import Dict, Optional

from pydantic import ConfigDict, Field, SecretStr, field_validator

from datahub.configuration._config_enum import ConfigEnum
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.source.matillion.constants import (
    MATILLION_EU1_URL,
    MATILLION_US1_URL,
    MAX_EXECUTIONS_PER_PIPELINE_WARNING_THRESHOLD,
    MAX_REQUEST_TIMEOUT_WARNING_THRESHOLD,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

logger = logging.getLogger(__name__)


class MatillionRegion(ConfigEnum):
    EU1 = "EU1"
    US1 = "US1"

    def get_url(self) -> str:
        if self == MatillionRegion.EU1:
            return MATILLION_EU1_URL
        elif self == MatillionRegion.US1:
            return MATILLION_US1_URL
        else:
            raise ValueError(f"Unknown region: {self}")


class NamespacePlatformMapping(ConfigModel):
    platform_instance: Optional[str] = Field(
        default=None,
        description="DataHub platform instance to use for datasets from this namespace",
    )

    env: str = Field(
        default=DEFAULT_ENV,
        description="Environment (PROD, DEV, etc.) to use for datasets from this namespace",
    )

    database: Optional[str] = Field(
        default=None,
        description="Default database name to prepend if dataset name doesn't include database context",
    )

    default_schema: Optional[str] = Field(
        default=None,
        description="Default schema name to prepend if dataset name doesn't include schema context",
        alias="schema",
    )

    convert_urns_to_lowercase: bool = Field(
        default=False,
        description=(
            "Whether to convert dataset URNs to lowercase for this namespace."
        ),
    )


class MatillionAPIConfig(ConfigModel):
    api_token: SecretStr = Field(
        description="Matillion API bearer token for authentication"
    )
    region: MatillionRegion = Field(
        default=MatillionRegion.EU1,
        description="Matillion Data Productivity Cloud region (EU1 or US1)",
    )
    custom_base_url: Optional[str] = Field(
        default=None,
        description="Custom API base URL (for testing or on-premise installations). Overrides region if set.",
    )
    request_timeout_sec: int = Field(
        default=30, description="Request timeout in seconds"
    )

    @field_validator("custom_base_url")
    @classmethod
    def validate_custom_base_url(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            if not v:
                raise ValueError("custom_base_url cannot be empty string")
            if not (v.startswith("http://") or v.startswith("https://")):
                raise ValueError("custom_base_url must start with http:// or https://")
            return v.rstrip("/")
        return v

    def get_base_url(self) -> str:
        if self.custom_base_url:
            return self.custom_base_url
        return self.region.get_url()

    @field_validator("request_timeout_sec")
    @classmethod
    def validate_request_timeout_sec(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("request_timeout_sec must be positive")
        if v > MAX_REQUEST_TIMEOUT_WARNING_THRESHOLD:
            logger.warning(
                f"request_timeout_sec is set to {v} seconds, which is quite high. "
                f"Consider a value below {MAX_REQUEST_TIMEOUT_WARNING_THRESHOLD} seconds to prevent long-running API calls."
            )
        return v


class MatillionSourceReport(StaleEntityRemovalSourceReport):
    projects_scanned: int = 0
    environments_scanned: int = 0
    pipelines_scanned: int = 0
    pipelines_emitted: int = 0
    agents_scanned: int = 0
    executions_scanned: int = 0
    schedules_scanned: int = 0
    repositories_scanned: int = 0
    filtered_pipelines: list[str] = Field(default_factory=list)
    filtered_projects: list[str] = Field(default_factory=list)
    filtered_streaming_pipelines: list[str] = Field(default_factory=list)
    api_calls_count: int = 0
    containers_emitted: int = 0
    lineage_emitted: int = 0
    streaming_pipelines_scanned: int = 0
    streaming_pipelines_emitted: int = 0
    sql_parsing_attempts: int = 0
    sql_parsing_successes: int = 0
    sql_parsing_failures: int = 0
    schemas_preloaded: int = 0

    def report_projects_scanned(self, count: int = 1) -> None:
        self.projects_scanned += count

    def report_environments_scanned(self, count: int = 1) -> None:
        self.environments_scanned += count

    def report_pipelines_scanned(self, count: int = 1) -> None:
        self.pipelines_scanned += count

    def report_pipelines_emitted(self, count: int = 1) -> None:
        self.pipelines_emitted += count

    def report_containers_emitted(self, count: int = 1) -> None:
        self.containers_emitted += count

    def report_api_call(self, count: int = 1) -> None:
        self.api_calls_count += count

    def report_lineage_emitted(self, count: int = 1) -> None:
        self.lineage_emitted += count

    def report_streaming_pipeline_scanned(self, count: int = 1) -> None:
        self.streaming_pipelines_scanned += count

    def report_streaming_pipeline_emitted(self, count: int = 1) -> None:
        self.streaming_pipelines_emitted += count

    def report_executions_scanned(self, count: int = 1) -> None:
        self.executions_scanned += count


class MatillionSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    api_config: MatillionAPIConfig = Field(description="Matillion API configuration")

    env: str = Field(
        default=DEFAULT_ENV,
        description="The environment that all assets produced by DataHub platform ingestion source belong to",
    )

    platform_instance: Optional[str] = Field(
        default=None,
        description="The instance of the platform that all assets produced by this recipe belong to",
    )

    include_pipeline_executions: bool = Field(
        default=True,
        description="Whether to ingest Matillion Pipeline Execution history as DataProcessInstances. "
        "This provides operational metadata about each pipeline execution, including status, duration, and row counts.",
    )

    max_executions_per_pipeline: int = Field(
        default=10,
        description="Maximum number of recent pipeline executions to ingest per pipeline. Set to 0 to disable execution ingestion.",
    )

    include_lineage: bool = Field(
        default=True,
        description="Whether to extract lineage from Matillion pipelines. "
        "When enabled, extracts upstream and downstream dataset dependencies from pipeline lineage graphs.",
    )

    include_column_lineage: bool = Field(
        default=True,
        description="Whether to extract column-level lineage from Matillion pipelines. "
        "Requires include_lineage to be True. Provides fine-grained lineage between specific columns.",
    )

    parse_sql_for_lineage: bool = Field(
        default=True,
        description="Whether to parse SQL from OpenLineage events to extract additional column-level lineage. "
        "Requires DataHub graph access. When enabled, SQL queries are parsed to infer lineage beyond what's "
        "explicitly provided in OpenLineage column mappings.",
    )

    lineage_start_days_ago: int = Field(
        default=7,
        description=(
            "Number of days in the past to start fetching OpenLineage events. "
            "Events from this many days ago until now will be fetched."
        ),
    )

    namespace_to_platform_instance: Optional[Dict[str, NamespacePlatformMapping]] = (
        Field(
            default=None,
            description=(
                "Maps OpenLineage namespace prefixes to platform instance/environment using longest prefix matching. "
                "Unmapped namespaces extract platform from URI with defaults (env=PROD). "
                'Example: {"snowflake://prod-account": {"platform_instance": "snowflake_prod", "env": "PROD"}}'
            ),
        )
    )

    lineage_platform_mapping: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Override platform name mappings from OpenLineage namespaces to DataHub platforms. "
            "Only needed for non-standard platforms. See documentation for list of pre-mapped platforms. "
            'Example: {"customdb": "postgres", "mywarehouse": "snowflake"}'
        ),
    )

    include_streaming_pipelines: bool = Field(
        default=True,
        description="Whether to ingest Matillion streaming pipelines (CDC pipelines). "
        "Streaming pipelines are emitted as separate DataFlows with pipeline_type='streaming'.",
    )

    streaming_pipeline_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for filtering Matillion streaming pipelines to ingest.",
    )

    pipeline_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for filtering Matillion pipelines to ingest.",
    )

    project_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for filtering Matillion projects to ingest.",
    )

    extract_projects_to_containers: bool = Field(
        default=True,
        description="Whether to extract Matillion projects as DataHub containers. "
        "When enabled, pipelines are organized under project containers, providing hierarchical navigation.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="Stateful ingestion configuration."
    )

    model_config = ConfigDict(
        use_enum_values=True,
        extra="forbid",
    )

    @field_validator("max_executions_per_pipeline")
    @classmethod
    def validate_max_executions(cls, v: int) -> int:
        if v < 0:
            raise ValueError("max_executions_per_pipeline must be non-negative")
        if v > MAX_EXECUTIONS_PER_PIPELINE_WARNING_THRESHOLD:
            logger.warning(
                f"max_executions_per_pipeline is set to {v}, which is quite high. "
                f"This may result in many API calls and slower ingestion. "
                f"Consider using a value below {MAX_EXECUTIONS_PER_PIPELINE_WARNING_THRESHOLD}."
            )
        return v
