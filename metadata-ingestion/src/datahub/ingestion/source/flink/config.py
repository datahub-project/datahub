import logging
from typing import Dict, List, Literal, Optional

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    DatasetLineageProviderConfigBase,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.flink.constants import (
    DEFAULT_INCLUDE_JOB_STATES,
    VALID_FLINK_JOB_STATES,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

logger = logging.getLogger(__name__)


class FlinkConnectionConfig(ConfigModel):
    """Connection configuration for Flink REST APIs."""

    rest_api_url: str = Field(
        description="JobManager REST API endpoint (e.g., http://localhost:8081)."
    )

    sql_gateway_url: Optional[str] = Field(
        default=None,
        description="SQL Gateway REST API endpoint (e.g., http://localhost:8083). "
        "Enables platform resolution for SQL/Table API lineage. "
        "When provided, the connector resolves table references to their actual platform "
        "(kafka, postgres, iceberg, etc.) via catalog introspection.",
    )

    token: Optional[SecretStr] = Field(
        default=None,
        description="Bearer token for authentication. "
        "Mutually exclusive with username/password.",
    )

    username: Optional[str] = Field(
        default=None,
        description="Username for HTTP Basic authentication. Must be paired with 'password'.",
    )

    password: Optional[SecretStr] = Field(
        default=None,
        description="Password for HTTP Basic authentication. Must be paired with 'username'.",
    )

    timeout_seconds: int = Field(
        default=30,
        ge=1,
        description="HTTP request timeout in seconds.",
    )

    max_retries: int = Field(
        default=3,
        ge=0,
        description="Maximum retry attempts for failed HTTP requests with exponential backoff.",
    )

    verify_ssl: bool = Field(
        default=True,
        description="Verify SSL certificates for HTTPS connections.",
    )

    @field_validator("rest_api_url", "sql_gateway_url", mode="before")
    @classmethod
    def strip_trailing_slash(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            return v.rstrip("/")
        return v

    @model_validator(mode="after")
    def validate_auth(self) -> "FlinkConnectionConfig":
        if self.token and (self.username or self.password):
            raise ValueError(
                "Cannot specify both 'token' and 'username'/'password' authentication. "
                "Use either bearer token OR basic auth."
            )
        if bool(self.username) != bool(self.password):
            raise ValueError(
                "Both 'username' and 'password' must be provided together for basic auth."
            )
        return self


class CatalogPlatformDetail(ConfigModel):
    """Platform details for a Flink catalog, used in dataset URN construction.

    Provides two pieces of information for a given Flink catalog:

    - ``platform``: The DataHub platform name (e.g., "iceberg", "postgres").
      On Flink 1.20+, this is auto-detected via DESCRIBE CATALOG and only needs
      to be specified for catalogs where auto-detection fails. On Flink < 1.20,
      this is required for Iceberg and Paimon catalogs (which don't expose a
      ``connector`` property in SHOW CREATE TABLE).

    - ``platform_instance``: The DataHub platform instance (e.g., "prod-postgres").
      Used when a Flink cluster connects to multiple deployments of the same
      platform and you need distinct dataset URNs per deployment.

    Follows the same pattern as Fivetran's ``PlatformDetail`` and Looker's
    ``LookerConnectionDefinition``.
    """

    platform: Optional[str] = Field(
        default=None,
        description="DataHub platform name for datasets in this catalog "
        "(e.g., 'iceberg', 'postgres', 'kafka'). "
        "When omitted, the connector auto-detects the platform via SQL Gateway.",
    )

    platform_instance: Optional[str] = Field(
        default=None,
        description="DataHub platform instance for datasets in this catalog "
        "(e.g., 'prod-postgres', 'us-east-kafka'). "
        "Used to distinguish multiple deployments of the same platform.",
    )


class FlinkSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    DatasetLineageProviderConfigBase,
):
    """Source configuration for Flink connector."""

    connection: FlinkConnectionConfig = Field(
        description="Flink REST API connection configuration."
    )

    job_name_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter Flink jobs by name.",
    )

    include_job_states: List[str] = Field(
        default=DEFAULT_INCLUDE_JOB_STATES,
        description="Flink job states to include in ingestion.",
    )

    include_lineage: bool = Field(
        default=True,
        description="Extract source/sink lineage from Flink execution plans.",
    )

    include_run_history: bool = Field(
        default=True,
        description="Emit DataProcessInstance entities for job execution tracking.",
    )

    catalog_platform_map: Dict[str, CatalogPlatformDetail] = Field(
        default_factory=dict,
        description="Platform overrides for Flink catalogs, keyed by catalog name. "
        "Values take priority over SQL Gateway auto-detection. "
        "Example: {'ice_catalog': {'platform': 'iceberg', 'platform_instance': 'prod-iceberg'}, "
        "'pg_catalog': {'platform_instance': 'prod-postgres'}}. "
        "The 'platform' field overrides auto-detection. Required for Iceberg/Paimon catalogs "
        "on Flink < 1.20 (DESCRIBE CATALOG unavailable). On Flink 1.20+, platform is "
        "auto-detected via SQL Gateway unless overridden here. "
        "The 'platform_instance' field takes priority over the inherited "
        "platform_instance_map (platform -> platform_instance) for catalogs listed here.",
    )

    operator_granularity: Literal["job", "vertex"] = Field(
        default="job",
        description="DataJob granularity: 'job' emits one coalesced DataJob per flow, "
        "'vertex' emits one DataJob per operator/vertex in the execution plan.",
    )

    max_workers: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Max parallel threads for fetching job details from the Flink REST API.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion for soft-deleting stale entities.",
    )

    @field_validator("include_job_states")
    @classmethod
    def validate_job_states(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("include_job_states must not be empty.")
        for state in v:
            if state.upper() not in VALID_FLINK_JOB_STATES:
                raise ValueError(
                    f"Invalid Flink job state: '{state}'. "
                    f"Valid states: {', '.join(sorted(VALID_FLINK_JOB_STATES))}"
                )
        return [s.upper() for s in v]

    @model_validator(mode="after")
    def warn_lineage_without_sql_gateway(self) -> "FlinkSourceConfig":
        if self.include_lineage and not self.connection.sql_gateway_url:
            logger.info(
                "include_lineage is enabled but sql_gateway_url is not configured. "
                "Lineage for SQL/Table API jobs will be limited to DataStream Kafka sources/sinks. "
                "Configure sql_gateway_url to resolve platforms for SQL/Table API table references."
            )
        return self
