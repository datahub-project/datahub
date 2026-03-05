import logging
from typing import List, Literal, Optional

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
        "Required when include_catalog_metadata is enabled.",
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

    catalog_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter Flink catalogs by name. "
        "Only applies when include_catalog_metadata is enabled.",
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

    include_catalog_metadata: bool = Field(
        default=False,
        description="Extract catalog metadata (tables and schemas) via SQL Gateway. "
        "Requires sql_gateway_url in connection config.",
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
    def validate_catalog_integration(self) -> "FlinkSourceConfig":
        if self.include_catalog_metadata and not self.connection.sql_gateway_url:
            raise ValueError(
                "connection.sql_gateway_url must be configured when "
                "include_catalog_metadata is enabled."
            )
        return self
