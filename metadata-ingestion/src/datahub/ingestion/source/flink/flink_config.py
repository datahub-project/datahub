import logging
from typing import List, Optional

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern
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

logger = logging.getLogger(__name__)

VALID_FLINK_JOB_STATES = {
    "INITIALIZING",
    "CREATED",
    "RUNNING",
    "FAILING",
    "FAILED",
    "CANCELLING",
    "CANCELED",
    "FINISHED",
    "RESTARTING",
    "SUSPENDED",
    "RECONCILING",
}

DEFAULT_INCLUDE_JOB_STATES = ["RUNNING", "FINISHED", "FAILED", "CANCELED"]


class FlinkSourceConfig(
    StatefulIngestionConfigBase,
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
):
    rest_endpoint: str = Field(
        description="Flink JobManager REST API base URL, e.g. http://localhost:8081. "
        "This is the address where the Flink Web UI is accessible."
    )

    token: Optional[SecretStr] = Field(
        default=None,
        description="Bearer token for authentication. Mutually exclusive with username/password. "
        "Typically used with Flink deployments behind an API gateway or reverse proxy.",
    )

    username: Optional[str] = Field(
        default=None,
        description="Username for HTTP Basic authentication. Must be paired with 'password'. "
        "Used when the Flink REST API is secured with basic auth.",
    )

    password: Optional[SecretStr] = Field(
        default=None,
        description="Password for HTTP Basic authentication. Must be paired with 'username'.",
    )

    verify_ssl: bool = Field(
        default=True,
        description="Whether to verify SSL certificates when connecting to the Flink REST API. "
        "Set to False for self-signed certificates in development environments.",
    )

    kafka_platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance for Kafka dataset URNs. Must match the Kafka connector's "
        "'platform_instance' config for lineage edges to connect. If unset, Kafka URNs "
        "will be constructed with env only.",
    )

    kafka_env: Optional[str] = Field(
        default=None,
        description="Environment for Kafka dataset URNs. Defaults to the connector's 'env' if unset.",
    )

    max_workers: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Maximum number of parallel threads for fetching job details from the Flink REST API. "
        "Higher values speed up ingestion for clusters with many jobs but increase load on the JobManager.",
    )

    request_timeout_seconds: int = Field(
        default=30,
        ge=1,
        description="Timeout in seconds for each HTTP request to the Flink REST API.",
    )

    job_name_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter Flink jobs by name. "
        "Use 'allow' to include specific jobs and 'deny' to exclude them.",
    )

    include_job_states: List[str] = Field(
        default=DEFAULT_INCLUDE_JOB_STATES,
        description="List of Flink job states to ingest. "
        f"Valid states: {', '.join(sorted(VALID_FLINK_JOB_STATES))}.",
    )

    include_lineage: bool = Field(
        default=True,
        description="Whether to extract source/sink lineage from Flink execution plans. "
        "Lineage connects Flink jobs to their input and output Kafka topics.",
    )

    include_run_history: bool = Field(
        default=True,
        description="Whether to emit DataProcessInstance entities for job execution tracking. "
        "When enabled, each job execution appears as a run in the DataHub UI.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Configuration for stateful ingestion. When enabled, entities from previous "
        "runs that are no longer present will be soft-deleted.",
    )

    @field_validator("rest_endpoint")
    @classmethod
    def validate_rest_endpoint(cls, v: str) -> str:
        """Strip trailing slash for consistent URL joining."""
        return v.rstrip("/")

    @model_validator(mode="after")
    def validate_auth(self) -> "FlinkSourceConfig":
        """Ensure auth options are mutually exclusive and complete."""
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

    @model_validator(mode="after")
    def warn_kafka_platform_instance(self) -> "FlinkSourceConfig":
        """Warn when kafka_platform_instance is unset — lineage may silently not connect."""
        if self.include_lineage and not self.kafka_platform_instance:
            logger.warning(
                "kafka_platform_instance is not set. Kafka dataset URNs will use env='%s'. "
                "If your Kafka connector uses platform_instance, lineage will not connect. "
                "Set kafka_platform_instance to match.",
                self.env,
            )
        return self

    @field_validator("include_job_states")
    @classmethod
    def validate_job_states(cls, v: List[str]) -> List[str]:
        """Validate that all specified states are valid Flink job states."""
        for state in v:
            if state.upper() not in VALID_FLINK_JOB_STATES:
                raise ValueError(
                    f"Invalid Flink job state: '{state}'. "
                    f"Valid states: {', '.join(sorted(VALID_FLINK_JOB_STATES))}"
                )
        return [s.upper() for s in v]

    @property
    def cluster(self) -> str:
        """Cluster identifier for URN construction."""
        return self.platform_instance or self.env

    @property
    def kafka_dataset_env(self) -> str:
        """Environment for Kafka dataset URNs."""
        return self.kafka_env or self.env
