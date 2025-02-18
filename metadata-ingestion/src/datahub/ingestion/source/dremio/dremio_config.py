import os
from typing import List, Literal, Optional

import certifi
from pydantic import Field, validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.ge_profiling_config import GEProfilingBaseConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_config.operation_config import is_profiling_enabled


class DremioConnectionConfig(ConfigModel):
    hostname: Optional[str] = Field(
        default=None,
        description="Hostname or IP Address of the Dremio server",
    )

    port: int = Field(
        default=9047,
        description="Port of the Dremio REST API",
    )

    username: Optional[str] = Field(
        default=None,
        description="Dremio username",
    )

    authentication_method: Optional[str] = Field(
        default="PAT",
        description="Authentication method: 'password' or 'PAT' (Personal Access Token)",
    )

    password: Optional[str] = Field(
        default=None,
        description="Dremio password or Personal Access Token",
    )

    tls: bool = Field(
        default=True,
        description="Whether the Dremio REST API port is encrypted",
    )

    disable_certificate_verification: Optional[bool] = Field(
        default=False,
        description="Disable TLS certificate verification",
    )

    path_to_certificates: str = Field(
        default=certifi.where(),
        description="Path to SSL certificates",
    )

    is_dremio_cloud: bool = Field(
        default=False,
        description="Whether this is a Dremio Cloud instance",
    )

    # https://docs.dremio.com/cloud/reference/api/#api-endpoint
    dremio_cloud_region: Literal["US", "EU"] = Field(
        default="US",
        description="Dremio Cloud region ('US' or 'EU')",
    )

    dremio_cloud_project_id: Optional[str] = Field(
        default=None,
        description="ID of Dremio Cloud Project. Found in Project Settings in the Dremio Cloud UI",
    )

    @validator("authentication_method")
    def validate_auth_method(cls, value):
        allowed_methods = ["password", "PAT"]
        if value not in allowed_methods:
            raise ValueError(
                f"authentication_method must be one of {allowed_methods}",
            )
        return value

    @validator("password")
    def validate_password(cls, value, values):
        if values.get("authentication_method") == "PAT" and not value:
            raise ValueError(
                "Password (Personal Access Token) is required when using PAT authentication",
            )
        return value


class ProfileConfig(GEProfilingBaseConfig):
    query_timeout: int = Field(
        default=300, description="Time before cancelling Dremio profiling query"
    )
    include_field_median_value: bool = Field(
        default=False,
        hidden_from_docs=True,
        description="Median causes a number of issues in Dremio.",
    )


class DremioSourceMapping(EnvConfigMixin, PlatformInstanceConfigMixin, ConfigModel):
    platform: str = Field(
        description="Source connection made by Dremio (e.g. S3, Snowflake)",
    )
    source_name: str = Field(
        description="Alias of platform in Dremio connection",
    )


class DremioSourceConfig(
    DremioConnectionConfig,
    StatefulIngestionConfigBase,
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
):
    domain: Optional[str] = Field(
        default=None,
        description="Domain for all source objects.",
    )

    source_mappings: Optional[List[DremioSourceMapping]] = Field(
        default=None,
        description="Mappings from Dremio sources to DataHub platforms and datasets.",
    )

    # Entity Filters
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas to filter",
    )

    dataset_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables and views to filter in ingestion. Specify regex to match the entire table name in dremio.schema.table format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'dremio.public.customer.*'",
    )

    usage: BaseUsageConfig = Field(
        description="The usage config to use when generating usage statistics",
        default=BaseUsageConfig(),
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    # Profiling
    profile_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to profile",
    )
    profiling: ProfileConfig = Field(
        default=ProfileConfig(),
        description="Configuration for profiling",
    )

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )

    # Advanced Configs
    max_workers: int = Field(
        default=5 * (os.cpu_count() or 4),
        description="Number of worker threads to use for parallel processing",
    )

    include_query_lineage: bool = Field(
        default=False,
        description="Whether to include query-based lineage information.",
    )

    ingest_owner: bool = Field(
        default=True,
        description="Ingest Owner from source. This will override Owner info entered from UI",
    )
