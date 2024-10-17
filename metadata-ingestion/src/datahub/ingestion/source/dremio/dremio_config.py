import os
from typing import List, Literal, Optional

import certifi
from pydantic import Field, validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.metadata.schema_classes import FabricTypeClass


class ProfileConfig(GEProfilingConfig):
    partition_profiling_enabled: bool = Field(
        default=False,
        description="Partition profiling disabled for Dremio.",
    )
    include_field_median_value: bool = Field(
        default=False,
        description="Median causes a number of issues in Dremio.",
    )
    query_timeout: int = Field(
        default=300, description="Time before cancelling Dremio profiling query"
    )

    row_count: bool = True
    column_count: bool = True
    sample_values: bool = True


class DremioSourceMapping(ConfigModel):
    platform: Optional[str] = Field(
        default=None,
        description="Source connection made by Dremio (e.g. S3, Snowflake)",
    )
    platform_name: Optional[str] = Field(
        default=None,
        description="Alias of platform in Dremio connection",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance of source connection in Datahub",
    )
    dremio_source_type: Optional[str] = Field(
        default=None,
        description="Source connection made by Dremio (e.g. S3, Snowflake)",
    )
    env: Optional[str] = Field(
        default=FabricTypeClass.PROD,
        description="ENV in Datahub of source connection made by Dremio (e.g. PROD)",
    )
    root_path: Optional[str] = Field(
        default=None,
        description="Root path of source - Extracted from Dremio API",
        hidden_from_docs=True,
    )
    database_name: Optional[str] = Field(
        default=None,
        description="Database of source - Extracted from Dremio API",
        hidden_from_docs=True,
    )


class DremioSourceConfig(ConfigModel, StatefulIngestionConfigBase):

    # Dremio Connection Details
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
        default="password",
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

    # Dremio Cloud specific configs
    is_dremio_cloud: Optional[bool] = Field(
        default=False,
        description="Whether this is a Dremio Cloud instance",
    )
    dremio_cloud_region: Literal["US", "EMEA"] = Field(
        default="US",
        description="Dremio Cloud region ('US' or 'EMEA')",
    )

    # DataHub Environment details
    env: str = Field(
        default=FabricTypeClass.PROD,
        description="Environment to use in namespace when constructing URNs.",
    )

    platform_instance: Optional[str] = Field(
        default=None,
        description="The instance of the platform that all assets produced by this recipe belong to. "
        "This should be unique within the platform. "
        "See https://datahubproject.io/docs/platform-instances/ for more details.",
    )

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
        description="Regex patterns for schemas to filter",
    )

    # Profiling
    profile_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to profile",
    )
    profiling: ProfileConfig = Field(
        default=ProfileConfig(),
        description="Configuration for profiling",
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

    include_table_rename_lineage: bool = Field(
        default=True,
        description="Whether to include table rename lineage",
    )

    include_copy_lineage: bool = Field(
        default=True,
        description="Whether to include copy lineage",
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
