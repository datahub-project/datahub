from typing import Dict, Optional
from urllib.parse import urlparse

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.boomi_data_integration.constants import (
    DEFAULT_MAX_REQUESTS_PER_SECOND,
    DEFAULT_REQUEST_TIMEOUT_SECONDS,
    MAX_REQUESTS_PER_SECOND_LIMIT,
    REGION_API_URLS,
    DIRegion,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulIngestionConfigBase,
    StatefulStaleMetadataRemovalConfig,
)

_HTTP_SCHEMES = ("http", "https")


class ConnectionPlatformDetail(ConfigModel):
    platform: str = Field(
        description="DataHub platform of the connection, for example `bigquery` or `postgres`.",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance of the datasets reached through this connection.",
    )
    env: Optional[str] = Field(
        default=None,
        description="Environment of the datasets reached through this connection. Defaults to the source `env`.",
    )
    database: Optional[str] = Field(
        default=None,
        description=(
            "Top-level name the river does not carry. For BigQuery this is the "
            "project ID and is required for target lineage. For Redshift, Postgres "
            "and SQL Server it is prefixed to `schema.table`. For Databricks it is "
            "used when the river has no catalog."
        ),
    )
    convert_urns_to_lowercase: Optional[bool] = Field(
        default=None,
        description="Overrides `convert_lineage_urns_to_lowercase` for this connection.",
    )


class BoomiDataIntegrationSourceConfig(
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
    StatefulIngestionConfigBase,
):
    account_id: str = Field(
        description="Boomi Data Integration account ID.",
    )
    region: DIRegion = Field(
        default=DIRegion.US,
        description="Region the account is hosted in: `us`, `eu`, `il` or `au`. Ignored when `api_url` is set.",
    )
    api_url: Optional[str] = Field(
        default=None,
        description="API host, for example `https://api.rivery.io`. Overrides `region`.",
    )
    api_token: Optional[SecretStr] = Field(
        default=None,
        description=(
            "A single API token. The source plugin lists the environments the "
            "token can see and skips any it is not allowed to read. Set either "
            "this or `environment_tokens`."
        ),
    )
    environment_tokens: Optional[Dict[str, SecretStr]] = Field(
        default=None,
        description=(
            "API tokens keyed by environment ID. Boomi scopes each token to one "
            "environment, so use this to ingest several environments. Set either "
            "this or `api_token`."
        ),
    )
    environment_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns matched against the environment name.",
    )
    group_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns matched against the group name. Rivers in denied groups are skipped.",
    )
    river_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns matched against the river name.",
    )
    include_lineage: bool = Field(
        default=True,
        description="Emit lineage from rivers and logic steps to the tables they load.",
    )
    connection_type_platform_map: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Overrides the DataHub platform for a Data Integration target type, "
            'for example `{"azure_sql": "mssql"}`.'
        ),
    )
    connection_to_platform: Dict[str, ConnectionPlatformDetail] = Field(
        default_factory=dict,
        description=(
            "Platform details keyed by connection ID (`cross_id`). Needed for "
            "BigQuery targets, whose project is not part of the river."
        ),
    )
    # Not named convert_urns_to_lowercase: that name turns on the framework's
    # blanket URN lowercasing, which would undo per-connection opt-outs.
    convert_lineage_urns_to_lowercase: bool = Field(
        default=True,
        description="Lowercase the table names in dataset URNs built for lineage.",
    )
    max_requests_per_second: int = Field(
        default=DEFAULT_MAX_REQUESTS_PER_SECOND,
        gt=0,
        le=MAX_REQUESTS_PER_SECOND_LIMIT,
        description="Client-side cap on API calls per second.",
    )
    request_timeout_seconds: int = Field(
        default=DEFAULT_REQUEST_TIMEOUT_SECONDS,
        gt=0,
        description="Timeout for each API request.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion and stale entity removal.",
    )

    @field_validator("api_url")
    @classmethod
    def _validate_api_url(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        parsed = urlparse(value)
        if parsed.scheme not in _HTTP_SCHEMES or not parsed.netloc:
            raise ValueError(
                f"api_url must be an http(s) URL such as https://api.rivery.io, got {value!r}"
            )
        return value.rstrip("/")

    @model_validator(mode="after")
    def _validate_tokens(self) -> "BoomiDataIntegrationSourceConfig":
        if (self.api_token is None) == (self.environment_tokens is None):
            raise ValueError("Set exactly one of api_token or environment_tokens.")
        if self.environment_tokens is not None and not self.environment_tokens:
            raise ValueError("environment_tokens must not be empty.")
        return self

    @property
    def resolved_api_url(self) -> str:
        return self.api_url or REGION_API_URLS[self.region]
