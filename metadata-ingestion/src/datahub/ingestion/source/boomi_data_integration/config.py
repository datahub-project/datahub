from typing import Dict, List, Optional
from urllib.parse import urlparse

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.boomi_data_integration.constants import (
    API_CONSOLE_URLS,
    DEFAULT_MAX_REQUESTS_PER_SECOND,
    DEFAULT_MAX_RUNS_PER_RIVER,
    DEFAULT_REQUEST_TIMEOUT_SECONDS,
    DEFAULT_RUN_HISTORY_DAYS,
    HTTP_SCHEME,
    HTTPS_SCHEME,
    LOOPBACK_HOSTS,
    MAX_REQUESTS_PER_SECOND_LIMIT,
    MAX_RUN_HISTORY_DAYS,
    REGION_API_URLS,
    DIRegion,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulIngestionConfigBase,
    StatefulStaleMetadataRemovalConfig,
)


def _blank_to_none(value: Optional[SecretStr]) -> Optional[SecretStr]:
    if value is None or not value.get_secret_value().strip():
        return None
    return value


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
            "Top-level name the river does not carry. On a source connection it "
            "is the source database, required for source lineage from Postgres, "
            "SQL Server, Snowflake, Redshift and BigQuery (the project ID); MySQL "
            "and MariaDB sources do not need it. On a target connection it is the "
            "BigQuery project ID, required for target lineage; for Redshift, "
            "Postgres and SQL Server it is prefixed to `schema.table`; for "
            "Databricks it is used when the river has no catalog. It is also the "
            "default database for unqualified tables in custom queries and logic "
            "SQL steps."
        ),
    )
    default_schema: Optional[str] = Field(
        default=None,
        description="Default schema for unqualified tables in custom queries and logic SQL steps.",
    )
    convert_urns_to_lowercase: Optional[bool] = Field(
        default=None,
        description="Overrides `convert_lineage_urns_to_lowercase` for this connection.",
    )


class EnvironmentToken(ConfigModel):
    environment_id: str = Field(
        description="Boomi Data Integration environment ID.",
    )
    api_token: SecretStr = Field(
        description="API token scoped to this environment.",
    )

    @field_validator("environment_id")
    @classmethod
    def _validate_environment_id(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("environment_id must not be blank.")
        return value

    @field_validator("api_token")
    @classmethod
    def _validate_api_token(cls, value: SecretStr) -> SecretStr:
        if _blank_to_none(value) is None:
            raise ValueError("api_token must not be blank.")
        return value


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
        description=(
            "API host, for example `https://api.rivery.io`. Overrides `region`. "
            "Must use https, except for a loopback host."
        ),
    )
    console_url: Optional[str] = Field(
        default=None,
        description=(
            "Console base URL used for links to logic rivers and runs, for example "
            "`https://console.rivery.io`. Links are emitted for the `us` region "
            "without this setting; set it for other regions. Must be an https base "
            "URL without a query or fragment."
        ),
    )
    api_token: Optional[SecretStr] = Field(
        default=None,
        description=(
            "A single API token. The source plugin lists the environments the "
            "token can see and skips any it is not allowed to read. Set either "
            "this or `environment_tokens`."
        ),
    )
    environment_tokens: Optional[List[EnvironmentToken]] = Field(
        default=None,
        description=(
            "One `environment_id` and `api_token` pair per environment. Boomi "
            "scopes each token to one environment, so use this to ingest several "
            "environments. An environment listed here that cannot be read fails "
            "the run, so stale entity removal does not delete its entities. Set "
            "either this or `api_token`."
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
        description=(
            "Emit lineage from the tables rivers and logic steps read to the "
            "tables they load."
        ),
    )
    include_column_lineage: bool = Field(
        default=True,
        description=(
            "Emit column lineage from multi-table column mappings and from SQL in "
            "custom queries and logic SQL steps. Only used when `include_lineage` "
            "is on."
        ),
    )
    extract_owners: bool = Field(
        default=False,
        description=(
            "Emit the river creator as a data owner and the last editor as a "
            "technical owner. Rivers only carry display names, so they are "
            "resolved to emails through the users list, which needs the "
            "`user:list` scope; names that match no user or several users are "
            "skipped."
        ),
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
            "the database of most source connections and for BigQuery targets, "
            "since rivers and connections carry neither."
        ),
    )
    # Not named convert_urns_to_lowercase: that name turns on the framework's
    # blanket URN lowercasing, which would undo per-connection opt-outs.
    convert_lineage_urns_to_lowercase: bool = Field(
        default=True,
        description="Lowercase the table names in dataset URNs built for lineage.",
    )
    include_run_history: bool = Field(
        default=True,
        description=(
            "Emit recent river runs as run history (DataProcessInstance). Needs "
            "the `activity:list` scope (Activities: View in the console)."
        ),
    )
    run_history_days: int = Field(
        default=DEFAULT_RUN_HISTORY_DAYS,
        ge=1,
        le=MAX_RUN_HISTORY_DAYS,
        description=(
            "How many days of run history to read. Data Integration keeps run "
            "logs for two weeks."
        ),
    )
    max_runs_per_river: int = Field(
        default=DEFAULT_MAX_RUNS_PER_RIVER,
        gt=0,
        description="Most recent runs to emit for each river.",
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
        if not parsed.netloc:
            raise ValueError(
                f"api_url must be a URL such as https://api.rivery.io, got {value!r}"
            )
        # Plain http would send the bearer token in the clear; loopback is
        # allowed for local proxies and tests.
        if parsed.scheme != HTTPS_SCHEME and not (
            parsed.scheme == HTTP_SCHEME and parsed.hostname in LOOPBACK_HOSTS
        ):
            raise ValueError(
                f"api_url must use https unless the host is loopback, got {value!r}"
            )
        return value.rstrip("/")

    @field_validator("console_url")
    @classmethod
    def _validate_console_url(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        parsed = urlparse(value)
        # River paths are appended to this base, so a query or fragment would
        # swallow them.
        if (
            parsed.scheme != HTTPS_SCHEME
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                # The value is left out because it may carry a password.
                "console_url must be an https base URL such as https://console.rivery.io, "
                "without credentials, a query or a fragment"
            )
        return value.rstrip("/")

    @field_validator("api_token")
    @classmethod
    def _blank_api_token_is_unset(
        cls, value: Optional[SecretStr]
    ) -> Optional[SecretStr]:
        return _blank_to_none(value)

    @model_validator(mode="after")
    def _validate_tokens(self) -> "BoomiDataIntegrationSourceConfig":
        if (self.api_token is None) == (self.environment_tokens is None):
            raise ValueError("Set exactly one of api_token or environment_tokens.")
        if self.environment_tokens is not None:
            if not self.environment_tokens:
                raise ValueError("environment_tokens must not be empty.")
            ids = [entry.environment_id for entry in self.environment_tokens]
            if len(ids) != len(set(ids)):
                raise ValueError("environment_tokens lists an environment_id twice.")
        return self

    @property
    def resolved_api_url(self) -> str:
        return self.api_url or REGION_API_URLS[self.region]

    @property
    def resolved_console_url(self) -> Optional[str]:
        return self.console_url or API_CONSOLE_URLS.get(self.resolved_api_url)
