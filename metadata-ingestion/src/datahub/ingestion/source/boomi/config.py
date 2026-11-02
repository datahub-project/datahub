from typing import Dict, Optional
from urllib.parse import urlparse

from pydantic import Field, SecretStr, field_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.emitter.mce_builder import ALL_ENV_TYPES
from datahub.ingestion.source.boomi.constants import (
    DEFAULT_API_URL,
    DEFAULT_EXECUTION_HISTORY_DAYS,
    DEFAULT_MAX_EXECUTIONS_PER_PROCESS,
    DEFAULT_MAX_REQUESTS_PER_SECOND,
    DEFAULT_REQUEST_TIMEOUT_SECONDS,
    LOOPBACK_HOSTS,
    MAX_EXECUTION_HISTORY_DAYS,
    MAX_REQUESTS_PER_SECOND_LIMIT,
    SCHEME_HTTP,
    SCHEME_HTTPS,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulIngestionConfigBase,
    StatefulStaleMetadataRemovalConfig,
)


class BoomiConnectionMapping(ConfigModel):
    platform: str = Field(
        description="DataHub platform of the system behind the connection, for example `mssql` or `snowflake`.",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance used when that system was ingested into DataHub.",
    )
    env: Optional[str] = Field(
        default=None,
        description="Environment of the datasets. Defaults to the source's `env`.",
    )
    database: Optional[str] = Field(
        default=None,
        description=(
            "Database for table names that do not include one. Overrides the "
            "database read from the connection."
        ),
    )
    default_schema: Optional[str] = Field(
        default=None,
        description=(
            "Schema for table names that do not include one. Overrides the "
            "schema read from the connection."
        ),
    )
    convert_urns_to_lowercase: bool = Field(
        default=True,
        description=(
            "Lowercase table names read from operations and profiles. Set to "
            "`false` when the system was ingested with case-preserving URNs."
        ),
    )

    @field_validator("env")
    @classmethod
    def _validate_env(cls, value: Optional[str]) -> Optional[str]:
        # Same rule as EnvConfigMixin, so `dev` and the source's `DEV` give
        # the same dataset URNs.
        if value is None:
            return None
        if value.upper() not in ALL_ENV_TYPES:
            raise ValueError(f"env must be one of {ALL_ENV_TYPES}, found {value}")
        return value.upper()


class BoomiSourceConfig(
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
    StatefulIngestionConfigBase,
):
    account_id: str = Field(
        description="Boomi account ID, for example `mycompany-AB1CD2`.",
    )
    username: str = Field(
        description=(
            "Boomi user that owns the API token. The source plugin sends it as "
            "`BOOMI_TOKEN.<username>`, so do not add the prefix yourself."
        ),
    )
    api_token: SecretStr = Field(
        description="Platform API token created under Settings > My User Settings > Platform API Tokens.",
    )
    api_url: str = Field(
        default=DEFAULT_API_URL,
        description=(
            "Platform API host. Use `https://api.platform.gb.boomi.com` for accounts "
            "hosted in the UK. Must use https unless the host is a loopback address."
        ),
    )
    branch_name: Optional[str] = Field(
        default=None,
        description=(
            "Only ingest components on this branch. Set it when Branch and Merge is "
            "enabled on the account, usually to `main`. Leave unset otherwise."
        ),
    )
    folder_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns matched against the folder full path, for example "
            "`MyAccount/Production/Orders`. Processes in denied folders are skipped."
        ),
    )
    process_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns matched against the process name.",
    )
    environment_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns matched against the environment name. Only deployments "
            "to allowed environments count towards deployment properties and "
            "`include_undeployed_processes`. Execution records are not filtered by "
            "environment directly, because they name the runtime (atom), not the "
            "environment. Runs follow the ingested processes."
        ),
    )
    include_undeployed_processes: bool = Field(
        default=False,
        description=(
            "Also ingest processes that have no active deployment in an allowed "
            "environment. If the token cannot read deployments, all processes "
            "are ingested."
        ),
    )
    extract_owners: bool = Field(
        default=True,
        description=(
            "Emit the process creator as a data owner and the last modifier as a "
            "technical owner."
        ),
    )
    include_lineage: bool = Field(
        default=True,
        description=(
            "Emit lineage from each process to the datasets its connector "
            "operations read and write, and to the processes it calls. Needs "
            "one API call per distinct connection, operation and database profile."
        ),
    )
    connection_type_platform_map: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Map a connector `subType` to a DataHub platform, for connectors the "
            "source plugin cannot resolve itself, for example "
            "`{'officialboomi-X3979C-snowfl-prod': 'snowflake'}`. Experimental: "
            "the operation's object type ID is used as the table name."
        ),
    )
    connection_to_platform: Dict[str, BoomiConnectionMapping] = Field(
        default_factory=dict,
        description=(
            "Per-connection overrides keyed by the connection component ID. An "
            "entry wins over everything the source plugin reads from the "
            "connection, and supplies the platform instance, database and "
            "schema needed for URNs to match other sources."
        ),
    )
    include_execution_history: bool = Field(
        default=True,
        description=(
            "Emit recent process executions as run history (DataProcessInstance) "
            "on each ingested process. Requires the View Results privilege."
        ),
    )
    execution_history_days: int = Field(
        default=DEFAULT_EXECUTION_HISTORY_DAYS,
        ge=1,
        le=MAX_EXECUTION_HISTORY_DAYS,
        description=(
            "How many days of execution history to read. Boomi purges execution "
            "history after 30 days by default, so older runs are not available."
        ),
    )
    max_executions_per_process: int = Field(
        default=DEFAULT_MAX_EXECUTIONS_PER_PROCESS,
        gt=0,
        description="Most recent executions to emit for each process.",
    )
    max_requests_per_second: int = Field(
        default=DEFAULT_MAX_REQUESTS_PER_SECOND,
        gt=0,
        le=MAX_REQUESTS_PER_SECOND_LIMIT,
        description=(
            "Client-side cap on Platform API calls per second. Boomi allows 10 per "
            "account, shared with any other API client."
        ),
    )
    request_timeout_seconds: int = Field(
        default=DEFAULT_REQUEST_TIMEOUT_SECONDS,
        gt=0,
        description="Timeout for each Platform API request.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion and stale entity removal.",
    )

    @field_validator("api_url")
    @classmethod
    def _validate_api_url(cls, value: str) -> str:
        parsed = urlparse(value)
        if (
            parsed.scheme not in (SCHEME_HTTP, SCHEME_HTTPS)
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                # The value is left out because it may carry a password.
                "api_url must be a base URL such as https://api.boomi.com, without credentials, a query or a fragment"
            )
        # The client appends /api/rest/v1/<account_id>, so a full Platform API
        # URL here would double the prefix.
        if parsed.path not in ("", "/"):
            raise ValueError(
                f"api_url must be only the host, such as https://api.boomi.com, without a path, got {value!r}"
            )
        # The token is sent as basic auth, so plain http is only allowed for
        # local test servers.
        if parsed.scheme == SCHEME_HTTP and parsed.hostname not in LOOPBACK_HOSTS:
            raise ValueError(
                f"api_url must use https for non-loopback hosts, got {value!r}"
            )
        return value.rstrip("/")
