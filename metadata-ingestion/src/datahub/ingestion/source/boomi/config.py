from typing import Optional
from urllib.parse import urlparse

from pydantic import Field, SecretStr, field_validator

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.boomi.constants import (
    DEFAULT_API_URL,
    DEFAULT_MAX_REQUESTS_PER_SECOND,
    DEFAULT_REQUEST_TIMEOUT_SECONDS,
    LOOPBACK_HOSTS,
    MAX_REQUESTS_PER_SECOND_LIMIT,
    SCHEME_HTTP,
    SCHEME_HTTPS,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulIngestionConfigBase,
    StatefulStaleMetadataRemovalConfig,
)


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
            "`include_undeployed_processes`."
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
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                f"api_url must be a base URL such as https://api.boomi.com, without a query or fragment, got {value!r}"
            )
        # The token is sent as basic auth, so plain http is only allowed for
        # local test servers.
        if parsed.scheme == SCHEME_HTTP and parsed.hostname not in LOOPBACK_HOSTS:
            raise ValueError(
                f"api_url must use https for non-loopback hosts, got {value!r}"
            )
        return value.rstrip("/")
