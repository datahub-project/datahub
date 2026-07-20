from typing import Optional

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class TibcoEmsSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    base_url: str = Field(
        description="Base URL of the TIBCO EMS REST Proxy admin API, e.g. "
        "`https://ems-host.example.com:8080`.",
    )
    username: Optional[SecretStr] = Field(
        default=None, description="Username for HTTP basic authentication."
    )
    password: Optional[SecretStr] = Field(
        default=None, description="Password for HTTP basic authentication."
    )
    token: Optional[SecretStr] = Field(
        default=None,
        description="Bearer token used instead of basic auth (e.g. an OAuth2 access token).",
    )
    ca_certificate_path: Optional[str] = Field(
        default=None,
        description="Path to a CA bundle used to verify the server's TLS certificate.",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Whether to verify the server's TLS certificate. Prefer "
        "`ca_certificate_path` for private CAs over disabling verification.",
    )
    timeout: int = Field(default=30, description="Per-request timeout in seconds.")
    queue_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter EMS queues (by name) for ingestion.",
    )
    topic_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter EMS topics (by name) for ingestion.",
    )
    include_system_destinations: bool = Field(
        default=False,
        description="Ingest EMS internal destinations (names starting with `$sys.` "
        "or `$TMP$`). Disabled by default as these are not business data flows.",
    )
    include_bridges: bool = Field(
        default=True,
        description="Emit lineage between destinations derived from configured EMS bridges.",
    )
    emit_column_lineage: bool = Field(
        default=False,
        description="Also emit column-level lineage for bridges. An EMS bridge copies "
        "whole messages unchanged, so fields are matched by name using schemas read "
        "from DataHub (populated for the destinations by a schema-registry or other "
        "connector). Best-effort: emitted only where both endpoints have a schema and "
        "share field names. Requires a DataHub graph to be available.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion config for stale entity removal.",
    )

    @field_validator("base_url")
    @classmethod
    def _strip_trailing_slash(cls, value: str) -> str:
        return value.rstrip("/")

    @model_validator(mode="after")
    def _require_credentials(self) -> "TibcoEmsSourceConfig":
        has_basic = self.username is not None and self.password is not None
        has_token = self.token is not None
        if not (has_basic or has_token):
            raise ValueError(
                "Provide credentials via username/password or a bearer token."
            )
        return self
