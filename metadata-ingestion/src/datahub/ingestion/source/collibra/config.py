from enum import Enum
from typing import Dict, Optional

from pydantic import Field, SecretStr, model_validator

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.collibra.constants import PAGE_SIZE


class CollibraAuthMethod(str, Enum):
    OAUTH = "oauth"
    BASIC = "basic"
    TOKEN = "token"
    JWT = "jwt"
    SESSION = "session"


class CollibraSourceConfig(EnvConfigMixin, PlatformInstanceConfigMixin):
    url: str = Field(description="Base URL of the Collibra Platform / DGC instance.")
    auth_method: CollibraAuthMethod = Field(
        default=CollibraAuthMethod.OAUTH,
        description="Authentication method: oauth, basic, token, jwt, or session.",
    )
    client_id: Optional[str] = Field(
        default=None, description="OAuth2 client id (auth_method=oauth)."
    )
    client_secret: Optional[SecretStr] = Field(
        default=None, description="OAuth2 client secret (auth_method=oauth)."
    )
    username: Optional[str] = Field(
        default=None, description="Username (auth_method=basic or session)."
    )
    password: Optional[SecretStr] = Field(
        default=None, description="Password (auth_method=basic or session)."
    )
    token: Optional[SecretStr] = Field(
        default=None,
        description="Static bearer or JWT token (auth_method=token or jwt).",
    )
    verify_ssl: bool = Field(
        default=True, description="Verify TLS certificates on Collibra requests."
    )
    ca_cert_path: Optional[str] = Field(
        default=None,
        description="Path to a private CA bundle if Collibra uses a private CA.",
    )
    request_timeout: float = Field(
        default=60.0, description="Per-request timeout in seconds."
    )
    poll_interval: float = Field(
        default=2.0, description="Seconds between Output Module job status polls."
    )
    poll_max_attempts: int = Field(
        default=900,
        description="Max Output Module job status polls before giving up.",
    )
    max_workers: int = Field(
        default=4,
        description="Parallel extraction workers; also the HTTP connection pool size.",
    )
    page_size: int = Field(
        default=PAGE_SIZE, description="Page size for REST paging (DGC caps at 1000)."
    )
    force_paging: bool = Field(
        default=False,
        description="Use the REST/Output-Module paths even if GraphQL is available.",
    )
    force_offset_paging: bool = Field(
        default=False,
        description="Use offset paging instead of cursor paging (older DGC versions).",
    )
    community_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex allow/deny patterns for community names.",
    )
    domain_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex allow/deny patterns for domain names.",
    )
    asset_type_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex allow/deny patterns for asset-type names.",
    )
    type_mapping: Dict[str, str] = Field(
        default_factory=dict,
        description="Collibra type UUID -> DataHub target mapping.",
    )

    @model_validator(mode="after")
    def _validate_auth(self) -> "CollibraSourceConfig":
        method = self.auth_method
        missing = []
        if method == CollibraAuthMethod.OAUTH and not (
            self.client_id and self.client_secret
        ):
            missing = ["client_id", "client_secret"]
        elif method in (CollibraAuthMethod.BASIC, CollibraAuthMethod.SESSION) and not (
            self.username and self.password
        ):
            missing = ["username", "password"]
        elif method in (CollibraAuthMethod.TOKEN, CollibraAuthMethod.JWT) and (
            not self.token
        ):
            missing = ["token"]
        if missing:
            raise ValueError(
                f"auth_method={method.value} requires: {', '.join(missing)}"
            )
        return self
