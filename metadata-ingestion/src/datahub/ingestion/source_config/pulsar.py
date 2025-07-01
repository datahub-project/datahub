import re
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse

from pydantic import Field, validator

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
from datahub.utilities import config_clean


def _is_valid_hostname(hostname: str) -> bool:
    """
    Loosely ascii hostname validation. A hostname is considered valid when the total length does not exceed 253
    characters, contains valid characters and are max 63 octets per label.
    """
    if len(hostname) > 253:
        return False
    # Hostnames ending on a dot are valid, if present strip exactly one
    if hostname[-1] == ".":
        hostname = hostname[:-1]
    allowed = re.compile(r"(?!-)[A-Z\d-]{1,63}(?<!-)$", re.IGNORECASE)
    return all(allowed.match(x) for x in hostname.split("."))


class PulsarSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    web_service_url: str = Field(
        default="http://localhost:8080", description="The web URL for the cluster."
    )
    timeout: int = Field(
        default=5,
        description="Timout setting, how long to wait for the Pulsar rest api to send data before giving up",
    )
    # Mandatory for oauth authentication
    issuer_url: Optional[str] = Field(
        default=None,
        description="The complete URL for a Custom Authorization Server. Mandatory for OAuth based authentication.",
    )
    client_id: Optional[str] = Field(
        default=None, description="The application's client ID"
    )
    client_secret: Optional[str] = Field(
        default=None, description="The application's client secret"
    )
    # Mandatory for token authentication
    token: Optional[str] = Field(
        default=None,
        description="The access token for the application. Mandatory for token based authentication.",
    )
    verify_ssl: Union[bool, str] = Field(
        default=True,
        description="Either a boolean, in which case it controls whether we verify the server's TLS certificate, or a string, in which case it must be a path to a CA bundle to use.",
    )
    # By default, allow all topics and deny the pulsar system topics
    tenant_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern(allow=[".*"], deny=["pulsar"]),
        description="List of regex patterns for tenants to include/exclude from ingestion. By default all tenants are allowed.",
    )
    namespace_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern(allow=[".*"], deny=["public/functions"]),
        description="List of regex patterns for namespaces to include/exclude from ingestion. By default the functions namespace is denied.",
    )
    topic_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern(allow=[".*"], deny=["/__.*$"]),
        description="List of regex patterns for topics to include/exclude from ingestion. By default the Pulsar system topics are denied.",
    )
    exclude_individual_partitions: bool = Field(
        default=True,
        description="Extract each individual partitioned topic. e.g. when turned off a topic with 100 partitions will result in 100 Datasets.",
    )

    tenants: List[str] = Field(
        default=[],
        description="Listing all tenants requires superUser role, alternative you can set a list of tenants you want to scrape using the tenant admin role",
    )

    domain: Dict[str, AllowDenyPattern] = Field(
        default_factory=dict, description="Domain patterns"
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="see Stateful Ingestion"
    )

    oid_config: dict = Field(
        default_factory=dict, description="Placeholder for OpenId discovery document"
    )

    @validator("token")
    def ensure_only_issuer_or_token(
        cls, token: Optional[str], values: Dict[str, Optional[str]]
    ) -> Optional[str]:
        if token is not None and values.get("issuer_url") is not None:
            raise ValueError(
                "Expected only one authentication method, either issuer_url or token."
            )
        return token

    @validator("client_secret", always=True)
    def ensure_client_id_and_secret_for_issuer_url(
        cls, client_secret: Optional[str], values: Dict[str, Optional[str]]
    ) -> Optional[str]:
        if values.get("issuer_url") is not None and (
            client_secret is None or values.get("client_id") is None
        ):
            raise ValueError(
                "Missing configuration: client_id and client_secret are mandatory when issuer_url is set."
            )
        return client_secret

    @validator("web_service_url")
    def web_service_url_scheme_host_port(cls, val: str) -> str:
        # Tokenize the web url
        url = urlparse(val)

        if url.scheme not in ["http", "https"]:
            raise ValueError(f"Scheme should be http or https, found {url.scheme}")

        if not _is_valid_hostname(url.hostname.__str__()):
            raise ValueError(
                f"Not a valid hostname, hostname contains invalid characters, found {url.hostname}"
            )

        return config_clean.remove_trailing_slashes(val)
