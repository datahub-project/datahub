import re
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse

from pydantic import Field, validator

from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.configuration.source_common import DEFAULT_ENV, DatasetSourceConfigBase
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
)
from datahub.utilities import config_clean


class PulsarSourceStatefulIngestionConfig(StatefulIngestionConfig):
    """
    Specialization of the basic StatefulIngestionConfig to add custom config.
    This will be used to override the stateful_ingestion config param of StatefulIngestionConfigBase
    in the PulsarSourceConfig.
    """

    remove_stale_metadata: bool = True


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


class PulsarSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigBase):
    env: str = DEFAULT_ENV
    # The web URL for the cluster.
    web_service_url: str = "http://localhost:8080"
    # Timout setting, how long to wait for the Pulsar rest api to send data before giving up
    timeout: int = 5
    # Mandatory for oauth authentication
    issuer_url: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    # Mandatory for token authentication
    token: Optional[str] = None
    # Either a boolean, in which case it controls whether we verify the server's TLS certificate, or a string,
    # in which case it must be a path to a CA bundle to use.
    verify_ssl: Union[bool, str] = True
    # By default, allow all topics and deny the pulsar system topics
    tenant_patterns: AllowDenyPattern = AllowDenyPattern(allow=[".*"], deny=["pulsar"])
    namespace_patterns: AllowDenyPattern = AllowDenyPattern(
        allow=[".*"], deny=["public/functions"]
    )
    topic_patterns: AllowDenyPattern = AllowDenyPattern(allow=[".*"], deny=["/__.*$"])
    # Exclude partition topics. e.g. topics ending on _partition_N where N is a number
    exclude_individual_partitions: bool = True
    # Listing all tenants requires superUser role, alternative you can set tenants you want to scrape
    # using the tenant admin role
    tenants: List[str] = []

    domain: Dict[str, AllowDenyPattern] = dict()
    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[PulsarSourceStatefulIngestionConfig] = None

    # Placeholder for OpenId discovery document
    oid_config: dict = Field(default_factory=dict)

    @validator("token")
    def ensure_only_issuer_or_token(
        cls, token: Optional[str], values: Dict[str, Optional[str]]
    ) -> Optional[str]:
        if token is not None and values.get("issuer_url") is not None:
            raise ConfigurationError(
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
            raise ConfigurationError(
                "Missing configuration: client_id and client_secret are mandatory when issuer_url is set."
            )
        return client_secret

    @validator("web_service_url")
    def web_service_url_scheme_host_port(cls, val: str) -> str:
        # Tokenize the web url
        url = urlparse(val)

        if url.scheme not in ["http", "https"]:
            raise ConfigurationError(
                f"Scheme should be http or https, found {url.scheme}"
            )

        if not _is_valid_hostname(url.hostname.__str__()):
            raise ConfigurationError(
                f"Not a valid hostname, hostname contains invalid characters, found {url.hostname}"
            )

        return config_clean.remove_trailing_slashes(val)
