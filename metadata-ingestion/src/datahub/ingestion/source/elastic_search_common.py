import base64
from typing import Any, Dict, Optional, Tuple, Union

from opensearchpy import OpenSearch
from pydantic import field_validator
from pydantic.fields import Field

from datahub.configuration.common import ConfigModel, TransparentSecretStr
from datahub.configuration.validate_host_port import validate_host_port
from datahub.utilities.config_clean import remove_protocol


def _api_key_authorization(api_key: Union[Tuple[str, str], str]) -> str:
    # An (id, api_key) pair is base64-encoded as "id:api_key"; a plain string is passed
    # through as the already-encoded token.
    if isinstance(api_key, tuple):
        token = base64.b64encode(f"{api_key[0]}:{api_key[1]}".encode()).decode()
    else:
        token = api_key
    return f"ApiKey {token}"


class ElasticsearchConnectionConfig(ConfigModel):
    """Connection settings for an Elasticsearch/OpenSearch cluster.

    Extracted so any consumer (the Elasticsearch source, the sql-queries query
    log reader, etc.) can build an authenticated client the same way.
    """

    host: str = Field(
        default="localhost:9200", description="The elastic search host URI."
    )
    username: Optional[str] = Field(
        default=None, description="The username credential."
    )
    password: Optional[TransparentSecretStr] = Field(
        default=None, description="The password credential."
    )
    api_key: Optional[Union[Tuple[str, str], str]] = Field(
        default=None,
        description="API Key authentication. Accepts either a list with id and api_key (UTF-8 representation), or a base64 encoded string of id and api_key combined by ':'.",
    )

    use_ssl: bool = Field(
        default=False, description="Whether to use SSL for the connection or not."
    )

    verify_certs: bool = Field(
        default=False, description="Whether to verify SSL certificates."
    )

    ca_certs: Optional[str] = Field(
        default=None, description="Path to a certificate authority (CA) certificate."
    )

    client_cert: Optional[str] = Field(
        default=None,
        description="Path to the file containing the private key and the certificate, or cert only if using client_key.",
    )

    client_key: Optional[str] = Field(
        default=None,
        description="Path to the file containing the private key if using separate cert and key files.",
    )

    ssl_assert_hostname: bool = Field(
        default=False, description="Use hostname verification if not False."
    )

    ssl_assert_fingerprint: Optional[str] = Field(
        default=None,
        description="Verify the supplied certificate fingerprint if not None.",
    )

    url_prefix: str = Field(
        default="",
        description="There are cases where an enterprise would have multiple elastic search clusters. One way for them to manage is to have a single endpoint for all the elastic search clusters and use url_prefix for routing requests to different clusters.",
    )

    @field_validator("host", mode="after")
    @classmethod
    def host_colon_port_comma(cls, host_val: str) -> str:
        for entry in host_val.split(","):
            entry = remove_protocol(entry)
            for suffix in ["/"]:
                if entry.endswith(suffix):
                    entry = entry[: -len(suffix)]
            validate_host_port(entry)
        return host_val

    @property
    def http_auth(self) -> Optional[Tuple[str, str]]:
        return (
            None
            if self.username is None
            else (
                self.username,
                self.password.get_secret_value() if self.password else "",
            )
        )

    def get_client(self) -> OpenSearch:
        # opensearch-py has no api_key param and silently drops unknown kwargs, so it must
        # be sent as an Authorization header instead.
        extra_client_args: Dict[str, Any] = {}
        if self.api_key is not None:
            extra_client_args["headers"] = {
                "Authorization": _api_key_authorization(self.api_key)
            }
        return OpenSearch(
            self.host,
            http_auth=self.http_auth,
            use_ssl=self.use_ssl,
            verify_certs=self.verify_certs,
            ca_certs=self.ca_certs,
            client_cert=self.client_cert,
            client_key=self.client_key,
            ssl_assert_hostname=self.ssl_assert_hostname,
            ssl_assert_fingerprint=self.ssl_assert_fingerprint,
            url_prefix=self.url_prefix,
            **extra_client_args,
        )
