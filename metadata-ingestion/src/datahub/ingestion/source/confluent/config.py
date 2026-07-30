from typing import List, Optional, Tuple

from pydantic import Field, model_validator

from datahub.configuration.common import ConfigModel, TransparentSecretStr
from datahub.ingestion.source.confluent.constants import (
    CATALOG_GRAPHQL_PATH,
    DEFAULT_PAGE_SIZE,
    DEFAULT_TIMEOUT_SECONDS,
    MAX_PAGE_SIZE,
)


class ConfluentStreamCatalogConfig(ConfigModel):
    """
    Sources subclass this to add their own `include_*` toggles. Credentials are only
    validated for presence when a source calls `validate_connection`, because some
    sources can inherit the endpoint and key from an existing Schema Registry
    connection rather than having them set here.
    """

    enabled: bool = Field(
        default=False,
        description="Query the Confluent Cloud Stream Catalog GraphQL API. "
        "Confluent Cloud only, and requires Stream Governance to be enabled on the environment.",
    )
    schema_registry_url: Optional[str] = Field(
        default=None,
        description="Schema Registry endpoint, e.g. `https://psrc-xxxxx.us-east-1.aws.confluent.cloud`. "
        f"The Stream Catalog GraphQL endpoint is derived as `<this>{CATALOG_GRAPHQL_PATH}`.",
    )
    api_key: Optional[str] = Field(
        default=None,
        description="Schema Registry API key, used for Basic authentication against the Stream Catalog.",
    )
    api_secret: Optional[TransparentSecretStr] = Field(
        default=None,
        description="Schema Registry API secret, used for Basic authentication against the Stream Catalog.",
    )
    page_size: int = Field(
        default=DEFAULT_PAGE_SIZE,
        description="Number of entities to request per Stream Catalog GraphQL page.",
    )
    timeout_seconds: int = Field(
        default=DEFAULT_TIMEOUT_SECONDS,
        description="Timeout in seconds for each Stream Catalog GraphQL request.",
    )

    @model_validator(mode="after")
    def validate_catalog_settings(self) -> "ConfluentStreamCatalogConfig":
        if not self.enabled:
            return self

        if self.schema_registry_url:
            if not self.schema_registry_url.startswith(("http://", "https://")):
                raise ValueError(
                    "Configuration error: 'schema_registry_url' must be a valid HTTP(S) URL. "
                    f"Got: '{self.schema_registry_url}'. "
                    "Expected format: https://psrc-xxxxx.region.provider.confluent.cloud"
                )
            self.schema_registry_url = self.schema_registry_url.rstrip("/")

        if not 1 <= self.page_size <= MAX_PAGE_SIZE:
            raise ValueError(
                "Configuration error: 'page_size' must be between 1 and "
                f"{MAX_PAGE_SIZE}. Got: {self.page_size}."
            )

        if self.timeout_seconds <= 0:
            raise ValueError(
                f"Configuration error: 'timeout_seconds' must be positive. Got: {self.timeout_seconds}."
            )

        return self

    def validate_connection(self, config_path: str) -> None:
        """
        Sources call this once any inherited defaults have been applied. `config_path`
        names the recipe block the fields live under, so the error points at the right
        place (e.g. `confluent_catalog`).
        """
        missing: List[str] = [
            name
            for name, value in (
                ("schema_registry_url", self.schema_registry_url),
                ("api_key", self.api_key),
                ("api_secret", self.api_secret),
            )
            if not value
        ]
        if not missing:
            return

        raise ValueError(
            f"Configuration error: '{config_path}.enabled' is true but "
            f"{', '.join(repr(f'{config_path}.{name}') for name in missing)} "
            f"{'is' if len(missing) == 1 else 'are'} not set. "
            "The Stream Catalog requires a Schema Registry endpoint and API key/secret."
        )

    def get_graphql_endpoint(self) -> str:
        assert self.schema_registry_url is not None, (
            "schema_registry_url is required when the Stream Catalog is enabled"
        )
        return f"{self.schema_registry_url}{CATALOG_GRAPHQL_PATH}"

    def get_credentials(self) -> Tuple[str, str]:
        assert self.api_key is not None and self.api_secret is not None, (
            "api_key and api_secret are required when the Stream Catalog is enabled"
        )
        return self.api_key, self.api_secret.get_secret_value()
