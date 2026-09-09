from typing import List, Optional, Tuple
from urllib.parse import urlparse

from pydantic import Field, model_validator

from datahub.configuration.common import ConfigModel, TransparentSecretStr
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.confluent.constants import (
    CATALOG_GRAPHQL_PATH,
    CONFLUENT_CLOUD_DOMAIN_SUFFIX,
    DEFAULT_PAGE_SIZE,
    DEFAULT_TIMEOUT_SECONDS,
    MAX_PAGE_SIZE,
)


class ConfluentStreamCatalogConfig(ConfigModel):
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
    api_key: Optional[TransparentSecretStr] = Field(
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

        self.normalize_schema_registry_url()

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

    def normalize_schema_registry_url(self) -> None:
        if not self.schema_registry_url:
            return
        if not self.schema_registry_url.startswith("https://"):
            raise ValueError(
                "Configuration error: 'schema_registry_url' must use HTTPS to protect credentials in transit. "
                f"Got: '{self.schema_registry_url}'. "
                "Expected format: https://psrc-xxxxx.region.provider.confluent.cloud"
            )
        self.schema_registry_url = self.schema_registry_url.rstrip("/")

    def validate_connection(self) -> None:
        missing: List[str] = [
            name
            for name, value in (
                ("schema_registry_url", self.schema_registry_url),
                ("api_key", self.api_key),
                ("api_secret", self.api_secret),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                "Configuration error: 'confluent_catalog.enabled' is true but "
                f"{', '.join(repr(f'confluent_catalog.{name}') for name in missing)} "
                f"{'is' if len(missing) == 1 else 'are'} not set. "
                "The Stream Catalog requires a Schema Registry endpoint and API key/secret."
            )
        self.normalize_schema_registry_url()

    def is_confluent_cloud_endpoint(self) -> bool:
        host = urlparse(self.schema_registry_url or "").hostname or ""
        return host.endswith(CONFLUENT_CLOUD_DOMAIN_SUFFIX)

    def check_confluent_cloud_endpoint(self, report: SourceReport) -> bool:
        if self.is_confluent_cloud_endpoint():
            return True
        report.warning(
            message="'confluent_catalog' is enabled but the Schema Registry endpoint is "
            "not a Confluent Cloud one — the Stream Catalog is Confluent Cloud only and "
            "will be skipped",
            context=f"schema_registry_url={self.schema_registry_url}",
        )
        return False

    def get_graphql_endpoint(self) -> str:
        if not self.schema_registry_url:
            raise ValueError(
                "schema_registry_url is required when the Stream Catalog is enabled"
            )
        return f"{self.schema_registry_url}{CATALOG_GRAPHQL_PATH}"

    def get_credentials(self) -> Tuple[str, str]:
        if (
            self.api_key is None
            or not self.api_key.get_secret_value().strip()
            or self.api_secret is None
            or not self.api_secret.get_secret_value().strip()
        ):
            raise ValueError(
                "api_key and api_secret are required when the Stream Catalog is enabled"
            )
        return self.api_key.get_secret_value(), self.api_secret.get_secret_value()
