from typing import Dict, Optional

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.source_common import (
    DatasetLineageProviderConfigBase,
    LowerCaseDatasetUrnConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities import config_clean


class MetabaseConfig(
    DatasetLineageProviderConfigBase,
    StatefulIngestionConfigBase,
    LowerCaseDatasetUrnConfigMixin,
):
    # See the Metabase /api/session endpoint for details
    # https://www.metabase.com/docs/latest/api-documentation.html#post-apisession
    connect_uri: str = Field(default="localhost:3000", description="Metabase host URL.")
    display_uri: Optional[str] = Field(
        default=None,
        description="optional URL to use in links (if `connect_uri` is only for ingestion)",
    )
    username: Optional[str] = Field(
        default=None,
        description="Metabase username, used when an API key is not provided.",
    )
    password: Optional[SecretStr] = Field(
        default=None,
        description="Metabase password, used when an API key is not provided.",
    )

    # https://www.metabase.com/learn/metabase-basics/administration/administration-and-operation/metabase-api#example-get-request
    api_key: Optional[SecretStr] = Field(
        default=None,
        description="Metabase API key. If provided, the username and password will be ignored. Recommended method.",
    )
    request_timeout_sec: float = Field(
        default=30.0,
        description="Timeout in seconds for each HTTP request to the Metabase API. "
        "Prevents ingestion from hanging indefinitely on an unresponsive server.",
    )
    database_alias_map: Optional[dict] = Field(
        default=None,
        description="Database name map to use when constructing dataset URN.",
    )
    engine_platform_map: Optional[Dict[str, str]] = Field(
        default=None,
        description="Custom mappings between metabase database engines and DataHub platforms",
    )
    database_id_to_instance_map: Optional[Dict[str, str]] = Field(
        default=None,
        description="Custom mappings between metabase database id and DataHub platform instance",
    )
    default_schema: str = Field(
        default="public",
        description="Default schema name to use when schema is not provided in an SQL query",
    )
    exclude_other_user_collections: bool = Field(
        default=False,
        description="Flag that if true, exclude other user collections",
    )
    extract_collections_as_tags: bool = Field(
        default=True,
        description="Extract Metabase collections as tags on dashboards and charts",
    )
    extract_models: bool = Field(
        default=True,
        description="Extract Metabase models (saved questions used as data sources) as datasets",
    )
    convert_lineage_urns_to_lowercase: bool = Field(
        default=False,
        description="Whether to convert dataset (table) names to lowercase when creating lineage URNs. "
        "Column names always preserve their original case to match upstream source connectors. "
        "Most DataHub connectors (including Postgres, ClickHouse, BigQuery) preserve the original case from the database. "
        "Only set to true if your upstream connector explicitly lowercases table names (rare).",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @field_validator("connect_uri", "display_uri", mode="after")
    @classmethod
    def remove_trailing_slash(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        return config_clean.remove_trailing_slashes(v)

    @model_validator(mode="after")
    def default_display_uri_to_connect_uri(self) -> "MetabaseConfig":
        if self.display_uri is None:
            self.display_uri = self.connect_uri
        return self

    @model_validator(mode="after")
    def require_credentials(self) -> "MetabaseConfig":
        has_api_key = self.api_key is not None
        has_user_pass = self.username is not None and self.password is not None
        if not has_api_key and not has_user_pass:
            raise ValueError(
                "Metabase authentication is required: provide either `api_key`, "
                "or both `username` and `password`."
            )
        return self
