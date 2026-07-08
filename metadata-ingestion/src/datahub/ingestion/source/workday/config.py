from typing import Dict, Optional

from pydantic import Field, SecretStr, field_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel, HiddenFromDocs
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
from datahub.ingestion.source.workday.constants import (
    PRISM_API_VERSION_DEFAULT,
    WORKDAY_PLATFORM,
)
from datahub.utilities import config_clean


class DataSourcePlatformConfig(ConfigModel):
    """Maps a Workday Prism data source to the DataHub platform it was ingested
    under, so external upstreams resolve to the real warehouse dataset."""

    platform: Optional[str] = Field(
        default=None,
        description="DataHub platform name (for example `snowflake`) the external "
        "data behind this Prism data source was ingested under. Set this to emit "
        "lineage from Prism tables to the upstream warehouse dataset.",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="The platform instance the upstream warehouse was ingested under.",
    )
    env: Optional[str] = Field(
        default=None,
        description="The environment the upstream warehouse was ingested under. "
        "Defaults to this connector's `env` when unset.",
    )
    dataset_name: Optional[str] = Field(
        default=None,
        description="Fully qualified upstream dataset name (for example "
        "`db.schema.table`) to link to. When unset the Prism data source's own "
        "name is used as the dataset name.",
    )
    convert_urns_to_lowercase: bool = Field(
        default=True,
        description="Lowercase the upstream dataset URN. Set to false when the "
        "warehouse was ingested with case preserved so lineage URNs match.",
    )


class WorkdayConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    platform: HiddenFromDocs[str] = Field(default=WORKDAY_PLATFORM)

    base_url: str = Field(
        description=(
            "Workday services host base URL, for example "
            "`https://wd2-impl-services1.workday.com`. Do not include the tenant "
            "or API path; those are added from `tenant` and the API version."
        )
    )
    tenant: str = Field(
        description="Workday tenant identifier (the tenant segment of your "
        "Workday URLs).",
    )
    client_id: str = Field(
        description="OAuth 2.0 client ID from the Workday 'Register API Client "
        "for Integrations' task (Client Credentials grant)."
    )
    client_secret: SecretStr = Field(
        description="OAuth 2.0 client secret paired with `client_id`."
    )
    token_url: Optional[str] = Field(
        default=None,
        description="Override for the full OAuth token endpoint URL. Defaults to "
        "`{base_url}/ccx/oauth2/{tenant}/token`.",
    )
    prism_api_version: str = Field(
        default=PRISM_API_VERSION_DEFAULT,
        description="Prism Analytics REST API version segment (for example `v3`).",
    )

    verify_ssl: bool = Field(
        default=True,
        description="Whether to verify SSL certificates for Workday API calls.",
    )
    timeout_seconds: int = Field(
        default=30,
        gt=0,
        description="HTTP request timeout in seconds.",
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        description="Maximum retry attempts for transient API failures.",
    )
    page_size: int = Field(
        default=100,
        gt=0,
        description="Number of objects requested per paginated Prism API call.",
    )

    table_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter Prism tables by name.",
    )
    dataset_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter Prism datasets by name.",
    )
    data_source_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter Prism data sources by name.",
    )
    report_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter Workday-sourced reports by name.",
    )

    extract_tables: bool = Field(
        default=True,
        description="Whether to extract Prism tables as DataHub datasets with schemas.",
    )
    extract_datasets: bool = Field(
        default=True,
        description="Whether to extract Prism dataset (pipeline) definitions as "
        "DataHub datasets.",
    )
    extract_data_sources: bool = Field(
        default=True,
        description="Whether to extract Prism data sources as DataHub datasets.",
    )
    extract_reports: bool = Field(
        default=True,
        description=(
            "Whether to extract Workday-sourced Prism data sources (RaaS custom "
            "reports and Workday business-object sources) as report datasets with "
            "the `Report` subtype."
        ),
    )
    extract_lineage: bool = Field(
        default=True,
        description=(
            "Whether to emit lineage from Prism tables to the datasets and data "
            "sources they derive from, and from external data sources to mapped "
            "warehouse datasets."
        ),
    )
    include_external_tables: bool = Field(
        default=True,
        description="Whether to include tables/data sources with `sourceType: "
        "External` (data uploaded from outside Workday).",
    )
    ingest_owner: bool = Field(
        default=True,
        description="Whether to map Workday createdBy/owner fields to DataHub "
        "ownership aspects.",
    )

    data_source_platform_mapping: Dict[str, DataSourcePlatformConfig] = Field(
        default_factory=dict,
        description=(
            "Optional mapping from a Prism data source name to the platform, "
            "platform instance, environment, and dataset name the external data "
            "behind it was ingested under. Lets external Prism inputs resolve to "
            "the real upstream warehouse dataset for cross-platform lineage. "
            "Data sources with no entry are emitted as native Workday datasets."
        ),
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion config with stale entity removal support.",
    )

    @field_validator("base_url", mode="after")
    @classmethod
    def normalize_base_url(cls, value: str) -> str:
        return config_clean.remove_trailing_slashes(value)

    @field_validator("tenant", "client_id", mode="after")
    @classmethod
    def not_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("value must not be empty")
        return value.strip()
