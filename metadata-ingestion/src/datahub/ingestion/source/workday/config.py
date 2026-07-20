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
    WQL_API_VERSION_DEFAULT,
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
    wql_api_version: str = Field(
        default=WQL_API_VERSION_DEFAULT,
        description="Workday Query Language (WQL) REST API version segment. Used "
        "when `extract_business_objects` or `extract_custom_reports` is enabled.",
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
    bucket_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter Prism buckets by name.",
    )
    report_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter Workday-sourced reports by name.",
    )
    business_object_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter WQL business objects by name.",
    )
    custom_report_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter Workday custom reports by name.",
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
    extract_buckets: bool = Field(
        default=False,
        description=(
            "Whether to extract Prism buckets (upload staging areas) as datasets "
            "and link each as an upstream of the table it publishes to. Off by "
            "default as buckets are transient load-path detail."
        ),
    )
    extract_reports: bool = Field(
        default=True,
        description=(
            "Whether to extract Workday-sourced Prism data sources (RaaS custom "
            "reports and Workday business-object sources) as report datasets with "
            "the `Report` subtype."
        ),
    )
    extract_schema_details: bool = Field(
        default=True,
        description=(
            "Fetch each Prism table/dataset's full definition via a per-object "
            "detail call. Prism list endpoints typically omit schema fields, "
            "lineage relationships, timestamps, and transformation logic, so this "
            "is required to populate them. Disable to save one API call per object "
            "if your tenant's list responses already include full detail."
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
    extract_column_level_lineage: bool = Field(
        default=True,
        description=(
            "Whether to emit column-level lineage from a Prism dataset's per-field "
            "mappings (requires `extract_schema_details` and `extract_lineage`)."
        ),
    )
    extract_transformation_logic: bool = Field(
        default=True,
        description=(
            "Whether to capture a Prism dataset's transformation pipeline (Data "
            "Prep Language) as view properties (requires `extract_schema_details`)."
        ),
    )
    include_row_counts: bool = Field(
        default=True,
        description=(
            "Whether to emit dataset profiles with Prism table row counts when the "
            "table detail response includes them (requires `extract_schema_details`)."
        ),
    )
    include_operational_stats: bool = Field(
        default=True,
        description=(
            "Whether to emit an operation aspect capturing a Prism table's last "
            "refresh time when available (requires `extract_schema_details`)."
        ),
    )
    extract_business_objects: bool = Field(
        default=False,
        description=(
            "Whether to ingest Workday business objects from the WQL metadata API "
            "as datasets with schema (the tenant's queryable data-source catalog). "
            "Beyond Prism; off by default."
        ),
    )
    extract_business_object_relationships: bool = Field(
        default=True,
        description=(
            "Whether to emit lineage between business objects from their declared "
            "references (requires `extract_business_objects`)."
        ),
    )
    extract_custom_reports: bool = Field(
        default=False,
        description=(
            "Whether to ingest Workday custom report (RaaS) definitions, "
            "enumerated via WQL, as report datasets linked to the business object "
            "they read from. Beyond Prism; off by default."
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
        "ownership aspects. Owner security groups (when reported) are emitted as "
        "corpGroup business owners alongside the individual technical owner.",
    )
    ingest_tags: bool = Field(
        default=True,
        description="Whether to map Workday catalog classifications to DataHub "
        "tags at the dataset and field level.",
    )
    ingest_glossary_terms: bool = Field(
        default=True,
        description="Whether to map Workday business-glossary term links to "
        "DataHub glossary terms.",
    )
    use_functional_area_containers: bool = Field(
        default=True,
        description="Whether to nest business objects and reports under a "
        "functional-area (subject-area) sub-container when the API reports one, "
        "instead of placing them directly under the tenant container.",
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default_factory=dict,
        description=(
            "Map from a DataHub domain (urn or name) to the regex patterns of "
            "object names that belong to it. The first matching domain is applied "
            "to each emitted dataset."
        ),
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
