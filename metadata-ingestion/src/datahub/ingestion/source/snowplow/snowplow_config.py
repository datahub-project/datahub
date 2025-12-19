"""
Configuration classes for Snowplow source.

Supports two deployment modes:
1. BDP (Behavioral Data Platform) - Managed Snowplow with Console API
2. Open Source - Self-hosted with Iglu registry access
"""

import logging
from datetime import datetime
from typing import List, Optional

import pydantic
from pydantic import Field, ValidationInfo, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.snowplow.constants import (
    DEFAULT_SCHEMA_TYPES,
    SchemaType,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class SnowplowBDPConnectionConfig(ConfigModel):
    """
    Connection configuration for Snowplow BDP (Behavioral Data Platform).

    Use this for managed Snowplow deployments with Console API access.
    """

    # Organization
    organization_id: str = Field(
        description="Organization UUID (found in BDP Console URL)"
    )

    # Authentication (v3)
    api_key_id: str = Field(description="API Key ID from BDP Console credentials")

    api_key: pydantic.SecretStr = Field(
        description="API Key secret from BDP Console credentials"
    )

    # API endpoint
    console_api_url: str = Field(
        default="https://console.snowplowanalytics.com/api/msc/v1",
        description="BDP Console API base URL",
    )

    # Connection options
    timeout_seconds: int = Field(
        default=60,
        description="Request timeout in seconds",
    )

    max_retries: int = Field(
        default=3,
        description="Maximum number of retry attempts for failed requests",
    )

    @field_validator("console_api_url", mode="after")
    @classmethod
    def validate_api_url(cls, v: str) -> str:
        """Validate and normalize API URL."""
        if not v.startswith(("http://", "https://")):
            raise ValueError("console_api_url must start with http:// or https://")
        return v.rstrip("/")  # Remove trailing slash


class IgluConnectionConfig(ConfigModel):
    """
    Connection configuration for Iglu Schema Registry.

    Use this for open-source Snowplow deployments or as fallback for BDP.
    """

    # Iglu server URL
    iglu_server_url: str = Field(
        description="Iglu server base URL (e.g., 'https://iglu.acme.com' or 'http://iglucentral.com')"
    )

    # Authentication (optional for public registries)
    api_key: Optional[pydantic.SecretStr] = Field(
        default=None,
        description="API key for private Iglu registry (UUID format)",
    )

    # Connection options
    timeout_seconds: int = Field(
        default=30,
        description="Request timeout in seconds",
    )

    @field_validator("iglu_server_url", mode="after")
    @classmethod
    def validate_iglu_url(cls, v: str) -> str:
        """Validate and normalize Iglu URL."""
        if not v.startswith(("http://", "https://")):
            raise ValueError("iglu_server_url must start with http:// or https://")
        return v.rstrip("/")


class DestinationMapping(ConfigModel):
    """Mapping configuration for a Snowplow warehouse destination."""

    destination_id: str = Field(
        description="Snowplow destination UUID from data models"
    )

    platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance to prepend to dataset name in URN (e.g., 'prod_snowflake')",
    )

    env: str = Field(
        default="PROD",
        description="Environment for warehouse datasets (e.g., PROD, DEV)",
    )


class WarehouseLineageConfig(ConfigModel):
    """
    Configuration for extracting lineage to warehouse destinations.

    This feature creates table-level lineage from atomic.events to derived tables
    by querying the Snowplow BDP Data Models API.

    IMPORTANT: Disabled by default because warehouse connectors (Snowflake, BigQuery, etc.)
    provide more detailed lineage by parsing actual SQL queries, including:
    - Column-level lineage
    - Transformation logic
    - Complete dependency graphs

    Only enable this if:
    - You want quick table-level lineage without setting up warehouse connector
    - You don't have access to warehouse query logs
    - You want to document Data Models API metadata specifically
    """

    enabled: bool = Field(
        default=False,
        description="Enable warehouse lineage extraction via data models API. "
        "Disabled by default - prefer using warehouse connector (Snowflake, BigQuery) for detailed lineage.",
    )

    platform_instance: Optional[str] = Field(
        default=None,
        description="Default platform instance prefix for warehouse URNs (e.g., 'prod_snowflake'). "
        "Applied globally unless overridden by destination_mappings.",
    )

    env: str = Field(
        default="PROD",
        description="Default environment for warehouse datasets",
    )

    destination_mappings: List[DestinationMapping] = Field(
        default_factory=list,
        description="Per-destination platform instance mappings. Overrides global platform_instance for specific destinations.",
    )

    validate_urns: bool = Field(
        default=True,
        description="Validate that warehouse table URNs exist in DataHub before creating lineage. "
        "Requires DataHub Graph API access. Set to False to skip validation.",
    )


class FieldTaggingConfig(ConfigModel):
    """Configuration for auto-tagging schema fields."""

    enabled: bool = Field(default=True, description="Enable automatic field tagging")

    # Tag types to enable
    tag_schema_version: bool = Field(
        default=True,
        description="Tag fields with schema version (e.g., snowplow_schema_v1-0-0)",
    )

    tag_event_type: bool = Field(
        default=True,
        description="Tag fields with event type (e.g., snowplow_event_checkout)",
    )

    tag_data_class: bool = Field(
        default=True,
        description="Tag fields with data classification (e.g., PII, Sensitive)",
    )

    tag_authorship: bool = Field(
        default=True,
        description="Tag fields with authorship (e.g., added_by_ryan_smith)",
    )

    # Field version tracking
    track_field_versions: bool = Field(
        default=False,
        description="Track which version each field was added in. "
        "When enabled, compares schema versions to determine when fields were introduced. "
        "Tags fields with their introduction version and adds 'Added in version X' to descriptions. "
        "Disabled by default as it requires fetching all schema versions (slower ingestion).",
    )

    # Structured properties vs tags
    use_structured_properties: bool = Field(
        default=True,
        description="Use structured properties for field metadata instead of (or in addition to) tags. "
        "Structured properties provide strongly-typed metadata with better querying capabilities. "
        "When enabled, field authorship, version, timestamp, and classification are emitted as "
        "structured properties on the schemaField entity. "
        "Note: Requires structured property definitions to be registered in DataHub first. "
        "See snowplow_field_structured_properties.yaml in the connector directory.",
    )

    emit_tags_and_structured_properties: bool = Field(
        default=False,
        description="Emit both tags and structured properties for fields. "
        "When True, both tags and structured properties are emitted. "
        "When False, only the method specified by use_structured_properties is used. "
        "Useful during migration from tags to structured properties.",
    )

    pii_tags_only: bool = Field(
        default=False,
        description="When emit_tags_and_structured_properties is true, only emit tags for PII/sensitive data classification. "
        "Version, authorship, and event type will only be in structured properties, not as tags. "
        "Useful when you want detailed structured properties but only highlight PII fields with tags.",
    )

    # Custom tag patterns
    schema_version_pattern: str = Field(
        default="snowplow_schema_v{version}",
        description="Pattern for schema version tags. Use {version} placeholder.",
    )

    event_type_pattern: str = Field(
        default="snowplow_event_{name}",
        description="Pattern for event type tags. Use {name} placeholder.",
    )

    authorship_pattern: str = Field(
        default="added_by_{author}",
        description="Pattern for authorship tags. Use {author} placeholder.",
    )

    # PII detection strategy
    use_pii_enrichment: bool = Field(
        default=True,
        description="Extract PII fields from PII Pseudonymization enrichment config",
    )

    # Fallback PII detection patterns (if enrichment not configured)
    pii_field_patterns: List[str] = Field(
        default_factory=lambda: [
            "email",
            "user_id",
            "ip_address",
            "phone",
            "ssn",
            "credit_card",
            "user_fingerprint",
            "network_userid",
            "domain_userid",
        ],
        description="Field name patterns to classify as PII (fallback if enrichment not available)",
    )

    sensitive_field_patterns: List[str] = Field(
        default_factory=lambda: [
            "password",
            "token",
            "secret",
            "key",
            "auth",
        ],
        description="Field name patterns to classify as Sensitive",
    )


class PerformanceConfig(ConfigModel):
    """
    Performance and scaling configuration.

    Controls parallel processing, caching, and limits for large-scale deployments.
    """

    # API concurrency
    max_concurrent_api_calls: int = Field(
        default=10,
        description="Maximum concurrent API calls for deployment fetching. "
        "Increase for faster ingestion of large organizations with many schemas. "
        "Recommended: 5-20 depending on API rate limits.",
    )

    enable_parallel_fetching: bool = Field(
        default=True,
        description="Enable parallel fetching of schema deployments. "
        "Significantly speeds up ingestion when field version tracking is enabled. "
        "Disable for debugging or if API rate limits are strict.",
    )


class SnowplowSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """
    Configuration for Snowplow source.

    Supports two modes:
    1. BDP mode: Uses Snowplow BDP Console API (requires bdp_connection)
    2. Iglu mode: Uses Iglu Schema Registry only (requires iglu_connection)
    """

    # ============================================
    # Connection Configuration
    # ============================================

    # BDP Console API connection (for managed Snowplow)
    bdp_connection: Optional[SnowplowBDPConnectionConfig] = Field(
        default=None,
        description="BDP Console API connection (required for BDP mode)",
    )

    # Iglu Schema Registry connection (for open-source or fallback)
    iglu_connection: Optional[IgluConnectionConfig] = Field(
        default=None,
        description="Iglu Schema Registry connection (required for Iglu mode, optional for BDP mode as fallback)",
    )

    # ============================================
    # Filtering Configuration
    # ============================================

    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas to filter (vendor/name format)",
    )

    event_spec_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for event specifications to filter",
    )

    tracking_scenario_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tracking scenarios to filter",
    )

    data_product_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for data products to filter",
    )

    # ============================================
    # Feature Flags
    # ============================================

    extract_event_specifications: bool = Field(
        default=True,
        description="Extract event specifications (requires BDP connection)",
    )

    extract_tracking_scenarios: bool = Field(
        default=True,
        description="Extract tracking scenarios (requires BDP connection)",
    )

    extract_data_products: bool = Field(
        default=False,
        description="Extract data products (requires BDP connection, experimental feature)",
    )

    extract_pipelines: bool = Field(
        default=True,
        description="Extract pipelines as DataFlow entities (requires BDP connection)",
    )

    extract_enrichments: bool = Field(
        default=True,
        description="Extract enrichments as DataJob entities linked to pipelines (requires BDP connection)",
    )

    enrichment_owner: Optional[str] = Field(
        default=None,
        description="Default owner for enrichments (e.g., 'data-platform@company.com'). "
        "Applied as DATAOWNER to all enrichment DataJobs. "
        "Leave empty to skip enrichment ownership.",
    )

    include_hidden_schemas: bool = Field(
        default=False,
        description="Include schemas marked as hidden in BDP Console",
    )

    include_version_in_urn: bool = Field(
        default=False,
        description="Include version in dataset URN (legacy behavior). "
        "When False (recommended), version is stored in dataset properties instead. "
        "Set to True for backwards compatibility with existing metadata.",
    )

    # ============================================
    # Schema Extraction Options
    # ============================================

    schema_types_to_extract: list = Field(
        default_factory=lambda: DEFAULT_SCHEMA_TYPES,
        description="Schema types to extract: 'event' and/or 'entity'",
    )

    deployed_since: Optional[str] = Field(
        default=None,
        description="Only extract schemas deployed/updated since this timestamp (ISO 8601 format: 2025-12-15T00:00:00Z). "
        "Enables incremental ingestion by filtering based on deployment timestamps. "
        "Leave empty to fetch all schemas.",
    )

    schema_page_size: int = Field(
        default=100,
        description="Number of schemas to fetch per API page (default: 100). "
        "Adjust based on organization size and API performance.",
    )

    extract_standard_schemas: bool = Field(
        default=True,
        description="Extract Snowplow standard schemas from Iglu Central that are referenced by event specifications. "
        "Standard schemas (vendor: com.snowplowanalytics.*) are not in the Data Structures API but are publicly available. "
        "When enabled, creates dataset entities for standard schemas and completes lineage from event specs. "
        "Only fetches schemas that are actually referenced, not all standard schemas. "
        "Disable if you don't want to fetch from Iglu Central.",
    )

    iglu_central_url: str = Field(
        default="http://iglucentral.com",
        description="Iglu Central base URL for fetching Snowplow standard schemas",
    )

    # ============================================
    # Field Tagging
    # ============================================

    field_tagging: FieldTaggingConfig = Field(
        default_factory=FieldTaggingConfig,
        description="Field tagging configuration for auto-tagging schema fields",
    )

    # ============================================
    # Warehouse Lineage (via Data Models API)
    # ============================================

    warehouse_lineage: WarehouseLineageConfig = Field(
        default_factory=WarehouseLineageConfig,
        description="Warehouse lineage configuration for linking enrichment outputs to warehouse tables",
    )

    # ============================================
    # Performance & Scaling
    # ============================================

    performance: PerformanceConfig = Field(
        default_factory=PerformanceConfig,
        description="Performance and scaling configuration for large deployments",
    )

    # ============================================
    # Stateful Ingestion
    # ============================================

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion configuration for deletion detection",
    )

    # ============================================
    # Validators
    # ============================================

    @model_validator(mode="after")
    def validate_connections(self) -> "SnowplowSourceConfig":
        """Validate that at least one connection type is configured."""
        if self.bdp_connection is None and self.iglu_connection is None:
            raise ValueError(
                "Either bdp_connection or iglu_connection must be configured. "
                "BDP connection is required for managed Snowplow deployments. "
                "Iglu connection is required for open-source deployments."
            )

        # Iglu-only mode: automatic discovery via /api/schemas endpoint
        if self.bdp_connection is None and self.iglu_connection is not None:
            logging.getLogger(__name__).info(
                "Iglu-only mode: will use automatic schema discovery via /api/schemas endpoint. "
                "Requires Iglu Server 0.6+ with list schemas support."
            )

        return self

    @field_validator("extract_event_specifications", mode="after")
    @classmethod
    def validate_event_specs(cls, v: bool, info: ValidationInfo) -> bool:
        """Warn if event specifications are enabled without BDP connection."""
        if v:
            bdp_conn = info.data.get("bdp_connection")
            if bdp_conn is None:
                logging.getLogger(__name__).warning(
                    "extract_event_specifications is enabled but bdp_connection is not configured. "
                    "Event specifications are only available via BDP Console API."
                )
        return v

    @field_validator("extract_tracking_scenarios", mode="after")
    @classmethod
    def validate_tracking_scenarios(cls, v: bool, info: ValidationInfo) -> bool:
        """Warn if tracking scenarios are enabled without BDP connection."""
        if v:
            bdp_conn = info.data.get("bdp_connection")
            if bdp_conn is None:
                logging.getLogger(__name__).warning(
                    "extract_tracking_scenarios is enabled but bdp_connection is not configured. "
                    "Tracking scenarios are only available via BDP Console API."
                )
        return v

    @field_validator("schema_types_to_extract", mode="after")
    @classmethod
    def validate_schema_types(cls, v: list) -> list:
        """Validate schema types."""
        allowed = {st.value for st in SchemaType}
        for schema_type in v:
            if schema_type not in allowed:
                raise ValueError(
                    f"schema_types_to_extract must contain only: {', '.join(allowed)}. "
                    f"Got: {schema_type}"
                )
        if not v:
            raise ValueError("schema_types_to_extract cannot be empty")
        return v

    @field_validator("deployed_since", mode="after")
    @classmethod
    def validate_deployed_since(cls, v: Optional[str]) -> Optional[str]:
        """Validate deployed_since is valid ISO 8601 timestamp."""
        if v is None:
            return v

        try:
            # Try parsing with Z suffix (common format)
            datetime.fromisoformat(v.replace("Z", "+00:00"))
            return v
        except ValueError as e:
            raise ValueError(
                f"deployed_since must be valid ISO 8601 timestamp "
                f"(e.g., '2025-12-15T00:00:00Z' or '2025-12-15T00:00:00+00:00'). "
                f"Got: '{v}'"
            ) from e
