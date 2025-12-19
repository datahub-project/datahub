"""
Pydantic models for Snowplow API responses.

These models provide type safety and validation for data returned from:
- Snowplow BDP Console API
- Iglu Schema Registry API
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

# ============================================
# Authentication Models
# ============================================


class TokenResponse(BaseModel):
    """JWT token response from v3 authentication."""

    model_config = ConfigDict(populate_by_name=True)

    access_token: str = Field(alias="accessToken")


# ============================================
# Data Structure (Schema) Models
# ============================================


class SchemaMetadata(BaseModel):
    """Metadata for a data structure."""

    hidden: bool = Field(
        default=False, description="Whether schema is hidden in console"
    )
    schema_type: Optional[str] = Field(
        None, alias="schemaType", description="Type: 'event' or 'entity'"
    )
    custom_data: Dict[str, str] = Field(
        default_factory=dict,
        alias="customData",
        description="Custom key-value metadata",
    )

    model_config = ConfigDict(populate_by_name=True)


class SchemaSelf(BaseModel):
    """Self-describing schema identifier (Iglu format)."""

    vendor: str = Field(
        description="Schema vendor (e.g., 'com.snowplowanalytics.snowplow')"
    )
    name: str = Field(description="Schema name (e.g., 'page_view')")
    format: str = Field(description="Schema format (typically 'jsonschema')")
    version: str = Field(description="SchemaVer version (MODEL-REVISION-ADDITION)")

    model_config = ConfigDict(populate_by_name=True)


class SchemaData(BaseModel):
    """JSON Schema definition with self-describing wrapper."""

    schema_ref: str = Field(
        alias="$schema",
        description="JSON Schema meta-schema URI",
    )
    self_descriptor: SchemaSelf = Field(
        alias="self", description="Self-describing schema identifier"
    )
    type: str = Field(description="JSON Schema type (typically 'object')")
    description: Optional[str] = Field(None, description="Schema description")
    properties: Dict[str, Any] = Field(
        default_factory=dict, description="Schema properties"
    )
    required: List[str] = Field(
        default_factory=list, description="Required property names"
    )
    additional_properties: Optional[bool] = Field(
        None, alias="additionalProperties", description="Allow additional properties"
    )

    model_config = ConfigDict(populate_by_name=True)


class DataStructure(BaseModel):
    """Complete data structure (schema) from BDP API."""

    hash: Optional[str] = Field(
        None, description="Data structure hash (unique identifier)"
    )
    vendor: Optional[str] = Field(None, description="Schema vendor")
    name: Optional[str] = Field(None, description="Schema name")
    meta: Optional[SchemaMetadata] = Field(
        None, description="Schema metadata (may be None in list responses)"
    )
    data: Optional[SchemaData] = Field(
        None, description="JSON Schema definition (may be None in list responses)"
    )
    deployments: List["DataStructureDeployment"] = Field(
        default_factory=list, description="Deployment history (if included in response)"
    )

    model_config = ConfigDict(populate_by_name=True)


class DataStructureVersion(BaseModel):
    """Version information for a data structure."""

    version: str = Field(description="Version number (MODEL-REVISION-ADDITION)")
    created_at: Optional[str] = Field(None, alias="createdAt")
    updated_at: Optional[str] = Field(None, alias="updatedAt")

    model_config = ConfigDict(populate_by_name=True)


class DataStructureDeployment(BaseModel):
    """Deployment information for a data structure."""

    version: str = Field(description="Deployed version")
    patch_level: Optional[int] = Field(None, alias="patchLevel")
    content_hash: Optional[str] = Field(None, alias="contentHash")
    env: Optional[str] = Field(
        None, description="Deployment environment (e.g., 'PROD', 'DEV')"
    )
    ts: Optional[str] = Field(None, description="Deployment timestamp (ISO 8601)")
    message: Optional[str] = Field(None, description="Deployment message")
    initiator: Optional[str] = Field(
        None, description="Name of person who deployed (full name)"
    )
    initiator_id: Optional[str] = Field(
        None,
        alias="initiatorId",
        description="ID of person who deployed (for user lookup)",
    )

    model_config = ConfigDict(populate_by_name=True)


# ============================================
# Event Specification Models
# ============================================


class EventSchemaReference(BaseModel):
    """Reference to an event schema within an event specification."""

    vendor: str
    name: str
    version: str

    model_config = ConfigDict(populate_by_name=True)


class EntitySchemaReference(BaseModel):
    """
    Reference to an entity/context schema within an event specification.

    The API returns entity schemas with an Iglu URI format:
    iglu:vendor/name/jsonschema/version

    Example: iglu:com.datahub/user/jsonschema/1-0-0
    """

    source: str = Field(description="Iglu URI (iglu:vendor/name/jsonschema/version)")
    min_cardinality: int = Field(
        default=0,
        alias="minCardinality",
        description="Minimum number of times this entity must be attached (0 = optional)",
    )
    max_cardinality: Optional[int] = Field(
        default=None,
        alias="maxCardinality",
        description="Maximum number of times this entity can be attached (None = unlimited)",
    )
    json_schema: Optional[Dict[str, Any]] = Field(
        default=None,
        alias="schema",
        description="JSON Schema definition (optional, included in some API responses)",
    )

    model_config = ConfigDict(populate_by_name=True)

    def parse_iglu_uri(self) -> tuple[str, str, str]:
        """
        Parse Iglu URI into vendor, name, version.

        Example: iglu:com.datahub/user/jsonschema/1-0-0
        Returns: ("com.datahub", "user", "1-0-0")
        """
        # Remove "iglu:" prefix
        uri = self.source.replace("iglu:", "")

        # Split by / -> ["com.datahub", "user", "jsonschema", "1-0-0"]
        parts = uri.split("/")

        if len(parts) != 4:
            raise ValueError(f"Invalid Iglu URI format: {self.source}")

        vendor = parts[0]
        name = parts[1]
        # Skip parts[2] which is "jsonschema"
        version = parts[3]

        return vendor, name, version


class EntitiesSection(BaseModel):
    """
    Entities section from event specification API.

    Documents which entity/context schemas are attached to this event.
    """

    tracked: List[EntitySchemaReference] = Field(
        default_factory=list,
        description="Entity schemas that are tracked with this event",
    )

    model_config = ConfigDict(populate_by_name=True)


class EventSpecification(BaseModel):
    """
    Event specification from BDP API.

    Event specifications document which event and entity schemas are used together.
    This allows creating accurate lineage showing which entities flow with which events.

    API returns two formats:
    1. List format: {"eventSchemas": [{vendor, name, version}, ...]}
    2. Detail format: {"event": {"source": "iglu:..."}, "entities": {"tracked": [...]}}
    """

    id: str = Field(description="Event specification ID")
    version: Optional[int] = Field(
        None, description="Event specification version number"
    )
    revision: Optional[int] = Field(
        None, description="Event specification revision number"
    )
    name: str = Field(description="Event specification name")
    description: Optional[str] = Field(
        None, description="Event specification description"
    )

    # Legacy list format (still supported by some API endpoints)
    event_schemas: List[EventSchemaReference] = Field(
        default_factory=list,
        alias="eventSchemas",
        description="Referenced event schemas (legacy list format)",
    )

    # New detail format (used by individual event spec endpoint)
    event: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Event schema reference with Iglu URI and optional schema (detail format: {source: 'iglu:...', schema: {...}})",
    )

    entities: Optional[EntitiesSection] = Field(
        default=None,
        description="Entity schemas attached to this event (only in detail format)",
    )

    status: Optional[str] = Field(None, description="Status (draft, published, etc.)")
    created_at: Optional[str] = Field(None, alias="createdAt")
    updated_at: Optional[str] = Field(None, alias="updatedAt")
    data_product_id: Optional[str] = Field(None, alias="dataProductId")

    # Additional fields from BDP API
    triggers: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Trigger definitions for when this event should be sent",
    )
    source_applications: List[str] = Field(
        default_factory=list,
        alias="sourceApplications",
        description="Application IDs that use this event specification",
    )
    author: Optional[str] = Field(None, description="Author UUID")
    message: Optional[str] = Field(None, description="Commit/change message")
    date: Optional[str] = Field(None, description="Last modification date (ISO 8601)")

    model_config = ConfigDict(populate_by_name=True)

    def get_event_iglu_uri(self) -> Optional[str]:
        """
        Get the event Iglu URI from either format.

        Returns:
            Iglu URI string (e.g., "iglu:com.acme/checkout_started/jsonschema/1-0-0")
            or None if not available
        """
        if self.event and "source" in self.event:
            return self.event["source"]
        return None

    def get_entity_iglu_uris(self) -> List[str]:
        """
        Get all entity Iglu URIs attached to this event.

        Returns:
            List of Iglu URI strings
        """
        if not self.entities or not self.entities.tracked:
            return []
        return [entity.source for entity in self.entities.tracked]


class EventSpecificationsResponse(BaseModel):
    """
    Response from listing event specifications.

    API Reference: https://docs.snowplow.io/docs/data-product-studio/event-specifications/api/

    Event specifications API uses wrapped format with data, includes, and errors fields.
    """

    data: List[EventSpecification] = Field(description="List of event specifications")
    includes: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Additional information (e.g., change history)",
    )
    errors: List[Dict[str, Any]] = Field(
        default_factory=list, description="Warnings or errors"
    )

    model_config = ConfigDict(populate_by_name=True)


# ============================================
# Tracking Scenario Models
# ============================================


class TrackingScenario(BaseModel):
    """Tracking scenario from BDP API."""

    id: str = Field(description="Tracking scenario ID")
    name: str = Field(description="Tracking scenario name")
    description: Optional[str] = Field(
        None, description="Tracking scenario description"
    )
    event_specs: List[str] = Field(
        default_factory=list,
        alias="eventSpecs",
        description="Event specification IDs",
    )
    status: Optional[str] = Field(None, description="Status")
    created_at: Optional[str] = Field(None, alias="createdAt")
    updated_at: Optional[str] = Field(None, alias="updatedAt")

    model_config = ConfigDict(populate_by_name=True)


class TrackingScenariosResponse(BaseModel):
    """
    Response from listing tracking scenarios.

    API Reference: https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-tracking-scenarios/api/

    Tracking scenarios API uses wrapped format with data, includes, and errors fields.
    """

    data: List[TrackingScenario] = Field(description="List of tracking scenarios")
    includes: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Additional information (e.g., change history)",
    )
    errors: List[Dict[str, Any]] = Field(
        default_factory=list, description="Warnings or errors"
    )

    model_config = ConfigDict(populate_by_name=True)


# ============================================
# User Models
# ============================================


class User(BaseModel):
    """User information from BDP API."""

    id: str = Field(description="User ID (for mapping initiatorId)")
    email: Optional[str] = Field(None, description="User email address")
    name: Optional[str] = Field(None, description="User full name")
    display_name: Optional[str] = Field(
        None, alias="displayName", description="User display name"
    )

    model_config = ConfigDict(populate_by_name=True)


# ============================================
# Data Product Models (Optional)
# ============================================


class EventSpecReference(BaseModel):
    """Reference to event spec in data product."""

    id: str = Field(description="Event spec ID")
    url: Optional[str] = Field(None, description="Event spec API URL")

    model_config = ConfigDict(populate_by_name=True)


class DataProduct(BaseModel):
    """Data product from BDP API."""

    id: str = Field(description="Data product ID")
    name: str = Field(description="Data product name")
    description: Optional[str] = Field(None, description="Data product description")
    organization_id: Optional[str] = Field(
        None, alias="organizationId", description="Organization ID"
    )
    domain: Optional[str] = Field(None, description="Business domain")
    owner: Optional[str] = Field(None, description="Owner email or team")
    access_instructions: Optional[str] = Field(
        None, alias="accessInstructions", description="Access instructions"
    )
    status: Optional[str] = Field(
        None, description="Status (draft, published, deprecated)"
    )
    event_specs: List[EventSpecReference] = Field(
        default_factory=list,
        alias="eventSpecs",
        description="Event specification references",
    )
    source_applications: List[str] = Field(
        default_factory=list,
        alias="sourceApplications",
        description="Source application IDs",
    )
    type: Optional[str] = Field(None, description="Data product type")
    lock_status: Optional[str] = Field(
        None, alias="lockStatus", description="Lock status"
    )
    created_at: Optional[str] = Field(None, alias="createdAt")
    updated_at: Optional[str] = Field(None, alias="updatedAt")

    model_config = ConfigDict(populate_by_name=True)


class DataProductsIncludes(BaseModel):
    """Includes section from data products API response."""

    owners: List[Dict[str, Any]] = Field(default_factory=list)
    event_specs: List[Dict[str, Any]] = Field(default_factory=list, alias="eventSpecs")
    source_applications: List[Dict[str, Any]] = Field(
        default_factory=list, alias="sourceApplications"
    )

    model_config = ConfigDict(populate_by_name=True)


class DataProductsResponse(BaseModel):
    """
    Response from listing data products.

    API Reference: https://docs.snowplow.io/docs/data-product-studio/data-products/api/

    Data products API uses wrapped format with data, includes, and errors fields.
    Note: includes is a dict with owners, eventSpecs, and sourceApplications arrays.
    """

    data: List[DataProduct] = Field(description="List of data products")
    includes: DataProductsIncludes = Field(
        default_factory=DataProductsIncludes,
        description="Additional information (owners, event specs, source applications)",
    )
    errors: List[Dict[str, Any]] = Field(
        default_factory=list, description="Warnings or errors"
    )
    metadata: Optional[Dict[str, Any]] = Field(None, description="Response metadata")

    model_config = ConfigDict(populate_by_name=True)


# ============================================
# Iglu Schema Registry Models
# ============================================


class IgluSchema(BaseModel):
    """
    Schema from Iglu registry (same format as SchemaData).

    This is an alias for clarity when working with Iglu-specific responses.
    """

    schema_ref: str = Field(alias="$schema")
    self_descriptor: SchemaSelf = Field(alias="self")
    type: str
    description: Optional[str] = None
    properties: Dict[str, Any] = Field(default_factory=dict)
    required: List[str] = Field(default_factory=list)
    additional_properties: Optional[bool] = Field(None, alias="additionalProperties")

    model_config = ConfigDict(populate_by_name=True)


# ============================================
# Organization Models (for container hierarchy)
# ============================================


class WarehouseSourceMetadata(BaseModel):
    """Metadata for warehouse/lake destination."""

    account_locator: Optional[str] = Field(
        None,
        alias="accountLocator",
        description="Warehouse account locator (Snowflake)",
    )
    project_id: Optional[str] = Field(
        None, alias="projectId", description="GCP project ID (BigQuery)"
    )
    dataset_id: Optional[str] = Field(
        None, alias="datasetId", description="BigQuery dataset ID"
    )
    # Add more warehouse-specific metadata fields as needed

    model_config = ConfigDict(populate_by_name=True)


class WarehouseSource(BaseModel):
    """Warehouse/lake destination configuration."""

    name: str = Field(
        description="Warehouse type (snowflake, bigquery, redshift, databricks, etc.)"
    )
    metadata: Optional[WarehouseSourceMetadata] = Field(
        None, description="Warehouse-specific metadata"
    )

    model_config = ConfigDict(populate_by_name=True)


class Organization(BaseModel):
    """Organization information (constructed from API responses and config)."""

    id: str = Field(description="Organization UUID")
    name: Optional[str] = Field(None, description="Organization name")
    region: Optional[str] = Field(None, description="Organization region")
    created_at: Optional[str] = Field(None, alias="createdAt")
    source: Optional[WarehouseSource] = Field(
        None, description="Warehouse/lake destination configuration"
    )

    model_config = ConfigDict(populate_by_name=True)


# ============================================
# Pipeline Models (DataFlow in DataHub)
# ============================================


class PipelineConfig(BaseModel):
    """Pipeline configuration."""

    collector_endpoints: List[str] = Field(
        default_factory=list,
        alias="collectorEndpoints",
        description="Collector endpoints",
    )
    incomplete_stream_deployed: bool = Field(
        default=False,
        alias="incompleteStreamDeployed",
        description="Whether incomplete stream is deployed",
    )
    enrich_accept_invalid: bool = Field(
        default=False,
        alias="enrichAcceptInvalid",
        description="Whether enrichment accepts invalid events",
    )
    enrich_atomic_fields_limits: Dict[str, Any] = Field(
        default_factory=dict,
        alias="enrichAtomicFieldsLimits",
        description="Enrichment atomic fields limits",
    )

    model_config = ConfigDict(populate_by_name=True)


class Pipeline(BaseModel):
    """Pipeline from BDP API (represents a DataFlow in DataHub)."""

    id: str = Field(description="Pipeline UUID")
    name: str = Field(description="Pipeline name")
    status: str = Field(description="Pipeline status (ready, starting, stopping, etc.)")
    label: Optional[str] = Field(
        None, description="Environment label (prod, dev, etc.)"
    )
    workspace_id: Optional[str] = Field(
        None, alias="workspaceId", description="Workspace UUID"
    )
    k8s_namespace_id: Optional[str] = Field(
        None, alias="k8sNamespaceId", description="Kubernetes namespace UUID"
    )
    k8s_cluster_id: Optional[str] = Field(
        None, alias="k8sClusterId", description="Kubernetes cluster UUID"
    )
    cloud_setup_id: Optional[str] = Field(
        None, alias="cloudSetupId", description="Cloud setup UUID"
    )
    config: Optional[PipelineConfig] = Field(None, description="Pipeline configuration")
    created_at: Optional[str] = Field(None, alias="createdAt")
    updated_at: Optional[str] = Field(None, alias="updatedAt")

    model_config = ConfigDict(populate_by_name=True)


class PipelinesResponse(BaseModel):
    """
    Response from listing pipelines.

    API Reference: Discovered via testing - /organizations/{organizationId}/pipelines/v1

    Note: Unlike other endpoints, this returns {pipelines: [...]} not {data: [...]}
    """

    pipelines: List[Pipeline] = Field(description="List of pipelines")

    model_config = ConfigDict(populate_by_name=True)


# ============================================
# Enrichment Models (DataJob in DataHub)
# ============================================


class EnrichmentContentData(BaseModel):
    """Enrichment configuration data."""

    enabled: bool = Field(description="Whether enrichment is enabled")
    name: str = Field(description="Enrichment name")
    parameters: Dict[str, Any] = Field(
        default_factory=dict, description="Enrichment parameters"
    )
    vendor: str = Field(description="Enrichment vendor")
    emit_event: Optional[bool] = Field(
        None,
        alias="emitEvent",
        description="Whether to emit event (for PII enrichment)",
    )

    model_config = ConfigDict(populate_by_name=True)


class EnrichmentContent(BaseModel):
    """Enrichment content with schema and data."""

    schema_ref: str = Field(
        alias="schema", description="Iglu schema reference for enrichment config"
    )
    data: EnrichmentContentData = Field(description="Enrichment configuration data")

    model_config = ConfigDict(populate_by_name=True)


class Enrichment(BaseModel):
    """Enrichment from BDP API (represents a DataJob in DataHub)."""

    id: str = Field(description="Enrichment UUID")
    filename: str = Field(description="Enrichment configuration filename")
    content: Optional[EnrichmentContent] = Field(
        None,
        description="Enrichment configuration (None if disabled and not configured)",
    )
    enabled: bool = Field(description="Whether enrichment is enabled")
    last_update: str = Field(alias="lastUpdate", description="Last update timestamp")

    model_config = ConfigDict(populate_by_name=True)

    @property
    def schema_ref(self) -> str:
        """Get the enrichment schema reference."""
        return self.content.schema_ref if self.content else ""

    @property
    def parameters(self) -> Dict[str, Any]:
        """Get the enrichment parameters."""
        return (
            self.content.data.parameters if self.content and self.content.data else {}
        )


# ============================================================================
# Data Models API
# ============================================================================


class DataModel(BaseModel):
    """
    Data model configuration from Snowplow BDP API.

    Data models define transformations and outputs to warehouse destinations.
    Used to create lineage from enrichment outputs to warehouse tables.
    """

    id: str = Field(description="Data model UUID")
    organization_id: str = Field(
        alias="organizationId", description="Organization UUID"
    )
    data_product_id: str = Field(alias="dataProductId", description="Data product UUID")
    name: str = Field(description="Data model name")
    owner: Optional[str] = Field(None, description="Owner email or identifier")
    description: Optional[str] = Field(None, description="Data model description")

    # Warehouse destination
    destination: Optional[str] = Field(
        None, description="Destination UUID (warehouse connection reference)"
    )
    query_engine: Optional[str] = Field(
        None,
        alias="queryEngine",
        description="Query engine type: snowflake, bigquery, databricks, redshift",
    )
    table_name: Optional[str] = Field(
        None,
        alias="tableName",
        description="Fully qualified table name in warehouse (e.g., database.schema.table)",
    )

    # Model configuration
    model_type: Optional[str] = Field(
        None, alias="modelType", description="Model type: view, table, etc."
    )

    # Timestamps
    created_at: Optional[str] = Field(
        None, alias="createdAt", description="Creation timestamp"
    )
    updated_at: Optional[str] = Field(
        None, alias="updatedAt", description="Last update timestamp"
    )

    model_config = ConfigDict(populate_by_name=True)


# ============================================================================
# Destinations API (v3)
# ============================================================================


class DestinationTarget(BaseModel):
    """Target (warehouse) configuration for a destination."""

    id: str = Field(description="Target UUID")
    label: str = Field(description="Target label")
    name: str = Field(description="Target name")
    status: str = Field(description="Target status")
    env_name: str = Field(alias="envName", description="Environment name")
    target_type: str = Field(
        alias="targetType", description="Target type: snowflake, bigquery, etc."
    )
    config: Dict[str, Any] = Field(
        description="Target configuration with database, schema, etc."
    )

    model_config = ConfigDict(populate_by_name=True)


class Destination(BaseModel):
    """
    Destination (loader) configuration from Snowplow BDP API.

    Destinations define how enriched events are loaded into warehouses.
    Used to create lineage from enrichments to atomic events tables.
    """

    id: str = Field(description="Destination UUID")
    pipeline_id: str = Field(alias="pipelineId", description="Pipeline UUID")
    loader_name: str = Field(alias="loaderName", description="Loader name")
    loader_type: str = Field(
        alias="loaderType",
        description="Loader type: snowflake_loader, bigquery_loader, etc.",
    )
    destination_type: str = Field(
        alias="destinationType",
        description="Destination type: snowflake, bigquery, databricks, redshift",
    )
    status: str = Field(description="Destination status")
    target: DestinationTarget = Field(description="Target warehouse configuration")
    created_at: Optional[str] = Field(
        None, alias="createdAt", description="Creation timestamp"
    )

    model_config = ConfigDict(populate_by_name=True)
