"""
Pydantic models for Snowplow API responses.

These models provide type safety and validation for data returned from:
- Snowplow BDP Console API
- Iglu Schema Registry API
"""

import re
from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional, Pattern

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ============================================
# Enum Types for Discriminated Unions
# ============================================


class EventSpecFormat(str, Enum):
    """
    Discriminator for EventSpecification API formats.

    The BDP API returns event specifications in two formats depending on the endpoint:
    - LEGACY: List endpoint returns {eventSchemas: [{vendor, name, version}, ...]}
    - DETAIL: Individual endpoint returns {event: {source: "iglu:..."}, entities: {...}}
    - EMPTY: No schema information (draft specs)
    """

    LEGACY = "legacy"
    DETAIL = "detail"
    EMPTY = "empty"


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

    # SchemaVer pattern: MODEL-REVISION-ADDITION (e.g., "1-0-0", "2-1-3")
    SCHEMAVER_PATTERN: ClassVar[Pattern[str]] = re.compile(r"^\d+-\d+-\d+$")

    vendor: str = Field(
        description="Schema vendor (e.g., 'com.snowplowanalytics.snowplow')"
    )
    name: str = Field(description="Schema name (e.g., 'page_view')")
    format: str = Field(description="Schema format (typically 'jsonschema')")
    version: str = Field(description="SchemaVer version (MODEL-REVISION-ADDITION)")

    model_config = ConfigDict(populate_by_name=True)

    @field_validator("version")
    @classmethod
    def validate_version_format(cls, v: str) -> str:
        """Validate version follows SchemaVer format (MODEL-REVISION-ADDITION)."""
        if not cls.SCHEMAVER_PATTERN.match(v):
            raise ValueError(
                f"Invalid SchemaVer format: '{v}'. Expected format: MODEL-REVISION-ADDITION (e.g., '1-0-0')"
            )
        return v


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


class DataStructureListItem(BaseModel):
    """
    Data structure from BDP list endpoint.

    The list endpoint (/data-structures) returns minimal information:
    hash, vendor, and name. Full schema details require a separate detail fetch.
    """

    hash: str = Field(description="Data structure hash (unique identifier)")
    vendor: str = Field(description="Schema vendor")
    name: str = Field(description="Schema name")

    model_config = ConfigDict(populate_by_name=True)


class DataStructure(BaseModel):
    """
    Complete data structure (schema) from BDP API.

    Used for detail endpoint responses (/data-structures/{hash}/versions/{version})
    which include full schema definition and metadata.
    """

    hash: Optional[str] = Field(
        None, description="Data structure hash (unique identifier)"
    )
    vendor: Optional[str] = Field(None, description="Schema vendor")
    name: Optional[str] = Field(None, description="Schema name")
    meta: Optional[SchemaMetadata] = Field(None, description="Schema metadata")
    data: Optional[SchemaData] = Field(None, description="JSON Schema definition")
    deployments: List["DataStructureDeployment"] = Field(
        default_factory=list, description="Deployment history"
    )

    model_config = ConfigDict(populate_by_name=True)

    @classmethod
    def from_list_item(cls, item: DataStructureListItem) -> "DataStructure":
        """Create a DataStructure from a list item (for enrichment with detail data)."""
        return cls(
            hash=item.hash,
            vendor=item.vendor,
            name=item.name,
        )

    def get_latest_deployment(
        self, prefer_env: str = "PROD"
    ) -> Optional["DataStructureDeployment"]:
        """
        Get the latest deployment, preferring the specified environment.

        Args:
            prefer_env: Environment to prefer (default: "PROD")

        Returns:
            The latest deployment for the preferred env, or latest overall if none
            in preferred env. Returns None if no deployments exist.
        """
        if not self.deployments:
            return None

        # Prefer deployments from specified environment
        preferred = [d for d in self.deployments if d.env == prefer_env]
        candidates = preferred if preferred else self.deployments

        # Sort by timestamp descending (handle None timestamps)
        return max(candidates, key=lambda d: d.ts or "", default=None)


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

    # Iglu URI pattern: iglu:vendor/name/format/version
    IGLU_URI_PATTERN: ClassVar[Pattern[str]] = re.compile(
        r"^iglu:[a-zA-Z0-9._-]+/[a-zA-Z0-9_-]+/[a-zA-Z]+/\d+-\d+-\d+$"
    )

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

    @field_validator("source")
    @classmethod
    def validate_iglu_uri(cls, v: str) -> str:
        """Validate source is a valid Iglu URI format."""
        if not cls.IGLU_URI_PATTERN.match(v):
            raise ValueError(
                f"Invalid Iglu URI format: '{v}'. "
                f"Expected format: iglu:vendor/name/format/version (e.g., 'iglu:com.acme/event/jsonschema/1-0-0')"
            )
        return v

    @field_validator("min_cardinality")
    @classmethod
    def validate_min_cardinality(cls, v: int) -> int:
        """Validate min_cardinality is non-negative."""
        if v < 0:
            raise ValueError(f"min_cardinality must be non-negative, got {v}")
        return v

    @field_validator("max_cardinality")
    @classmethod
    def validate_max_cardinality(cls, v: Optional[int]) -> Optional[int]:
        """Validate max_cardinality is positive if set."""
        if v is not None and v < 1:
            raise ValueError(f"max_cardinality must be positive or None, got {v}")
        return v

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


class EventSchemaDetail(BaseModel):
    """
    Event schema detail from event specification API.

    Used in the detail format of event specifications (individual endpoint).
    Contains the Iglu URI source and optional schema constraints.

    Note: source is optional because the API can return different event formats.
    """

    source: Optional[str] = Field(
        None, description="Iglu URI (e.g., 'iglu:com.acme/event/jsonschema/1-0-0')"
    )
    schema_constraints: Optional[Dict[str, Any]] = Field(
        None,
        alias="schema",
        description="JSON schema constraints for validation",
    )

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class TriggerDefinition(BaseModel):
    """
    Trigger definition for when an event should be sent.

    Triggers define conditions under which an event specification
    should be used by tracking implementations.
    """

    type: Optional[str] = Field(
        None,
        description="Trigger type (e.g., 'page_view', 'click', 'form_submit')",
    )
    conditions: Optional[Dict[str, Any]] = Field(
        None,
        description="Conditions for when to trigger",
    )

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class EventSpecification(BaseModel):
    """
    Event specification from BDP API.

    Event specifications document which event and entity schemas are used together.
    This allows creating accurate lineage showing which entities flow with which events.

    API returns two formats (use `format` property to detect):
    - LEGACY: List endpoint returns {"eventSchemas": [{vendor, name, version}, ...]}
    - DETAIL: Individual endpoint returns {"event": {"source": "iglu:..."}, "entities": {...}}
    - EMPTY: Draft specs with no schema information

    Type-Safe Usage:
        event_spec = get_event_specification(id)
        if event_spec.format == EventSpecFormat.LEGACY:
            for schema in event_spec.event_schemas:
                process(schema.vendor, schema.name, schema.version)
        elif event_spec.format == EventSpecFormat.DETAIL:
            uri = event_spec.get_event_iglu_uri()
            entities = event_spec.get_entity_iglu_uris()
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
    event: Optional[EventSchemaDetail] = Field(
        default=None,
        description="Event schema reference with Iglu URI and optional schema (detail format: {source: 'iglu:...', schema: {...}})",
    )

    entities: Optional[EntitiesSection] = Field(
        default=None,
        description="Entity schemas attached to this event (only in detail format)",
    )

    # Known status values (API may return others in future)
    VALID_STATUSES: ClassVar[set[str]] = {
        "draft",
        "published",
        "deprecated",
        "archived",
    }

    status: Optional[str] = Field(
        None, description="Status: draft, published, deprecated, or archived"
    )
    created_at: Optional[str] = Field(None, alias="createdAt")
    updated_at: Optional[str] = Field(None, alias="updatedAt")
    data_product_id: Optional[str] = Field(None, alias="dataProductId")

    # Additional fields from BDP API
    triggers: List[TriggerDefinition] = Field(
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

    # --- Format Detection ---

    @property
    def format(self) -> EventSpecFormat:
        """
        Detect which API format this event specification uses.

        Returns:
            EventSpecFormat.LEGACY if event_schemas list is populated
            EventSpecFormat.DETAIL if event.source is present
            EventSpecFormat.EMPTY if no schema information is available
        """
        if self.event_schemas:
            return EventSpecFormat.LEGACY
        if self.event and self.event.source:
            return EventSpecFormat.DETAIL
        return EventSpecFormat.EMPTY

    def is_legacy_format(self) -> bool:
        """Check if this uses the legacy event_schemas list format."""
        return self.format == EventSpecFormat.LEGACY

    def is_detail_format(self) -> bool:
        """Check if this uses the detail format with Iglu URIs."""
        return self.format == EventSpecFormat.DETAIL

    def has_schema_info(self) -> bool:
        """Check if this event spec has any schema information."""
        return self.format != EventSpecFormat.EMPTY

    # --- Type-Safe Accessors ---

    def get_event_iglu_uri(self) -> Optional[str]:
        """
        Get the event Iglu URI from detail format.

        Returns:
            Iglu URI string (e.g., "iglu:com.acme/checkout_started/jsonschema/1-0-0")
            or None if not available (legacy format or empty)
        """
        if self.event and self.event.source:
            return self.event.source
        return None

    def get_entity_iglu_uris(self) -> List[str]:
        """
        Get all entity Iglu URIs attached to this event (detail format).

        Returns:
            List of Iglu URI strings, empty list if legacy format or no entities
        """
        if not self.entities or not self.entities.tracked:
            return []
        return [entity.source for entity in self.entities.tracked]

    def get_all_iglu_uris(self) -> List[str]:
        """
        Get all Iglu URIs from this event spec (event + entities).

        Only returns URIs from detail format. For legacy format, use event_schemas directly.

        Returns:
            List of all Iglu URIs (event schema + entity schemas)
        """
        uris: List[str] = []
        event_uri = self.get_event_iglu_uri()
        if event_uri:
            uris.append(event_uri)
        uris.extend(self.get_entity_iglu_uris())
        return uris

    def get_schema_count(self) -> int:
        """
        Get total count of referenced schemas (works with both formats).

        Returns:
            Number of event + entity schemas referenced
        """
        if self.format == EventSpecFormat.LEGACY:
            return len(self.event_schemas)
        if self.format == EventSpecFormat.DETAIL:
            count = 1 if self.get_event_iglu_uri() else 0
            count += len(self.get_entity_iglu_uris())
            return count
        return 0


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

    # Known status values (API may return others in future)
    VALID_STATUSES: ClassVar[set[str]] = {
        "draft",
        "published",
        "deprecated",
        "archived",
    }

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
    status: Optional[str] = Field(
        None, description="Status: draft, published, deprecated, or archived"
    )
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

    # Known status values (API may return others in future)
    VALID_STATUSES: ClassVar[set[str]] = {
        "draft",
        "published",
        "deprecated",
        "archived",
    }

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
        None, description="Status: draft, published, deprecated, or archived"
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

    # Known pipeline status values from BDP API
    VALID_STATUSES: ClassVar[set[str]] = {
        "ready",
        "starting",
        "stopping",
        "stopped",
        "running",
        "error",
        "unknown",
    }

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

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Validate status is a known pipeline status, warn if unknown."""
        import logging

        if v.lower() not in cls.VALID_STATUSES:
            logging.getLogger(__name__).warning(
                f"Unknown pipeline status '{v}'. Known statuses: {cls.VALID_STATUSES}"
            )
        return v


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
