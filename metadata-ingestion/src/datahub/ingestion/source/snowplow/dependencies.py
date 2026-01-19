"""
Dependency injection container for Snowplow processors.

This module defines explicit dependencies that processors need,
reducing coupling and improving testability.

Separates immutable dependencies from mutable shared state.
"""

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Set

from datahub.ingestion.source.snowplow.builders.lineage_builder import LineageBuilder
from datahub.ingestion.source.snowplow.builders.ownership_builder import (
    OwnershipBuilder,
)
from datahub.ingestion.source.snowplow.builders.urn_factory import SnowplowURNFactory
from datahub.ingestion.source.snowplow.enrichment_lineage.registry import (
    EnrichmentLineageRegistry,
)
from datahub.ingestion.source.snowplow.services.deployment_fetcher import (
    DeploymentFetcher,
)
from datahub.ingestion.source.snowplow.services.error_handler import ErrorHandler
from datahub.ingestion.source.snowplow.services.field_tagging import FieldTagger
from datahub.ingestion.source.snowplow.services.iglu_client import IgluClient
from datahub.ingestion.source.snowplow.snowplow_client import SnowplowBDPClient
from datahub.ingestion.source.snowplow.snowplow_config import SnowplowSourceConfig
from datahub.ingestion.source.snowplow.snowplow_report import SnowplowSourceReport
from datahub.ingestion.source.snowplow.utils.cache_manager import CacheManager
from datahub.utilities.file_backed_collections import FileBackedDict

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.ingestion.source.snowplow.models.snowplow_models import Pipeline
    from datahub.metadata.schema_classes import (
        SchemaFieldClass,
        StructuredPropertyValueAssignmentClass,
    )

logger = logging.getLogger(__name__)


@dataclass
class IngestionState:
    """
    Mutable shared state populated during ingestion.

    This is separate from ProcessorDependencies to distinguish between:
    - Dependencies: immutable configuration passed to processors
    - State: mutable runtime data shared across processors

    Processors can read and write to this shared state during extraction.

    Encapsulation Methods:
    - Predicate methods (has_*, can_*) check if state is available
    - Setter methods set related state together
    - Clear methods reset state groups
    - Registration methods track collections
    """

    # Pipeline state
    physical_pipeline: Optional["Pipeline"] = None  # Physical pipeline object
    pipeline_dataflow_urn: Optional[str] = None
    parsed_events_urn: Optional[str] = None
    warehouse_table_urn: Optional[str] = None

    # Event spec state
    event_spec_dataflow_urns: Dict[str, str] = field(default_factory=dict)
    emitted_event_spec_ids: Set[str] = field(default_factory=set)
    emitted_event_spec_urns: List[str] = field(
        default_factory=list
    )  # Track event spec dataset URNs
    event_spec_to_schema_urns: Dict[str, List[str]] = field(
        default_factory=dict
    )  # Map event spec URN -> list of schema URNs it references
    event_spec_id: Optional[str] = None  # First event spec ID for naming
    event_spec_name: Optional[str] = None  # First event spec name for naming

    # Schema extraction state
    atomic_event_urn: Optional[str] = None
    atomic_event_fields: List["SchemaFieldClass"] = field(default_factory=list)
    extracted_schema_urns: List[str] = field(default_factory=list)
    # SQLite-backed storage for schema fields, indexed by URN for O(1) lookups.
    # Uses FileBackedDict for memory efficiency with large schemas (5000+).
    # Has built-in LRU cache (900 items) so small datasets stay fast.
    extracted_schema_fields_by_urn: FileBackedDict[List["SchemaFieldClass"]] = field(
        default_factory=FileBackedDict
    )
    first_event_schema_vendor: Optional[str] = (
        None  # First event schema for parsed events naming
    )
    first_event_schema_name: Optional[str] = (
        None  # First event schema for parsed events naming
    )
    pii_fields_cache: Optional[set] = None  # Cache for PII fields from data structures
    # Field version tracking: schema_key -> (field_path -> version_added)
    field_version_cache: Dict[str, Dict[str, str]] = field(default_factory=dict)
    # Track referenced Iglu URIs from event specs (for standard schema extraction)
    referenced_iglu_uris: Set[str] = field(default_factory=set)
    # Track structured properties by field path for propagation to event_spec datasets
    # Key: field_path, Value: list of structured property value assignments
    field_structured_properties: Dict[
        str, List["StructuredPropertyValueAssignmentClass"]
    ] = field(default_factory=dict)

    def close(self) -> None:
        """
        Clean up resources, particularly the FileBackedDict.

        Should be called when ingestion is complete to ensure
        SQLite resources are properly released.
        """
        self.extracted_schema_fields_by_urn.close()

    # --- Predicate methods ---

    def has_warehouse_table(self) -> bool:
        """Check if warehouse table URN is available."""
        return self.warehouse_table_urn is not None

    def has_atomic_event(self) -> bool:
        """Check if atomic event dataset has been created."""
        return self.atomic_event_urn is not None

    def has_parsed_events(self) -> bool:
        """Check if parsed events URN is available."""
        return self.parsed_events_urn is not None

    def has_pipeline(self) -> bool:
        """Check if pipeline state is available."""
        return self.physical_pipeline is not None

    def can_emit_column_lineage(self) -> bool:
        """Check if prerequisites for column lineage emission are met."""
        return self.has_warehouse_table() and self.has_parsed_events()

    def has_first_event_schema(self) -> bool:
        """Check if first event schema info is available for naming."""
        return (
            self.first_event_schema_vendor is not None
            and self.first_event_schema_name is not None
        )

    # --- Setter methods for related state ---

    def set_atomic_event(self, urn: str, fields: List["SchemaFieldClass"]) -> None:
        """Set atomic event URN and fields together."""
        self.atomic_event_urn = urn
        self.atomic_event_fields = fields

    def set_first_event_schema(self, vendor: str, name: str) -> None:
        """Set first event schema vendor and name together."""
        self.first_event_schema_vendor = vendor
        self.first_event_schema_name = name

    def set_first_event_spec(self, spec_id: str, spec_name: str) -> None:
        """Set first event spec ID and name together."""
        self.event_spec_id = spec_id
        self.event_spec_name = spec_name

    # --- Registration methods for tracking collections ---

    def register_event_spec(self, spec_id: str, spec_urn: str) -> None:
        """Register an emitted event spec for tracking."""
        self.emitted_event_spec_ids.add(spec_id)
        self.emitted_event_spec_urns.append(spec_urn)

    def register_schema_urn(self, urn: str) -> None:
        """Register an extracted schema URN."""
        self.extracted_schema_urns.append(urn)

    def register_schema_fields(
        self, schema_urn: str, fields: List["SchemaFieldClass"]
    ) -> None:
        """
        Register all schema fields for a schema URN.

        Uses dict indexed by URN for O(1) lookups instead of O(n) list scans.

        Args:
            schema_urn: The schema's URN
            fields: List of schema fields to register
        """
        self.extracted_schema_fields_by_urn[schema_urn] = fields

    def get_fields_for_schema(self, schema_urn: str) -> List["SchemaFieldClass"]:
        """
        Get fields for a specific schema URN. O(1) lookup.

        Args:
            schema_urn: The schema's URN

        Returns:
            List of fields for the schema, or empty list if not found
        """
        return self.extracted_schema_fields_by_urn.get(schema_urn, [])

    def get_fields_for_schemas(
        self, schema_urns: Iterable[str]
    ) -> List["SchemaFieldClass"]:
        """
        Get fields for multiple schema URNs. O(k) where k = number of URNs.

        Args:
            schema_urns: Collection of schema URNs to look up

        Returns:
            Combined list of fields from all matching schemas
        """
        result: List["SchemaFieldClass"] = []
        for urn in schema_urns:
            result.extend(self.extracted_schema_fields_by_urn.get(urn, []))
        return result

    def get_all_extracted_fields(self) -> List["SchemaFieldClass"]:
        """
        Get all extracted schema fields across all schemas.

        Used by ParsedEventsBuilder to create combined schema.

        Returns:
            List of all schema fields
        """
        result: List["SchemaFieldClass"] = []
        for fields in self.extracted_schema_fields_by_urn.values():
            result.extend(fields)
        return result

    def get_extracted_fields_count(self) -> int:
        """Get total count of extracted schema fields."""
        return sum(
            len(fields) for fields in self.extracted_schema_fields_by_urn.values()
        )

    def register_iglu_uri(self, iglu_uri: str) -> None:
        """Register a referenced Iglu URI from an event spec."""
        self.referenced_iglu_uris.add(iglu_uri)

    def is_event_spec_emitted(self, spec_id: str) -> bool:
        """Check if an event spec has already been emitted."""
        return spec_id in self.emitted_event_spec_ids

    # --- Clear methods for resetting state groups ---

    def clear_event_spec_state(self) -> None:
        """Clear all event spec related state."""
        self.event_spec_dataflow_urns.clear()
        self.emitted_event_spec_ids.clear()
        self.emitted_event_spec_urns.clear()
        self.event_spec_to_schema_urns.clear()
        self.event_spec_id = None
        self.event_spec_name = None

    def clear_schema_state(self) -> None:
        """Clear all schema extraction related state."""
        self.atomic_event_urn = None
        self.atomic_event_fields.clear()
        self.extracted_schema_urns.clear()
        self.extracted_schema_fields_by_urn.clear()
        self.first_event_schema_vendor = None
        self.first_event_schema_name = None
        self.pii_fields_cache = None
        self.field_version_cache.clear()
        self.referenced_iglu_uris.clear()

    def clear_pipeline_state(self) -> None:
        """Clear pipeline related state."""
        self.physical_pipeline = None
        self.pipeline_dataflow_urn = None
        self.parsed_events_urn = None
        self.warehouse_table_urn = None


@dataclass
class ProcessorDependencies:
    """
    Explicit immutable dependencies for processors.

    This container makes dependencies explicit and reduces coupling between
    processors and the main source. Instead of accessing `self.source.*`,
    processors receive only what they need.

    Note: This contains ONLY immutable dependencies. Mutable shared state
    is managed separately via IngestionState and passed to processors separately.

    Benefits:
    - Clear, explicit dependencies (no hidden coupling)
    - Easy to test (mock only what's needed)
    - Clean separation: immutable config vs mutable state
    - Type-safe (all dependencies are typed)
    """

    # Core dependencies
    config: SnowplowSourceConfig
    report: SnowplowSourceReport
    cache: CacheManager
    platform: str  # Platform name (e.g., "snowplow")

    # Builders and utilities
    urn_factory: SnowplowURNFactory
    ownership_builder: OwnershipBuilder
    lineage_builder: LineageBuilder
    enrichment_lineage_registry: EnrichmentLineageRegistry
    error_handler: ErrorHandler
    field_tagger: FieldTagger

    # Optional dependencies (not all processors need these)
    bdp_client: Optional[SnowplowBDPClient] = None
    iglu_client: Optional[IgluClient] = None  # For open-source Iglu-based extraction
    graph: Optional["DataHubGraph"] = None  # For URN validation
    deployment_fetcher: Optional[DeploymentFetcher] = (
        None  # For fetching deployment history
    )
