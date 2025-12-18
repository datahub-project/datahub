"""
Dependency injection container for Snowplow processors.

This module defines explicit dependencies that processors need,
reducing coupling and improving testability.

Separates immutable dependencies from mutable shared state.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

from datahub.ingestion.source.snowplow.builders.lineage_builder import LineageBuilder
from datahub.ingestion.source.snowplow.builders.ownership_builder import (
    OwnershipBuilder,
)
from datahub.ingestion.source.snowplow.builders.urn_factory import SnowplowURNFactory
from datahub.ingestion.source.snowplow.enrichment_lineage.registry import (
    EnrichmentLineageRegistry,
)
from datahub.ingestion.source.snowplow.field_tagging import FieldTagger
from datahub.ingestion.source.snowplow.iglu_client import IgluClient
from datahub.ingestion.source.snowplow.services.error_handler import ErrorHandler
from datahub.ingestion.source.snowplow.snowplow_client import SnowplowBDPClient
from datahub.ingestion.source.snowplow.snowplow_config import SnowplowSourceConfig
from datahub.ingestion.source.snowplow.snowplow_report import SnowplowSourceReport
from datahub.ingestion.source.snowplow.utils.cache_manager import CacheManager

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.metadata.schema_classes import SchemaFieldClass


@dataclass
class IngestionState:
    """
    Mutable shared state populated during ingestion.

    This is separate from ProcessorDependencies to distinguish between:
    - Dependencies: immutable configuration passed to processors
    - State: mutable runtime data shared across processors

    Processors can read and write to this shared state during extraction.
    """

    # Pipeline state
    physical_pipeline: Optional[object] = None  # Physical pipeline object
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
    extracted_schema_fields: List[Tuple[str, "SchemaFieldClass"]] = field(
        default_factory=list
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
