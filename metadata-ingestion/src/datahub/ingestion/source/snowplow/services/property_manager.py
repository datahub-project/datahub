"""
Structured property management for Snowplow field metadata.

Registers and manages structured property definitions for tracking field
authorship, versioning, and classification in DataHub.

Supports auto-creation of structured properties if they don't exist in DataHub,
removing the need for manual setup before running the connector.
"""

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.constants import (
    DataClassification,
    StructuredPropertyId,
)
from datahub.ingestion.source.snowplow.snowplow_config import SnowplowSourceConfig
from datahub.metadata.com.linkedin.pegasus2avro.structured import (
    StructuredPropertyDefinition,
)
from datahub.metadata.schema_classes import (
    PropertyValueClass,
    StructuredPropertyDefinitionClass,
)
from datahub.metadata.urns import (
    DataTypeUrn,
    EntityTypeUrn,
    SchemaFieldUrn,
    StructuredPropertyUrn,
)

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)


@dataclass
class PropertyRegistrationResult:
    """Result of structured property registration."""

    created: List[str] = field(default_factory=list)
    already_existed: List[str] = field(default_factory=list)
    failed: List[str] = field(default_factory=list)

    @property
    def total_processed(self) -> int:
        return len(self.created) + len(self.already_existed) + len(self.failed)

    @property
    def success(self) -> bool:
        return len(self.failed) == 0


class PropertyManager:
    """
    Manages structured property definitions for Snowplow field metadata.

    Handles registration of field authorship, versioning, and classification
    structured properties in DataHub.
    """

    def __init__(self, config: SnowplowSourceConfig):
        """
        Initialize property manager.

        Args:
            config: Snowplow source configuration
        """
        self.config = config
        self._last_registration_result: Optional[PropertyRegistrationResult] = None

    def _property_exists(self, graph: "DataHubGraph", property_id: str) -> bool:
        """
        Check if a structured property already exists in DataHub.

        Args:
            graph: DataHub graph client
            property_id: The property ID (e.g., 'snowplow.fieldAuthor')

        Returns:
            True if property exists, False otherwise
        """
        urn = StructuredPropertyUrn(property_id).urn()
        try:
            aspect = graph.get_aspect(urn, StructuredPropertyDefinitionClass)
            return aspect is not None
        except Exception as e:
            logger.debug(f"Error checking property existence for {property_id}: {e}")
            return False

    @property
    def last_registration_result(self) -> Optional[PropertyRegistrationResult]:
        """Get the result of the last registration attempt."""
        return self._last_registration_result

    def _get_property_definitions(self) -> List[Dict[str, Any]]:
        """
        Get the list of structured property definitions for Snowplow field metadata.

        Returns:
            List of property definition dictionaries
        """
        return [
            {
                "id": StructuredPropertyId.FIELD_AUTHOR,
                "display_name": "Field Author",
                "description": "The person who added this field to the Snowplow event specification or data structure. "
                "This tracks the initiator of the deployment that introduced the field.",
                "value_type": "string",
                "cardinality": "SINGLE",
            },
            {
                "id": StructuredPropertyId.FIELD_VERSION_ADDED,
                "display_name": "Version Added",
                "description": "The schema version when this field was first added to the event specification. "
                "Format: Major-Minor-Patch (e.g., '1-0-2' for version 1.0.2)",
                "value_type": "string",
                "cardinality": "SINGLE",
            },
            {
                "id": StructuredPropertyId.FIELD_ADDED_TIMESTAMP,
                "display_name": "Added Timestamp",
                "description": "ISO 8601 timestamp when this field was first added to the event specification. "
                "Format: YYYY-MM-DDTHH:MM:SSZ (e.g., '2024-01-15T10:30:00Z')",
                "value_type": "string",
                "cardinality": "SINGLE",
            },
            {
                "id": StructuredPropertyId.FIELD_DATA_CLASS,
                "display_name": "Data Classification",
                "description": "The data classification level for this field based on PII enrichment analysis.",
                "value_type": "string",
                "cardinality": "SINGLE",
                "allowed_values": [
                    {
                        "value": DataClassification.PII.value,
                        "description": "Contains personally identifiable information",
                    },
                    {
                        "value": DataClassification.SENSITIVE.value,
                        "description": "Contains sensitive business data",
                    },
                    {
                        "value": DataClassification.PUBLIC.value,
                        "description": "Public information, safe to share externally",
                    },
                    {
                        "value": DataClassification.INTERNAL.value,
                        "description": "Internal information, not for external sharing",
                    },
                ],
            },
            {
                "id": StructuredPropertyId.FIELD_EVENT_TYPE,
                "display_name": "Event Type",
                "description": "The type of Snowplow event this field belongs to. "
                "Values: 'self_describing', 'atomic', 'context'",
                "value_type": "string",
                "cardinality": "SINGLE",
                "allowed_values": [
                    {
                        "value": "self_describing",
                        "description": "Custom self-describing event field",
                    },
                    {"value": "atomic", "description": "Snowplow atomic event field"},
                    {"value": "context", "description": "Context entity field"},
                ],
            },
        ]

    def _build_property_mcp(
        self, prop: Dict[str, Any]
    ) -> MetadataChangeProposalWrapper:
        """
        Build MCP for a structured property definition.

        Args:
            prop: Property definition dictionary

        Returns:
            MetadataChangeProposalWrapper for the property definition
        """
        urn = StructuredPropertyUrn(prop["id"]).urn()

        # Build allowed values if provided
        allowed_values = None
        if "allowed_values" in prop:
            allowed_values = [
                PropertyValueClass(value=av["value"], description=av["description"])
                for av in prop["allowed_values"]
            ]

        aspect = StructuredPropertyDefinition(
            qualifiedName=prop["id"],
            displayName=prop["display_name"],
            description=prop["description"],
            valueType=DataTypeUrn(f"datahub.{prop['value_type']}").urn(),
            entityTypes=[
                EntityTypeUrn(f"datahub.{SchemaFieldUrn.ENTITY_TYPE}").urn(),
            ],
            cardinality=prop["cardinality"],
            allowedValues=allowed_values,
        )

        return MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=aspect,
        )

    def register_structured_properties_sync(
        self, graph: Optional["DataHubGraph"]
    ) -> bool:
        """
        Register structured property definitions synchronously via graph client.

        This method checks if each property already exists in DataHub before creating it.
        This enables auto-creation of properties on first run while being idempotent
        on subsequent runs.

        This approach:
        1. Checks if each property exists using the Graph API
        2. Only creates properties that don't exist
        3. Logs which properties were created vs already existed
        4. Stores the result for reporting

        Args:
            graph: DataHub graph client for direct API access

        Returns:
            True if all properties are available (created or existed), False on failures
        """
        if not self.config.field_tagging.use_structured_properties:
            return True

        if not graph:
            logger.warning(
                "Graph client not available - structured property definitions "
                "will be emitted as workunits (may fail if batched with assignments)"
            )
            return False

        logger.info(
            "Checking and registering Snowplow field structured property definitions"
        )

        properties = self._get_property_definitions()
        result = PropertyRegistrationResult()

        for prop in properties:
            property_id = prop["id"]
            try:
                # Check if property already exists
                if self._property_exists(graph, property_id):
                    logger.debug(f"Structured property already exists: {property_id}")
                    result.already_existed.append(property_id)
                    continue

                # Create the property
                mcp = self._build_property_mcp(prop)
                graph.emit_mcp(mcp)
                logger.info(f"Created structured property: {property_id}")
                result.created.append(property_id)

            except Exception as e:
                logger.warning(
                    f"Failed to register structured property {property_id}: {e}. "
                    "Field structured properties may fail validation."
                )
                result.failed.append(property_id)

        # Store result for reporting
        self._last_registration_result = result

        # Log summary
        if result.created:
            logger.info(
                f"Created {len(result.created)} new structured properties: "
                f"{', '.join(result.created)}"
            )
        if result.already_existed:
            logger.info(
                f"Found {len(result.already_existed)} existing structured properties"
            )
        if result.failed:
            logger.warning(
                f"Failed to register {len(result.failed)} structured properties: "
                f"{', '.join(result.failed)}"
            )

        return result.success

    def register_structured_properties(self) -> Iterable[MetadataWorkUnit]:
        """
        Register structured property definitions for Snowplow field metadata.

        These structured properties are used to track field authorship, versioning,
        and classification. This method yields workunits for property definitions.

        Note: Prefer register_structured_properties_sync() when a graph client
        is available to avoid batch validation issues.

        Yields:
            MetadataWorkUnit containing structured property definitions
        """
        if not self.config.field_tagging.use_structured_properties:
            return

        logger.info("Registering Snowplow field structured property definitions")

        properties = self._get_property_definitions()

        for prop in properties:
            yield self._build_property_mcp(prop).as_workunit()

        logger.info(
            f"Registered {len(properties)} Snowplow field structured property definitions"
        )
