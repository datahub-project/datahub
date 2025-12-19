"""
Structured property management for Snowplow field metadata.

Registers and manages structured property definitions for tracking field
authorship, versioning, and classification in DataHub.
"""

import logging
from typing import Any, Dict, Iterable, List

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.constants import (
    DataClassification,
    StructuredPropertyId,
)
from datahub.ingestion.source.snowplow.snowplow_config import SnowplowSourceConfig
from datahub.metadata.com.linkedin.pegasus2avro.structured import (
    StructuredPropertyDefinition,
)
from datahub.metadata.schema_classes import PropertyValueClass
from datahub.metadata.urns import (
    DataTypeUrn,
    EntityTypeUrn,
    SchemaFieldUrn,
    StructuredPropertyUrn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper

logger = logging.getLogger(__name__)


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

    def register_structured_properties(self) -> Iterable[MetadataWorkUnit]:
        """
        Register structured property definitions for Snowplow field metadata.

        These structured properties are used to track field authorship, versioning,
        and classification. This method automatically creates the property definitions
        in DataHub if they don't already exist.

        Yields:
            MetadataWorkUnit containing structured property definitions
        """
        if not self.config.field_tagging.use_structured_properties:
            return

        logger.info("Registering Snowplow field structured property definitions")

        # Define structured properties
        properties: List[Dict[str, Any]] = [
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

        # Create structured property definitions
        for prop in properties:
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
                # lastModified is auto-generated by backend
            )

            # Emit the structured property definition
            # Use UPSERT (default) to create or update properties
            yield MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=aspect,
                # changeType defaults to UPSERT, which creates or updates
            ).as_workunit()

        logger.info("Registered 5 Snowplow field structured property definitions")
