"""
Structured Property MCP Builder

Builds DataHub MCPs for structured properties.
"""

import logging
from typing import Any, Dict, List

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.rdf.entities.base import EntityMCPBuilder
from datahub.ingestion.source.rdf.entities.structured_property.ast import (
    DataHubStructuredProperty,
    DataHubStructuredPropertyValue,
)
from datahub.metadata.schema_classes import (
    DataHubSearchConfigClass,
    PropertyValueClass,
    SearchFieldTypeClass,
    StructuredPropertiesClass,
    StructuredPropertyDefinitionClass,
    StructuredPropertyValueAssignmentClass,
)

logger = logging.getLogger(__name__)


def _normalize_qualified_name(name: str) -> str:
    """
    Normalize a name for use as a qualified name in DataHub.

    DataHub requires qualified names to not contain spaces.
    Replaces spaces with underscores.

    Args:
        name: The original name (may contain spaces)

    Returns:
        Normalized name with spaces replaced by underscores
    """
    return name.replace(" ", "_")


class StructuredPropertyMCPBuilder(EntityMCPBuilder[DataHubStructuredProperty]):
    """
    Builds DataHub MCPs for structured properties.
    """

    @property
    def entity_type(self) -> str:
        return "structured_property"

    def build_mcps(
        self, entity: DataHubStructuredProperty, context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """Build MCPs for a single structured property definition."""
        try:
            # Create search configuration (required for properties to appear in filters/sidebar)
            search_config = DataHubSearchConfigClass(
                enableAutocomplete=True,
                addToFilters=True,
                queryByDefault=True,
                fieldType=SearchFieldTypeClass.TEXT,
            )

            # Convert allowed values
            allowed_values = None
            if entity.allowed_values:
                allowed_values = [
                    PropertyValueClass(value=v) for v in entity.allowed_values
                ]

            # Extract qualified name from URN to ensure it matches the URN format
            # The URN format is: urn:li:structuredProperty:{qualifiedName}
            # So we extract the qualifiedName by removing the prefix
            urn_str = str(entity.urn)
            if urn_str.startswith("urn:li:structuredProperty:"):
                qualified_name = urn_str.replace("urn:li:structuredProperty:", "", 1)
                logger.debug(
                    f"Extracted qualifiedName '{qualified_name}' from URN '{urn_str}' for property '{entity.name}'"
                )
            else:
                # Fallback: normalize the name if URN format is unexpected
                qualified_name = _normalize_qualified_name(entity.name)
                logger.warning(
                    f"Unexpected URN format for structured property '{entity.name}': {urn_str}. "
                    f"Using normalized name as qualifiedName: {qualified_name}"
                )

            # Validate entity types - skip if none are valid
            if not entity.entity_types:
                logger.debug(
                    f"Skipping structured property '{entity.name}' (URN: {urn_str}): no valid entity types"
                )
                return []

            # Build the structured property definition
            property_def = StructuredPropertyDefinitionClass(
                qualifiedName=qualified_name,
                displayName=entity.name,  # Keep original name with spaces for display
                valueType=entity.value_type,
                description=entity.description,
                entityTypes=entity.entity_types,
                allowedValues=allowed_values,
                searchConfiguration=search_config,
            )

            # Add cardinality if specified
            if entity.cardinality:
                if entity.cardinality.upper() == "MULTIPLE":
                    property_def.cardinality = "MULTIPLE"
                else:
                    property_def.cardinality = "SINGLE"

            mcp = MetadataChangeProposalWrapper(
                entityUrn=str(entity.urn),
                aspect=property_def,
            )

            logger.debug(
                f"Created structured property definition MCP for '{entity.name}' "
                f"(URN: {urn_str}, qualifiedName: {qualified_name}, entityTypes: {entity.entity_types})"
            )
            return [mcp]

        except Exception as e:
            logger.warning(
                f"Error building MCP for structured property {entity.name}: {e}"
            )
            return []

    def build_all_mcps(
        self, entities: List[DataHubStructuredProperty], context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """Build MCPs for all structured properties."""
        mcps = []
        for entity in entities:
            mcps.extend(self.build_mcps(entity, context))
        return mcps

    def build_value_assignments(
        self,
        values: List[DataHubStructuredPropertyValue],
        context: Dict[str, Any] = None,
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Build MCPs for structured property value assignments.

        Groups value assignments by entity to create a single MCP per entity.
        """
        mcps = []

        # Group values by entity
        entity_values: Dict[str, List[DataHubStructuredPropertyValue]] = {}
        for val in values:
            if val.entity_urn not in entity_values:
                entity_values[val.entity_urn] = []
            entity_values[val.entity_urn].append(val)

        # Build MCPs
        for entity_urn, vals in entity_values.items():
            try:
                properties = []
                for v in vals:
                    properties.append(
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=v.property_urn, values=[v.value]
                        )
                    )

                structured_props = StructuredPropertiesClass(properties=properties)

                mcp = MetadataChangeProposalWrapper(
                    entityUrn=entity_urn,
                    aspect=structured_props,
                )
                mcps.append(mcp)

            except Exception as e:
                logger.warning(
                    f"Error building value assignment MCP for {entity_urn}: {e}"
                )

        return mcps

    @staticmethod
    def create_structured_property_values_mcp(
        entity_urn: str, prop_values: List[DataHubStructuredPropertyValue]
    ) -> MetadataChangeProposalWrapper:
        """
        Static method for backward compatibility with tests.

        Creates a single MCP for structured property value assignments on an entity.
        Filters out empty/null values.
        """
        # Filter out empty values
        valid_values = [v for v in prop_values if v.value and v.value.strip()]

        if not valid_values:
            raise ValueError(
                f"No valid structured property values provided for {entity_urn}"
            )

        # Use instance method
        builder = StructuredPropertyMCPBuilder()
        mcps = builder.build_value_assignments(valid_values)

        if not mcps:
            raise ValueError(f"Failed to create MCP for {entity_urn}")

        # Return the first MCP (should be the only one for a single entity)
        return mcps[0]

    def build_post_processing_mcps(
        self, datahub_graph: Any, context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Build MCPs for structured property value assignments.

        This handles value assignments that must be created after property
        definitions and target entities (datasets, glossary terms) exist.

        Args:
            datahub_graph: The complete DataHubGraph AST
            context: Optional context (should include 'report' for entity counting)

        Returns:
            List of MCPs for structured property value assignments
        """
        structured_property_values = getattr(
            datahub_graph, "structured_property_values", []
        )
        if not structured_property_values:
            return []

        report = context.get("report") if context else None

        # Build set of defined property URNs (from AST - these are the ones that passed conversion)
        defined_property_urns = {
            str(prop.urn) for prop in datahub_graph.structured_properties
        }

        logger.debug(
            f"Found {len(defined_property_urns)} structured property definitions in AST. "
            f"Processing {len(structured_property_values)} value assignments."
        )

        # Filter values to only include properties with definitions
        valid_property_values = []
        skipped_count = 0
        skipped_properties = set()
        for prop_value in structured_property_values:
            if prop_value.property_urn in defined_property_urns:
                valid_property_values.append(prop_value)
            else:
                skipped_count += 1
                skipped_properties.add(prop_value.property_urn)
                logger.debug(
                    f"Skipping structured property value for undefined property: {prop_value.property_urn} on {prop_value.entity_urn}. "
                    f"This property definition was likely filtered out during conversion or MCP building."
                )

        if skipped_count > 0:
            logger.debug(
                f"Skipped {skipped_count} structured property value assignments for {len(skipped_properties)} undefined properties: {sorted(skipped_properties)}"
            )

        logger.debug(
            f"Processing {len(valid_property_values)} structured property value assignments"
        )

        # Use MCP builder's build_value_assignments method
        if not valid_property_values:
            return []

        try:
            value_mcps = self.build_value_assignments(valid_property_values)
            for _ in value_mcps:
                if report:
                    report.report_entity_emitted()
            return value_mcps
        except Exception as e:
            logger.warning(f"Failed to create MCPs for structured property values: {e}")
            return []
