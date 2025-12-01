"""
Structured Property Converter

Converts RDF structured properties to DataHub AST format.
"""

import logging
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.rdf.entities.base import EntityConverter
from datahub.ingestion.source.rdf.entities.data_product.urn_generator import (
    DataProductUrnGenerator,  # For data product URNs
)
from datahub.ingestion.source.rdf.entities.dataset.urn_generator import (
    DatasetUrnGenerator,  # For dataset URNs
)
from datahub.ingestion.source.rdf.entities.glossary_term.urn_generator import (
    GlossaryTermUrnGenerator,  # For glossary term URNs
)
from datahub.ingestion.source.rdf.entities.structured_property.ast import (
    DataHubStructuredProperty,
    DataHubStructuredPropertyValue,
    RDFStructuredProperty,
    RDFStructuredPropertyValue,
)
from datahub.ingestion.source.rdf.entities.structured_property.urn_generator import (
    StructuredPropertyUrnGenerator,
)
from datahub.metadata.urns import StructuredPropertyUrn

logger = logging.getLogger(__name__)


class StructuredPropertyConverter(
    EntityConverter[RDFStructuredProperty, DataHubStructuredProperty]
):
    """
    Converts RDF structured properties to DataHub AST format.
    """

    @property
    def entity_type(self) -> str:
        return "structured_property"

    def __init__(self):
        """Initialize the converter with entity-specific generators."""
        # Use entity-specific generators
        self.property_urn_generator = StructuredPropertyUrnGenerator()
        self.dataset_urn_generator = DatasetUrnGenerator()
        self.data_product_urn_generator = DataProductUrnGenerator()
        self.glossary_term_urn_generator = GlossaryTermUrnGenerator()

    def convert(
        self, rdf_entity: RDFStructuredProperty, context: Dict[str, Any] = None
    ) -> Optional[DataHubStructuredProperty]:
        """Convert a single RDF structured property to DataHub format."""
        try:
            # Map entity types to DataHub entity types first
            # If the property has entity types that can't be mapped, skip it
            entity_types = self._map_entity_types(rdf_entity.entity_types)

            # Skip properties with no valid entity types after mapping
            # This includes:
            # - Properties that had entity types but none could be mapped
            # - Properties with empty entity_types list
            if not entity_types:
                # Generate URN to show which property is being skipped
                urn_str = self.property_urn_generator.generate_structured_property_urn(
                    rdf_entity.uri
                )
                logger.debug(
                    f"Skipping structured property '{rdf_entity.name}' (URN: {urn_str}): no valid DataHub entity types "
                    f"(original types: {rdf_entity.entity_types if rdf_entity.entity_types else 'empty'})"
                )
                return None

            # Generate URN using entity-specific generator
            urn_str = self.property_urn_generator.generate_structured_property_urn(
                rdf_entity.uri
            )
            urn = StructuredPropertyUrn.from_string(urn_str)

            # Map value type to DataHub type
            value_type = self._map_value_type(rdf_entity.value_type)

            return DataHubStructuredProperty(
                urn=urn,
                name=rdf_entity.name,
                description=rdf_entity.description,
                value_type=value_type,
                allowed_values=rdf_entity.allowed_values,
                entity_types=entity_types,
                cardinality=rdf_entity.cardinality,
                properties=rdf_entity.properties,
            )

        except Exception as e:
            logger.warning(
                f"Error converting structured property {rdf_entity.name}: {e}"
            )
            return None

    def convert_all(
        self, rdf_entities: List[RDFStructuredProperty], context: Dict[str, Any] = None
    ) -> List[DataHubStructuredProperty]:
        """Convert all RDF structured properties to DataHub format."""
        results = []
        for entity in rdf_entities:
            converted = self.convert(entity, context)
            if converted:
                results.append(converted)
        return results

    def convert_values(
        self,
        rdf_values: List[RDFStructuredPropertyValue],
        context: Dict[str, Any] = None,
    ) -> List[DataHubStructuredPropertyValue]:
        """Convert structured property value assignments to DataHub format."""
        results = []
        environment = context.get("environment", "PROD") if context else "PROD"

        for rdf_val in rdf_values:
            try:
                # Generate entity URN based on type
                if rdf_val.entity_type == "dataset":
                    # Platform will default to "logical" if None via URN generator
                    platform = rdf_val.platform
                    entity_urn = self.dataset_urn_generator.generate_dataset_urn(
                        rdf_val.entity_uri, platform, environment
                    )
                elif rdf_val.entity_type == "dataProduct":
                    entity_urn = (
                        self.data_product_urn_generator.generate_data_product_urn(
                            rdf_val.entity_uri
                        )
                    )
                else:
                    # Default to glossary term for glossaryTerm and other types
                    entity_urn = (
                        self.glossary_term_urn_generator.generate_glossary_term_urn(
                            rdf_val.entity_uri
                        )
                    )

                # Generate property URN using entity-specific generator
                property_urn = (
                    self.property_urn_generator.generate_structured_property_urn(
                        rdf_val.property_uri
                    )
                )

                results.append(
                    DataHubStructuredPropertyValue(
                        entity_urn=entity_urn,
                        property_urn=property_urn,
                        property_name=rdf_val.property_name,
                        value=rdf_val.value,
                        entity_type=rdf_val.entity_type,
                    )
                )

            except Exception as e:
                logger.warning(f"Error converting structured property value: {e}")

        return results

    def _map_value_type(self, rdf_type: str) -> str:
        """
        Map RDF value type to DataHub value type.

        DataHub only supports these valueTypes:
        - urn:li:dataType:datahub.string
        - urn:li:dataType:datahub.rich_text
        - urn:li:dataType:datahub.number
        - urn:li:dataType:datahub.date
        - urn:li:dataType:datahub.urn

        Note: DataHub does NOT support boolean - map to string.
        """
        type_mapping = {
            "string": "urn:li:dataType:datahub.string",
            "rich_text": "urn:li:dataType:datahub.rich_text",
            "richtext": "urn:li:dataType:datahub.rich_text",
            "number": "urn:li:dataType:datahub.number",
            "integer": "urn:li:dataType:datahub.number",
            "decimal": "urn:li:dataType:datahub.number",
            "float": "urn:li:dataType:datahub.number",
            "date": "urn:li:dataType:datahub.date",
            "datetime": "urn:li:dataType:datahub.date",
            "urn": "urn:li:dataType:datahub.urn",
            "uri": "urn:li:dataType:datahub.urn",
            # Boolean not supported by DataHub - map to string
            "boolean": "urn:li:dataType:datahub.string",
            "bool": "urn:li:dataType:datahub.string",
        }
        return type_mapping.get(rdf_type.lower(), "urn:li:dataType:datahub.string")

    def _map_entity_types(self, rdf_types: List[str]) -> List[str]:
        """
        Map RDF entity types to DataHub entity type URNs.

        DataHub only supports these entityTypes:
        - urn:li:entityType:datahub.dataset
        - urn:li:entityType:datahub.schemaField
        - urn:li:entityType:datahub.dashboard
        - urn:li:entityType:datahub.chart
        - urn:li:entityType:datahub.dataFlow
        - urn:li:entityType:datahub.dataJob
        - urn:li:entityType:datahub.glossaryTerm
        - urn:li:entityType:datahub.glossaryNode
        - urn:li:entityType:datahub.container
        - urn:li:entityType:datahub.dataProduct
        - urn:li:entityType:datahub.domain
        - urn:li:entityType:datahub.corpUser
        - urn:li:entityType:datahub.corpGroup

        Returns only valid DataHub entity types, filtering out unmappable ones.
        """
        # Valid DataHub entity types (case-insensitive keys)
        type_mapping = {
            "dataset": "urn:li:entityType:datahub.dataset",
            "schemafield": "urn:li:entityType:datahub.schemaField",
            "dashboard": "urn:li:entityType:datahub.dashboard",
            "chart": "urn:li:entityType:datahub.chart",
            "dataflow": "urn:li:entityType:datahub.dataFlow",
            "datajob": "urn:li:entityType:datahub.dataJob",
            "glossaryterm": "urn:li:entityType:datahub.glossaryTerm",
            "glossarynode": "urn:li:entityType:datahub.glossaryNode",
            "container": "urn:li:entityType:datahub.container",
            "dataproduct": "urn:li:entityType:datahub.dataProduct",
            "domain": "urn:li:entityType:datahub.domain",
            "corpuser": "urn:li:entityType:datahub.corpUser",
            "corpgroup": "urn:li:entityType:datahub.corpGroup",
            "user": "urn:li:entityType:datahub.corpUser",
            "group": "urn:li:entityType:datahub.corpGroup",
        }

        # Only return valid mapped types
        result = []
        for t in rdf_types:
            mapped = type_mapping.get(t.lower())
            if mapped:
                result.append(mapped)
            else:
                logger.debug(f"Skipping unmappable entity type: {t}")

        return result
