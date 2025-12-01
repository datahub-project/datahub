"""
Assertion Converter

Converts RDF assertions to DataHub AST format.
"""

import logging
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.rdf.entities.assertion.ast import (
    DataHubAssertion,
    RDFAssertion,
)
from datahub.ingestion.source.rdf.entities.base import EntityConverter

logger = logging.getLogger(__name__)


class AssertionConverter(EntityConverter[RDFAssertion, DataHubAssertion]):
    """
    Converts RDF assertions to DataHub AST format.
    """

    @property
    def entity_type(self) -> str:
        return "assertion"

    def convert(
        self, rdf_entity: RDFAssertion, context: Dict[str, Any] = None
    ) -> Optional[DataHubAssertion]:
        """Convert a single RDF assertion to DataHub format."""
        try:
            return DataHubAssertion(
                assertion_key=rdf_entity.assertion_key,
                assertion_type=rdf_entity.assertion_type,
                dataset_urn=rdf_entity.dataset_urn,
                field_name=rdf_entity.field_name,
                description=rdf_entity.description,
                operator=rdf_entity.operator,
                parameters=rdf_entity.parameters,
                properties=rdf_entity.properties,
            )

        except Exception as e:
            logger.warning(
                f"Error converting assertion {rdf_entity.assertion_key}: {e}"
            )
            return None

    def convert_all(
        self, rdf_entities: List[RDFAssertion], context: Dict[str, Any] = None
    ) -> List[DataHubAssertion]:
        """Convert all RDF assertions to DataHub format."""
        results = []
        for entity in rdf_entities:
            converted = self.convert(entity, context)
            if converted:
                results.append(converted)
        return results
