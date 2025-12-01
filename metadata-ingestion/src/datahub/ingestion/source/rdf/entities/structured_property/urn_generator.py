"""
Structured Property URN Generator

Entity-specific URN generation for structured properties.
"""

from urllib.parse import urlparse

from datahub.ingestion.source.rdf.core.urn_generator import UrnGeneratorBase


class StructuredPropertyUrnGenerator(UrnGeneratorBase):
    """URN generator for structured property entities."""

    def generate_structured_property_urn(self, iri: str) -> str:
        """
        Generate a hierarchical structured property URN from an IRI.

        Args:
            iri: The RDF IRI

        Returns:
            DataHub structured property URN with hierarchical structure
        """
        # Parse the IRI
        parsed = urlparse(iri)

        # Create property name by preserving the IRI path structure
        property_name = self._preserve_iri_structure(parsed)

        # Generate DataHub structured property URN
        return f"urn:li:structuredProperty:{property_name}"
