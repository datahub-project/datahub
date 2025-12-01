"""
Data Product URN Generator

Entity-specific URN generation for data products.
"""

from urllib.parse import urlparse

from datahub.ingestion.source.rdf.core.urn_generator import UrnGeneratorBase


class DataProductUrnGenerator(UrnGeneratorBase):
    """URN generator for data product entities."""

    def generate_data_product_urn(self, iri: str) -> str:
        """
        Generate a hierarchical DataProduct URN from an IRI.

        Args:
            iri: The RDF IRI

        Returns:
            DataHub DataProduct URN with hierarchical structure
        """
        # Parse the IRI
        parsed = urlparse(iri)

        # Create product name by preserving the IRI path structure
        product_name = self._preserve_iri_structure(parsed)

        # Generate DataHub data product URN
        return f"urn:li:dataProduct:{product_name}"
