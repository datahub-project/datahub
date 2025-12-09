"""
Glossary Term URN Generator

Entity-specific URN generation for glossary terms and glossary nodes.
"""

from typing import Optional
from urllib.parse import urlparse

from datahub.ingestion.source.rdf.core.urn_generator import UrnGeneratorBase


class GlossaryTermUrnGenerator(UrnGeneratorBase):
    """URN generator for glossary term entities."""

    def generate_glossary_term_urn(self, iri: str) -> str:
        """
        Generate a hierarchical glossary term URN from an IRI.

        Args:
            iri: The RDF IRI

        Returns:
            DataHub glossary term URN with hierarchical structure
        """
        # Parse the IRI
        parsed = urlparse(iri)

        # Create term name by preserving the IRI path structure
        term_name = self._preserve_iri_structure(parsed)

        # Generate DataHub glossary term URN
        return f"urn:li:glossaryTerm:{term_name}"

    def generate_glossary_node_urn_from_name(
        self, node_name: str, parent_urn: Optional[str] = None
    ) -> str:
        """
        Generate a glossary node URN from a node name (preserves case).

        Args:
            node_name: The glossary node name
            parent_urn: Optional parent node URN

        Returns:
            DataHub glossary node URN
        """
        if parent_urn:
            parent_path = parent_urn.replace("urn:li:glossaryNode:", "")
            return f"urn:li:glossaryNode:{parent_path}/{node_name}"
        else:
            return f"urn:li:glossaryNode:{node_name}"
