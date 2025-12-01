"""
Glossary Term URN Generator

Entity-specific URN generation for glossary terms and glossary nodes.
"""

from typing import List, Optional
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

    def generate_glossary_node_urn(
        self, iri: str, parent_urn: Optional[str] = None
    ) -> str:
        """
        Generate a hierarchical glossary node URN from an IRI.

        Args:
            iri: The RDF IRI
            parent_urn: Optional parent node URN

        Returns:
            DataHub glossary node URN with hierarchical structure
        """
        # Parse the IRI
        parsed = urlparse(iri)

        # Create node name by preserving the IRI path structure (preserves case)
        node_name = self._preserve_iri_structure(parsed)

        # Generate DataHub glossary node URN
        if parent_urn:
            parent_path = parent_urn.replace("urn:li:glossaryNode:", "")
            return f"urn:li:glossaryNode:{parent_path}/{node_name}"
        else:
            return f"urn:li:glossaryNode:{node_name}"

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

    def generate_glossary_node_hierarchy_from_urn(
        self, glossary_node_urn: str
    ) -> List[str]:
        """
        Generate a list of parent glossary node URNs from a glossary node URN.
        Creates the full hierarchy from root to the target node.

        Args:
            glossary_node_urn: The target glossary node URN

        Returns:
            List of parent glossary node URNs in hierarchical order
        """
        # Extract the path from the URN
        path = glossary_node_urn.replace("urn:li:glossaryNode:", "")

        if not path:
            return []

        # Split the path into segments
        segments = path.split("/")

        # Build hierarchy from root to target
        hierarchy = []
        current_path = ""

        for _i, segment in enumerate(segments):
            if current_path:
                current_path += f"/{segment}"
            else:
                current_path = segment

            # Create URN for this level
            hierarchy.append(f"urn:li:glossaryNode:{current_path}")

        return hierarchy

    def extract_name_from_glossary_node_urn(self, glossary_node_urn: str) -> str:
        """
        Extract the name from a glossary node URN (preserves case).

        Args:
            glossary_node_urn: The glossary node URN

        Returns:
            The glossary node name
        """
        return glossary_node_urn.replace("urn:li:glossaryNode:", "")

    def urn_to_uri(self, urn: str) -> Optional[str]:
        """
        Convert a DataHub glossary term URN back to its original URI.

        Args:
            urn: The DataHub glossary term URN

        Returns:
            The original URI, or None if conversion fails
        """
        try:
            if urn.startswith("urn:li:glossaryTerm:"):
                # Extract the term name from the URN
                term_name = urn.replace("urn:li:glossaryTerm:", "")
                # Convert back to URI by adding http:// prefix
                return f"http://{term_name}"
            else:
                # For other URN types, we don't have reverse conversion yet
                self.logger.warning(f"Cannot convert URN to URI: {urn}")
                return None
        except Exception as e:
            self.logger.error(f"Error converting URN to URI: {e}")
            return None
