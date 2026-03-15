"""
Glossary Term URN Generator

Entity-specific URN generation for glossary terms and glossary nodes.
"""

import logging
from typing import Optional

from datahub.emitter.mce_builder import datahub_guid
from datahub.ingestion.source.rdf.core.urn_generator import UrnGeneratorBase
from datahub.utilities.urn_encoder import UrnEncoder

logger = logging.getLogger(__name__)


class GlossaryTermUrnGenerator(UrnGeneratorBase):
    """URN generator for glossary term entities."""

    def generate_glossary_term_urn(self, iri: str) -> str:
        """
        Generate a hierarchical glossary term URN from an IRI.

        Uses DataHub's standard format with dot notation for path segments.
        Handles non-ASCII characters and reserved characters according to DataHub standards.

        Args:
            iri: The RDF IRI

        Returns:
            DataHub glossary term URN with hierarchical structure (dot notation)

        Raises:
            ValueError: If IRI is invalid

        Example:
            >>> generator = GlossaryTermUrnGenerator()
            >>> generator.generate_glossary_term_urn("http://example.com/path/to/term")
            'urn:li:glossaryTerm:example.com.path.to.term'
        """
        if not iri or not isinstance(iri, str):
            raise ValueError(f"IRI must be a non-empty string, got: {iri}")

        # Parse the IRI to get path segments
        path_segments = self.derive_path_from_iri(iri, include_last=True)

        if not path_segments:
            raise ValueError(f"Cannot extract path segments from IRI: {iri}")

        # Join path segments with dot notation (DataHub standard format)
        # This aligns with DataHub's standard make_glossary_term_urn() behavior
        term_id = ".".join(path_segments)

        # Check for non-ASCII characters - fall back to GUID if present
        if any(ord(c) > 127 for c in term_id):
            logger.warning(
                f"IRI '{iri}' contains non-ASCII characters. Using GUID for URN generation."
            )
            term_id = datahub_guid({"path": term_id, "iri": iri})
        else:
            # Encode reserved characters (comma, parentheses, etc.)
            term_id = UrnEncoder.encode_string(term_id)

            # Check if encoded ID still contains problematic characters
            if UrnEncoder.contains_extended_reserved_char(term_id):
                logger.warning(
                    f"IRI '{iri}' contains problematic characters after encoding. Using GUID for URN generation."
                )
                term_id = datahub_guid({"path": term_id, "iri": iri})

        return f"urn:li:glossaryTerm:{term_id}"

    def generate_glossary_node_urn_from_name(
        self, node_name: str, parent_urn: Optional[str] = None
    ) -> str:
        """
        Generate a glossary node URN from a node name.

        Uses DataHub's standard format with dot notation for hierarchical paths.

        Args:
            node_name: The glossary node name
            parent_urn: Optional parent node URN

        Returns:
            DataHub glossary node URN

        Raises:
            ValueError: If node_name is invalid

        Example:
            >>> generator = GlossaryTermUrnGenerator()
            >>> generator.generate_glossary_node_urn_from_name("Finance")
            'urn:li:glossaryNode:Finance'
            >>> generator.generate_glossary_node_urn_from_name("Loans", "urn:li:glossaryNode:Finance")
            'urn:li:glossaryNode:Finance.Loans'
        """
        if not node_name or not isinstance(node_name, str):
            raise ValueError(f"Node name must be a non-empty string, got: {node_name}")

        node_name = node_name.strip()
        if not node_name:
            raise ValueError("Node name cannot be empty")

        if parent_urn:
            if not isinstance(parent_urn, str) or not parent_urn.startswith(
                "urn:li:glossaryNode:"
            ):
                raise ValueError(f"Invalid parent URN format: {parent_urn}")

            # Extract parent path and append node name with dot notation
            parent_path = parent_urn.replace("urn:li:glossaryNode:", "")
            if not parent_path:
                raise ValueError(f"Parent URN has no path: {parent_urn}")

            # Build hierarchical path with dot notation
            full_path = f"{parent_path}.{node_name}"
        else:
            full_path = node_name

        # Check for non-ASCII characters
        if any(ord(c) > 127 for c in full_path):
            logger.warning(
                f"Node name '{node_name}' contains non-ASCII characters. Using GUID for URN generation."
            )
            node_id = datahub_guid({"path": full_path, "node_name": node_name})
        else:
            # Encode reserved characters
            node_id = UrnEncoder.encode_string(full_path)

            # Check if encoded ID still contains problematic characters
            if UrnEncoder.contains_extended_reserved_char(node_id):
                logger.warning(
                    f"Node name '{node_name}' contains problematic characters after encoding. Using GUID for URN generation."
                )
                node_id = datahub_guid({"path": full_path, "node_name": node_name})

        return f"urn:li:glossaryNode:{node_id}"
