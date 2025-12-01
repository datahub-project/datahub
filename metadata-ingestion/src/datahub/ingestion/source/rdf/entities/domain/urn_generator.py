"""
Domain URN Generator

Entity-specific URN generation for domains.
"""

from typing import List, Optional
from urllib.parse import urlparse

from datahub.ingestion.source.rdf.core.urn_generator import UrnGeneratorBase


class DomainUrnGenerator(UrnGeneratorBase):
    """URN generator for domain entities."""

    def generate_domain_urn(self, domain_path: tuple[str, ...]) -> str:
        """
        Generate a domain URN from a domain path.

        Args:
            domain_path: The domain path as a tuple of segments (e.g., ("bank.com", "loans"))

        Returns:
            DataHub domain URN
        """
        # Convert tuple to string
        domain_path_str = "/".join(domain_path)
        return f"urn:li:domain:{domain_path_str}"

    def generate_domain_urn_from_name(
        self, domain_name: str, parent_urn: Optional[str] = None
    ) -> str:
        """
        Generate a domain URN from a domain name (preserves case).

        Args:
            domain_name: The domain name
            parent_urn: Optional parent domain URN

        Returns:
            DataHub domain URN
        """
        if parent_urn:
            parent_path = parent_urn.replace("urn:li:domain:", "")
            return f"urn:li:domain:{parent_path}/{domain_name}"
        else:
            return f"urn:li:domain:{domain_name}"

    def generate_domain_urn_from_iri(self, iri: str) -> str:
        """
        Generate a domain URN directly from a domain IRI, removing any trailing slash.

        Args:
            iri: The domain IRI (e.g., "http://example.com/FINANCE/")

        Returns:
            DataHub domain URN without trailing slash in the path
        """
        parsed = urlparse(iri)
        path = self._preserve_iri_structure(parsed).rstrip("/")
        return f"urn:li:domain:{path}"

    def generate_domain_hierarchy_from_urn(self, domain_urn: str) -> List[str]:
        """
        Generate a list of parent domain URNs from a domain URN.
        Creates the full hierarchy from root to the target domain.

        Args:
            domain_urn: The target domain URN

        Returns:
            List of parent domain URNs in hierarchical order
        """
        # Extract the path from the URN
        path = domain_urn.replace("urn:li:domain:", "")

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
            hierarchy.append(f"urn:li:domain:{current_path}")

        return hierarchy

    def extract_name_from_domain_urn(self, domain_urn: str) -> str:
        """
        Extract the name from a domain URN (preserves case).

        Args:
            domain_urn: The domain URN

        Returns:
            The domain name
        """
        return domain_urn.replace("urn:li:domain:", "")
