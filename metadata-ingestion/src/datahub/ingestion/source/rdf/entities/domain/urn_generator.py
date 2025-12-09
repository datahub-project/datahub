"""
Domain URN Generator

Entity-specific URN generation for domains.
"""

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
