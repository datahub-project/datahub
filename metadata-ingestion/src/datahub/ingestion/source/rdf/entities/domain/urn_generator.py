"""
Domain URN Generator

Entity-specific URN generation for domains.
"""

import logging

from datahub.emitter.mce_builder import datahub_guid
from datahub.ingestion.source.rdf.core.urn_generator import UrnGeneratorBase
from datahub.utilities.urn_encoder import UrnEncoder

logger = logging.getLogger(__name__)


class DomainUrnGenerator(UrnGeneratorBase):
    """URN generator for domain entities."""

    def generate_domain_urn(self, domain_path: tuple[str, ...]) -> str:
        """
        Generate a domain URN from a domain path.

        Uses DataHub's standard format with dot notation for path segments.
        Handles non-ASCII characters and reserved characters according to DataHub standards.

        Args:
            domain_path: The domain path as a tuple of segments (e.g., ("bank.com", "loans"))

        Returns:
            DataHub domain URN

        Raises:
            ValueError: If domain_path is invalid

        Example:
            >>> generator = DomainUrnGenerator()
            >>> generator.generate_domain_urn(("bank.com", "loans"))
            'urn:li:domain:bank.com.loans'
        """
        if not domain_path or not isinstance(domain_path, tuple):
            raise ValueError(
                f"Domain path must be a non-empty tuple, got: {domain_path}"
            )

        if not all(isinstance(seg, str) and seg.strip() for seg in domain_path):
            raise ValueError(
                f"All domain path segments must be non-empty strings: {domain_path}"
            )

        # Join path segments with dot notation (DataHub standard format)
        domain_id = ".".join(domain_path)

        # Check for non-ASCII characters - fall back to GUID if present
        if any(ord(c) > 127 for c in domain_id):
            logger.warning(
                f"Domain path '{domain_path}' contains non-ASCII characters. Using GUID for URN generation."
            )
            domain_id = datahub_guid({"path": domain_id, "domain_path": domain_path})
        else:
            # Encode reserved characters (comma, parentheses, etc.)
            domain_id = UrnEncoder.encode_string(domain_id)

            # Check if encoded ID still contains problematic characters
            if UrnEncoder.contains_extended_reserved_char(domain_id):
                logger.warning(
                    f"Domain path '{domain_path}' contains problematic characters after encoding. Using GUID for URN generation."
                )
                domain_id = datahub_guid(
                    {"path": domain_id, "domain_path": domain_path}
                )

        return f"urn:li:domain:{domain_id}"
