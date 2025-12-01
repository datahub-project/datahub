"""
Dataset URN Generator

Entity-specific URN generation for datasets.
"""

from typing import Optional
from urllib.parse import urlparse

from datahub.ingestion.source.rdf.core.urn_generator import UrnGeneratorBase


class DatasetUrnGenerator(UrnGeneratorBase):
    """URN generator for dataset entities."""

    def generate_dataset_urn(
        self, iri: str, platform: Optional[str], environment: str
    ) -> str:
        """
        Generate a hierarchical dataset URN from an IRI.

        Args:
            iri: The RDF IRI
            platform: Platform URN (e.g., "urn:li:dataPlatform:mysql"),
                     platform name (e.g., "mysql"), or None (defaults to "logical")
            environment: Environment (e.g., "PROD", "DEV")

        Returns:
            DataHub dataset URN with hierarchical structure
        """
        # Parse the IRI
        parsed = urlparse(iri)

        # Create dataset name by preserving the IRI path structure
        dataset_name = self._preserve_iri_structure(parsed)

        # Normalize platform (defaults to "logical" if None)
        platform_name = self._normalize_platform(platform)
        platform_urn = f"urn:li:dataPlatform:{platform_name}"

        # Generate DataHub dataset URN with the platform URN
        return f"urn:li:dataset:({platform_urn},{dataset_name},{environment})"

    def generate_schema_field_urn(self, dataset_urn: str, field_path: str) -> str:
        """
        Generate a schema field URN from dataset URN and field path.

        Args:
            dataset_urn: The dataset URN (e.g., "urn:li:dataset:(urn:li:dataPlatform:mysql,ACCOUNTS/Account_Details,PROD)")
            field_path: The field path (e.g., "account_id")

        Returns:
            DataHub schema field URN
        """
        return f"urn:li:schemaField:({dataset_urn},{field_path})"
