"""
Lineage URN Generator

Entity-specific URN generation for lineage activities and relationships.
"""

from urllib.parse import urlparse

from datahub.ingestion.source.rdf.core.urn_generator import UrnGeneratorBase


class LineageUrnGenerator(UrnGeneratorBase):
    """URN generator for lineage entities."""

    def generate_lineage_activity_urn(self, iri: str) -> str:
        """
        Generate a hierarchical lineage activity URN from an IRI.

        Args:
            iri: The RDF IRI

        Returns:
            DataHub lineage activity URN with hierarchical structure
        """
        # Parse the IRI
        parsed = urlparse(iri)

        # Create activity name by preserving the IRI path structure
        activity_name = self._preserve_iri_structure(parsed)

        # Generate DataHub lineage activity URN
        return f"urn:li:dataJob:{activity_name}"

    def generate_data_job_urn(
        self, platform: str, job_name: str, environment: str
    ) -> str:
        """
        Generate a DataJob URN from platform, job name, and environment.

        Args:
            platform: The platform name (dbt, spark, airflow, etc.)
            job_name: The job name
            environment: The environment (PROD, DEV, etc.)

        Returns:
            DataHub DataJob URN
        """
        return f"urn:li:dataJob:({platform},{job_name},{environment})"

    def generate_data_flow_urn(
        self, flow_name: str, platform: str, environment: str
    ) -> str:
        """
        Generate a DataFlow URN from flow name and platform.

        Args:
            flow_name: The flow name
            platform: The platform name (dbt, spark, airflow, etc.)
            environment: The environment (PROD, DEV, etc.)

        Returns:
            DataHub DataFlow URN
        """
        return f"urn:li:dataFlow:({platform},{flow_name},{environment})"
