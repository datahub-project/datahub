"""
Assertion URN Generator

Entity-specific URN generation for assertions.
"""

from datahub.ingestion.source.rdf.core.urn_generator import UrnGeneratorBase


class AssertionUrnGenerator(UrnGeneratorBase):
    """URN generator for assertion entities."""

    def generate_assertion_urn(
        self, dataset_urn: str, field_name: str, operator: str
    ) -> str:
        """
        Generate a deterministic assertion URN based on dataset, field, and constraint type.

        Args:
            dataset_urn: The dataset URN (e.g., "urn:li:dataset:(urn:li:dataPlatform:mysql,TRADING/LOANS/COMMERCIAL/Commercial_Lending,PROD)")
            field_name: The field name (e.g., "Loan-to-Value Ratio")
            operator: The assertion operator (e.g., "pattern", "range_min", "range_max")

        Returns:
            Deterministic assertion URN
        """
        # Extract dataset name from dataset URN
        dataset_urn_parts = dataset_urn.split(",")
        if len(dataset_urn_parts) < 2:
            raise ValueError(
                f"Invalid dataset URN format: {dataset_urn}. Expected format: urn:li:dataset:(platform,path,env)"
            )
        dataset_name = dataset_urn_parts[1]

        # Sanitize field name to remove spaces and problematic characters
        sanitized_field_name = (
            field_name.replace(" ", "_")
            .replace(",", "_")
            .replace("(", "")
            .replace(")", "")
        )

        # Generate assertion URN with simpler format
        # Format: urn:li:assertion:(platform,dataset_name_field_operator)
        platform_part = dataset_urn_parts[0]
        platform_name = platform_part.split("urn:li:dataPlatform:")[1]

        # Create a single identifier combining all parts
        assertion_id = (
            f"{platform_name}_{dataset_name}_{sanitized_field_name}_{operator}"
        )

        assertion_urn = f"urn:li:assertion:({assertion_id})"

        return assertion_urn
