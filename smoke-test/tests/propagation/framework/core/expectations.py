"""Base expectation class for propagation testing.

This module defines the base ExpectationBase class that all specific expectation
types inherit from. Specific expectation implementations live in their respective
plugin folders (plugins/documentation, plugins/term, plugins/tag).
"""

from abc import abstractmethod
from typing import TYPE_CHECKING, Optional

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.metadata.schema_classes import MetadataAttributionClass


class ExpectationBase(BaseModel):
    """Base class for all typed expectations with Pydantic validation."""

    @abstractmethod
    def get_expectation_type(self) -> str:
        """Return the type identifier for this expectation."""
        pass

    @abstractmethod
    def check_expectation(
        self,
        graph_client: "DataHubGraph",
        action_urn: Optional[str] = None,
        rollback: bool = False,
    ) -> None:
        """Check if the expectation is met. Raises AssertionError if not."""
        pass

    def explain(self) -> Optional[str]:
        """Optional method to provide human-readable explanation of the expectation."""
        return None

    def get_expectation_urn(self) -> Optional[str]:
        """Get the URN for this expectation. Override in subclasses for specific URN types."""
        return None

    def validate_expectation(self) -> bool:
        """Validate this expectation's configuration using Pydantic validation."""
        # Pydantic automatically validates on instantiation
        # If we reach here, the model is valid
        return True

    def get_urn(
        self, platform: str, dataset_name: str, field_name: Optional[str] = None
    ) -> str:
        """Generate URN for dataset or schema field."""
        dataset_urn = (
            f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},PROD)"
        )
        if field_name:
            return f"urn:li:schemaField:({dataset_urn},{field_name})"
        return dataset_urn

    def check_attribution(
        self,
        action_urn: str,
        attribution: Optional["MetadataAttributionClass"],
        expected_source: Optional[str] = None,
        expected_origin: Optional[str] = None,
        expected_via: Optional[str] = None,
    ) -> None:
        """Validate attribution details against expectation.
        Raises AssertionError if expectation is not met.
        """
        if not attribution:
            raise AssertionError("Expected attribution but got None")

        if expected_source and attribution.source != expected_source:
            raise AssertionError(
                f"Attribution source mismatch: expected {expected_source}, got {attribution.source}"
            )

        if not attribution.sourceDetail:
            raise AssertionError("Expected sourceDetail but got None")

        # Check origin
        if expected_origin:
            actual_origin = (
                attribution.sourceDetail.get("origin")
                if hasattr(attribution.sourceDetail, "get")
                else None
            )
            if actual_origin != expected_origin:
                raise AssertionError(
                    f"Origin mismatch: expected {expected_origin}, got {actual_origin}"
                )

        # Check via
        if expected_via:
            actual_via = (
                attribution.sourceDetail.get("via")
                if hasattr(attribution.sourceDetail, "get")
                else None
            )
            if actual_via != expected_via:
                raise AssertionError(
                    f"Via mismatch: expected {expected_via}, got {actual_via}"
                )

        # Check propagated flag
        actual_propagated = (
            attribution.sourceDetail.get("propagated")
            if hasattr(attribution.sourceDetail, "get")
            else None
        )
        if actual_propagated != "true":
            raise AssertionError(
                f"Propagated flag mismatch: expected 'true', got {actual_propagated}"
            )

        # Check actor - Allow either system user (local vs CI environments)
        allowed_actors = [
            "urn:li:corpuser:__datahub_system",
            "urn:li:corpuser:admin",
        ]
        if attribution.actor not in allowed_actors:
            raise AssertionError(
                f"Actor mismatch: expected one of {allowed_actors}, got {attribution.actor}"
            )


class SchemaFieldExpectation(ExpectationBase):
    """Base class for expectations that operate on schema fields."""

    # Required fields for schema field expectations
    platform: str = Field(
        ..., min_length=1, description="Data platform (e.g., hive, mysql)"
    )
    dataset_name: str = Field(..., min_length=1, description="Dataset name")
    field_name: str = Field(..., min_length=1, description="Field name")

    def get_expectation_urn(self) -> Optional[str]:
        """Get the schema field URN for this expectation."""
        return self.get_urn(self.platform, self.dataset_name, self.field_name)


class DatasetExpectation(ExpectationBase):
    """Base class for expectations that operate on datasets."""

    # Required fields for dataset expectations
    platform: str = Field(
        ..., min_length=1, description="Data platform (e.g., hive, mysql)"
    )
    dataset_name: str = Field(..., min_length=1, description="Dataset name")

    def get_expectation_urn(self) -> Optional[str]:
        """Get the dataset URN for this expectation."""
        return self.get_urn(self.platform, self.dataset_name)
