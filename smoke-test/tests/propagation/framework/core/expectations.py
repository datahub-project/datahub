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

    # Flag to indicate if this expectation represents propagated content
    # Set to False for expectations on directly added/mutated content that shouldn't be rolled back
    is_propagated: bool = Field(
        True,
        description="Whether this expectation represents propagated content (default: True)",
    )

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

    def explain(self, rollback: bool = False) -> Optional[str]:
        """Optional method to provide human-readable explanation of the expectation.

        Args:
            rollback: Whether this explanation is for rollback validation (default: False)
        """
        return None

    def get_expectation_urn(self) -> Optional[str]:
        """Get the URN for this expectation. Override in subclasses for specific URN types."""
        return None

    def validate_expectation(self) -> bool:
        """Validate this expectation's configuration using Pydantic validation."""
        # Pydantic automatically validates on instantiation
        # If we reach here, the model is valid
        return True

    def check_attribution(
        self,
        action_urn: str,
        attribution: Optional["MetadataAttributionClass"],
        expected_source: Optional[str] = None,
        expected_origin: Optional[str] = None,
        expected_via: Optional[str] = None,
        expected_depth: Optional[int] = None,
        expected_direction: Optional[str] = None,
        expected_relationship: Optional[str] = None,
    ) -> None:
        """Validate attribution details against expectation.
        Raises AssertionError if expectation is not met.

        Args:
            action_urn: Action URN for validation
            attribution: MetadataAttributionClass to validate
            expected_source: Expected propagation source URN
            expected_origin: Expected origin entity URN
            expected_via: Expected via entity URN
            expected_depth: Expected propagation depth (1 for first hop, 2 for second hop, etc.)
            expected_direction: Expected propagation direction ('down', 'up', etc.)
            expected_relationship: Expected propagation relationship ('lineage', etc.)
        """
        if not attribution:
            raise AssertionError("Expected attribution but got None")

        self._check_attribution_source(attribution, expected_source)
        source_detail = self._parse_source_detail(attribution)

        self._check_attribution_fields(
            source_detail,
            expected_origin,
            expected_via,
            expected_depth,
            expected_direction,
            expected_relationship,
        )
        self._check_attribution_flags_and_actor(source_detail, attribution)

    @staticmethod
    def _check_attribution_source(
        attribution: "MetadataAttributionClass", expected_source: Optional[str]
    ) -> None:
        """Check attribution source matches expected value."""
        if expected_source and attribution.source != expected_source:
            raise AssertionError(
                f"Attribution source mismatch: expected {expected_source}, got {attribution.source}"
            )

    @staticmethod
    def _parse_source_detail(attribution: "MetadataAttributionClass") -> dict:
        """Parse and validate sourceDetail from attribution."""
        if not attribution.sourceDetail:
            raise AssertionError("Expected sourceDetail but got None")

        source_detail = attribution.sourceDetail
        if isinstance(source_detail, str):
            import json

            try:
                source_detail = json.loads(source_detail)
            except json.JSONDecodeError:
                raise AssertionError(
                    f"Failed to parse sourceDetail as JSON: {source_detail}"
                )

        return source_detail

    def _check_attribution_fields(
        self,
        source_detail: dict,
        expected_origin: Optional[str],
        expected_via: Optional[str],
        expected_depth: Optional[int],
        expected_direction: Optional[str],
        expected_relationship: Optional[str],
    ) -> None:
        """Check attribution fields (origin, via, depth, direction, relationship)."""
        self._check_field_value(source_detail, "origin", expected_origin, "Origin")
        self._check_field_value(source_detail, "via", expected_via, "Via")
        self._check_depth_field(source_detail, expected_depth)
        self._check_field_value(
            source_detail,
            "propagation_direction",
            expected_direction,
            "Propagation direction",
        )
        self._check_field_value(
            source_detail,
            "propagation_relationship",
            expected_relationship,
            "Propagation relationship",
        )

    @staticmethod
    def _check_field_value(
        source_detail: dict,
        field_name: str,
        expected_value: Optional[str],
        error_prefix: str,
    ) -> None:
        """Check a specific field value in source detail."""
        if expected_value:
            actual_value = (
                source_detail.get(field_name) if hasattr(source_detail, "get") else None
            )
            if actual_value != expected_value:
                raise AssertionError(
                    f"{error_prefix} mismatch: expected {expected_value}, got {actual_value}"
                )

    @staticmethod
    def _check_depth_field(source_detail: dict, expected_depth: Optional[int]) -> None:
        """Check propagation depth field with type conversion."""
        if expected_depth is not None:
            actual_depth_str = (
                source_detail.get("propagation_depth")
                if hasattr(source_detail, "get")
                else None
            )
            if actual_depth_str is None:
                raise AssertionError("Expected propagation_depth but got None")
            try:
                actual_depth = int(actual_depth_str)
            except (ValueError, TypeError):
                raise AssertionError(
                    f"Invalid propagation_depth format: {actual_depth_str}"
                )
            if actual_depth != expected_depth:
                raise AssertionError(
                    f"Propagation depth mismatch: expected {expected_depth}, got {actual_depth}"
                )

    @staticmethod
    def _check_attribution_flags_and_actor(
        source_detail: dict, attribution: "MetadataAttributionClass"
    ) -> None:
        """Check propagated flag and actor values."""
        # Check propagated flag
        actual_propagated = (
            source_detail.get("propagated") if hasattr(source_detail, "get") else None
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

    # Required field for schema field expectations
    field_urn: str = Field(..., min_length=1, description="Schema field URN")

    def get_expectation_urn(self) -> Optional[str]:
        """Get the schema field URN for this expectation."""
        return self.field_urn


class DatasetExpectation(ExpectationBase):
    """Base class for expectations that operate on datasets."""

    # Required field for dataset expectations
    dataset_urn: str = Field(..., min_length=1, description="Dataset URN")

    def get_expectation_urn(self) -> Optional[str]:
        """Get the dataset URN for this expectation."""
        return self.dataset_urn
