"""Expectation classes for structured property propagation testing."""

from typing import TYPE_CHECKING, Any, Optional

from pydantic import Field

from tests.propagation.framework.core.expectations import DatasetExpectation

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph


class DatasetStructuredPropertyPropagationExpectation(DatasetExpectation):
    """Expectation that a structured property should be propagated to a dataset."""

    structured_property_urn: str = Field(
        ..., min_length=1, description="Expected structured property URN"
    )
    expected_value: Any = Field(..., description="Expected property value")
    expected_value_type: str = Field("string", description="Expected value type")

    def get_expectation_type(self) -> str:
        """Return the expectation type identifier."""
        return "dataset_structured_property_propagation"

    def check_expectation(
        self,
        graph_client: "DataHubGraph",
        action_urn: Optional[str] = None,
        rollback: bool = False,
    ) -> None:
        """Check if the structured property was propagated to the dataset."""
        from datahub.metadata.schema_classes import StructuredPropertiesClass

        # Use the dataset URN directly
        dataset_urn = self.dataset_urn

        # Get structured properties aspect
        aspect = graph_client.get_aspect(dataset_urn, StructuredPropertiesClass)

        if rollback:
            # During rollback, expect the structured property to NOT be present or not have the expected value
            if aspect and aspect.properties:
                # Check if our specific structured property with expected value exists
                found_expected_property = False
                for prop_assignment in aspect.properties:
                    if prop_assignment.propertyUrn == self.structured_property_urn:
                        if prop_assignment.values and len(prop_assignment.values) > 0:
                            actual_value = prop_assignment.values[0]

                            # Compare values based on type
                            if self.expected_value_type == "string":
                                value_matches = str(actual_value) == str(
                                    self.expected_value
                                )
                            elif self.expected_value_type == "number":
                                value_matches = float(actual_value) == float(
                                    self.expected_value
                                )
                            else:
                                value_matches = actual_value == self.expected_value

                            if value_matches:
                                found_expected_property = True
                                break

                assert not found_expected_property, (
                    f"Expected structured property '{self.structured_property_urn}' with value '{self.expected_value}' "
                    f"to be removed from dataset {self.dataset_urn} after rollback, but it was still found"
                )
        else:
            # Normal mode: expect the structured property to be present with correct value
            assert aspect is not None, (
                f"No structured properties aspect found on dataset {self.dataset_urn}"
            )

            assert aspect.properties, (
                f"No structured properties found on dataset {self.dataset_urn}"
            )

            # Check if the expected structured property exists with correct value
            property_found = False
            for prop_assignment in aspect.properties:
                if prop_assignment.propertyUrn == self.structured_property_urn:
                    property_found = True

                    assert prop_assignment.values and len(prop_assignment.values) > 0, (
                        f"Structured property '{self.structured_property_urn}' has no values on dataset {self.dataset_urn}"
                    )

                    actual_value = prop_assignment.values[0]

                    # Compare values based on type
                    if self.expected_value_type == "string":
                        expected_matches = str(actual_value) == str(self.expected_value)
                    elif self.expected_value_type == "number":
                        expected_matches = float(actual_value) == float(
                            self.expected_value
                        )
                    else:
                        expected_matches = actual_value == self.expected_value

                    assert expected_matches, (
                        f"Structured property '{self.structured_property_urn}' has value '{actual_value}' "
                        f"but expected '{self.expected_value}' on dataset {self.dataset_urn}"
                    )
                    break

            assert property_found, (
                f"Structured property '{self.structured_property_urn}' not found on dataset {self.dataset_urn}"
            )

    def explain(self, rollback: bool = False) -> Optional[str]:
        """Provide human-readable explanation of the expectation."""
        if rollback:
            return (
                f"Expected removal of structured property '{self.structured_property_urn}' "
                f"with value '{self.expected_value}' from dataset {self.dataset_urn}"
            )
        else:
            return (
                f"Expected structured property '{self.structured_property_urn}' "
                f"with value '{self.expected_value}' on dataset {self.dataset_urn}"
            )
