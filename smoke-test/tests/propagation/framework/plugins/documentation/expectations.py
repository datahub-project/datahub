"""Documentation-specific expectation classes for documentation propagation validation."""

from typing import TYPE_CHECKING, Any, Optional

from pydantic import Field

from tests.propagation.framework.core.expectations import (
    DatasetExpectation,
    SchemaFieldExpectation,
)

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph


# Helper methods for all documentation expectations
def get_documentation_aspect(graph_client: "DataHubGraph", urn: str) -> Any:
    """Get the documentation aspect for the given URN."""
    from datahub.metadata.schema_classes import DocumentationClass

    return graph_client.get_aspect(urn, DocumentationClass)


class DocumentationPropagationExpectation(SchemaFieldExpectation):
    """Expectation for field-level documentation propagation using Pydantic validation."""

    # Documentation-specific fields
    expected_description: str = Field(
        ..., min_length=1, description="Expected description"
    )

    # Optional fields with defaults
    origin_dataset: Optional[str] = Field("", description="Origin dataset name")
    origin_field: Optional[str] = Field("", description="Origin field name")
    is_live: bool = Field(False, description="Whether this is for live testing")
    propagation_found: bool = Field(
        True, description="Whether propagation is expected to be found"
    )
    propagation_source: Optional[str] = Field(
        None, description="Expected propagation source"
    )
    propagation_origin: Optional[str] = Field(
        None, description="Expected propagation origin"
    )
    propagation_via: Optional[str] = Field(
        None, description="Expected propagation via field"
    )

    def get_expectation_type(self) -> str:
        """Return the expectation type identifier."""
        return "documentation_propagation"

    def check_expectation(
        self,
        graph_client: "DataHubGraph",
        action_urn: Optional[str] = None,
        rollback: bool = False,
    ) -> None:
        """Check if documentation propagation expectation is met."""
        schema_field_urn = self.get_urn(
            self.platform, self.dataset_name, self.field_name
        )
        documentation_aspect = get_documentation_aspect(graph_client, schema_field_urn)

        if self.propagation_found and not rollback:
            if not documentation_aspect or not documentation_aspect.documentations:
                raise AssertionError(
                    f"Expected documentation propagation but none found for {schema_field_urn}"
                )

            first_element = documentation_aspect.documentations[0]
            if first_element.documentation != self.expected_description:
                raise AssertionError(
                    f"Documentation mismatch: expected '{self.expected_description}', got '{first_element.documentation}'"
                )

            if action_urn and first_element.attribution:
                self.check_attribution(
                    action_urn,
                    first_element.attribution,
                    self.propagation_source,
                    self.propagation_origin,
                    self.propagation_via,
                )
        else:
            # Either no propagation expected or rollback mode
            if documentation_aspect and documentation_aspect.documentations:
                if rollback:
                    raise AssertionError("Documentation should have been rolled back")
                elif not self.propagation_found:
                    raise AssertionError("Unexpected documentation found")


class DatasetDocumentationPropagationExpectation(DatasetExpectation):
    """Expectation for dataset-level documentation propagation using Pydantic validation."""

    # Documentation-specific fields
    expected_description: str = Field(
        ..., min_length=1, description="Expected description"
    )

    # Optional fields with defaults
    origin_dataset: str = Field("", description="Origin dataset name")
    is_live: bool = Field(False, description="Whether this is for live testing")

    def get_expectation_type(self) -> str:
        """Return the expectation type identifier."""
        return "dataset_documentation_propagation"

    def check_expectation(
        self,
        graph_client: "DataHubGraph",
        action_urn: Optional[str] = None,
        rollback: bool = False,
    ) -> None:
        """Check if dataset documentation propagation expectation is met."""
        dataset_urn = self.get_urn(self.platform, self.dataset_name)
        documentation_aspect = get_documentation_aspect(graph_client, dataset_urn)

        assert documentation_aspect is not None, (
            f"Expected to have documentation aspect on dataset {self.dataset_name} but it was missing"
        )
        assert (
            documentation_aspect.documentations
            and len(documentation_aspect.documentations) > 0
        ), (
            f"Expected to have documentation entries on dataset {self.dataset_name} but none found"
        )
        first_doc = documentation_aspect.documentations[0]
        assert first_doc.documentation == self.expected_description, (
            f"Expected to have documentation '{self.expected_description}' on dataset {self.dataset_name} "
            f"but found '{first_doc.documentation}'"
        )


class NoDocumentationPropagationExpectation(SchemaFieldExpectation):
    """Expectation for no documentation propagation using Pydantic validation."""

    # No additional fields needed beyond base schema field expectation

    # Optional fields
    is_live: bool = Field(False, description="Whether this is for live testing")

    def get_expectation_type(self) -> str:
        """Return the expectation type identifier."""
        return "no_documentation_propagation"

    def check_expectation(
        self,
        graph_client: "DataHubGraph",
        action_urn: Optional[str] = None,
        rollback: bool = False,
    ) -> None:
        """Check that no documentation propagation occurred."""
        schema_field_urn = self.get_urn(
            self.platform, self.dataset_name, self.field_name
        )
        documentation_aspect = get_documentation_aspect(graph_client, schema_field_urn)

        assert (
            documentation_aspect is None
            or not documentation_aspect.documentations
            or len(documentation_aspect.documentations) == 0
        ), (
            f"Expected no documentation on {self.dataset_name}.{self.field_name} "
            f"but found documentation with {len(documentation_aspect.documentations) if documentation_aspect and documentation_aspect.documentations else 0} entries"
        )
