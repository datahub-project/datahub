"""Term-specific expectation classes for glossary term propagation validation."""

from typing import TYPE_CHECKING, Any, Optional

from pydantic import Field

from tests.propagation.framework.core.expectations import SchemaFieldExpectation

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph


# Helper methods for all term expectations
def get_glossary_terms_aspect(graph_client: "DataHubGraph", urn: str) -> Any:
    """Get the glossary terms aspect for the given URN."""
    from datahub.metadata.schema_classes import GlossaryTermsClass

    return graph_client.get_aspect(urn, GlossaryTermsClass)


class TermPropagationExpectation(SchemaFieldExpectation):
    """Expectation for glossary term propagation using Pydantic validation."""

    # Term-specific required fields
    expected_term_urn: str = Field(
        ..., min_length=1, description="Expected glossary term URN"
    )

    # Optional fields with defaults
    origin_dataset: str = Field("", description="Origin dataset name")
    origin_field: str = Field("", description="Origin field name")
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
        return "term_propagation"

    def check_expectation(
        self,
        graph_client: "DataHubGraph",
        action_urn: Optional[str] = None,
        rollback: bool = False,
    ) -> None:
        """Check if term propagation expectation is met."""
        from datahub.metadata.schema_classes import EditableSchemaMetadataClass
        from datahub.metadata.urns import Urn

        field_urn = self.get_urn(self.platform, self.dataset_name, self.field_name)
        dataset_urn = Urn.create_from_string(field_urn).entity_ids[0]
        field_path = Urn.create_from_string(field_urn).entity_ids[1]

        editable_schema_metadata_aspect = graph_client.get_aspect(
            dataset_urn, EditableSchemaMetadataClass
        )

        if not editable_schema_metadata_aspect:
            if self.propagation_found and not rollback:
                raise AssertionError(
                    f"Expected propagation but no editable schema metadata aspect found for {dataset_urn}"
                )
            return

        field_info = next(
            (
                x
                for x in editable_schema_metadata_aspect.editableSchemaFieldInfo
                if x.fieldPath == field_path
            ),
            None,
        )

        if not field_info:
            if self.propagation_found and not rollback:
                raise AssertionError(
                    f"Expected propagation but field {field_path} not found in editable schema"
                )
            return

        if not field_info.glossaryTerms:
            if self.propagation_found and not rollback:
                raise AssertionError(
                    f"Expected propagation but field {field_path} has no glossary terms"
                )
            return

        terms = field_info.glossaryTerms.terms
        terms_propagated_from_action = []
        if action_urn:
            terms_propagated_from_action = [
                x for x in terms if x.attribution and x.attribution.source == action_urn
            ]

        if self.propagation_found and not rollback:
            if action_urn:
                expected_term = next(
                    (
                        x
                        for x in terms_propagated_from_action
                        if x.urn == self.expected_term_urn
                    ),
                    None,
                )

                if not expected_term:
                    raise AssertionError(
                        f"Expected term {self.expected_term_urn} not found in propagated terms from action"
                    )

                self.check_attribution(
                    action_urn,
                    expected_term.attribution,
                    self.propagation_source,
                    self.propagation_origin,
                    self.propagation_via,
                )
            else:
                # Simple check without action attribution
                term_urns = [term.urn for term in terms]
                if self.expected_term_urn not in term_urns:
                    raise AssertionError(
                        f"Expected to have term '{self.expected_term_urn}' on {self.dataset_name}.{self.field_name} "
                        f"but found terms: {term_urns}"
                    )

        elif rollback and action_urn:
            # Check that expected terms were rolled back
            rollback_terms = [
                x
                for x in terms_propagated_from_action
                if x.urn == self.expected_term_urn
            ]
            if rollback_terms:
                raise AssertionError("Propagated term should have been rolled back")

    def explain(self) -> str:
        """Return a human-friendly explanation of what this expectation does."""
        term_name = (
            self.expected_term_urn.split(":")[-1]
            if self.expected_term_urn
            else "unknown term"
        )
        explanation = f"💡 Expect term '{term_name}' to be propagated to field '{self.field_name}' in dataset '{self.dataset_name}'"

        if self.origin_dataset and self.origin_field:
            explanation += f" from origin field '{self.origin_field}' in dataset '{self.origin_dataset}'"

        if self.is_live:
            explanation += " (during live propagation)"

        return explanation


class NoTermPropagationExpectation(SchemaFieldExpectation):
    """Expectation for no glossary term propagation using Pydantic validation."""

    # No additional fields needed beyond base schema field expectation

    # Optional fields
    is_live: bool = Field(False, description="Whether this is for live testing")

    def get_expectation_type(self) -> str:
        """Return the expectation type identifier."""
        return "no_term_propagation"

    def check_expectation(
        self,
        graph_client: "DataHubGraph",
        action_urn: Optional[str] = None,
        rollback: bool = False,
    ) -> None:
        """Check that no term propagation occurred."""
        schema_field_urn = self.get_urn(
            self.platform, self.dataset_name, self.field_name
        )
        glossary_terms_aspect = get_glossary_terms_aspect(
            graph_client, schema_field_urn
        )

        assert (
            glossary_terms_aspect is None
            or not glossary_terms_aspect.terms
            or len(glossary_terms_aspect.terms) == 0
        ), (
            f"Expected no glossary terms on {self.dataset_name}.{self.field_name} "
            f"but found: {[term.urn for term in glossary_terms_aspect.terms] if glossary_terms_aspect and glossary_terms_aspect.terms else 'None'}"
        )

    def explain(self) -> str:
        """Return a human-friendly explanation of what this expectation does."""
        explanation = f"🚫 Expect NO terms to be propagated to field '{self.field_name}' in dataset '{self.dataset_name}'"

        if self.is_live:
            explanation += " (during live propagation)"

        return explanation
