"""Term-specific expectation classes for glossary term propagation validation."""

from typing import TYPE_CHECKING, Any, Optional

from pydantic import Field

from tests.propagation.framework.core.expectations import (
    DatasetExpectation,
    SchemaFieldExpectation,
)

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
        validation_data = self._prepare_validation_data(graph_client, rollback)
        if not validation_data:
            return

        self._execute_validation(validation_data, action_urn, rollback, graph_client)

    def _prepare_validation_data(self, graph_client, rollback):
        """Prepare all data needed for validation."""
        from datahub.utilities.urns.urn import Urn

        # Use the field URN directly
        field_urn = self.field_urn
        dataset_urn = Urn.from_string(field_urn).entity_ids[0]
        field_path = Urn.from_string(field_urn).entity_ids[1]

        # Try to get terms from both locations: dataset-level and field-level
        term_urns = []

        # Check dataset-level terms in EditableSchemaMetadataClass
        field_info = self._get_field_info_from_dataset(
            graph_client, dataset_urn, field_path
        )
        if field_info and field_info.glossaryTerms and field_info.glossaryTerms.terms:
            term_urns.extend([term.urn for term in field_info.glossaryTerms.terms])

        # Check field-level terms in GlossaryTermsClass on field URN
        field_level_terms = self._get_field_level_terms(graph_client, field_urn)
        if field_level_terms and field_level_terms.terms:
            term_urns.extend([term.urn for term in field_level_terms.terms])

        # Remove duplicates while preserving order
        unique_term_urns = list(dict.fromkeys(term_urns))

        # If no terms found and propagation is expected, that's an error
        if not unique_term_urns and self.propagation_found and not rollback:
            # Check if we have any metadata at all for better error messages
            has_dataset_metadata = (
                self._get_editable_schema_metadata(graph_client, dataset_urn)
                is not None
            )
            has_field_metadata = field_level_terms is not None

            if not has_dataset_metadata and not has_field_metadata:
                raise AssertionError(
                    f"Expected term propagation but no metadata found for field {field_urn}"
                )
            else:
                raise AssertionError(
                    f"Expected term propagation but no terms found for field {field_urn}"
                )

        return {
            "field_info": field_info,  # Keep for compatibility
            "term_urns": unique_term_urns,
            "field_urn": field_urn,
        }

    def _execute_validation(self, validation_data, action_urn, rollback, graph_client):
        """Execute validation based on mode."""
        term_urns = validation_data["term_urns"]

        terms_propagated_from_action = self._get_terms_from_action(
            validation_data, action_urn, graph_client
        )

        if self.propagation_found and not rollback:
            self._validate_propagation_found(
                action_urn, terms_propagated_from_action, term_urns
            )
        elif rollback and action_urn:
            self._validate_rollback(terms_propagated_from_action)
        elif not self.propagation_found and not rollback:
            self._validate_no_propagation(term_urns)

    def _get_field_info_from_dataset(self, graph_client, dataset_urn, field_path):
        """Get field info from dataset-level EditableSchemaMetadataClass (no validation)."""
        from datahub.metadata.schema_classes import EditableSchemaMetadataClass

        editable_schema_metadata_aspect = graph_client.get_aspect(
            dataset_urn, EditableSchemaMetadataClass
        )

        if (
            not editable_schema_metadata_aspect
            or not editable_schema_metadata_aspect.editableSchemaFieldInfo
        ):
            return None

        field_info = next(
            (
                x
                for x in editable_schema_metadata_aspect.editableSchemaFieldInfo
                if x.fieldPath == field_path
            ),
            None,
        )
        return field_info

    def _get_field_level_terms(self, graph_client, field_urn):
        """Get GlossaryTermsClass directly from field URN."""
        return get_glossary_terms_aspect(graph_client, field_urn)

    def _get_editable_schema_metadata(self, graph_client, dataset_urn):
        """Get EditableSchemaMetadataClass from dataset URN."""
        from datahub.metadata.schema_classes import EditableSchemaMetadataClass

        return graph_client.get_aspect(dataset_urn, EditableSchemaMetadataClass)

    def _get_terms_from_action(self, validation_data, action_urn, graph_client):
        """Get terms that were propagated by the specific action."""
        if not action_urn:
            return []

        terms_from_action = []
        field_info = validation_data.get("field_info")
        field_urn = validation_data.get("field_urn")

        # Check dataset-level terms (field_info from EditableSchemaMetadataClass)
        if field_info and field_info.glossaryTerms and field_info.glossaryTerms.terms:
            terms_from_action.extend(
                [
                    x
                    for x in field_info.glossaryTerms.terms
                    if hasattr(x, "attribution")
                    and x.attribution
                    and x.attribution.source == action_urn
                ]
            )

        # Check field-level terms (GlossaryTermsClass on field URN)
        if field_urn:
            field_level_terms = self._get_field_level_terms(graph_client, field_urn)
            if field_level_terms and field_level_terms.terms:
                terms_from_action.extend(
                    [
                        x
                        for x in field_level_terms.terms
                        if hasattr(x, "attribution")
                        and x.attribution
                        and x.attribution.source == action_urn
                    ]
                )

        return terms_from_action

    def _validate_propagation_found(
        self, action_urn, terms_propagated_from_action, term_urns
    ):
        """Validate that expected propagation occurred."""
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
            if self.expected_term_urn not in term_urns:
                raise AssertionError(
                    f"Expected to have term '{self.expected_term_urn}' on {self.field_urn} "
                    f"but found terms: {term_urns}"
                )

    def _validate_rollback(self, terms_propagated_from_action):
        """Validate that terms were properly rolled back."""
        rollback_terms = [
            x for x in terms_propagated_from_action if x.urn == self.expected_term_urn
        ]
        if rollback_terms:
            raise AssertionError("Propagated term should have been rolled back")

    def _validate_no_propagation(self, term_urns):
        """Validate that no unexpected propagation occurred."""
        if self.expected_term_urn in term_urns:
            raise AssertionError(f"Unexpected term found: {self.expected_term_urn}")

    def explain(self, rollback: bool = False) -> str:
        """Return a human-friendly explanation of what this expectation does."""
        term_name = (
            self.expected_term_urn.split(":")[-1]
            if self.expected_term_urn
            else "unknown term"
        )

        if rollback:
            explanation = f"💡 Expected removal of term '{term_name}' from field '{self.field_urn}'"
        else:
            explanation = f"💡 Expect term '{term_name}' to be propagated to field '{self.field_urn}'"

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
        # These fields are guaranteed to be non-None after validation
        # Use the field URN directly
        schema_field_urn = self.field_urn
        glossary_terms_aspect = get_glossary_terms_aspect(
            graph_client, schema_field_urn
        )

        assert (
            glossary_terms_aspect is None
            or not glossary_terms_aspect.terms
            or len(glossary_terms_aspect.terms) == 0
        ), (
            f"Expected no glossary terms on {self.field_urn} "
            f"but found: {[term.urn for term in glossary_terms_aspect.terms] if glossary_terms_aspect and glossary_terms_aspect.terms else 'None'}"
        )

    def explain(self, rollback: bool = False) -> str:
        """Return a human-friendly explanation of what this expectation does."""
        # For "no propagation" expectations, rollback mode doesn't change the meaning
        explanation = f"🚫 Expect NO terms to be propagated to field '{self.field_urn}'"

        if self.is_live:
            explanation += " (during live propagation)"

        return explanation


# Dataset-level term expectations
class DatasetTermPropagationExpectation(DatasetExpectation):
    """Expectation for dataset-level glossary term propagation using Pydantic validation."""

    # Term-specific required fields
    expected_term_urn: str = Field(
        ..., min_length=1, description="Expected glossary term URN"
    )

    # Optional fields with defaults
    origin_dataset: str = Field("", description="Origin dataset name")
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

    def get_expectation_type(self) -> str:
        """Return the expectation type identifier."""
        return "dataset_term_propagation"

    def check_expectation(
        self,
        graph_client: "DataHubGraph",
        action_urn: Optional[str] = None,
        rollback: bool = False,
    ) -> None:
        """Check if dataset term propagation expectation is met."""
        dataset_urn = self.dataset_urn
        glossary_terms_aspect = get_glossary_terms_aspect(graph_client, dataset_urn)

        if rollback:
            # During rollback, expect the term to NOT be present or not have the expected value
            if glossary_terms_aspect and glossary_terms_aspect.terms:
                # Check if our specific expected term exists
                found_expected_term = any(
                    term.urn == self.expected_term_urn
                    for term in glossary_terms_aspect.terms
                )
                assert not found_expected_term, (
                    f"Expected term '{self.expected_term_urn}' to be removed from dataset {self.dataset_urn} "
                    f"after rollback, but it was still found"
                )
        else:
            # Normal mode: expect the term to be present
            assert glossary_terms_aspect is not None, (
                f"Expected to have glossary terms aspect on dataset {self.dataset_urn} but it was missing"
            )
            assert (
                glossary_terms_aspect.terms and len(glossary_terms_aspect.terms) > 0
            ), (
                f"Expected to have glossary terms on dataset {self.dataset_urn} but none found"
            )

            # Check if the expected term exists
            term_found = any(
                term.urn == self.expected_term_urn
                for term in glossary_terms_aspect.terms
            )
            assert term_found, (
                f"Expected term '{self.expected_term_urn}' not found on dataset {self.dataset_urn}. "
                f"Found terms: {[term.urn for term in glossary_terms_aspect.terms]}"
            )

            # If action_urn is provided, check attribution
            if action_urn:
                expected_term = next(
                    (
                        term
                        for term in glossary_terms_aspect.terms
                        if term.urn == self.expected_term_urn
                    ),
                    None,
                )
                if expected_term and expected_term.attribution:
                    self.check_attribution(
                        action_urn,
                        expected_term.attribution,
                        self.propagation_source,
                        self.propagation_origin,
                        None,  # No "via" field for dataset-level propagation
                    )

    def explain(self, rollback: bool = False) -> Optional[str]:
        """Provide human-readable explanation of the expectation."""
        term_name = (
            self.expected_term_urn.split(":")[-1]
            if self.expected_term_urn
            else "unknown term"
        )

        if rollback:
            explanation = f"💡 Expected removal of dataset term '{term_name}' from dataset {self.dataset_urn}"
        else:
            explanation = (
                f"💡 Expected dataset term '{term_name}' on dataset {self.dataset_urn}"
            )

        if self.origin_dataset:
            explanation += f" from origin dataset '{self.origin_dataset}'"

        if self.is_live:
            explanation += " (during live propagation)"

        return explanation


class NoDatasetTermPropagationExpectation(DatasetExpectation):
    """Expectation for no dataset-level glossary term propagation using Pydantic validation."""

    # No additional fields needed beyond base dataset expectation

    # Optional fields
    is_live: bool = Field(False, description="Whether this is for live testing")

    def get_expectation_type(self) -> str:
        """Return the expectation type identifier."""
        return "no_dataset_term_propagation"

    def check_expectation(
        self,
        graph_client: "DataHubGraph",
        action_urn: Optional[str] = None,
        rollback: bool = False,
    ) -> None:
        """Check that no dataset term propagation occurred."""
        dataset_urn = self.dataset_urn
        glossary_terms_aspect = get_glossary_terms_aspect(graph_client, dataset_urn)

        assert (
            glossary_terms_aspect is None
            or not glossary_terms_aspect.terms
            or len(glossary_terms_aspect.terms) == 0
        ), (
            f"Expected no glossary terms on dataset {self.dataset_urn} "
            f"but found: {[term.urn for term in glossary_terms_aspect.terms] if glossary_terms_aspect and glossary_terms_aspect.terms else 'None'}"
        )

    def explain(self, rollback: bool = False) -> Optional[str]:
        """Provide human-readable explanation of the expectation."""
        # For "no propagation" expectations, rollback mode doesn't change the meaning
        explanation = (
            f"🚫 Expected NO terms to be propagated to dataset {self.dataset_urn}"
        )

        if self.is_live:
            explanation += " (during live propagation)"

        return explanation
