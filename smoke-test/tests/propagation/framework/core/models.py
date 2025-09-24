"""Common data models and expectations for propagation tests."""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Union

import pydantic
from pydantic import BaseModel

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import MetadataChangeProposalClass

logger = logging.getLogger(__name__)


class PropagationExpectation(BaseModel, ABC):
    """Base expectation class for propagation tests."""

    schema_field_urn: str  # the field that should be tested
    propagation_found: bool  # whether any propagation is expected
    propagation_source: Optional[str] = None  # the expected propagation source field
    propagation_via: Optional[str] = None  # the expected propagation via field
    propagation_origin: Optional[str] = None  # the expected propagation origin field

    @abstractmethod
    def explain(self) -> str:
        """Return a human-friendly explanation of what this expectation does."""
        pass

    def get_expectation_urn(self) -> Optional[str]:
        """Get the URN for this expectation (legacy compatibility)."""
        return self.schema_field_urn


class PropagationTestScenario(BaseModel):
    """Container for all test scenario data."""

    class Config:
        arbitrary_types_allowed = True

    base_graph: List[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]
    base_expectations: List[Any]
    pre_bootstrap_mutations: List[MetadataChangeProposalWrapper] = pydantic.Field(
        default_factory=list
    )
    mutations: List[MetadataChangeProposalWrapper]
    mutation_objects: List[Any] = pydantic.Field(
        default_factory=list
    )  # Store original mutation objects for explain() method
    post_mutation_expectations: List[Any] = pydantic.Field(default_factory=list)
    run_bootstrap: bool = True
    skip_bootstrap_on_timeout: bool = False  # Skip bootstrap if stats endpoint fails
    debug_mcps: bool = False  # Print all MCPs when enabled
    verbose_mode: bool = True  # Print human-friendly explanations when enabled
    cleanup_entities: bool = (
        True  # Automatically cleanup test entities (enabled by default)
    )

    def add_mutations(self, mutation_list: List[MetadataChangeProposalWrapper]) -> None:
        """Add mutations to the scenario from a mutation class result."""
        self.mutations.extend(mutation_list)
        self._merge_editable_schema_mutations()

    def add_mutation_objects(self, mutations: List[Any], verbose: bool = True) -> None:
        """Add mutation objects directly - applies mutations internally with explanations.

        Args:
            mutations: List of mutation objects that contain dataset_urn
            verbose: Whether to print mutation explanations
        """
        for mutation in mutations:
            # Print mutation explanation if verbose
            if verbose:
                logger.info(f"📝 {mutation.explain()}")

            # Store the original mutation object for later use in logging
            self.mutation_objects.append(mutation)

            # Apply the mutation (no URN needed since it's stored in the mutation)
            mcps = mutation.apply_mutation()
            self.mutations.extend(mcps)

        # Merge any conflicting schema mutations
        self._merge_editable_schema_mutations()

    def add_pre_bootstrap_mutations(
        self, mutations: List[Any], verbose: bool = True
    ) -> None:
        """Add pre-bootstrap mutation objects - applies mutations before bootstrap runs.

        Args:
            mutations: List of mutation objects that contain dataset_urn
            verbose: Whether to print mutation explanations
        """
        for mutation in mutations:
            # Print mutation explanation if verbose
            if verbose:
                logger.info(f"📝 Pre-bootstrap: {mutation.explain()}")

            # Apply the mutation (no URN needed since it's stored in the mutation)
            mcps = mutation.apply_mutation()
            self.pre_bootstrap_mutations.extend(mcps)

    def _merge_editable_schema_mutations(self) -> None:
        """Merge EditableSchemaMetadataClass mutations that target the same entityUrn."""
        from datahub.metadata.schema_classes import EditableSchemaMetadataClass

        # Group mutations by entityUrn and aspect type
        schema_mutations_by_urn: Dict[str, List[MetadataChangeProposalWrapper]] = {}
        other_mutations: List[MetadataChangeProposalWrapper] = []

        for mutation in self.mutations:
            if isinstance(mutation.aspect, EditableSchemaMetadataClass):
                entity_urn = mutation.entityUrn
                if entity_urn and entity_urn not in schema_mutations_by_urn:
                    schema_mutations_by_urn[entity_urn] = []
                if entity_urn:
                    schema_mutations_by_urn[entity_urn].append(mutation)
            else:
                other_mutations.append(mutation)

        # Merge mutations for each entityUrn
        merged_mutations = []
        for entity_urn, mutations in schema_mutations_by_urn.items():
            if len(mutations) == 1:
                # No merging needed
                merged_mutations.append(mutations[0])
            else:
                # Merge multiple mutations
                merged_mutation = self._merge_schema_mutations(entity_urn, mutations)
                merged_mutations.append(merged_mutation)

        # Update mutations list with merged results
        self.mutations = other_mutations + merged_mutations

    def _merge_schema_mutations(
        self, entity_urn: str, mutations: List[MetadataChangeProposalWrapper]
    ) -> MetadataChangeProposalWrapper:
        """Merge multiple EditableSchemaMetadataClass mutations for the same entity."""
        from datahub.emitter.mcp import MetadataChangeProposalWrapper
        from datahub.metadata.schema_classes import EditableSchemaMetadataClass

        timestamps = self._extract_timestamps(mutations)
        merged_fields = self._merge_field_infos(mutations)

        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=EditableSchemaMetadataClass(
                editableSchemaFieldInfo=list(merged_fields.values()),
                created=timestamps["created"],
                lastModified=timestamps["modified"],
            ),
        )

    def _extract_timestamps(
        self, mutations: List[MetadataChangeProposalWrapper]
    ) -> Dict[str, Any]:
        """Extract latest timestamps from mutations."""
        created_stamps = []
        modified_stamps = []

        for m in mutations:
            assert m.aspect is not None, (
                "Aspect should not be None for EditableSchemaMetadataClass mutations"
            )
            if hasattr(m.aspect, "created") and m.aspect.created:
                created_stamps.append(m.aspect.created)
            if hasattr(m.aspect, "lastModified") and m.aspect.lastModified:
                modified_stamps.append(m.aspect.lastModified)

        return {
            "created": max(created_stamps, key=lambda x: x.time)
            if created_stamps
            else None,
            "modified": max(modified_stamps, key=lambda x: x.time)
            if modified_stamps
            else None,
        }

    def _merge_field_infos(
        self, mutations: List[MetadataChangeProposalWrapper]
    ) -> Dict[str, Any]:
        """Merge field infos by fieldPath from multiple mutations."""
        from datahub.metadata.schema_classes import (
            EditableSchemaFieldInfoClass,
            EditableSchemaMetadataClass,
        )

        merged_fields = {}

        for mutation in mutations:
            assert mutation.aspect is not None, (
                "Aspect should not be None for EditableSchemaMetadataClass mutations"
            )
            assert isinstance(mutation.aspect, EditableSchemaMetadataClass), (
                "Aspect should be EditableSchemaMetadataClass"
            )
            schema_field_info = mutation.aspect.editableSchemaFieldInfo
            if schema_field_info:
                for field_info in schema_field_info:
                    field_path = field_info.fieldPath
                    if field_path not in merged_fields:
                        merged_fields[field_path] = EditableSchemaFieldInfoClass(
                            fieldPath=field_path,
                            description=field_info.description,
                            globalTags=field_info.globalTags,
                            glossaryTerms=field_info.glossaryTerms,
                        )
                    else:
                        self._merge_field_metadata(
                            merged_fields[field_path], field_info
                        )

        return merged_fields

    def _merge_field_metadata(self, existing: Any, new_field: Any) -> None:
        """Merge metadata from new_field into existing field."""
        # Merge description (latest wins)
        if new_field.description:
            existing.description = new_field.description

        # Merge tags (combine unique tags)
        if new_field.globalTags and new_field.globalTags.tags:
            if not existing.globalTags:
                existing.globalTags = new_field.globalTags
            else:
                existing_tag_urns = {tag.tag for tag in existing.globalTags.tags}
                for new_tag in new_field.globalTags.tags:
                    if new_tag.tag not in existing_tag_urns:
                        existing.globalTags.tags.append(new_tag)

        # Merge terms (combine unique terms)
        if new_field.glossaryTerms and new_field.glossaryTerms.terms:
            if not existing.glossaryTerms:
                existing.glossaryTerms = new_field.glossaryTerms
            else:
                existing_term_urns = {term.urn for term in existing.glossaryTerms.terms}
                for new_term in new_field.glossaryTerms.terms:
                    if new_term.urn not in existing_term_urns:
                        existing.glossaryTerms.terms.append(new_term)

    def get_urns(self) -> list[str]:
        """Extract all URNs referenced in this scenario."""
        urns: Set[str] = set()
        for mcp in self.base_graph:
            if mcp.entityUrn:
                urns.add(mcp.entityUrn)
        for mcp in self.mutations:
            if mcp.entityUrn:
                urns.add(mcp.entityUrn)
        for expectation in self.base_expectations + self.post_mutation_expectations:
            # Get URN using the standard method - works for all expectation types
            expectation_urn = expectation.get_expectation_urn()
            if expectation_urn:
                urns.add(expectation_urn)
        return list(urns)
