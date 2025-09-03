"""Common data models and expectations for propagation tests."""

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Set, Union

import pydantic
from pydantic import BaseModel

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import MetadataChangeProposalClass


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


class DocumentationPropagationExpectation(PropagationExpectation):
    """Expectation for documentation propagation tests."""

    propagated_description: Optional[str] = None  # the expected propagated description


class TermPropagationExpectation(PropagationExpectation):
    """Expectation for glossary term propagation tests."""

    propagated_term: Optional[str] = None  # the expected propagated term


class TagPropagationExpectation(PropagationExpectation):
    """Expectation for tag propagation tests."""

    propagated_tag: Optional[str] = None  # the expected propagated tag


class PropagationTestScenario(BaseModel):
    """Container for all test scenario data."""

    class Config:
        arbitrary_types_allowed = True

    base_graph: List[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]
    base_expectations: List[Any]
    mutations: List[MetadataChangeProposalWrapper]
    post_mutation_expectations: List[Any] = pydantic.Field(default_factory=list)
    run_bootstrap: bool = True
    skip_bootstrap_on_timeout: bool = False  # Skip bootstrap if stats endpoint fails
    debug_mcps: bool = False  # Print all MCPs when enabled
    verbose_mode: bool = True  # Print human-friendly explanations when enabled
    cleanup_entities: bool = (
        True  # Automatically cleanup test entities (enabled by default)
    )

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
