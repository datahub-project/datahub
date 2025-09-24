"""Typed mutation dataclasses for propagation testing.

This module defines strongly-typed mutation classes that replace the dict-based
approach, providing better type safety and IDE support.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Union

if TYPE_CHECKING:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import MetadataChangeProposalClass


@dataclass(frozen=True)
class BaseMutation(ABC):
    """Base class for all typed mutations."""

    dataset_urn: str

    @property
    def dataset_name(self) -> str:
        """Extract dataset name from the URN."""
        from datahub.utilities.urns.urn import Urn

        try:
            parsed_urn = Urn.from_string(self.dataset_urn)
            # For dataset URNs, the dataset name is the last part of entity_ids[1]
            # Format: urn:li:dataset:(urn:li:dataPlatform:platform,dataset_name,env)
            return parsed_urn.entity_ids[1]
        except Exception:
            # Fallback: return the URN itself
            return self.dataset_urn

    @abstractmethod
    def get_mutation_type(self) -> str:
        """Return the type identifier for this mutation."""
        pass

    @abstractmethod
    def explain(self) -> str:
        """Return a human-readable explanation of what this mutation does."""
        pass

    @abstractmethod
    def apply_mutation(
        self,
    ) -> Union[
        List["MetadataChangeProposalWrapper"], List["MetadataChangeProposalClass"]
    ]:
        """Apply this mutation and return the appropriate MCPs."""
        pass


@dataclass(frozen=True)
class FieldMutation(BaseMutation):
    """Base class for field-level mutations."""

    field_name: str
