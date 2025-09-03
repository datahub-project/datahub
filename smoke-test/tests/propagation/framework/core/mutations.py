"""Typed mutation dataclasses for propagation testing.

This module defines strongly-typed mutation classes that replace the dict-based
approach, providing better type safety and IDE support.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper


@dataclass(frozen=True)
class BaseMutation(ABC):
    """Base class for all typed mutations."""

    dataset_name: str

    @abstractmethod
    def get_mutation_type(self) -> str:
        """Return the type identifier for this mutation."""
        pass

    @abstractmethod
    def apply_mutation(self, dataset_urn: str) -> "MetadataChangeProposalWrapper":
        """Apply this mutation and return the appropriate MCP."""
        pass


@dataclass(frozen=True)
class FieldMutation(BaseMutation):
    """Base class for field-level mutations."""

    field_name: str
