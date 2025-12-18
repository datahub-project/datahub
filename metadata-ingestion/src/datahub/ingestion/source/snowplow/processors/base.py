"""
Base processor interface for Snowplow entity extraction.

Defines the common interface that all entity processors must implement,
following the Strategy pattern for extracting different types of metadata.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterable

from datahub.ingestion.api.workunit import MetadataWorkUnit

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.dependencies import (
        IngestionState,
        ProcessorDependencies,
    )


class EntityProcessor(ABC):
    """
    Base class for all Snowplow entity processors.

    Each processor handles extraction of one entity type (schemas, pipelines, etc.)
    and can be tested independently from the main source class.

    Uses dependency injection pattern - processors receive explicit dependencies
    and shared state separately, maintaining clean separation between immutable
    config and mutable runtime data.
    """

    def __init__(self, deps: "ProcessorDependencies", state: "IngestionState"):
        """
        Initialize processor with dependencies and shared state.

        Args:
            deps: Explicit immutable dependencies needed by this processor
            state: Shared mutable state populated during extraction
        """
        self.deps = deps
        self.state = state
        self.config = deps.config
        self.report = deps.report
        self.cache = deps.cache
        self.urn_factory = deps.urn_factory

    @abstractmethod
    def extract(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract metadata for this entity type.

        Yields:
            MetadataWorkUnit: Metadata work units to be processed by DataHub

        Raises:
            Exception: Any errors during extraction (should be logged and handled)
        """
        pass

    def is_enabled(self) -> bool:
        """
        Check if this processor should run.

        Returns:
            bool: True if processor is enabled based on config, False otherwise

        Default implementation returns True. Override to add config-based enabling.
        """
        return True

    def __repr__(self) -> str:
        """String representation of processor."""
        return f"{self.__class__.__name__}()"
