"""Base classes for step processors."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional
from xml.etree.ElementTree import (
    Element,  # nosec B405 - only for type hints; parsing goes through defusedxml
)

from datahub.ingestion.source.pentaho.context import ProcessingContext

if TYPE_CHECKING:
    # PentahoSource cannot be imported at runtime: pentaho imports
    # step_processors, which imports this module, so the reference is circular.
    from datahub.ingestion.source.pentaho.pentaho import PentahoSource


class StepProcessor(ABC):
    """Base class for processing different step types."""

    def __init__(self, source: "PentahoSource"):
        self.source = source
        self.config = source.config

    @abstractmethod
    def can_process(self, step_type: str) -> bool:
        """Check if this processor can handle the given step type."""
        pass

    @abstractmethod
    def process(
        self,
        step: Element,
        context: ProcessingContext,
        root: Optional[Element] = None,
    ) -> None:
        """Process the step and update context with lineage information."""
        pass
