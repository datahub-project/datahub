"""Base classes for step processors."""

import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from datahub.ingestion.source.pentaho.context import ProcessingContext
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
        step: ET.Element,
        context: "ProcessingContext",
        root: Optional[ET.Element] = None,
    ):
        """Process the step and update context with lineage information."""
        pass
