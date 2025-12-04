#!/usr/bin/env python3
"""
Target Factory Interface

This module provides the abstract interface for output targets.
Only TargetInterface is needed for the ingestion source.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

from rdflib import Graph

from datahub.ingestion.source.rdf.core.ast import DataHubGraph

logger = logging.getLogger(__name__)


class TargetInterface(ABC):
    """Abstract interface for output targets."""

    @abstractmethod
    def execute(
        self, datahub_ast: DataHubGraph, rdf_graph: Graph = None
    ) -> Dict[str, Any]:
        """Execute the target with the DataHub AST."""
        pass

    @abstractmethod
    def get_target_info(self) -> dict:
        """Get information about this target."""
        pass
