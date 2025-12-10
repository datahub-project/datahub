#!/usr/bin/env python3
"""
Shared AST (Abstract Syntax Tree) representations for RDF-to-DataHub transpilation.

This module defines shared data structures that aggregate entity types.
Entity-specific AST classes are now in their respective entity modules.
"""

from typing import Any, Dict, List

# Shared classes that are used across multiple entity types


class RDFGraph:
    """
    Internal AST representation of the complete RDF graph.

    Simplified: Static fields for the 2-3 entity types we support.
    """

    def __init__(self):
        # Entity fields (explicit for the types we support)
        self.glossary_terms: List[Any] = []
        self.relationships: List[Any] = []
        self.domains: List[Any] = []

        # Special fields
        self.metadata: Dict[str, Any] = {}


# DataHub AST Classes (Internal representation before SDK object creation)

# Aggregate classes that collect entity types


class DataHubGraph:
    """
    Internal AST representation of the complete DataHub graph.

    Simplified: Static fields for the 2-3 entity types we support.
    No dynamic initialization needed.
    """

    def __init__(self):
        # Entity fields (explicit for the 2-3 types we support)
        self.glossary_terms: List[Any] = []
        self.relationships: List[Any] = []
        self.domains: List[Any] = []

        # Special fields
        self.metadata: Dict[str, Any] = {}

    def get_summary(self) -> Dict[str, int]:
        """
        Get a summary of the DataHub graph contents.

        Returns:
            Dictionary mapping field names to entity counts
        """
        return {
            "glossary_terms": len(self.glossary_terms),
            "relationships": len(self.relationships),
            "domains": len(self.domains),
        }
