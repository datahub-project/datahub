#!/usr/bin/env python3
"""
Base RDF Dialect interface and types.

This module defines the common interface that all RDF dialects must implement.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional

from rdflib import Graph, URIRef


class RDFDialect(Enum):
    """RDF modeling dialects for different approaches."""

    DEFAULT = "default"  # SKOS-based business glossary (default)
    FIBO = "fibo"  # OWL-based formal ontology
    GENERIC = "generic"  # Mixed or unknown approach


class RDFDialectInterface(ABC):
    """Abstract base class for RDF dialect implementations."""

    @property
    @abstractmethod
    def dialect_type(self) -> RDFDialect:
        """Return the dialect type."""
        pass

    @abstractmethod
    def detect(self, graph: Graph) -> bool:
        """
        Detect if this dialect matches the given RDF graph.

        Args:
            graph: RDFLib Graph to analyze

        Returns:
            True if this dialect matches the graph
        """
        pass

    @abstractmethod
    def matches_subject(self, graph: Graph, subject: URIRef) -> bool:
        """
        Check if a specific subject matches this dialect.

        Args:
            graph: RDFLib Graph containing the subject
            subject: URIRef to check

        Returns:
            True if the subject matches this dialect
        """
        pass

    @abstractmethod
    def classify_entity_type(self, graph: Graph, subject: URIRef) -> Optional[str]:
        """
        Classify the entity type of a subject using dialect-specific rules.

        Args:
            graph: RDFLib Graph containing the subject
            subject: URIRef to classify

        Returns:
            Entity type string or None if not applicable
        """
        pass

    @abstractmethod
    def looks_like_glossary_term(self, graph: Graph, uri: URIRef) -> bool:
        """
        Check if a URI looks like a glossary term in this dialect.

        Args:
            graph: RDFLib Graph containing the URI
            uri: URIRef to check

        Returns:
            True if the URI looks like a glossary term
        """
        pass

    @abstractmethod
    def looks_like_structured_property(self, graph: Graph, uri: URIRef) -> bool:
        """
        Check if a URI looks like a structured property in this dialect.

        Args:
            graph: RDFLib Graph containing the URI
            uri: URIRef to check

        Returns:
            True if the URI looks like a structured property
        """
        pass
