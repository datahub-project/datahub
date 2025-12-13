#!/usr/bin/env python3
"""
Dialect Router implementation.

This router handles dialect detection and routing for different RDF modeling approaches.
"""

from typing import Optional

from rdflib import Graph, URIRef

from datahub.ingestion.source.rdf.dialects.base import RDFDialect, RDFDialectInterface
from datahub.ingestion.source.rdf.dialects.bcbs239 import DefaultDialect
from datahub.ingestion.source.rdf.dialects.fibo import FIBODialect
from datahub.ingestion.source.rdf.dialects.generic import GenericDialect


class DialectRouter(RDFDialectInterface):
    """Router that handles dialect detection and routing."""

    def __init__(self, forced_dialect: Optional[RDFDialect] = None):
        """
        Initialize the dialect router.

        Args:
            forced_dialect: If provided, force this dialect instead of auto-detection
        """
        self.forced_dialect = forced_dialect
        self._available_dialects = [DefaultDialect(), FIBODialect(), GenericDialect()]

    @property
    def dialect_type(self) -> RDFDialect:
        """Return the dialect type."""
        if self.forced_dialect:
            return self.forced_dialect
        return RDFDialect.DEFAULT  # Default fallback

    def detect(self, graph: Graph) -> bool:
        """
        Detect if this router can handle the given RDF graph.

        Args:
            graph: RDFLib Graph to analyze

        Returns:
            Always True (router can handle any graph)
        """
        return True

    def matches_subject(self, graph: Graph, subject: URIRef) -> bool:
        """
        Check if a specific subject matches any dialect.

        Args:
            graph: RDFLib Graph containing the subject
            subject: URIRef to check

        Returns:
            True if the subject matches any dialect
        """
        # If forced dialect, use that
        if self.forced_dialect:
            dialect = self._get_dialect_by_type(self.forced_dialect)
            return dialect.matches_subject(graph, subject)

        # Otherwise, try each dialect
        for dialect in self._available_dialects:
            if dialect.matches_subject(graph, subject):
                return True

        return False

    def classify_entity_type(self, graph: Graph, subject: URIRef) -> Optional[str]:
        """
        Classify the entity type using the appropriate dialect.

        Args:
            graph: RDFLib Graph containing the subject
            subject: URIRef to classify

        Returns:
            Entity type string or None if not applicable
        """
        # If forced dialect, use that
        if self.forced_dialect:
            dialect = self._get_dialect_by_type(self.forced_dialect)
            return dialect.classify_entity_type(graph, subject)

        # Otherwise, try each dialect in order of specificity
        for dialect in self._available_dialects:
            if dialect.matches_subject(graph, subject):
                return dialect.classify_entity_type(graph, subject)

        return None

    def looks_like_glossary_term(self, graph: Graph, uri: URIRef) -> bool:
        """
        Check if a URI looks like a glossary term using the appropriate dialect.

        Args:
            graph: RDFLib Graph containing the URI
            uri: URIRef to check

        Returns:
            True if the URI looks like a glossary term
        """
        # If forced dialect, use that
        if self.forced_dialect:
            dialect = self._get_dialect_by_type(self.forced_dialect)
            return dialect.looks_like_glossary_term(graph, uri)

        # Otherwise, try each dialect
        for dialect in self._available_dialects:
            if dialect.matches_subject(graph, uri):
                return dialect.looks_like_glossary_term(graph, uri)

        return False

    def looks_like_structured_property(self, graph: Graph, uri: URIRef) -> bool:
        """
        Check if a URI looks like a structured property using the appropriate dialect.

        Args:
            graph: RDFLib Graph containing the URI
            uri: URIRef to check

        Returns:
            True if the URI looks like a structured property
        """
        # If forced dialect, use that
        if self.forced_dialect:
            dialect = self._get_dialect_by_type(self.forced_dialect)
            return dialect.looks_like_structured_property(graph, uri)

        # Otherwise, try each dialect
        for dialect in self._available_dialects:
            if dialect.matches_subject(graph, uri):
                return dialect.looks_like_structured_property(graph, uri)

        return False

    def _get_dialect_by_type(self, dialect_type: RDFDialect) -> RDFDialectInterface:
        """Get a dialect instance by type."""
        for dialect in self._available_dialects:
            if dialect.dialect_type == dialect_type:
                return dialect

        # Fallback to default
        return DefaultDialect()

    def get_detected_dialect(self, graph: Graph) -> RDFDialect:
        """
        Get the detected dialect for a graph.

        Args:
            graph: RDFLib Graph to analyze

        Returns:
            The detected dialect type
        """
        if self.forced_dialect:
            return self.forced_dialect

        # Try each dialect in order of specificity
        for dialect in self._available_dialects:
            if dialect.detect(graph):
                return dialect.dialect_type

        # Fallback to default
        return RDFDialect.DEFAULT
