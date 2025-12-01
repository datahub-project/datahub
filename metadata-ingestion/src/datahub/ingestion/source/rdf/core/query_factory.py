#!/usr/bin/env python3
"""
Query Factory Interface

This module provides a factory interface for creating different types of queries.
Supports SPARQL queries and custom queries with dependency injection.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

from rdflib import Graph

logger = logging.getLogger(__name__)


class QueryInterface(ABC):
    """Abstract interface for queries."""

    @abstractmethod
    def execute(self, graph: Graph) -> Graph:
        """Execute the query against the graph and return results."""
        pass

    @abstractmethod
    def get_query_info(self) -> dict:
        """Get information about this query."""
        pass


class SPARQLQuery(QueryInterface):
    """Query that executes SPARQL against the graph."""

    def __init__(self, sparql_query: str, description: str = None):
        self.sparql_query = sparql_query
        self.description = description or "SPARQL Query"

    def execute(self, graph: Graph) -> Graph:
        """Execute SPARQL query against the graph."""
        try:
            logger.info(f"Executing SPARQL query: {self.description}")

            # Execute SPARQL query
            results = graph.query(self.sparql_query)

            # Convert results to a new graph
            result_graph = Graph()

            # Handle different result types
            if hasattr(results, "bindings"):
                # SELECT query results
                for binding in results.bindings:
                    # Convert bindings to triples (simplified)
                    # This is a basic implementation - could be enhanced
                    for var_name, value in binding.items():
                        if value:
                            # Create a simple triple representation
                            subject = f"urn:query:result:{var_name}"
                            predicate = "urn:query:has_value"
                            result_graph.add((subject, predicate, value))
            else:
                # CONSTRUCT/DESCRIBE query results
                result_graph = results

            logger.info(f"SPARQL query executed: {len(result_graph)} triples in result")
            return result_graph

        except Exception as e:
            logger.error(f"SPARQL query execution failed: {e}")
            raise

    def get_query_info(self) -> dict:
        """Get SPARQL query information."""
        return {
            "type": "sparql",
            "description": self.description,
            "query_length": len(self.sparql_query),
            "query_preview": self.sparql_query[:100] + "..."
            if len(self.sparql_query) > 100
            else self.sparql_query,
        }


class PassThroughQuery(QueryInterface):
    """Query that passes through the entire graph unchanged."""

    def __init__(self, description: str = "Pass-through Query"):
        self.description = description

    def execute(self, graph: Graph) -> Graph:
        """Pass through the entire graph unchanged."""
        logger.info(f"Executing pass-through query: {self.description}")
        logger.info(f"Pass-through query executed: {len(graph)} triples")
        return graph

    def get_query_info(self) -> dict:
        """Get pass-through query information."""
        return {"type": "pass_through", "description": self.description}


class FilterQuery(QueryInterface):
    """Query that filters the graph based on criteria."""

    def __init__(self, filter_criteria: Dict[str, Any], description: str = None):
        self.filter_criteria = filter_criteria
        self.description = description or "Filter Query"

    def execute(self, graph: Graph) -> Graph:
        """Execute filter query against the graph."""
        try:
            logger.info(f"Executing filter query: {self.description}")

            result_graph = Graph()

            # Apply filters based on criteria
            for subject, predicate, obj in graph:
                include = True

                # Filter by subject pattern
                if "subject_pattern" in self.filter_criteria:
                    pattern = self.filter_criteria["subject_pattern"]
                    if pattern not in str(subject):
                        include = False

                # Filter by predicate pattern
                if "predicate_pattern" in self.filter_criteria:
                    pattern = self.filter_criteria["predicate_pattern"]
                    if pattern not in str(predicate):
                        include = False

                # Filter by object pattern
                if "object_pattern" in self.filter_criteria:
                    pattern = self.filter_criteria["object_pattern"]
                    if pattern not in str(obj):
                        include = False

                # Filter by namespace
                if "namespace" in self.filter_criteria:
                    namespace = self.filter_criteria["namespace"]
                    if not str(subject).startswith(namespace):
                        include = False

                if include:
                    result_graph.add((subject, predicate, obj))

            logger.info(f"Filter query executed: {len(result_graph)} triples in result")
            return result_graph

        except Exception as e:
            logger.error(f"Filter query execution failed: {e}")
            raise

    def get_query_info(self) -> dict:
        """Get filter query information."""
        return {
            "type": "filter",
            "description": self.description,
            "criteria": self.filter_criteria,
        }


class CustomQuery(QueryInterface):
    """Query that executes custom logic."""

    def __init__(self, query_function, description: str = None):
        self.query_function = query_function
        self.description = description or "Custom Query"

    def execute(self, graph: Graph) -> Graph:
        """Execute custom query function."""
        try:
            logger.info(f"Executing custom query: {self.description}")
            result_graph = self.query_function(graph)
            logger.info(f"Custom query executed: {len(result_graph)} triples in result")
            return result_graph
        except Exception as e:
            logger.error(f"Custom query execution failed: {e}")
            raise

    def get_query_info(self) -> dict:
        """Get custom query information."""
        function_name = getattr(self.query_function, "__name__", None)
        if function_name is None:
            raise ValueError("Query function has no name attribute")
        return {
            "type": "custom",
            "description": self.description,
            "function_name": function_name,
        }


class QueryFactory:
    """Factory for creating queries."""

    @staticmethod
    def create_sparql_query(sparql_query: str, description: str = None) -> SPARQLQuery:
        """Create a SPARQL query."""
        return SPARQLQuery(sparql_query, description)

    @staticmethod
    def create_pass_through_query(description: str = None) -> PassThroughQuery:
        """Create a pass-through query."""
        return PassThroughQuery(description)

    @staticmethod
    def create_filter_query(
        filter_criteria: Dict[str, Any], description: str = None
    ) -> FilterQuery:
        """Create a filter query."""
        return FilterQuery(filter_criteria, description)

    @staticmethod
    def create_custom_query(query_function, description: str = None) -> CustomQuery:
        """Create a custom query."""
        return CustomQuery(query_function, description)

    @staticmethod
    def create_query_from_config(query_type: str, **kwargs) -> QueryInterface:
        """Create a query from configuration."""
        if query_type == "sparql":
            sparql_query = kwargs.get("sparql_query")
            if not sparql_query:
                raise ValueError("sparql_query required for SPARQL query")
            description = kwargs.get("description")
            return QueryFactory.create_sparql_query(sparql_query, description)

        elif query_type == "pass_through":
            description = kwargs.get("description")
            return QueryFactory.create_pass_through_query(description)

        elif query_type == "filter":
            filter_criteria = kwargs.get("filter_criteria", {})
            description = kwargs.get("description")
            return QueryFactory.create_filter_query(filter_criteria, description)

        elif query_type == "custom":
            query_function = kwargs.get("query_function")
            if not query_function:
                raise ValueError("query_function required for custom query")
            description = kwargs.get("description")
            return QueryFactory.create_custom_query(query_function, description)

        else:
            raise ValueError(f"Unknown query type: {query_type}")
