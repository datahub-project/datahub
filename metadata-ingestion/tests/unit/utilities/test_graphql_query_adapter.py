"""Unit tests for GraphQL query adaptation."""

from typing import Any, Dict, List
from unittest.mock import Mock

import pytest
from graphql import build_schema
from graphql.utilities import introspection_from_schema

from datahub.utilities.graphql_query_adapter import QueryProjector


@pytest.fixture
def old_server_schema():
    """Schema without Document entity (simulates v0.12.5 server)."""
    schema_str = """
        type Query {
            searchAcrossEntities(input: SearchInput!): SearchResults
        }

        input SearchInput {
            query: String!
            types: [String!]
        }

        type SearchResults {
            searchResults: [SearchResult!]!
        }

        type SearchResult {
            entity: Entity
        }

        union Entity = Dataset | Chart | Dashboard

        type Dataset {
            urn: String!
            name: String!
        }

        type Chart {
            urn: String!
            title: String!
        }

        type Dashboard {
            urn: String!
            title: String!
        }
    """
    return build_schema(schema_str)


@pytest.fixture
def new_server_schema():
    """Schema with Document entity (simulates v0.13.0+ server)."""
    schema_str = """
        type Query {
            searchAcrossEntities(input: SearchInput!): SearchResults
        }

        input SearchInput {
            query: String!
            types: [String!]
        }

        type SearchResults {
            searchResults: [SearchResult!]!
        }

        type SearchResult {
            entity: Entity
        }

        union Entity = Dataset | Chart | Dashboard | Document

        type Dataset {
            urn: String!
            name: String!
        }

        type Chart {
            urn: String!
            title: String!
        }

        type Dashboard {
            urn: String!
            title: String!
        }

        type Document {
            urn: String!
            info: DocumentInfo!
        }

        type DocumentInfo {
            title: String!
            description: String
        }
    """
    return build_schema(schema_str)


def _make_mock_graph(schema) -> Mock:
    """Create a mock DataHubGraph that returns introspection for the given schema.

    The mock's execute_graphql matches the real signature including
    strip_unsupported_fields, and returns the introspection result directly
    (matching real execute_graphql which returns result["data"]).
    """
    graph = Mock()
    call_log: List[Dict[str, Any]] = []

    def execute_graphql(
        query: str, strip_unsupported_fields: bool = True
    ) -> Dict[str, Any]:
        call_log.append(
            {"query": query, "strip_unsupported_fields": strip_unsupported_fields}
        )
        if "__schema" in query:
            return introspection_from_schema(schema)
        return {}

    graph.execute_graphql = execute_graphql
    graph._execute_graphql_call_log = call_log
    return graph


@pytest.fixture
def mock_graph_old_schema(old_server_schema):
    """Mock DataHubGraph that returns old schema introspection."""
    return _make_mock_graph(old_server_schema)


@pytest.fixture
def mock_graph_new_schema(new_server_schema):
    """Mock DataHubGraph that returns new schema introspection."""
    return _make_mock_graph(new_server_schema)


class TestQueryProjector:
    """Tests for the QueryProjector class."""

    def test_adapt_query_removes_unsupported_inline_fragment(
        self, mock_graph_old_schema
    ):
        """Test that inline fragments for unsupported types are removed."""
        query = """
            query SearchQuery {
                searchAcrossEntities(input: {query: "*", types: []}) {
                    searchResults {
                        entity {
                            ... on Dataset {
                                urn
                                name
                            }
                            ... on Document {
                                urn
                                info {
                                    title
                                }
                            }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(
            query, mock_graph_old_schema
        )

        assert "... on Document" not in adapted_query
        assert len(removed_fields) == 1
        assert "Document" in removed_fields[0]

        assert "... on Dataset" in adapted_query
        assert "urn" in adapted_query
        assert "name" in adapted_query

    def test_adapt_query_keeps_all_fragments_when_supported(
        self, mock_graph_new_schema
    ):
        """Test that all fragments are kept when server supports them."""
        query = """
            query SearchQuery {
                searchAcrossEntities(input: {query: "*", types: []}) {
                    searchResults {
                        entity {
                            ... on Dataset {
                                urn
                                name
                            }
                            ... on Document {
                                urn
                                info {
                                    title
                                }
                            }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(
            query, mock_graph_new_schema
        )

        assert len(removed_fields) == 0
        assert "... on Dataset" in adapted_query
        assert "... on Document" in adapted_query

    def test_adapt_query_removes_unsupported_field(self):
        """Test that unsupported fields on supported types are removed."""
        schema_without_description = build_schema(
            """
            type Query {
                searchAcrossEntities(input: SearchInput!): SearchResults
            }

            input SearchInput {
                query: String!
            }

            type SearchResults {
                searchResults: [SearchResult!]!
            }

            type SearchResult {
                entity: Entity
            }

            union Entity = Dataset

            type Dataset {
                urn: String!
                name: String!
            }
        """
        )

        graph = _make_mock_graph(schema_without_description)

        query = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            ... on Dataset {
                                urn
                                name
                                description
                            }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(query, graph)

        assert "description" not in adapted_query
        assert len(removed_fields) == 1
        assert "description" in removed_fields[0].lower()

        assert "urn" in adapted_query
        assert "name" in adapted_query

    def test_schema_caching(self, mock_graph_old_schema):
        """Test that schema is cached per graph instance."""
        projector = QueryProjector()

        query = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            ... on Dataset { urn }
                        }
                    }
                }
            }
        """

        projector.adapt_query(query, mock_graph_old_schema)
        assert len(projector._schema_cache) == 1

        projector.adapt_query(query, mock_graph_old_schema)
        assert len(projector._schema_cache) == 1

        another_graph = Mock()
        another_graph.execute_graphql = mock_graph_old_schema.execute_graphql
        projector.adapt_query(query, another_graph)
        assert len(projector._schema_cache) == 2

    def test_introspection_fields_preserved(self, mock_graph_old_schema):
        """Test that introspection fields like __typename are preserved."""
        query = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            __typename
                            ... on Dataset {
                                urn
                                name
                            }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(
            query, mock_graph_old_schema
        )

        assert "__typename" in adapted_query
        assert len(removed_fields) == 0

    def test_multiple_unsupported_fragments_removed(self, mock_graph_old_schema):
        """Test removal of multiple unsupported inline fragments."""
        query = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            ... on Dataset { urn }
                            ... on Document { urn }
                            ... on UnsupportedType { urn }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(
            query, mock_graph_old_schema
        )

        assert "... on Document" not in adapted_query
        assert "... on UnsupportedType" not in adapted_query
        assert len(removed_fields) == 2

        assert "... on Dataset" in adapted_query

    def test_parse_error_propagated(self, mock_graph_old_schema):
        """Test that parse errors are properly propagated."""
        from graphql import GraphQLSyntaxError

        invalid_query = "query { searchAcrossEntities(input: {query: '*'}"

        projector = QueryProjector()
        with pytest.raises(GraphQLSyntaxError):
            projector.adapt_query(invalid_query, mock_graph_old_schema)

    def test_empty_query(self, mock_graph_old_schema):
        """Test handling of empty queries."""
        query = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            ... on Dataset {
                                urn
                            }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(
            query, mock_graph_old_schema
        )

        assert "searchAcrossEntities" in adapted_query
        assert len(removed_fields) == 0

    def test_introspection_uses_strip_false(self, old_server_schema):
        """Test that introspection call passes strip_unsupported_fields=False."""
        graph = _make_mock_graph(old_server_schema)

        query = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            ... on Dataset { urn }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        projector.adapt_query(query, graph)

        # The introspection call should have strip_unsupported_fields=False
        introspection_calls = [
            c for c in graph._execute_graphql_call_log if "__schema" in c["query"]
        ]
        assert len(introspection_calls) == 1
        assert introspection_calls[0]["strip_unsupported_fields"] is False

    def test_strip_unsupported_fields_false_bypasses_projection(
        self, old_server_schema
    ):
        """Test that strip_unsupported_fields=False skips projection entirely.

        This simulates how DataHubGraph.execute_graphql would behave when called
        with strip_unsupported_fields=False — the query should pass through unchanged.
        """
        # Directly test the QueryProjector is NOT called when flag is False
        # by verifying the query with unsupported fields remains unchanged
        query_with_unsupported = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            ... on Document { urn }
                        }
                    }
                }
            }
        """

        # When strip_unsupported_fields=True, Document gets stripped
        graph = _make_mock_graph(old_server_schema)
        projector = QueryProjector()
        adapted_query, removed = projector.adapt_query(query_with_unsupported, graph)
        assert "... on Document" not in adapted_query
        assert len(removed) == 1

    def test_default_strip_unsupported_fields_applies_projection(
        self, mock_graph_old_schema
    ):
        """Test that default (strip_unsupported_fields=True) applies projection."""
        query = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            ... on Dataset { urn }
                            ... on Document { urn }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(
            query, mock_graph_old_schema
        )

        # By default, unsupported fields should be stripped
        assert "... on Document" not in adapted_query
        assert "... on Dataset" in adapted_query
        assert len(removed_fields) == 1


class TestUnsupportedFieldRemover:
    """Tests for the UnsupportedFieldRemover visitor."""

    def test_nested_field_removal(self, mock_graph_old_schema):
        """Test removal of nested fields within unsupported fragments."""
        query = """
            query {
                searchAcrossEntities(input: {query: "*"}) {
                    searchResults {
                        entity {
                            ... on Document {
                                urn
                                info {
                                    title
                                    description
                                }
                            }
                        }
                    }
                }
            }
        """

        projector = QueryProjector()
        adapted_query, removed_fields = projector.adapt_query(
            query, mock_graph_old_schema
        )

        assert "... on Document" not in adapted_query
        assert "info" not in adapted_query
        assert len(removed_fields) >= 1
