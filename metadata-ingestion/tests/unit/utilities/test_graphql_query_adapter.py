"""Unit tests for GraphQL query adaptation."""

from typing import Any, Dict
from unittest.mock import Mock

import pytest
from graphql import build_schema

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


@pytest.fixture
def mock_graph_old_schema(old_server_schema):
    """Mock DataHubGraph that returns old schema introspection."""
    graph = Mock()

    # Mock the introspection query response
    def execute_graphql(query: str) -> Dict[str, Any]:
        # For introspection queries, return schema data
        if "__schema" in query:
            from graphql.utilities import introspection_from_schema

            introspection = introspection_from_schema(old_server_schema)
            return {"data": introspection}
        return {}

    graph.execute_graphql = execute_graphql
    return graph


@pytest.fixture
def mock_graph_new_schema(new_server_schema):
    """Mock DataHubGraph that returns new schema introspection."""
    graph = Mock()

    # Mock the introspection query response
    def execute_graphql(query: str) -> Dict[str, Any]:
        # For introspection queries, return schema data
        if "__schema" in query:
            from graphql.utilities import introspection_from_schema

            introspection = introspection_from_schema(new_server_schema)
            return {"data": introspection}
        return {}

    graph.execute_graphql = execute_graphql
    return graph


class TestQueryProjector:
    """Tests for the QueryProjector class."""

    def test_adapt_query_removes_unsupported_inline_fragment(
        self, mock_graph_old_schema
    ):
        """Test that inline fragments for unsupported types are removed."""
        # Query expecting Document entity (v0.13.0+)
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

        # The Document fragment should be removed
        assert "... on Document" not in adapted_query
        assert len(removed_fields) == 1
        assert "Document" in removed_fields[0]

        # Dataset fragment should remain
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

        # Nothing should be removed
        assert len(removed_fields) == 0
        assert "... on Dataset" in adapted_query
        assert "... on Document" in adapted_query

    def test_adapt_query_removes_unsupported_field(self, mock_graph_old_schema):
        """Test that unsupported fields on supported types are removed."""
        # Create a schema without the 'description' field on Dataset
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

        graph = Mock()

        def execute_graphql(query: str) -> Dict[str, Any]:
            if "__schema" in query:
                from graphql.utilities import introspection_from_schema

                introspection = introspection_from_schema(schema_without_description)
                return {"data": introspection}
            return {}

        graph.execute_graphql = execute_graphql

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

        # The description field should be removed
        assert "description" not in adapted_query
        assert len(removed_fields) == 1
        assert "description" in removed_fields[0].lower()

        # Other fields should remain
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

        # First call should cache the schema
        projector.adapt_query(query, mock_graph_old_schema)
        initial_cache_size = len(projector._schema_cache)
        assert initial_cache_size == 1

        # Second call with same graph should use cached schema
        projector.adapt_query(query, mock_graph_old_schema)
        assert len(projector._schema_cache) == initial_cache_size

        # Call with different graph should create new cache entry
        another_graph = Mock()
        another_graph.execute_graphql = mock_graph_old_schema.execute_graphql
        projector.adapt_query(query, another_graph)
        assert len(projector._schema_cache) == initial_cache_size + 1

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

        # __typename should always be preserved
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

        # Both Document and UnsupportedType should be removed
        assert "... on Document" not in adapted_query
        assert "... on UnsupportedType" not in adapted_query
        assert len(removed_fields) == 2

        # Dataset should remain
        assert "... on Dataset" in adapted_query

    def test_parse_error_propagated(self, mock_graph_old_schema):
        """Test that parse errors are properly propagated."""
        from graphql import GraphQLSyntaxError

        # Invalid GraphQL syntax - missing closing brace
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

        # Should work normally even with minimal query
        assert "searchAcrossEntities" in adapted_query
        assert len(removed_fields) == 0


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

        # Entire Document fragment (including nested fields) should be removed
        assert "... on Document" not in adapted_query
        assert "info" not in adapted_query
        assert len(removed_fields) >= 1
