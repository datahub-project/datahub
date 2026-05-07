"""Smoke tests for GraphQL query projection against a live DataHub instance."""

import logging

import pytest

from datahub.configuration.common import GraphError

logger = logging.getLogger(__name__)


# --- A. Basic integration — projection doesn't break valid queries ---


def test_simple_query_passes_through(graph_client):
    """S1: Simple valid query works with strip_unsupported_fields=True."""
    query = "{ me { corpUser { urn username } } }"

    logger.info("Executing simple query with projection enabled")
    result = graph_client.execute_graphql(query, strip_unsupported_fields=True)

    assert result is not None
    assert "me" in result
    assert "corpUser" in result["me"]
    assert "urn" in result["me"]["corpUser"]
    logger.info(f"Got user urn: {result['me']['corpUser']['urn']}")


def test_search_query_with_known_entity_types(graph_client):
    """S2: Search query with known entity type returns results."""
    query = """
        query {
            searchAcrossEntities(input: {query: "*", types: [DATASET], start: 0, count: 1}) {
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

    logger.info("Executing search query with Dataset inline fragment")
    result = graph_client.execute_graphql(query, strip_unsupported_fields=True)

    assert result is not None
    assert "searchAcrossEntities" in result
    logger.info(
        f"Got {len(result['searchAcrossEntities']['searchResults'])} search results"
    )


def test_strip_false_still_works(graph_client):
    """S3: strip_unsupported_fields=False works for valid queries."""
    query = "{ me { corpUser { urn username } } }"

    logger.info("Executing valid query with projection disabled")
    result = graph_client.execute_graphql(query, strip_unsupported_fields=False)

    assert result is not None
    assert "me" in result
    assert "corpUser" in result["me"]
    logger.info("strip_unsupported_fields=False works correctly for valid queries")


# --- B. Forward compatibility — newer client, current server ---


def test_nonexistent_type_fragment_stripped(graph_client):
    """S4: Inline fragment for non-existent type is stripped."""
    query = """
        query {
            searchAcrossEntities(input: {query: "*", types: [DATASET], start: 0, count: 1}) {
                searchResults {
                    entity {
                        ... on Dataset {
                            urn
                        }
                        ... on FakeEntityTypeThatDoesNotExist {
                            urn
                        }
                    }
                }
            }
        }
    """

    logger.info("Executing query with fake entity type fragment")
    result = graph_client.execute_graphql(query, strip_unsupported_fields=True)

    assert result is not None
    assert "searchAcrossEntities" in result
    logger.info("Query with non-existent type fragment succeeded after stripping")


def test_unknown_field_on_known_type_stripped(graph_client):
    """S5: Unknown field on known type is stripped."""
    query = """
        query {
            searchAcrossEntities(input: {query: "*", types: [DATASET], start: 0, count: 1}) {
                searchResults {
                    entity {
                        ... on Dataset {
                            urn
                            nonExistentFieldXyz123
                        }
                    }
                }
            }
        }
    """

    logger.info("Executing query with unknown field on Dataset")
    result = graph_client.execute_graphql(query, strip_unsupported_fields=True)

    assert result is not None
    assert "searchAcrossEntities" in result
    logger.info("Query with unknown field succeeded after stripping")


def test_multiple_unknown_fragments_and_fields(graph_client):
    """S6: Multiple unknown fragments + fields in one query are all stripped."""
    query = """
        query {
            searchAcrossEntities(input: {query: "*", types: [DATASET], start: 0, count: 1}) {
                searchResults {
                    entity {
                        ... on Dataset {
                            urn
                            nonExistentFieldXyz123
                        }
                        ... on FakeEntityTypeThatDoesNotExist {
                            urn
                        }
                    }
                }
            }
        }
    """

    logger.info("Executing query with both unknown type and unknown field")
    result = graph_client.execute_graphql(query, strip_unsupported_fields=True)

    assert result is not None
    assert "searchAcrossEntities" in result
    logger.info("Query with multiple unsupported elements succeeded after stripping")


# --- C. Backwards compatibility — opt-out behavior ---


def test_strip_false_with_bad_field_fails(graph_client):
    """S7: strip_unsupported_fields=False with unknown field causes server error."""
    query = """
        query {
            searchAcrossEntities(input: {query: "*", types: [DATASET], start: 0, count: 1}) {
                searchResults {
                    entity {
                        ... on Dataset {
                            urn
                            nonExistentFieldXyz123
                        }
                    }
                }
            }
        }
    """

    logger.info("Executing query with unknown field and projection disabled")
    with pytest.raises(GraphError):
        graph_client.execute_graphql(query, strip_unsupported_fields=False)
    logger.info("GraphError raised as expected when projection is disabled")


# --- D. Schema caching ---


def test_schema_cached_across_calls(graph_client):
    """S8: Schema is cached — introspection only happens once."""
    query = "{ me { corpUser { urn } } }"

    logger.info("Executing two queries to verify schema caching")
    graph_client.execute_graphql(query, strip_unsupported_fields=True)
    graph_client.execute_graphql(query, strip_unsupported_fields=True)

    projector = graph_client._query_projector
    assert projector._cached_schema is not None, (
        "Schema should be cached after first call"
    )
    assert len(projector._query_cache) >= 1, "Query result should be cached"
    logger.info(
        f"Schema cached (generation={projector._schema_generation}), "
        f"query cache has {len(projector._query_cache)} entry"
    )


# --- E. Complex real-world queries ---


def test_query_with_variables_and_operation_name(graph_client):
    """S10: Parameterized query with variables and operation_name works."""
    query = """
        query SearchDatasets($input: SearchAcrossEntitiesInput!) {
            searchAcrossEntities(input: $input) {
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
    variables = {"input": {"query": "*", "types": ["DATASET"], "start": 0, "count": 1}}

    logger.info("Executing parameterized query with variables and operation_name")
    result = graph_client.execute_graphql(
        query,
        variables=variables,
        operation_name="SearchDatasets",
        strip_unsupported_fields=True,
    )

    assert result is not None
    assert "searchAcrossEntities" in result
    logger.info("Parameterized query with projection succeeded")
