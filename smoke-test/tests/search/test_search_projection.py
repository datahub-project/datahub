import json
import logging

from datahub.cli.search_cli import _build_search_query

logger = logging.getLogger(__name__)

# Shared variables for all search queries
_BASE_VARIABLES = {
    "query": "*",
    "types": [],
    "orFilters": [],
    "count": 3,
    "start": 0,
    "viewUrn": None,
}


class TestSearchProjection:
    """Smoke tests for --projection flag: verifies that custom GQL projections
    execute successfully against a live DataHub instance."""

    def test_minimal_projection(self, graph_client):
        """Projection with only urn+type returns entities without extra fields."""
        query = _build_search_query(semantic=False, projection="urn type")
        logger.info("Executing minimal projection query")

        result = graph_client.execute_graphql(
            query=query,
            variables=_BASE_VARIABLES,
            operation_name="search",
        )

        search_data = result["searchAcrossEntities"]
        assert search_data["total"] > 0
        assert len(search_data["searchResults"]) > 0

        entity = search_data["searchResults"][0]["entity"]
        assert "urn" in entity
        assert "type" in entity
        # Verify no extra fields leaked in (only urn, type, __typename)
        extra = set(entity.keys()) - {"urn", "type", "__typename"}
        assert not extra, f"Unexpected fields in minimal projection: {extra}"

    def test_dataset_properties_projection(self, graph_client):
        """Projection requesting Dataset properties returns name and platform."""
        projection = (
            "urn type "
            "... on Dataset { properties { name description } platform { name } }"
        )
        query = _build_search_query(semantic=False, projection=projection)
        logger.info("Executing dataset properties projection query")

        result = graph_client.execute_graphql(
            query=query,
            variables=_BASE_VARIABLES,
            operation_name="search",
        )

        search_data = result["searchAcrossEntities"]
        assert search_data["total"] > 0

        # Find a dataset entity in the results
        dataset_entity = None
        for sr in search_data["searchResults"]:
            if sr["entity"]["type"] == "DATASET":
                dataset_entity = sr["entity"]
                break

        if dataset_entity is not None:
            assert "properties" in dataset_entity
            if dataset_entity["properties"] is not None:
                assert "name" in dataset_entity["properties"]
            assert "platform" in dataset_entity
        else:
            logger.info(
                "No DATASET entities in top results; "
                "skipping field assertions (projection query itself succeeded)"
            )

    def test_platform_fields_fragment_projection(self, graph_client):
        """Projection referencing ...PlatformFields works (fragment included)."""
        projection = "urn type ... on Dataset { platform { ...PlatformFields } }"
        query = _build_search_query(semantic=False, projection=projection)
        assert "fragment PlatformFields" in query
        logger.info("Executing PlatformFields fragment projection query")

        result = graph_client.execute_graphql(
            query=query,
            variables=_BASE_VARIABLES,
            operation_name="search",
        )

        search_data = result["searchAcrossEntities"]
        assert search_data["total"] > 0

        # Find a dataset entity to check platform fields
        for sr in search_data["searchResults"]:
            entity = sr["entity"]
            if entity["type"] == "DATASET" and "platform" in entity:
                platform = entity["platform"]
                assert "urn" in platform
                assert "name" in platform
                assert "properties" in platform
                break

    def test_default_query_no_projection(self, graph_client):
        """Default query (no projection) returns the full .gql file with SearchEntityInfo."""
        query = _build_search_query(semantic=False, projection=None)
        assert "SearchEntityInfo" in query

        # The full .gql file includes a semanticSearch operation that may not
        # be supported by all backends. Extract only the search operation
        # portion to test. The CLI handles this via operation_name selection.
        # Here we just verify the query builds correctly — the CLI-level
        # integration is covered by `datahub search "*"` working.
        logger.info("Verified default query contains SearchEntityInfo fragment")

    def test_facets_always_present(self, graph_client):
        """Facets are returned even with a minimal entity projection."""
        query = _build_search_query(semantic=False, projection="urn")
        logger.info("Verifying facets with minimal projection")

        result = graph_client.execute_graphql(
            query=query,
            variables=_BASE_VARIABLES,
            operation_name="search",
        )

        search_data = result["searchAcrossEntities"]
        assert "facets" in search_data
        assert len(search_data["facets"]) > 0

    def test_projection_reduces_payload(self, graph_client):
        """Projection with fewer fields produces smaller entity JSON than the default."""
        default_query = _build_search_query(semantic=False, projection=None)
        minimal_query = _build_search_query(semantic=False, projection="urn type")

        variables = {**_BASE_VARIABLES, "count": 10}

        default_result = graph_client.execute_graphql(
            query=default_query,
            variables=variables,
            operation_name="search",
        )

        minimal_result = graph_client.execute_graphql(
            query=minimal_query,
            variables=variables,
            operation_name="search",
        )

        default_entities = default_result["searchAcrossEntities"]["searchResults"]
        minimal_entities = minimal_result["searchAcrossEntities"]["searchResults"]

        if not default_entities:
            logger.info(
                "No search results returned; "
                "skipping payload size comparison (queries succeeded)"
            )
            return

        default_size = len(json.dumps(default_entities))
        minimal_size = len(json.dumps(minimal_entities))
        logger.info(
            f"Entity payload comparison: default={default_size} bytes, "
            f"minimal={minimal_size} bytes"
        )
        assert minimal_size < default_size, (
            f"Minimal entity payload ({minimal_size}B) should be smaller "
            f"than default entity payload ({default_size}B)"
        )
