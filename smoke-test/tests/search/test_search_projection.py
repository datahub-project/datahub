import json
import logging
from typing import Any, Dict

import pytest

from datahub.cli.search_cli import _build_search_query
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DatasetPropertiesClass
from tests.utilities.domains import Domain
from tests.utils import unique_dataset_urn, wait_for_writes_to_sync, with_test_retry

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

# Shared variables for all search queries
_BASE_VARIABLES = {
    "query": "*",
    "types": [],
    "orFilters": [],
    "count": 10,
    "start": 0,
    "viewUrn": None,
}


def _urn_filter_variables(dataset_urn: str) -> Dict[str, Any]:
    return {
        **_BASE_VARIABLES,
        "types": ["DATASET"],
        "count": 1,
        "orFilters": [
            {
                "and": [
                    {
                        "field": "urn",
                        "values": [dataset_urn],
                        "condition": "EQUAL",
                    }
                ]
            }
        ],
    }


@with_test_retry()
def _execute_search_with_results(graph_client, query: str, variables: Dict[str, Any]):
    result = graph_client.execute_graphql(
        query=query,
        variables=variables,
        operation_name="search",
    )
    search_data = result["searchAcrossEntities"]
    assert search_data["total"] > 0
    assert len(search_data["searchResults"]) > 0
    return result


@with_test_retry()
def _entity_json_for_urn(
    graph_client, query: str, variables: Dict[str, Any], expected_urn: str
) -> str:
    result = graph_client.execute_graphql(
        query=query,
        variables=variables,
        operation_name="search",
    )
    matching = [
        sr["entity"]
        for sr in result["searchAcrossEntities"]["searchResults"]
        if sr.get("entity") and sr["entity"].get("urn") == expected_urn
    ]
    assert matching, f"Expected {expected_urn} in search results"
    return json.dumps(matching[0])


class TestSearchProjection:
    """Smoke tests for --projection flag: verifies that custom GQL projections
    execute successfully against a live DataHub instance."""

    def test_minimal_projection(self, graph_client):
        """Projection with only urn+type returns entities without extra fields."""
        query = _build_search_query(semantic=False, projection="urn type")
        logger.info("Executing minimal projection query")

        result = _execute_search_with_results(graph_client, query, _BASE_VARIABLES)

        search_data = result["searchAcrossEntities"]
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
        """Compare default vs minimal projection JSON for the same ingested entity.

        query:'*' with count:10 is not stable under concurrent ingest/delete:
        the two searches can return different entities, so payload size is
        not a same-document comparison.
        """
        dataset_urn = unique_dataset_urn("projection-payload")
        graph_client.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetPropertiesClass(name="projection-payload"),
            )
        )
        wait_for_writes_to_sync()
        try:
            variables = _urn_filter_variables(dataset_urn)
            default_query = _build_search_query(semantic=False, projection=None)
            minimal_query = _build_search_query(semantic=False, projection="urn type")

            default_json = _entity_json_for_urn(
                graph_client, default_query, variables, dataset_urn
            )
            minimal_json = _entity_json_for_urn(
                graph_client, minimal_query, variables, dataset_urn
            )

            default_size = len(default_json)
            minimal_size = len(minimal_json)
            logger.info(
                "Entity payload comparison for %s: default=%s bytes, minimal=%s bytes",
                dataset_urn,
                default_size,
                minimal_size,
            )
            assert minimal_size < default_size, (
                f"Minimal entity payload ({minimal_size}B) should be smaller "
                f"than default entity payload ({default_size}B)"
            )
        finally:
            graph_client.hard_delete_entity(dataset_urn)
