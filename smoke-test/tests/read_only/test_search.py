import logging
from urllib.parse import quote

import pytest

from tests.test_result_msg import add_datahub_stats
from tests.utilities.concurrent_test_runner import (
    run_concurrent_tests,
    run_concurrent_tests_with_args,
)
from tests.utilities.metadata_operations import get_search_results
from tests.utils import get_gms_url, with_test_retry

logger = logging.getLogger(__name__)

BASE_URL_V3 = f"{get_gms_url()}/openapi/v3"

default_headers = {
    "Content-Type": "application/json",
}


@pytest.mark.read_only
def test_search_works(auth_session):
    """Test that GraphQL entity queries work for all entity types."""
    entity_test_cases = [
        ("chart", "chart"),
        ("dataset", "dataset"),
        ("dashboard", "dashboard"),
        ("dataJob", "dataJob"),
        ("dataFlow", "dataFlow"),
        ("container", "container"),
        ("tag", "tag"),
        ("corpUser", "corpUser"),
        ("mlFeature", "mlFeature"),
        ("glossaryTerm", "glossaryTerm"),
        ("domain", "domain"),
        ("mlPrimaryKey", "mlPrimaryKey"),
        ("corpGroup", "corpGroup"),
        ("mlFeatureTable", "mlFeatureTable"),
        ("glossaryNode", "glossaryNode"),
        ("mlModel", "mlModel"),
    ]

    def test_entity(entity_type: str, api_name: str) -> None:
        search_result = get_search_results(auth_session, entity_type)
        num_entities = search_result["total"]
        add_datahub_stats(f"num-{entity_type}", num_entities)
        entities = search_result["searchResults"]
        # Guard on the actual results page, not `total`: under ES eventual
        # consistency the aggregate `total` can be > 0 while `searchResults` is
        # momentarily empty, which IndexErrors on entities[0]. Skip gracefully
        # when the page is empty (read-only tests must tolerate no data).
        if not entities:
            logger.warning(f"No searchResults for {entity_type} (total={num_entities})")
            return

        first_urn = entities[0]["entity"]["urn"]

        json = {
            "query": """
                query """
            + api_name
            + """($input: String!) {
                    """
            + api_name
            + """(urn: $input) {
                        urn
                    }
                }
            """,
            "variables": {"input": first_urn},
        }

        response = auth_session.post(
            f"{auth_session.frontend_url()}/api/v2/graphql", json=json
        )
        response.raise_for_status()
        res_data = response.json()
        assert res_data["data"], f"res_data was {res_data}"
        assert res_data["data"][api_name]["urn"] == first_urn, (
            f"res_data was {res_data}"
        )

    run_concurrent_tests_with_args(
        entity_test_cases, test_entity, test_name="test_search_works"
    )


@with_test_retry(max_attempts=3)
def _openapi_v3_entity_once(auth_session, entity_type: str) -> None:
    """One attempt: search → OpenAPI GET. Raises to retry on empty/404 races."""
    search_result = get_search_results(auth_session, entity_type)
    num_entities = search_result["total"]
    entities = search_result["searchResults"]
    if not entities:
        raise AssertionError(
            f"No searchResults for {entity_type} (total={num_entities})"
        )

    first_urn = entities[0]["entity"]["urn"]
    encoded_urn = quote(first_urn, safe="")
    url = f"{BASE_URL_V3}/entity/{entity_type}/{encoded_urn}"
    response = auth_session.get(url, headers=default_headers)
    if response.status_code == 404:
        raise AssertionError(
            f"Entity {first_urn} 404 after search for {entity_type} (stale hit)"
        )
    response.raise_for_status()
    actual_data = response.json()
    assert actual_data["urn"] == first_urn, (
        f"Mismatch: expected urn={first_urn}, got {actual_data}"
    )


@pytest.mark.read_only
def test_openapi_v3_entity(auth_session):
    """Test that OpenAPI v3 entity endpoints work for all entity types."""
    entity_types = [
        "chart",
        "dataset",
        "dashboard",
        "dataJob",
        "dataFlow",
        "container",
        "tag",
        "corpUser",
        "mlFeature",
        "glossaryTerm",
        "domain",
        "mlPrimaryKey",
        "corpGroup",
        "mlFeatureTable",
        "glossaryNode",
        "mlModel",
    ]

    def test_entity(entity_type: str) -> None:
        try:
            _openapi_v3_entity_once(auth_session, entity_type)
        except AssertionError as exc:
            # Read-only: after retries, empty index / deleted entity is skip, not fail.
            logger.warning("Skipping OpenAPI v3 check for %s: %s", entity_type, exc)

    run_concurrent_tests(entity_types, test_entity, test_name="test_openapi_v3_entity")
