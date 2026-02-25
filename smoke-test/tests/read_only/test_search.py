import logging
from urllib.parse import quote

import pytest

from tests.test_result_msg import add_datahub_stats
from tests.utilities.concurrent_test_runner import (
    run_concurrent_tests,
    run_concurrent_tests_with_args,
)
from tests.utilities.metadata_operations import get_search_results
from tests.utils import get_gms_url

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
        if num_entities == 0:
            logger.warning(f"No results for {entity_type}")
            return
        entities = search_result["searchResults"]

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
        search_result = get_search_results(auth_session, entity_type)
        num_entities = search_result["total"]
        if num_entities == 0:
            logger.warning(f"No results for {entity_type}")
            return
        entities = search_result["searchResults"]

        first_urn = entities[0]["entity"]["urn"]

        encoded_urn = quote(first_urn, safe="")
        url = f"{BASE_URL_V3}/entity/{entity_type}/{encoded_urn}"
        response = auth_session.get(url, headers=default_headers)
        response.raise_for_status()
        actual_data = response.json()
        logger.info(f"Entity Data for URN {first_urn}: {actual_data}")

        expected_data = {"urn": first_urn}

        assert actual_data["urn"] == expected_data["urn"], (
            f"Mismatch: expected {expected_data}, got {actual_data}"
        )

    run_concurrent_tests(entity_types, test_entity, test_name="test_openapi_v3_entity")
