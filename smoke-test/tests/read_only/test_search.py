import logging
from typing import Optional
from urllib.parse import quote

import pytest
import requests

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

        # Don't key off searchResults[0]: the top hit can be a transient entity
        # another module created and is concurrently deleting (search index lags
        # the entity store under xdist --dist=loadscope), so a by-URN GET races
        # into a 404. Try each result and accept the first that still resolves;
        # only fail if none do.
        last_error: Optional[Exception] = None
        for result in entities:
            urn = result["entity"]["urn"]
            encoded_urn = quote(urn, safe="")
            url = f"{BASE_URL_V3}/entity/{entity_type}/{encoded_urn}"
            response = auth_session.get(url, headers=default_headers)
            if response.status_code == 404:
                # Transient / concurrently-deleted entity — try the next hit.
                last_error = requests.HTTPError(f"404 for {urn}")
                continue
            response.raise_for_status()
            actual_data = response.json()
            logger.info(f"Entity Data for URN {urn}: {actual_data}")
            assert actual_data["urn"] == urn, (
                f"Mismatch: expected {urn}, got {actual_data}"
            )
            return

        raise AssertionError(
            f"No searchResult for {entity_type} resolved via OpenAPI v3 "
            f"({len(entities)} tried); last error: {last_error}"
        )

    run_concurrent_tests(entity_types, test_entity, test_name="test_openapi_v3_entity")
