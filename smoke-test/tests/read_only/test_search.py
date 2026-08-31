import logging
from urllib.parse import quote

import pytest
import tenacity

from tests.test_result_msg import add_datahub_stats
from tests.utilities.concurrent_test_runner import (
    run_concurrent_tests,
    run_concurrent_tests_with_args,
)
from tests.utilities.domains import Domain
from tests.utilities.metadata_operations import get_search_results
from tests.utils import get_gms_url

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

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


class _OpenApiTransientSkip(AssertionError):
    """Empty search page or stale 404 — skip after retries in read-only tests."""


@tenacity.retry(
    retry=tenacity.retry_if_exception_type(_OpenApiTransientSkip),
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_fixed(20),
    reraise=True,
)
def _openapi_v3_entity_once(auth_session, entity_type: str) -> None:
    """One attempt: search → OpenAPI GET.

    Retries only ``_OpenApiTransientSkip`` (empty/404 races). Plain
    ``AssertionError`` on URN mismatch fails immediately — must not burn
    retry budget masking a real backend bug.
    """
    search_result = get_search_results(auth_session, entity_type)
    num_entities = search_result["total"]
    entities = search_result["searchResults"]
    if not entities:
        raise _OpenApiTransientSkip(
            f"No searchResults for {entity_type} (total={num_entities})"
        )

    first_urn = entities[0]["entity"]["urn"]
    encoded_urn = quote(first_urn, safe="")
    url = f"{BASE_URL_V3}/entity/{entity_type}/{encoded_urn}"
    response = auth_session.get(url, headers=default_headers)
    if response.status_code == 404:
        raise _OpenApiTransientSkip(
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
        except _OpenApiTransientSkip as exc:
            # Empty search / 404 under ES lag — yellow skip, not silent green.
            # run_concurrent_tests treats Skipped separately; all-skip → pytest.skip.
            pytest.skip(f"OpenAPI v3 {entity_type}: {exc}")

    run_concurrent_tests(entity_types, test_entity, test_name="test_openapi_v3_entity")
