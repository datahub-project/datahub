import logging

import pytest

from tests.utilities.metadata_operations import get_data_product, get_search_results

logger = logging.getLogger(__name__)


@pytest.mark.read_only
def test_dataproduct_search_works(auth_session):
    """Test that data product search and GraphQL queries work."""
    # Search for data products
    search_result = get_search_results(auth_session, "dataProduct")
    num_entities = search_result["total"]

    logger.info(f"Found {num_entities} data products")

    if num_entities == 0:
        logger.warning("No data products found - skipping GraphQL query test")
        return

    # Get the first data product URN
    entities = search_result["searchResults"]
    first_urn = entities[0]["entity"]["urn"]

    logger.info(f"Testing GraphQL query for data product: {first_urn}")

    # Test GraphQL query to fetch data product by URN using shared utility
    data_product = get_data_product(auth_session, first_urn)

    # Validate response
    assert data_product, f"No data product returned for URN {first_urn}"
    assert data_product["urn"] == first_urn, (
        f"Expected URN {first_urn}, got {data_product['urn']}"
    )
    assert data_product["type"] == "DATA_PRODUCT", (
        f"Expected type DATA_PRODUCT, got {data_product['type']}"
    )

    # Validate properties exist (they should always be present for a data product)
    assert data_product["properties"], "Data product properties should not be None"
    assert data_product["properties"]["name"], "Data product name should not be empty"

    logger.info(
        f"Successfully validated data product: {data_product['properties']['name']}"
    )
