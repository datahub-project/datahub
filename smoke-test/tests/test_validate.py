"""
Ad-hoc validation tests for agent-driven workflows.

These tests are designed to be invoked by agents to validate features via API
calls. Configure behavior via environment variables:

    VALIDATE_QUERY - GraphQL query to execute and validate returns no errors
    VALIDATE_URN   - Entity URN to check for existence
    VALIDATE_SEARCH_QUERY - Search query text
    VALIDATE_SEARCH_URN   - Expected URN in search results

Usage:
    python3 scripts/datahub_dev.py test tests/test_validate.py
    VALIDATE_URN="urn:li:dataset:..." python3 scripts/datahub_dev.py test tests/test_validate.py::test_entity_exists
"""

import logging
import os

import pytest

from tests.utilities.api_validator import DataHubAPIValidator
from tests.utils import execute_graphql

logger = logging.getLogger(__name__)


@pytest.fixture
def validator(auth_session):
    return DataHubAPIValidator(auth_session)


def test_graphql_query(auth_session):
    """Run an arbitrary GraphQL query and validate it returns no errors.

    Configure via VALIDATE_QUERY env var.
    """
    query = os.environ.get("VALIDATE_QUERY")
    if not query:
        pytest.skip("VALIDATE_QUERY not set")

    result = execute_graphql(auth_session, query)
    assert "errors" not in result, f"GraphQL errors: {result.get('errors')}"
    logger.info("Query succeeded: %s", list(result.get("data", {}).keys()))


def test_entity_exists(validator):
    """Validate that a specific entity exists.

    Configure via VALIDATE_URN env var.
    """
    urn = os.environ.get("VALIDATE_URN")
    if not urn:
        pytest.skip("VALIDATE_URN not set")

    assert validator.validate_entity_exists(urn), f"Entity not found: {urn}"
    logger.info("Entity exists: %s", urn)


def test_search_returns(validator):
    """Validate that a search query returns an expected URN.

    Configure via VALIDATE_SEARCH_QUERY and VALIDATE_SEARCH_URN env vars.
    """
    query = os.environ.get("VALIDATE_SEARCH_QUERY")
    expected_urn = os.environ.get("VALIDATE_SEARCH_URN")
    if not query or not expected_urn:
        pytest.skip("VALIDATE_SEARCH_QUERY and VALIDATE_SEARCH_URN must be set")

    entity_type = os.environ.get("VALIDATE_SEARCH_TYPE", "dataset")
    assert validator.validate_search_returns(query, expected_urn, entity_type), (
        f"Search for '{query}' did not return {expected_urn}"
    )
    logger.info("Search '%s' returned expected URN: %s", query, expected_urn)


def test_feature_flag(validator):
    """Validate a feature flag has an expected value.

    Configure via VALIDATE_FLAG_NAME and VALIDATE_FLAG_VALUE env vars.
    """
    flag_name = os.environ.get("VALIDATE_FLAG_NAME")
    flag_value = os.environ.get("VALIDATE_FLAG_VALUE")
    if not flag_name or flag_value is None:
        pytest.skip("VALIDATE_FLAG_NAME and VALIDATE_FLAG_VALUE must be set")

    expected = flag_value.lower() in ("true", "1", "yes")
    assert validator.validate_feature_flag(flag_name, expected), (
        f"Flag {flag_name} != {expected}"
    )
    logger.info("Flag %s == %s", flag_name, expected)


def test_openapi_endpoint(validator):
    """Validate an OpenAPI endpoint returns expected status.

    Configure via VALIDATE_ENDPOINT_METHOD, VALIDATE_ENDPOINT_PATH,
    and VALIDATE_ENDPOINT_STATUS env vars.
    """
    method = os.environ.get("VALIDATE_ENDPOINT_METHOD", "GET")
    path = os.environ.get("VALIDATE_ENDPOINT_PATH")
    expected_status = os.environ.get("VALIDATE_ENDPOINT_STATUS", "200")
    if not path:
        pytest.skip("VALIDATE_ENDPOINT_PATH not set")

    result = validator.validate_openapi_endpoint(method, path, int(expected_status))
    assert result["ok"], (
        f"{method} {path}: expected {expected_status}, got {result['status_code']}"
    )
    logger.info("%s %s returned %d", method, path, result["status_code"])
