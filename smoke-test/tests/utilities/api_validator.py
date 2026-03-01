"""
API-based validation utilities for agent-driven testing workflows.

Provides a DataHubAPIValidator class that validates DataHub features via API
calls, avoiding the need for heavyweight Cypress tests.

Usage:
    from tests.utilities.api_validator import DataHubAPIValidator

    validator = DataHubAPIValidator(auth_session)
    assert validator.validate_entity_exists("urn:li:dataset:...")
    assert validator.validate_search_returns("my dataset", "urn:li:dataset:...")
"""

import logging
from typing import Any, Dict, List, Optional

from tests.utils import execute_graphql, get_gms_url

logger = logging.getLogger(__name__)


class DataHubAPIValidator:
    """API-based validation for DataHub features."""

    def __init__(self, auth_session):
        self.auth_session = auth_session
        self.gms_url = get_gms_url()

    def validate_entity_exists(self, urn: str) -> bool:
        """Check if an entity exists in DataHub."""
        query = """
        query getEntity($urn: String!) {
            dataset(urn: $urn) {
                urn
            }
        }
        """
        try:
            result = execute_graphql(self.auth_session, query, {"urn": urn})
            return result.get("data", {}).get("dataset") is not None
        except Exception:
            # Try generic entity endpoint for non-dataset entities
            response = self.auth_session.get(f"{self.gms_url}/entities/{urn}")
            return response.status_code == 200

    def validate_entity_has_aspect(self, urn: str, aspect: str) -> bool:
        """Check if an entity has a specific aspect."""
        response = self.auth_session.get(
            f"{self.gms_url}/aspects/{urn}?aspect={aspect}&version=0"
        )
        if response.status_code == 200:
            data = response.json()
            return data is not None and len(data) > 0
        return False

    def validate_search_returns(
        self, query: str, expected_urn: str, entity_type: str = "dataset"
    ) -> bool:
        """Validate that a search query returns a specific URN."""
        graphql_query = """
        query search($input: SearchInput!) {
            search(input: $input) {
                searchResults {
                    entity {
                        urn
                    }
                }
            }
        }
        """
        variables = {
            "input": {
                "type": entity_type.upper(),
                "query": query,
                "start": 0,
                "count": 20,
            }
        }
        result = execute_graphql(self.auth_session, graphql_query, variables)
        search_results = (
            result.get("data", {}).get("search", {}).get("searchResults", [])
        )
        found_urns = [r["entity"]["urn"] for r in search_results]
        return expected_urn in found_urns

    def validate_graphql_query(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        jsonpath_assertions: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Execute a GraphQL query and optionally validate JSONPath assertions.

        Args:
            query: GraphQL query string
            variables: Query variables
            jsonpath_assertions: List of dicts with 'path' and 'expected' keys.
                Path uses dot notation (e.g., 'data.dataset.name').

        Returns:
            The query result dict. Raises AssertionError if assertions fail.
        """
        result = execute_graphql(self.auth_session, query, variables or {})
        assert "errors" not in result, f"GraphQL errors: {result.get('errors')}"

        if jsonpath_assertions:
            for assertion in jsonpath_assertions:
                path = assertion["path"]
                expected = assertion["expected"]
                actual = _resolve_path(result, path)
                assert actual == expected, (
                    f"JSONPath '{path}': expected {expected!r}, got {actual!r}"
                )

        return result

    def validate_feature_flag(self, flag_name: str, expected_value: bool) -> bool:
        """Validate a feature flag has the expected value via the dev API."""
        response = self.auth_session.get(f"{self.gms_url}/dev/featureFlags")
        if response.status_code != 200:
            logger.warning(
                "Dev API not available (status=%d). Is METADATA_TESTS_ENABLED=true?",
                response.status_code,
            )
            return False

        flags = response.json()
        if flag_name not in flags:
            logger.warning(
                "Flag '%s' not found. Available: %s", flag_name, list(flags.keys())
            )
            return False

        return flags[flag_name] == expected_value

    def validate_openapi_endpoint(
        self,
        method: str,
        path: str,
        expected_status: int,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Validate an OpenAPI endpoint returns expected status.

        Returns:
            Dict with 'status_code', 'ok', and 'body' keys.
        """
        url = f"{self.gms_url}{path}"
        method_fn = getattr(self.auth_session, method.lower())
        kwargs: Dict[str, Any] = {}
        if json_body is not None:
            kwargs["json"] = json_body

        response = method_fn(url, **kwargs)
        ok = response.status_code == expected_status

        body: Any = None
        try:
            body = response.json()
        except Exception:
            body = response.text

        if not ok:
            logger.warning(
                "Expected status %d but got %d for %s %s",
                expected_status,
                response.status_code,
                method.upper(),
                path,
            )

        return {
            "status_code": response.status_code,
            "ok": ok,
            "body": body,
        }


def _resolve_path(data: Any, path: str) -> Any:
    """Resolve a dot-notation path against nested dicts/lists."""
    parts = path.split(".")
    current = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            current = current[int(part)]
        else:
            return None
    return current
